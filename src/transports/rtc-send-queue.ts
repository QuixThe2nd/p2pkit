/** Structural subset of {@link RTCDataChannel} — dependency-free, browser-safe. */
export interface RTCDataChannelLike {
  readonly readyState: string
  readonly bufferedAmount: number
  bufferedAmountLowThreshold: number
  send(data: string | ArrayBuffer | ArrayBufferView): void
  onbufferedamountlow: ((ev: Event) => void) | null
  addEventListener?(type: "bufferedamountlow", listener: () => void): void
  removeEventListener?(type: "bufferedamountlow", listener: () => void): void
}

export interface RTCDataChannelSendQueueOptions {
  /** When set, enables queue-based backpressure instead of polling. */
  highWaterBytes?: number
  /**
   * Low-water mark passed to `bufferedAmountLowThreshold`.
   * Defaults to half of {@link highWaterBytes} when omitted.
   */
  lowWaterBytes?: number
  /** Invoked once the internal send queue has fully drained after backpressure. */
  onDrain?: () => void
  /**
   * Invoked exactly once when a native `channel.send()` throws while this
   * queue is flushing retained work — including on the delayed poll timer,
   * where the throw would otherwise escape as an uncaught timer exception.
   * When it fires, every pending job promise has already been rejected,
   * retained payloads have been released, owned timers have been cancelled,
   * and the queue has started refusing further sends until it is re-attached.
   * Bind this to the owning transport's error/disconnect path so even
   * synchronous `trySend`/`tryJob` producers learn about the failure.
   */
  onSendError?: (error: Error) => void
  /**
   * Opt-in bounded-retention contract for the job API
   * ({@link RTCDataChannelSendQueue.sendJob}/{@link RTCDataChannelSendQueue.tryJob}):
   * a job whose payload bytes would push queued-but-undelivered bytes past this
   * limit is rejected instead of buffered, so a saturated channel can never grow
   * this queue without bound. Payload sizes are actual wire bytes: strings count
   * their UTF-8 encoding, not UTF-16 code units. Default: unbounded (matching
   * the historical watermark queue).
   */
  maxQueuedBytes?: number
  /**
   * Opt-in companion to {@link maxQueuedBytes}: rejects a job when this many
   * jobs are already queued-but-undelivered, bounding queue entries however
   * small their payloads are.
   */
  maxQueuedJobs?: number
}

/** Default polling threshold when {@link highWaterBytes} is not configured. */
export const RTC_SEND_QUEUE_FLUSH_THRESHOLD = 1 << 20 // 1 MB

type SendPayload = string | ArrayBuffer | ArrayBufferView

/**
 * One entry of the single authoritative first-in-first-out backlog. Either a
 * single payload accepted from {@link RTCDataChannelSendQueue.send}/
 * {@link RTCDataChannelSendQueue.trySend} while backpressured, or a
 * whole-message job from {@link RTCDataChannelSendQueue.sendJob}/
 * {@link RTCDataChannelSendQueue.tryJob}. Sharing one ordered list is what
 * keeps mixed API use strictly FIFO — neither kind can overtake the other.
 */
type QueueEntry =
  | { kind: "payload"; data: SendPayload }
  | {
      kind: "job"
      payloads: SendPayload[]
      bytes: number
      next: number
      resolve: () => void
      reject: (error: Error) => void
    }

/**
 * Length of a string's UTF-8 encoding in bytes — the size the wire actually
 * carries (and `bufferedAmount` grows by), without allocating the encoding.
 * Surrogate pairs count 4; an unpaired surrogate encodes as U+FFFD (3), the
 * same byte `TextEncoder` produces.
 */
function utf8ByteLength(text: string): number {
  let bytes = 0
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i)
    if (code < 0x80) {
      bytes += 1
    } else if (code < 0x800) {
      bytes += 2
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const low = i + 1 < text.length ? text.charCodeAt(i + 1) : 0
      if (low >= 0xdc00 && low <= 0xdfff) {
        bytes += 4
        i++
      } else {
        bytes += 3
      }
    } else {
      bytes += 3
    }
  }
  return bytes
}

/** Wire bytes a payload will occupy: UTF-8 length for strings, byteLength for binary. */
function payloadByteLength(data: SendPayload): number {
  if (typeof data === "string") return utf8ByteLength(data)
  return data.byteLength
}

/**
 * Snapshot a payload for queue storage. Strings are immutable and returned
 * as-is; binary payloads are copied so a caller that reuses or frees its
 * buffer (e.g. C++ wasm heap memory) the moment send() resolves cannot
 * corrupt a queued send that flushes later.
 */
function snapshotPayload(data: SendPayload): SendPayload {
  if (typeof data === "string") return data
  const view =
    data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
  const copy = new Uint8Array(view.byteLength)
  copy.set(view)
  return copy
}

/**
 * Per-channel outgoing send queue with optional high/low water backpressure.
 * Attach to any native {@link RTCDataChannel} (or compatible stub) for ordered,
 * non-dropping sends. Used internally by {@link RTCTransport}; also suitable for
 * raw-binary producers that manage their own data channels.
 */
export class RTCDataChannelSendQueue {
  private channel?: RTCDataChannelLike
  private readonly highWaterBytes?: number
  private readonly lowWaterBytes?: number
  private readonly onDrain?: () => void
  private readonly onSendError?: (error: Error) => void
  private readonly maxQueuedBytes?: number
  private readonly maxQueuedJobs?: number
  /** The single authoritative FIFO of retained-but-undelivered work. */
  private readonly entries: QueueEntry[] = []
  /** Wire bytes of job payloads still retained (payload entries count via {@link RTCDataChannelSendQueue.bufferedAmount}). */
  private queuedBytes = 0
  private drainTimer?: ReturnType<typeof setTimeout>
  private lowHandler?: () => void
  private flushing = false
  private backpressured = false
  /** Set once a native send throws mid-drain; further sends reject until re-attach. */
  private failureError?: Error

  constructor(options: RTCDataChannelSendQueueOptions = {}) {
    this.highWaterBytes = options.highWaterBytes
    if (options.highWaterBytes !== undefined) {
      this.lowWaterBytes =
        options.lowWaterBytes ?? Math.floor(options.highWaterBytes / 2)
    } else {
      this.lowWaterBytes = options.lowWaterBytes
    }
    this.onDrain = options.onDrain
    this.onSendError = options.onSendError
    this.maxQueuedBytes = options.maxQueuedBytes
    this.maxQueuedJobs = options.maxQueuedJobs
  }

  /** Bind (or re-bind) this queue to a live data channel. */
  attach(channel: RTCDataChannelLike): void {
    this.detach()
    this.channel = channel
    if (this.highWaterBytes === undefined) return

    channel.bufferedAmountLowThreshold = this.lowWaterBytes!
    this.lowHandler = () => this.drain()
    if (channel.addEventListener) channel.addEventListener("bufferedamountlow", this.lowHandler)
    else channel.onbufferedamountlow = this.lowHandler
  }

  /** Detach from the current channel and clear any pending sends. */
  detach(): void {
    const channel = this.channel
    if (channel && this.lowHandler) {
      if (channel.removeEventListener) channel.removeEventListener("bufferedamountlow", this.lowHandler)
      else channel.onbufferedamountlow = null
    }
    this.failEntries(new Error("RTC send queue detached before delivery"))
    this.channel = undefined
    this.lowHandler = undefined
    this.failureError = undefined
    this.flushing = false
    this.backpressured = false
  }

  /** Channel `bufferedAmount` plus wire bytes still waiting in this queue. */
  get bufferedAmount(): number {
    let pending = 0
    for (const entry of this.entries) {
      if (entry.kind === "payload") {
        pending += payloadByteLength(entry.data)
      } else {
        for (let i = entry.next; i < entry.payloads.length; i++) {
          pending += payloadByteLength(entry.payloads[i]!)
        }
      }
    }
    return (this.channel?.bufferedAmount ?? 0) + pending
  }

  /**
   * Synchronous, lossy send attempt: returns `false` (dropping the payload)
   * when the channel is at or above the high-water mark, instead of queueing.
   *
   * Why this exists beside the async {@link send}: Emscripten `ccall` exports
   * (and other synchronous producers of lossy, time-sensitive traffic such as
   * per-tick game state) must learn acceptance in the same tick — a promise
   * would answer after the moment to resend has passed. Only meaningful with
   * {@link highWaterBytes} configured; without it, always sends and returns
   * true (legacy polling path has no drop policy).
   *
   * Never overtakes older work: when payloads or jobs are still queued, this
   * appends behind them (snapshot included) and reports `true` — the payload
   * was accepted and will flush in order.
   */
  trySend(data: SendPayload): boolean {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") return false
    if (this.failureError !== undefined) return false
    if (this.highWaterBytes !== undefined && this.entries.length > 0) {
      this.entries.push({ kind: "payload", data: snapshotPayload(data) })
      this.drain()
      return true
    }
    if (this.highWaterBytes !== undefined && channel.bufferedAmount >= this.highWaterBytes) {
      return false
    }
    channel.send(data)
    if (this.highWaterBytes === undefined) return true
    if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
    return true
  }

  /**
   * Send one payload. With {@link highWaterBytes} configured, payloads are queued
   * (never dropped) while the channel is at or above the high-water mark;
   * otherwise uses the legacy 1 MB polling flush (same as stock RTCTransport).
   * Queued payloads always flush in FIFO order: while any older work — payload
   * or job — is still queued, new payloads append behind it rather than sending
   * directly. The promise resolves on acceptance into the queue (the historical
   * contract), not on delivery; a later flush failure surfaces through
   * {@link RTCDataChannelSendQueueOptions.onSendError}.
   */
  async send(data: SendPayload): Promise<void> {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") throw new Error("RTC data channel is not open")
    if (this.failureError !== undefined) throw this.failureError

    if (this.highWaterBytes === undefined) {
      channel.send(data)
      await this.pollFlush(channel)
      return
    }

    if (channel.bufferedAmount >= this.highWaterBytes || this.entries.length > 0) {
      if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
      this.entries.push({ kind: "payload", data: snapshotPayload(data) })
      this.drain()
      return
    }

    channel.send(data)
    if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
  }

  /**
   * Opt-in job API for bounded, lossless consumers (direct-mode
   * {@link RTCTransport} in `src/transports/rtc.ts`): submit a burst of payloads
   * as ONE first-in-first-out unit — every payload of a job is handed to
   * `channel.send()` in order, and no later entry's payload is sent before an
   * earlier one has fully drained, so whole messages keep their order on the
   * wire. The promise resolves only after the last payload was actually handed
   * to the channel (not merely accepted while backpressured), and rejects —
   * without sending anything further — when the job would exceed
   * {@link maxQueuedBytes}/{@link maxQueuedJobs} retention, when the channel is
   * or becomes unusable, or when the queue detaches first ({@link detach}).
   *
   * While the channel sits above the high-water mark, delivery retries on a
   * short poll timer (bounded: the timer only lives while work is retained),
   * so progress does not depend solely on `bufferedamountlow` firing.
   */
  sendJob(payloads: SendPayload[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.enqueueJob(payloads, resolve, reject)
    })
  }

  /**
   * Synchronous counterpart to {@link sendJob} (same bounded acceptance, same
   * FIFO): returns whether the job was accepted by this bounded queue — never a
   * promise masquerading as success. Settlement is silent: delivery completes
   * unnoticed and rejection surfaces nowhere, matching producers that cannot
   * await (e.g. Emscripten `ccall` bridges) — a native send failure during the
   * drain is reported once through {@link RTCDataChannelSendQueueOptions.onSendError}
   * instead of throwing here.
   */
  tryJob(payloads: SendPayload[]): boolean {
    return this.enqueueJob(
      payloads,
      () => {},
      () => {},
    )
  }

  private enqueueJob(
    payloads: SendPayload[],
    resolve: () => void,
    reject: (error: Error) => void,
  ): boolean {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") {
      reject(new Error("RTC data channel is not open"))
      return false
    }
    if (this.failureError !== undefined) {
      reject(this.failureError)
      return false
    }
    if (payloads.length === 0) {
      resolve()
      return true
    }
    // Charge the budget before snapshotting anything: a rejected job must not
    // copy (or retain) its payloads, and string sizes are UTF-8 wire bytes.
    const bytes = payloads.reduce((total, item) => total + payloadByteLength(item), 0)
    if (this.overRetentionLimit(bytes)) {
      reject(new Error("RTC send queue retention limit exceeded"))
      return false
    }
    this.entries.push({
      kind: "job",
      payloads: payloads.map(snapshotPayload),
      bytes,
      next: 0,
      resolve,
      reject,
    })
    this.queuedBytes += bytes
    this.drain()
    return true
  }

  private overRetentionLimit(bytes: number): boolean {
    if (this.maxQueuedBytes !== undefined && this.queuedBytes + bytes > this.maxQueuedBytes) {
      return true
    }
    if (this.maxQueuedJobs !== undefined) {
      let jobs = 0
      for (const entry of this.entries) {
        if (entry.kind === "job") jobs++
      }
      if (jobs >= this.maxQueuedJobs) return true
    }
    return false
  }

  /**
   * The single owner of retained-work delivery: walks the FIFO head-first,
   * handing payloads to the channel while it stays under the applicable water
   * mark. Payload entries keep the raw flush contract (send only strictly below
   * the high-water mark); job entries keep the job contract (send down to the
   * mark, including the legacy 1 MB threshold when no high-water mark is
   * configured). While blocked, a short poll timer keeps progress independent
   * of `bufferedamountlow` events. Any native `channel.send()` throw is
   * contained here — see {@link handleSendFailure}.
   */
  private drain(): void {
    if (this.flushing) return
    this.flushing = true
    try {
      this.cancelDrainTimer()
      while (this.entries.length > 0) {
        const channel = this.channel
        if (!channel || channel.readyState !== "open") {
          this.failEntries(new Error("RTC data channel is not open"))
          return
        }
        const entry = this.entries[0]!
        if (entry.kind === "payload") {
          const high = this.highWaterBytes
          if (high !== undefined && channel.bufferedAmount >= high) {
            this.backpressured = true
            this.scheduleDrainTimer()
            return
          }
          channel.send(entry.data)
          this.entries.shift()
        } else {
          const limit = this.highWaterBytes ?? RTC_SEND_QUEUE_FLUSH_THRESHOLD
          if (channel.bufferedAmount > limit) {
            this.scheduleDrainTimer()
            return
          }
          // Advance only after a successful hand-off: a fragment whose send
          // threw must never be counted — or settled — as delivered.
          const payload = entry.payloads[entry.next]!
          channel.send(payload)
          entry.next++
          this.queuedBytes -= payloadByteLength(payload)
          if (entry.next >= entry.payloads.length) {
            this.entries.shift()
            entry.resolve()
          }
        }
      }
    } catch (error) {
      this.handleSendFailure(error)
      return
    } finally {
      this.flushing = false
    }
    if (this.backpressured) {
      this.backpressured = false
      this.onDrain?.()
    }
  }

  /**
   * Contain a native `channel.send()` throw from the drain — whether it happened
   * inside a timer callback, a low-water flush, or synchronously under
   * {@link tryJob}/{@link trySend}: reject every pending job (a partially-sent
   * job rejects — accepted is not delivered), release all retained payloads,
   * zero the byte budget, stop the poll timer, and surface the failure exactly
   * once through {@link RTCDataChannelSendQueueOptions.onSendError} so the
   * owning transport's error/disconnect path runs even for synchronous
   * producers. The queue refuses further work until re-attached.
   */
  private handleSendFailure(cause: unknown): void {
    this.cancelDrainTimer()
    const error = cause instanceof Error ? cause : new Error(String(cause))
    this.failureError = error
    this.queuedBytes = 0
    this.backpressured = false
    for (const entry of this.entries.splice(0)) {
      if (entry.kind === "job") entry.reject(error)
    }
    this.onSendError?.(error)
  }

  /** Reject all retained jobs (channel gone or queue detached) and drop payloads. */
  private failEntries(error: Error): void {
    this.cancelDrainTimer()
    this.queuedBytes = 0
    for (const entry of this.entries.splice(0)) {
      if (entry.kind === "job") entry.reject(error)
    }
  }

  private async pollFlush(channel: RTCDataChannelLike): Promise<void> {
    while (channel.readyState === "open" && channel.bufferedAmount > RTC_SEND_QUEUE_FLUSH_THRESHOLD) {
      await new Promise(resolve => setTimeout(resolve, 10))
    }
  }

  private scheduleDrainTimer(): void {
    if (this.drainTimer !== undefined) return
    this.drainTimer = setTimeout(() => {
      this.drainTimer = undefined
      this.drain()
    }, 10)
    if (typeof this.drainTimer === "object" && this.drainTimer && typeof this.drainTimer.unref === "function") {
      this.drainTimer.unref()
    }
  }

  private cancelDrainTimer(): void {
    if (this.drainTimer !== undefined) {
      clearTimeout(this.drainTimer)
      this.drainTimer = undefined
    }
  }
}

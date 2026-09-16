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
   * Opt-in bounded-retention contract for the job API
   * ({@link RTCDataChannelSendQueue.sendJob}/{@link RTCDataChannelSendQueue.tryJob}):
   * a job whose payload bytes would push queued-but-undelivered bytes past this
   * limit is rejected instead of buffered, so a saturated channel can never grow
   * this queue without bound. Default: unbounded (matching the historical
   * watermark queue).
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

/** One multi-payload send submitted as a unit through the job API. */
interface QueuedJob {
  payloads: SendPayload[]
  bytes: number
  next: number
  resolve: () => void
  reject: (error: Error) => void
}

function payloadByteLength(data: SendPayload): number {
  if (typeof data === "string") return data.length
  if (data instanceof ArrayBuffer) return data.byteLength
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
  private readonly maxQueuedBytes?: number
  private readonly maxQueuedJobs?: number
  private readonly queue: SendPayload[] = []
  private readonly jobs: QueuedJob[] = []
  private queuedBytes = 0
  private jobTimer?: ReturnType<typeof setTimeout>
  private lowHandler?: () => void
  private flushing = false
  private backpressured = false

  constructor(options: RTCDataChannelSendQueueOptions = {}) {
    this.highWaterBytes = options.highWaterBytes
    if (options.highWaterBytes !== undefined) {
      this.lowWaterBytes =
        options.lowWaterBytes ?? Math.floor(options.highWaterBytes / 2)
    } else {
      this.lowWaterBytes = options.lowWaterBytes
    }
    this.onDrain = options.onDrain
    this.maxQueuedBytes = options.maxQueuedBytes
    this.maxQueuedJobs = options.maxQueuedJobs
  }

  /** Bind (or re-bind) this queue to a live data channel. */
  attach(channel: RTCDataChannelLike): void {
    this.detach()
    this.channel = channel
    if (this.highWaterBytes === undefined) return

    channel.bufferedAmountLowThreshold = this.lowWaterBytes!
    this.lowHandler = () => this.flushQueue()
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
    this.channel = undefined
    this.lowHandler = undefined
    this.queue.length = 0
    this.flushing = false
    this.backpressured = false
    if (this.jobTimer !== undefined) {
      clearTimeout(this.jobTimer)
      this.jobTimer = undefined
    }
    this.failJobs(new Error("RTC send queue detached before delivery"))
  }

  /** Channel `bufferedAmount` plus bytes still waiting in this queue. */
  get bufferedAmount(): number {
    let pending = 0
    for (const item of this.queue) pending += payloadByteLength(item)
    return (this.channel?.bufferedAmount ?? 0) + pending + this.queuedBytes
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
   * Never overtakes {@link send}: when older payloads are still queued, this
   * appends behind them (snapshot included) and reports `true` — the payload
   * was accepted and will flush in order.
   */
  trySend(data: SendPayload): boolean {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") return false
    // Never overtake jobs: while any job payload is still queued, append behind
    // it as a job so FIFO holds across mixed API use.
    if (this.jobs.length > 0) return this.tryJob([data])
    if (this.highWaterBytes !== undefined && channel.bufferedAmount >= this.highWaterBytes) {
      return false
    }
    if (this.highWaterBytes !== undefined && this.queue.length > 0) {
      this.queue.push(snapshotPayload(data))
      this.tryFlushAfterSend()
      return true
    }
    channel.send(data)
    if (this.highWaterBytes === undefined) return true
    if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
    else this.tryFlushAfterSend()
    return true
  }

  /**
   * Send one payload. With {@link highWaterBytes} configured, payloads are queued
   * (never dropped) while the channel is at or above the high-water mark;
   * otherwise uses the legacy 1 MB polling flush (same as stock RTCTransport).
   * Queued payloads always flush in FIFO order: while any older payload is
   * still queued, new payloads append behind it rather than sending directly.
   */
  async send(data: SendPayload): Promise<void> {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") throw new Error("RTC data channel is not open")

    // Preserve FIFO across mixed API use: a payload cannot overtake queued jobs.
    if (this.jobs.length > 0) {
      await this.sendJob([data])
      return
    }

    if (this.highWaterBytes === undefined) {
      channel.send(data)
      await this.pollFlush(channel)
      return
    }

    if (channel.bufferedAmount >= this.highWaterBytes) {
      this.backpressured = true
      this.queue.push(snapshotPayload(data))
      return
    }

    // Older payloads are still queued: append behind them instead of sending
    // directly, so a drained channel (e.g. bufferedAmount fell without a
    // `bufferedamountlow` event) can never let a new payload overtake them.
    if (this.queue.length > 0) {
      this.queue.push(snapshotPayload(data))
      this.tryFlushAfterSend()
      return
    }

    channel.send(data)
    if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
    else this.tryFlushAfterSend()
  }

  /**
   * Opt-in job API for bounded, lossless consumers (direct-mode
   * {@link RTCTransport} in `src/transports/rtc.ts`): submit a burst of payloads
   * as ONE first-in-first-out unit — every payload of a job is handed to
   * `channel.send()` in order, and no later job's payload is sent before an
   * earlier job has fully drained, so whole messages keep their order on the
   * wire. The promise resolves only after the last payload was actually handed
   * to the channel (not merely accepted while backpressured), and rejects —
   * without sending anything further — when the job would exceed
   * {@link maxQueuedBytes}/{@link maxQueuedJobs} retention, when the channel is
   * or becomes unusable, or when the queue detaches first ({@link detach}).
   *
   * While the channel sits above the high-water mark, delivery retries on a
   * short poll timer (bounded: the timer only lives while jobs are pending), so
   * progress does not depend solely on `bufferedamountlow` firing.
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
   * await (e.g. Emscripten `ccall` bridges).
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
    if (payloads.length === 0) {
      resolve()
      return true
    }
    const snapshot = payloads.map(snapshotPayload)
    const bytes = snapshot.reduce((total, item) => total + payloadByteLength(item), 0)
    if (
      (this.maxQueuedBytes !== undefined && this.queuedBytes + bytes > this.maxQueuedBytes) ||
      (this.maxQueuedJobs !== undefined && this.jobs.length >= this.maxQueuedJobs)
    ) {
      reject(new Error("RTC send queue retention limit exceeded"))
      return false
    }
    this.jobs.push({ payloads: snapshot, bytes, next: 0, resolve, reject })
    this.queuedBytes += bytes
    this.drainJobs()
    return true
  }

  private drainJobs(): void {
    if (this.jobTimer !== undefined) {
      clearTimeout(this.jobTimer)
      this.jobTimer = undefined
    }
    while (this.jobs.length > 0) {
      const channel = this.channel
      if (!channel || channel.readyState !== "open") {
        this.failJobs(new Error("RTC data channel is not open"))
        return
      }
      const limit = this.highWaterBytes ?? RTC_SEND_QUEUE_FLUSH_THRESHOLD
      const job = this.jobs[0]!
      while (job.next < job.payloads.length) {
        if (channel.bufferedAmount > limit) {
          this.jobTimer = setTimeout(() => this.drainJobs(), 10)
          if (typeof this.jobTimer === "object" && this.jobTimer && typeof this.jobTimer.unref === "function") {
            this.jobTimer.unref()
          }
          return
        }
        channel.send(job.payloads[job.next++]!)
      }
      this.jobs.shift()
      this.queuedBytes -= job.bytes
      job.resolve()
    }
  }

  private failJobs(error: Error): void {
    if (this.jobs.length === 0) return
    this.queuedBytes = 0
    for (const job of this.jobs.splice(0)) job.reject(error)
  }

  private async pollFlush(channel: RTCDataChannelLike): Promise<void> {
    while (channel.readyState === "open" && channel.bufferedAmount > RTC_SEND_QUEUE_FLUSH_THRESHOLD) {
      await new Promise(resolve => setTimeout(resolve, 10))
    }
  }

  private tryFlushAfterSend(): void {
    if (!this.channel || this.highWaterBytes === undefined) return
    if (this.channel.bufferedAmount < this.highWaterBytes) this.flushQueue()
  }

  private flushQueue(): void {
    if (this.flushing || !this.channel || this.highWaterBytes === undefined) return
    this.flushing = true
    try {
      const channel = this.channel
      while (
        this.queue.length > 0 &&
        channel.readyState === "open" &&
        channel.bufferedAmount < this.highWaterBytes
      ) {
        channel.send(this.queue.shift()!)
      }
      if (this.queue.length === 0 && this.backpressured) {
        this.backpressured = false
        this.onDrain?.()
      }
    } finally {
      this.flushing = false
    }
  }
}

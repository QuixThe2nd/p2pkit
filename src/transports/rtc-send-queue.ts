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
}

/** Default polling threshold when {@link highWaterBytes} is not configured. */
export const RTC_SEND_QUEUE_FLUSH_THRESHOLD = 1 << 20 // 1 MB

type SendPayload = string | ArrayBuffer | ArrayBufferView

function payloadByteLength(data: SendPayload): number {
  if (typeof data === "string") return data.length
  if (data instanceof ArrayBuffer) return data.byteLength
  return data.byteLength
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
  private readonly queue: SendPayload[] = []
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
  }

  /** Channel `bufferedAmount` plus bytes still waiting in this queue. */
  get bufferedAmount(): number {
    let pending = 0
    for (const item of this.queue) pending += payloadByteLength(item)
    return (this.channel?.bufferedAmount ?? 0) + pending
  }

  /**
   * Send one payload. With {@link highWaterBytes} configured, payloads are queued
   * (never dropped) while the channel is at or above the high-water mark;
   * otherwise uses the legacy 1 MB polling flush (same as stock RTCTransport).
   */
  async send(data: SendPayload): Promise<void> {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") throw new Error("RTC data channel is not open")

    if (this.highWaterBytes === undefined) {
      channel.send(data)
      await this.pollFlush(channel)
      return
    }

    if (channel.bufferedAmount >= this.highWaterBytes) {
      this.backpressured = true
      this.queue.push(data)
      return
    }

    channel.send(data)
    if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true
    else this.tryFlushAfterSend()
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

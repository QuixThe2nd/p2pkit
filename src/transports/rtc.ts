import type { Transport, TransportEvents } from "./types.js"
import type { SignallingChannel, SignallingMessage } from "../signalling/types.js"
import type { RTCBackend } from "../backends/index.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"
import { Chunker } from "../framing/index.js"
import { randomId } from "../utils/id.js"
import { DEFAULT_ICE_SERVERS } from "../utils/ice.js"
import { RTCDataChannelSendQueue } from "./rtc-send-queue.js"

/** Specification for one negotiated RTCDataChannel when using multi-channel mode. */
export interface RTCChannelSpec {
  label: string
  ordered: boolean
  maxRetransmits?: number
  /**
   * Per-channel backpressure policy (raw mode only; default `"queue"`):
   * - `"queue"`: sends accepted while under the high-water mark queue in order
   *   and flush on `bufferedamountlow` (never dropped).
   * - `"drop"`: sends at or above the high-water mark are rejected
   *   synchronously (`sendOn` resolves `false`, {@link RTCTransport.trySendOn}
   *   returns `false`) so lossy, time-sensitive producers can resend fresh
   *   state instead of buffering stale state.
   */
  mode?: "queue" | "drop"
  /**
   * Per-channel override for the queue high-water mark; falls back to
   * {@link RTCTransportOptions.highWaterBytes} when omitted.
   */
  highWaterBytes?: number
  /**
   * Per-channel override for `bufferedAmountLowThreshold`; falls back to
   * {@link RTCTransportOptions.lowWaterBytes} (or half the effective
   * high-water mark) when omitted.
   */
  lowWaterBytes?: number
}

export interface RTCTransportOptions {
  self: PeerId
  remote: PeerId
  signalling: SignallingChannel
  backend: RTCBackend
  /** ICE servers (STUN/TURN). Defaults to public STUN — add TURN for reliability. */
  iceServers?: RTCIceServer[]
  /** Whether this side creates the offer + data channel. Set deterministically by `Peer`. */
  initiator: boolean
  /** Max characters per data-channel packet (see {@link Chunker}). */
  chunkSize?: number
  label?: string
  /**
   * When set, the initiator opens one data channel per spec and the receiver
   * binds incoming channels by matching `label`. `connect` fires only after
   * **all** spec channels are open.
   */
  channels?: RTCChannelSpec[]
  /**
   * Milliseconds from construction until `connect` must fire; otherwise emits
   * {@link RTCTransportConnectTimeoutError}, closes the link, and emits `disconnect`.
   */
  connectTimeoutMs?: number
  /**
   * Per-channel high-water backpressure (bytes). When set, sends queue instead of
   * polling at 1 MB. Applies to every channel (spec channels or the default one).
   */
  highWaterBytes?: number
  /**
   * Per-channel low-water mark for `bufferedAmountLowThreshold`.
   * Defaults to half of {@link highWaterBytes} when omitted.
   */
  lowWaterBytes?: number
  /**
   * Opt-in raw-binary mode (off by default, preserving the JSON paths):
   * `send`/`sendOn` payloads are binary (`ArrayBuffer` or `ArrayBufferView`)
   * and cross each data channel byte-for-byte as a single message — no JSON
   * encoding, no chunking; received messages are delivered to the `message`
   * handler as a fresh `Uint8Array` view with the channel index. Per-channel
   * drop/queue policy is controlled by {@link RTCChannelSpec.mode}.
   */
  raw?: boolean
}

/** Coerce a raw-mode payload to bytes; throws on non-binary input. */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  throw new Error("raw RTC payloads must be an ArrayBuffer or ArrayBufferView")
}

/** Distinct timeout error emitted when {@link RTCTransportOptions.connectTimeoutMs} elapses. */
export class RTCTransportConnectTimeoutError extends Error {
  override readonly name = "ErrorTimeout"
  readonly code = "ERR_RTC_CONNECT_TIMEOUT"

  constructor(timeoutMs: number) {
    super(`RTC transport connect timed out after ${timeoutMs}ms`)
  }
}

/** RTCTransport-specific events (extends base {@link TransportEvents}). */
export type RTCTransportEvents<T> = Omit<TransportEvents<T>, "message"> & {
  /**
   * Delivered JSON value. `channelIndex` is always present: `0` for the default
   * single-channel mode, or the index matching {@link RTCTransportOptions.channels}.
   */
  message: (msg: T, channelIndex: number) => void
  /** Fired when a channel's send queue fully drains after high-water backpressure. */
  drain: (channelIndex: number) => void
}

interface ChannelState {
  channel: RTCDataChannel
  queue: RTCDataChannelSendQueue
  chunker: Chunker
  spec?: RTCChannelSpec
}

/**
 * WebRTC data-channel transport (README §6, default). Negotiates offer/answer +
 * ICE over a {@link SignallingChannel}, then carries JSON values, transparently
 * chunking anything above the channel's size cap.
 */
export class RTCTransport<T = unknown> implements Transport<T> {
  readonly remote: PeerId
  readonly name = "rtc"

  private readonly self: PeerId
  private readonly signalling: SignallingChannel
  private readonly emitter = new Emitter<RTCTransportEvents<T>>()
  private readonly channelStates: ChannelState[] = []
  private readonly channelSpecs?: RTCChannelSpec[]
  private readonly expectedChannelCount: number
  private readonly connectTimeoutMs?: number
  private connectTimer?: ReturnType<typeof setTimeout>
  private connectEmitted = false
  private readonly openChannels = new Set<number>()
  private readonly pc: RTCPeerConnection
  private remoteDescriptionSet = false
  private readonly pendingCandidates: RTCIceCandidateInit[] = []
  private closed = false
  private emittedClose = false
  private readonly raw: boolean
  private rttMs = 0
  private rttTimer?: ReturnType<typeof setInterval>

  constructor(options: RTCTransportOptions) {
    this.self = options.self
    this.remote = options.remote
    this.signalling = options.signalling
    this.channelSpecs = options.channels
    this.expectedChannelCount = options.channels?.length ?? 1
    this.connectTimeoutMs = options.connectTimeoutMs
    this.raw = options.raw === true

    const iceServers = options.iceServers ?? DEFAULT_ICE_SERVERS
    this.pc = new options.backend.RTCPeerConnection({ iceServers })

    if (this.connectTimeoutMs !== undefined) {
      this.connectTimer = setTimeout(() => this.handleConnectTimeout(), this.connectTimeoutMs)
    }

    this.pc.onicecandidate = ev => {
      if (ev.candidate) {
        this.signalling.send({
          iceCandidate: ev.candidate.toJSON(),
          from: this.self,
          to: this.remote,
        })
      }
    }
    this.pc.onconnectionstatechange = () => {
      const state = this.pc.connectionState
      if (state === "failed" || state === "closed" || state === "disconnected") this.handleClose()
    }

    if (options.initiator) {
      if (options.channels) {
        for (let i = 0; i < options.channels.length; i++) {
          const spec = options.channels[i]!
          this.setupChannel(
            i,
            this.pc.createDataChannel(spec.label, {
              ordered: spec.ordered,
              maxRetransmits: spec.maxRetransmits,
            }),
            options,
          )
        }
      } else {
        this.setupChannel(
          0,
          this.pc.createDataChannel(options.label ?? "p2pkit"),
          options,
        )
      }
      void this.negotiate()
    } else {
      this.pc.ondatachannel = ev => {
        const label = ev.channel.label
        if (options.channels) {
          const index = options.channels.findIndex(spec => spec.label === label)
          if (index === -1) return
          this.setupChannel(index, ev.channel, options)
        } else {
          this.setupChannel(0, ev.channel, options)
        }
      }
    }

    this.signalling.onMessage(this.onSignal)
  }

  get bufferedAmount(): number {
    let total = 0
    for (const state of this.channelStates) total += state.queue.bufferedAmount
    return total
  }

  /** Bytes queued on one channel (spec index, or `0` in single-channel mode). */
  bufferedAmountOn(channelIndex: number): number {
    return this.channelStates[channelIndex]?.queue.bufferedAmount ?? 0
  }

  on<E extends keyof TransportEvents<T>>(event: E, handler: TransportEvents<T>[E]): void
  on(event: "drain", handler: (channelIndex: number) => void): void
  on(event: "message", handler: (msg: T, channelIndex: number) => void): void
  on(
    event: keyof RTCTransportEvents<T>,
    handler:
      | TransportEvents<T>[keyof TransportEvents<T>]
      | RTCTransportEvents<T>[keyof RTCTransportEvents<T>],
  ): void {
    this.emitter.on(event, handler as RTCTransportEvents<T>[keyof RTCTransportEvents<T>])
  }

  async send(value: T): Promise<void> {
    await this.sendOn(0, value)
  }

  /**
   * Send one value on the given channel. In raw mode (`raw: true`) binary
   * payloads cross byte-for-byte and the promise resolves `false` when a
   * `"drop"`-policy channel rejected the payload under backpressure (it never
   * rejects for that reason). In the default JSON mode always resolves `true`.
   */
  async sendOn(channelIndex: number, value: T): Promise<boolean> {
    const state = this.channelStates[channelIndex]
    if (!state || state.channel.readyState !== "open") {
      throw new Error("RTC transport is not open")
    }
    if (this.raw) {
      const bytes = toUint8Array(value)
      if (state.spec?.mode === "drop") return state.queue.trySend(bytes)
      await state.queue.send(bytes)
      return true
    }
    const groupId = randomId(8)
    const data = JSON.stringify(value)
    for (const packet of state.chunker.split(groupId, data)) {
      await state.queue.send(packet)
    }
    return true
  }

  /**
   * Synchronous acceptance for raw mode: queues on `"queue"`-policy channels
   * (fire-and-forget) and returns whether the payload was accepted; on
   * `"drop"`-policy channels returns `false` when it was rejected under
   * backpressure instead of queueing stale lossy state.
   *
   * Why this exists beside the async {@link sendOn}: Emscripten `ccall`
   * exports and other synchronous producers must learn send acceptance in the
   * same tick; a promise resolves too late to substitute fresh state.
   */
  trySendOn(channelIndex: number, value: T): boolean {
    if (!this.raw) throw new Error("trySendOn requires raw mode (options.raw)")
    const state = this.channelStates[channelIndex]
    if (!state || state.channel.readyState !== "open") return false
    const bytes = toUint8Array(value)
    if (state.spec?.mode === "drop") return state.queue.trySend(bytes)
    void state.queue.send(bytes)
    return true
  }

  /**
   * Last known round-trip time estimate for this link in milliseconds, from
   * polling `pc.getStats()` while connected; 0 when not connected.
   *
   * Why cached instead of exposing `getStats()` directly: synchronous C
   * exports (Emscripten `ccall`) cannot await a promise, so the transport
   * keeps a freshest-known value.
   */
  getRoundTripTimeMs(): number {
    return this.rttMs
  }

  disconnect(): void {
    this.closed = true
    this.clearConnectTimer()
    this.stopRttPolling()
    for (const state of this.channelStates) {
      state.queue.detach()
      try {
        state.channel.close()
      } catch {
        /* ignore */
      }
    }
    try {
      this.pc.close()
    } catch {
      /* ignore */
    }
    this.handleClose()
  }

  private setupChannel(
    index: number,
    channel: RTCDataChannel,
    options: RTCTransportOptions,
  ): void {
    const chunker = new Chunker({ maxPacketSize: options.chunkSize })
    const queue = new RTCDataChannelSendQueue({
      highWaterBytes: options.channels?.[index]?.highWaterBytes ?? options.highWaterBytes,
      lowWaterBytes: options.channels?.[index]?.lowWaterBytes ?? options.lowWaterBytes,
      onDrain: () => this.emitter.emit("drain", index),
    })
    queue.attach(channel)

    this.channelStates[index] = { channel, queue, chunker, spec: this.channelSpecs?.[index] }

    try {
      channel.binaryType = "arraybuffer"
    } catch {
      /* some backends fix this */
    }

    channel.onopen = () => this.onChannelOpen(index)
    channel.onmessage = ev => this.onData(index, ev.data as string | ArrayBuffer)
    channel.onclose = () => this.handleClose()
    channel.onerror = () => this.emitter.emit("error", new Error("RTC data channel error"))
    if (channel.readyState === "open") this.onChannelOpen(index)
  }

  private onChannelOpen(index: number): void {
    if (this.closed || this.connectEmitted) return
    this.openChannels.add(index)
    if (this.openChannels.size >= this.expectedChannelCount) this.emitConnect()
  }

  private emitConnect(): void {
    if (this.connectEmitted || this.closed) return
    this.connectEmitted = true
    this.clearConnectTimer()
    this.startRttPolling()
    this.emitter.emit("connect")
  }

  private startRttPolling(): void {
    if (this.rttTimer || typeof this.pc.getStats !== "function") return
    this.rttTimer = setInterval(() => {
      if (this.closed) {
        this.stopRttPolling()
        return
      }
      this.pc
        .getStats()
        .then(report => {
          let best = 0
          report.forEach(entry => {
            if (
              entry.type === "candidate-pair" &&
              entry.state === "succeeded" &&
              typeof entry.currentRoundTripTime === "number"
            ) {
              const ms = entry.currentRoundTripTime * 1000
              if (best === 0 || ms < best) best = ms
            }
          })
          if (best > 0) this.rttMs = Math.round(best)
        })
        .catch(() => {
          /* stats unavailable; keep last value */
        })
    }, 2000)
    if (typeof this.rttTimer === "object" && this.rttTimer && typeof this.rttTimer.unref === "function") {
      this.rttTimer.unref()
    }
  }

  private stopRttPolling(): void {
    if (this.rttTimer !== undefined) {
      clearInterval(this.rttTimer)
      this.rttTimer = undefined
    }
    this.rttMs = 0
  }

  private clearConnectTimer(): void {
    if (this.connectTimer !== undefined) {
      clearTimeout(this.connectTimer)
      this.connectTimer = undefined
    }
  }

  private handleConnectTimeout(): void {
    if (this.closed || this.connectEmitted || this.emittedClose) return
    this.closed = true
    this.clearConnectTimer()
    this.stopRttPolling()
    this.emitter.emit("error", new RTCTransportConnectTimeoutError(this.connectTimeoutMs!))
    for (const state of this.channelStates) {
      state.queue.detach()
      try {
        state.channel.close()
      } catch {
        /* ignore */
      }
    }
    try {
      this.pc.close()
    } catch {
      /* ignore */
    }
    this.handleClose()
  }

  private async negotiate(): Promise<void> {
    await this.signalling.ready
    const offer = await this.pc.createOffer()
    await this.pc.setLocalDescription(offer)
    this.sendDescription()
  }

  private sendDescription(): void {
    const description = this.pc.localDescription
    if (!description) return
    this.signalling.send({
      description: { type: description.type, sdp: description.sdp },
      from: this.self,
      to: this.remote,
    })
  }

  private onSignal = (message: SignallingMessage): void => {
    void this.handleSignal(message)
  }

  private async handleSignal(message: SignallingMessage): Promise<void> {
    if (this.closed) return
    if ("description" in message) {
      if (message.from !== this.remote || message.to !== this.self) return
      await this.pc.setRemoteDescription(message.description)
      this.remoteDescriptionSet = true
      await this.flushCandidates()
      if (message.description.type === "offer") {
        const answer = await this.pc.createAnswer()
        await this.pc.setLocalDescription(answer)
        this.sendDescription()
      }
    } else if ("iceCandidate" in message) {
      if (message.from !== this.remote || message.to !== this.self) return
      if (this.remoteDescriptionSet) await this.pc.addIceCandidate(message.iceCandidate)
      else this.pendingCandidates.push(message.iceCandidate)
    }
  }

  private async flushCandidates(): Promise<void> {
    for (const candidate of this.pendingCandidates.splice(0)) {
      try {
        await this.pc.addIceCandidate(candidate)
      } catch {
        /* stale candidate */
      }
    }
  }

  private onData(channelIndex: number, data: string | ArrayBuffer): void {
    const state = this.channelStates[channelIndex]
    if (!state) return
    if (this.raw) {
      if (typeof data === "string") return // raw contract is binary-only
      const bytes = data instanceof Uint8Array ? data : new Uint8Array(data)
      this.emitter.emit("message", bytes as T, channelIndex)
      return
    }
    const raw = typeof data === "string" ? data : new TextDecoder().decode(data)
    let packet: { id: string; i: number; n: number; part: string }
    try {
      packet = JSON.parse(raw)
    } catch {
      return
    }
    const full = state.chunker.ingest(packet)
    if (full === undefined) return
    try {
      this.emitter.emit("message", JSON.parse(full) as T, channelIndex)
    } catch {
      /* malformed payload */
    }
  }

  private handleClose(): void {
    if (this.emittedClose) return
    this.emittedClose = true
    this.clearConnectTimer()
    for (const state of this.channelStates) {
      state.chunker.reset()
      state.queue.detach()
    }
    this.emitter.emit("disconnect")
  }
}

import type { Transport, TransportEvents } from "./types.js"
import type { SignallingChannel, SignallingMessage } from "../signalling/types.js"
import type { RTCBackend } from "../backends/index.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"
import { Chunker } from "../framing/index.js"
import { randomId } from "../utils/id.js"
import { DEFAULT_ICE_SERVERS } from "../utils/ice.js"

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
}

const FLUSH_THRESHOLD = 1 << 20 // 1 MB queued → apply backpressure

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
  private readonly emitter = new Emitter<TransportEvents<T>>()
  private readonly chunker: Chunker
  private readonly pc: RTCPeerConnection
  private channel?: RTCDataChannel
  private remoteDescriptionSet = false
  private readonly pendingCandidates: RTCIceCandidateInit[] = []
  private closed = false
  private emittedClose = false

  constructor(options: RTCTransportOptions) {
    this.self = options.self
    this.remote = options.remote
    this.signalling = options.signalling
    this.chunker = new Chunker({ maxPacketSize: options.chunkSize })

    const iceServers = options.iceServers ?? DEFAULT_ICE_SERVERS
    this.pc = new options.backend.RTCPeerConnection({ iceServers })

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
      this.setupChannel(this.pc.createDataChannel(options.label ?? "p2pkit"))
      void this.negotiate()
    } else {
      this.pc.ondatachannel = ev => this.setupChannel(ev.channel)
    }

    this.signalling.onMessage(this.onSignal)
  }

  get bufferedAmount(): number {
    return this.channel?.bufferedAmount ?? 0
  }

  on<E extends keyof TransportEvents<T>>(event: E, handler: TransportEvents<T>[E]): void {
    this.emitter.on(event, handler)
  }

  async send(value: T): Promise<void> {
    const channel = this.channel
    if (!channel || channel.readyState !== "open") throw new Error("RTC transport is not open")
    const groupId = randomId(8)
    const data = JSON.stringify(value)
    for (const packet of this.chunker.split(groupId, data)) channel.send(packet)
    await this.flush(channel)
  }

  disconnect(): void {
    this.closed = true
    try {
      this.channel?.close()
    } catch {
      /* ignore */
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

  private setupChannel(channel: RTCDataChannel): void {
    this.channel = channel
    try {
      channel.binaryType = "arraybuffer"
    } catch {
      /* some backends fix this */
    }
    channel.onopen = () => this.emitter.emit("connect")
    channel.onmessage = ev => this.onData(ev.data as string | ArrayBuffer)
    channel.onclose = () => this.handleClose()
    channel.onerror = () => this.emitter.emit("error", new Error("RTC data channel error"))
    if (channel.readyState === "open") this.emitter.emit("connect")
  }

  private onData(data: string | ArrayBuffer): void {
    const raw = typeof data === "string" ? data : new TextDecoder().decode(data)
    let packet: { id: string; i: number; n: number; part: string }
    try {
      packet = JSON.parse(raw)
    } catch {
      return
    }
    const full = this.chunker.ingest(packet)
    if (full === undefined) return
    try {
      this.emitter.emit("message", JSON.parse(full) as T)
    } catch {
      /* malformed payload */
    }
  }

  private async flush(channel: RTCDataChannel): Promise<void> {
    while (channel.readyState === "open" && channel.bufferedAmount > FLUSH_THRESHOLD) {
      await new Promise(resolve => setTimeout(resolve, 10))
    }
  }

  private handleClose(): void {
    if (this.emittedClose) return
    this.emittedClose = true
    this.chunker.reset()
    this.emitter.emit("disconnect")
  }
}

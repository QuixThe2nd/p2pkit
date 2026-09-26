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
   *   synchronously: {@link RTCTransport.trySendOn} returns `false` and the
   *   async {@link RTCTransport.sendOn} rejects with
   *   {@link RTCTransportBackpressureDropError}, so lossy, time-sensitive
   *   producers can resend fresh state instead of buffering stale state.
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
   * {@link RTCTransportConnectTimeoutError}, closes the link, and emits
   * `disconnect`. Defaults to {@link DEFAULT_CONNECT_TIMEOUT_MS} so a slot
   * stuck pre-connect is always bounded; an explicit `0` disables the timer.
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
  /**
   * Opt-in direct-play hardening (off by default; the defaults keep public
   * STUN/TURN capability and the lax framing contract). When set, this
   * transport speaks only strict, bounded, fail-closed JSON over a single
   * ordered/reliable channel, as required of a direct (server-relayed-free)
   * link between two browsers:
   *
   * - `iceServers` is normalized through {@link directIceServers} (STUN only —
   *   no TURN/relays, no credentials), outgoing candidates/descriptions are
   *   validated ({@link validateDirectCandidate}/{@link validateDirectDescription}),
   *   end-of-gathering candidates (empty string) are never published, and
   *   incoming signalling is validated and bounded (pre-SDP candidate floods
   *   and signal floods close the link instead of accumulating).
   * - Framing uses a hardened `Chunker` (see `src/framing/index.ts`): malformed
   *   or budget-exceeding frames close the link once; receive floods are
   *   bounded per second; partial groups expire.
   * - Sends go through the queue's bounded job API: full-size concurrent sends
   *   keep FIFO and fail closed at bounded pressure instead of buffering
   *   without limit; {@link trySend} gives synchronous acceptance.
   * - Closing detaches the signalling handler (a signalling channel whose
   *   `onMessage` returns an unsubscribe function has it called).
   *
   * Incompatible with `channels` (one ordered/reliable channel) and `raw`.
   */
  direct?: boolean
}

/**
 * Handshake deadline applied when {@link RTCTransportOptions.connectTimeoutMs}
 * is omitted: a slot stuck pre-connect (ICE may never reach
 * failed/closed/disconnected if no candidates flow) must not linger forever.
 * An explicit `connectTimeoutMs: 0` opts out.
 */
const DEFAULT_CONNECT_TIMEOUT_MS = 30_000

/**
 * Direct-play resource bounds (used when {@link RTCTransportOptions.direct} is
 * set). Mirrors the hardening Dune City's direct/crossplay bridge shipped
 * against its vendored p2pkit fork.
 */
const DIRECT_MAX_BUFFERED = 1 << 20 // pause channel sends above 1 MB buffered
const DIRECT_MAX_OUTGOING_BYTES = 4 << 20 // reject sends beyond 4 MB retained
const DIRECT_MAX_OUTGOING_MESSAGES = 128 // reject sends beyond 128 retained messages
const DIRECT_MAX_SIGNALS = 128 // total signalling messages per connection
const DIRECT_MAX_SDP = 65_536
const DIRECT_MAX_CANDIDATE = 2048
const DIRECT_MAX_FRAME_BYTES = 131_072 // one received frame before parsing
const DIRECT_MAX_PACKETS_PER_SECOND = 4096 // received frames per rolling second
const DIRECT_MAX_BYTES_PER_SECOND = 16 * 1024 * 1024

/** Reject TURN configuration and relay candidates, including ones embedded in SDP. */
export function directIceServers(servers: RTCIceServer[]): RTCIceServer[] {
  if (!Array.isArray(servers) || servers.length > 8) throw new Error("Invalid STUN configuration")
  return servers.map(server => {
    const urls = typeof server.urls === "string" ? [server.urls] : server.urls
    if (
      !Array.isArray(urls) ||
      urls.length === 0 ||
      urls.length > 8 ||
      server.username ||
      server.credential ||
      urls.some(
        url => typeof url !== "string" || url.length > 256 || !/^stuns?:[a-zA-Z0-9.\[\]:-]+$/.test(url),
      )
    ) {
      throw new Error("Only STUN is supported for direct play")
    }
    return { urls: [...urls] }
  })
}

/**
 * Validate an ICE candidate for direct play: well-formed, bounded, and never a
 * relay. An empty `candidate` string is valid (end-of-gathering hint).
 */
export function validateDirectCandidate(candidate: unknown): asserts candidate is RTCIceCandidateInit {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    throw new Error("Invalid ICE candidate")
  }
  const { candidate: value, sdpMid, sdpMLineIndex } = candidate as RTCIceCandidateInit
  if (
    typeof value !== "string" ||
    value.length > DIRECT_MAX_CANDIDATE ||
    /[\r\n\0]/.test(value) ||
    (value !== "" &&
      (!value.startsWith("candidate:") ||
        /\styp\s+relay(?:\s|$)/.test(value) ||
        !/\styp (host|srflx|prflx)(?:\s|$)/.test(value))) ||
    (sdpMid !== undefined && sdpMid !== null && (typeof sdpMid !== "string" || !/^[A-Za-z0-9_-]{1,64}$/.test(sdpMid))) ||
    (sdpMLineIndex !== undefined &&
      sdpMLineIndex !== null &&
      (!Number.isInteger(sdpMLineIndex) || sdpMLineIndex < 0 || sdpMLineIndex > 16))
  ) {
    throw new Error("Invalid direct ICE candidate")
  }
}

/**
 * Validate a session description for direct play: an offer/answer carrying
 * exactly one data-channel media section with one sha-256 certificate
 * fingerprint, no embedded relay candidates, bounded size.
 */
export function validateDirectDescription(description: unknown): asserts description is RTCSessionDescriptionInit {
  if (!description || typeof description !== "object") throw new Error("Invalid session description")
  const { type, sdp } = description as RTCSessionDescriptionInit
  if (
    (type !== "offer" && type !== "answer") ||
    typeof sdp !== "string" ||
    sdp.length > DIRECT_MAX_SDP ||
    sdp.includes("\0") ||
    !/^v=0\r?\n/.test(sdp) ||
    !/^a=fingerprint:sha-256 (?:[0-9A-Fa-f]{2}:){31}[0-9A-Fa-f]{2}\r?$/m.test(sdp) ||
    (sdp.match(/^a=fingerprint:/gm)?.length ?? 0) !== 1 ||
    (sdp.match(/^m=application /gm)?.length ?? 0) !== 1 ||
    /^m=(?!application )/m.test(sdp)
  ) {
    throw new Error("Invalid data-channel description")
  }
  let count = 0
  for (const line of sdp.split(/\r?\n/)) {
    if (line.startsWith("a=candidate:")) {
      if (++count > DIRECT_MAX_SIGNALS) throw new Error("Too many ICE candidates")
      validateDirectCandidate({ candidate: line.slice(2) })
    }
  }
}

/** Coerce a raw-mode payload to bytes; throws on non-binary input. */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  throw new Error("raw RTC payloads must be an ArrayBuffer or ArrayBufferView")
}

const textEncoder = new TextEncoder()

/**
 * A reliability property read back as "not configured": browsers report `null`
 * for an unset `maxRetransmits`/`maxPacketLifeTime`, while some native backends
 * (e.g. @roamhq/wrtc) report the effective SCTP maximum 65535. Both spellings
 * mean fully reliable; anything lower is partially lossy.
 */
function isFullyReliable(value: number | null): boolean {
  return value === null || value === 65535
}

/** Distinct timeout error emitted when {@link RTCTransportOptions.connectTimeoutMs} elapses. */
export class RTCTransportConnectTimeoutError extends Error {
  override readonly name = "ErrorTimeout"
  readonly code = "ERR_RTC_CONNECT_TIMEOUT"

  constructor(timeoutMs: number) {
    super(`RTC transport connect timed out after ${timeoutMs}ms`)
  }
}

/**
 * Distinct error the async {@link RTCTransport.sendOn} rejects with in raw
 * mode when a `"drop"`-policy channel rejects the payload under backpressure
 * (the synchronous counterpart to {@link RTCTransport.trySendOn} returning
 * `false`). It never rejects for this reason on `"queue"`-policy channels or
 * in the default JSON mode.
 */
export class RTCTransportBackpressureDropError extends Error {
  override readonly name = "ErrorBackpressureDrop"
  readonly code = "ERR_RTC_BACKPRESSURE_DROP"

  constructor(channelIndex: number) {
    super(`RTC channel ${channelIndex} dropped raw payload under backpressure (drop mode)`)
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
  /** Direct-mode receive-flood accounting (one rolling one-second window). */
  traffic?: { start: number; packets: number; bytes: number }
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
  /** Any valid signal from the remote so far — proof the negotiation is progressing. */
  private signalReceived = false
  private readonly pendingCandidates: RTCIceCandidateInit[] = []
  private closed = false
  private emittedClose = false
  private readonly raw: boolean
  private readonly direct: boolean
  private readonly initiator: boolean
  private signalCount = 0
  private candidateCount = 0
  private signalChain = Promise.resolve()
  private unsubscribeSignalling?: () => void
  private expiryTimer?: ReturnType<typeof setInterval>
  private rttMs = 0
  private rttTimer?: ReturnType<typeof setInterval>

  constructor(options: RTCTransportOptions) {
    this.self = options.self
    this.remote = options.remote
    this.signalling = options.signalling
    this.channelSpecs = options.channels
    this.expectedChannelCount = options.channels?.length ?? 1
    // Bound the slot even when no deadline was configured; an explicit 0
    // keeps the escape hatch (no connect timer at all).
    const connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS
    this.connectTimeoutMs = connectTimeoutMs > 0 ? connectTimeoutMs : undefined
    this.raw = options.raw === true
    this.direct = options.direct === true
    this.initiator = options.initiator
    if (this.direct && options.channels) {
      throw new Error("direct mode uses a single ordered/reliable channel; channels is not supported")
    }
    if (this.direct && this.raw) {
      throw new Error("direct mode sends JSON values; raw is not supported")
    }

    const iceServers = this.direct
      ? directIceServers(options.iceServers ?? DEFAULT_ICE_SERVERS)
      : (options.iceServers ?? DEFAULT_ICE_SERVERS)
    this.pc = new options.backend.RTCPeerConnection({ iceServers })

    if (this.connectTimeoutMs !== undefined) {
      this.connectTimer = setTimeout(() => this.handleConnectTimeout(), this.connectTimeoutMs)
    }

    this.pc.onicecandidate = ev => {
      if (ev.candidate) {
        if (this.direct) {
          try {
            const candidate = ev.candidate.toJSON()
            validateDirectCandidate(candidate)
            // Browsers may report end-of-gathering as an object with
            // candidate="", before the final null event. It is a completion
            // hint, not another network address — omit it rather than publish
            // an empty candidate.
            if (candidate.candidate === "") return
            this.signalling.send({
              iceCandidate: candidate,
              from: this.self,
              to: this.remote,
            })
          } catch {
            this.failClosed("Could not exchange direct connection details")
          }
          return
        }
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
          this.pc.createDataChannel(
            options.label ?? "p2pkit",
            this.direct ? { ordered: true } : undefined,
          ),
          options,
        )
      }
      // A rejection here (offer creation failing, or the PC closing before
      // setLocalDescription settles — e.g. the app drops a fresh initiator
      // transport) must free the slot through the disconnect chain, not die
      // as an unhandled rejection. failClosed no-ops once already closed.
      void this.negotiate().catch(() =>
        this.failClosed(
          this.direct ? "Direct connection negotiation failed" : "Connection negotiation failed",
        ),
      )
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

    if (this.direct) {
      // Expire partially-received messages even when the peer goes silent
      // (hardened Chunker.ingest also checks on every frame).
      this.expiryTimer = setInterval(() => {
        if (this.closed || this.emittedClose) {
          this.stopExpiryTimer()
          return
        }
        try {
          for (const state of this.channelStates) state.chunker.checkDeadline()
        } catch {
          this.failClosed("A peer sent an incomplete message")
        }
      }, 1000)
      if (typeof this.expiryTimer === "object" && this.expiryTimer && typeof this.expiryTimer.unref === "function") {
        this.expiryTimer.unref()
      }
    }

    const unsubscribe = this.signalling.onMessage(this.onSignal) as unknown
    if (this.direct && typeof unsubscribe === "function") {
      this.unsubscribeSignalling = unsubscribe as () => void
    }
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
   * Send one value on the given channel; resolves `undefined` once the payload
   * has been handed to the send queue (default JSON mode, preserving the
   * historical `Promise<void>` contract). In raw mode (`raw: true`) binary
   * payloads cross byte-for-byte: on `"drop"`-policy channels the promise
   * rejects with {@link RTCTransportBackpressureDropError} when the payload is
   * rejected under backpressure (use {@link trySendOn} for synchronous
   * acceptance); on `"queue"`-policy channels it never rejects for that reason.
   */
  async sendOn(channelIndex: number, value: T): Promise<void> {
    const state = this.channelStates[channelIndex]
    if (!state || state.channel.readyState !== "open") {
      throw new Error("RTC transport is not open")
    }
    if (this.raw) {
      const bytes = toUint8Array(value)
      if (state.spec?.mode === "drop") {
        if (!state.queue.trySend(bytes)) throw new RTCTransportBackpressureDropError(channelIndex)
        return
      }
      await state.queue.send(bytes)
      return
    }
    if (this.direct) {
      // Whole-message job: fragments of one message never interleave with
      // another's, the promise settles only on full in-order delivery, and the
      // bounded queue fails closed under pressure instead of growing.
      const packets = [...state.chunker.split(randomId(8), JSON.stringify(value))]
      try {
        await state.queue.sendJob(packets)
      } catch (err) {
        this.failClosed("Direct connection send queue is full")
        throw err
      }
      return
    }
    const groupId = randomId(8)
    const data = JSON.stringify(value)
    for (const packet of state.chunker.split(groupId, data)) {
      await state.queue.send(packet)
    }
  }

  /**
   * Synchronous bounded acceptance for direct-mode JSON sends: the value is
   * fragmented and handed to the channel's bounded job queue, and this returns
   * whether it was accepted — never a promise masquerading as success. Delivery
   * completes silently (a synchronous producer, e.g. an Emscripten `ccall`
   * bridge, cannot await); the link's health, including a later close that
   * abandons an accepted job, surfaces on the `error`/`disconnect` events.
   * A value that cannot be framed under the hardened limits fails the link
   * (fail-closed, like the send-queue bound) and returns `false`.
   *
   * Throws outside direct mode (`options.direct`), which is the JSON-mode
   * counterpart of {@link trySendOn}'s raw-mode guard.
   */
  trySend(value: T): boolean {
    if (!this.direct) throw new Error("trySend requires direct mode (options.direct)")
    const state = this.channelStates[0]
    if (!state || state.channel.readyState !== "open") return false
    try {
      const packets = [...state.chunker.split(randomId(8), JSON.stringify(value))]
      return state.queue.tryJob(packets)
    } catch {
      this.failClosed("Direct connection could not deliver a message")
      return false
    }
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
    // Accepted for queued delivery; a later native send failure surfaces on
    // the `error` event instead of an unhandled rejection (the caller cannot
    // await a synchronous export).
    state.queue.send(bytes).catch(err => {
      this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
    })
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
    this.stopExpiryTimer()
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

  /**
   * Direct-mode fail-closed path: emit the cause once, then tear the link down
   * (`disconnect` emits the single `disconnect`; pending send jobs are rejected
   * by their queue detaching). Accepts the finished `Error` directly so a
   * native send failure reaches consumers unaltered.
   */
  private failClosed(cause: Error | string): void {
    if (this.closed || this.emittedClose) return
    try {
      this.emitter.emit("error", cause instanceof Error ? cause : new Error(cause))
    } finally {
      this.disconnect()
    }
  }

  private stopExpiryTimer(): void {
    if (this.expiryTimer !== undefined) {
      clearInterval(this.expiryTimer)
      this.expiryTimer = undefined
    }
  }

  private setupChannel(
    index: number,
    channel: RTCDataChannel,
    options: RTCTransportOptions,
  ): void {
    if (
      this.direct &&
      (channel.label !== (options.label ?? "p2pkit") ||
        !channel.ordered ||
        !isFullyReliable(channel.maxRetransmits) ||
        !isFullyReliable(channel.maxPacketLifeTime))
    ) {
      // A direct link carries lockstep JSON on exactly one ordered, reliable
      // channel; anything else cannot be trusted to be in step.
      try {
        channel.close()
      } catch {
        /* already closed */
      }
      this.failClosed("Invalid direct game channel")
      return
    }
    const chunker = new Chunker({ maxPacketSize: options.chunkSize, hardened: this.direct })
    const queue = new RTCDataChannelSendQueue({
      highWaterBytes:
        options.channels?.[index]?.highWaterBytes ??
        options.highWaterBytes ??
        (this.direct ? DIRECT_MAX_BUFFERED : undefined),
      lowWaterBytes: options.channels?.[index]?.lowWaterBytes ?? options.lowWaterBytes,
      onDrain: () => this.emitter.emit("drain", index),
      // A native send() throw while flushing retained work (including delayed,
      // timer-driven drains) already rejected every pending job in the queue;
      // surface it exactly once on the transport's error path. Direct mode
      // fails the whole link (lockstep JSON cannot continue past a lost
      // fragment); other modes report the error without tearing the transport.
      onSendError: error => {
        if (this.direct) this.failClosed(error)
        else this.emitter.emit("error", error)
      },
      maxQueuedBytes: this.direct ? DIRECT_MAX_OUTGOING_BYTES : undefined,
      maxQueuedJobs: this.direct ? DIRECT_MAX_OUTGOING_MESSAGES : undefined,
    })
    queue.attach(channel)

    this.channelStates[index] = {
      channel,
      queue,
      chunker,
      spec: this.channelSpecs?.[index],
      traffic: this.direct ? { start: performance.now(), packets: 0, bytes: 0 } : undefined,
    }

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
          // A closure (or a stopRttPolling that already cleared the cached
          // value) must not be undone by an in-flight getStats resolution.
          if (this.closed || this.rttTimer === undefined) return
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
    this.stopExpiryTimer()
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

  /**
   * Offer again for a handshake that never completed — the signalling path the
   * first offer went out on may have died with it (a lobby that dropped between
   * send and delivery), or the offer arrived before the far end was listening.
   * Only the initiator has anything to re-send; a link that is already up, or
   * whose negotiation is provably in flight (the remote has signalled back),
   * has nothing to gain, so all three no-op.
   */
  renegotiate(): void {
    if (
      this.closed ||
      this.emittedClose ||
      this.connectEmitted ||
      !this.initiator ||
      this.signalReceived
    ) {
      return
    }
    void this.negotiate().catch(() =>
      this.failClosed(
        this.direct ? "Direct connection negotiation failed" : "Connection negotiation failed",
      ),
    )
  }

  private sendDescription(): void {
    const description = this.pc.localDescription
    if (!description) return
    if (this.direct) validateDirectDescription(description)
    this.signalling.send({
      description: { type: description.type, sdp: description.sdp },
      from: this.self,
      to: this.remote,
    })
  }

  private onSignal = (message: SignallingMessage): void => {
    if (this.direct) {
      // Bound and serialize inbound signalling: a flood (before or after SDP)
      // fails the link rather than accumulating unbounded promises/candidates.
      if (this.closed || this.emittedClose) return
      if (
        !message ||
        typeof message !== "object" ||
        !("to" in message) ||
        message.from !== this.remote ||
        message.to !== this.self
      ) {
        return
      }
      if (++this.signalCount > DIRECT_MAX_SIGNALS) {
        this.signalCount--
        this.failClosed("Too many connection messages")
        return
      }
      this.signalChain = this.signalChain
        .then(() => this.handleSignal(message))
        .catch(() => {
          if (!this.closed && !this.emittedClose) this.failClosed("Invalid direct connection details")
        })
        .finally(() => {
          this.signalCount--
        })
      return
    }
    // Serialize inbound signalling and fail the link on a signal the current
    // session cannot apply (e.g. setRemoteDescription rejecting a stale or
    // colliding description from a rejoining peer): the slot frees through the
    // error → disconnect chain instead of dying as an unhandled rejection and
    // lingering. A renegotiation offer on a live session still applies — only
    // a description the peer connection rejects lands in the catch.
    this.signalChain = this.signalChain
      .then(() => this.handleSignal(message))
      .catch(err => {
        if (!this.closed && !this.emittedClose) {
          this.failClosed(err instanceof Error ? err : new Error("Invalid connection details"))
        }
      })
  }

  private async handleSignal(message: SignallingMessage): Promise<void> {
    if (this.closed) return
    if ("description" in message) {
      if (message.from !== this.remote || message.to !== this.self) return
      this.signalReceived = true
      if (this.direct) {
        validateDirectDescription(message.description)
        if (this.remoteDescriptionSet || message.description.type !== (this.initiator ? "answer" : "offer")) {
          throw new Error("Unexpected description")
        }
      }
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
      this.signalReceived = true
      if (this.direct) {
        validateDirectCandidate(message.iceCandidate)
        if (++this.candidateCount > DIRECT_MAX_SIGNALS) throw new Error("Too many candidates")
      }
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
    if (this.direct) {
      // One malformed, oversized or flooding frame ends the link: a peer that
      // cannot stay in step must not keep a half-open connection.
      try {
        if ((typeof data === "string" ? data.length : data.byteLength) > DIRECT_MAX_FRAME_BYTES) {
          throw new Error("Oversized fragment")
        }
        const raw = typeof data === "string" ? data : new TextDecoder("utf-8", { fatal: true }).decode(data)
        const traffic = (state.traffic ??= { start: performance.now(), packets: 0, bytes: 0 })
        const now = performance.now()
        if (now - traffic.start >= 1000) {
          traffic.start = now
          traffic.packets = 0
          traffic.bytes = 0
        }
        // Count before JSON parsing/reassembly, not only after a complete
        // message — well-formed floods are bounded too.
        traffic.packets++
        traffic.bytes += typeof data === "string" ? textEncoder.encode(raw).length : data.byteLength
        if (
          traffic.packets > DIRECT_MAX_PACKETS_PER_SECOND ||
          traffic.bytes > DIRECT_MAX_BYTES_PER_SECOND
        ) {
          throw new Error("Peer traffic limit exceeded")
        }
        const full = state.chunker.ingest(JSON.parse(raw))
        if (full !== undefined) this.emitter.emit("message", JSON.parse(full) as T, channelIndex)
      } catch {
        this.failClosed("A peer sent an invalid game frame")
      }
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
    this.stopRttPolling()
    this.stopExpiryTimer()
    // A closed direct link must not keep receiving signalling: detach the
    // handler when the signalling channel provided an unsubscribe function.
    if (this.unsubscribeSignalling) {
      const unsubscribe = this.unsubscribeSignalling
      this.unsubscribeSignalling = undefined
      try {
        unsubscribe()
      } catch {
        /* signalling already gone */
      }
    }
    for (const state of this.channelStates) {
      state.chunker.reset()
      state.queue.detach()
    }
    this.emitter.emit("disconnect")
  }
}

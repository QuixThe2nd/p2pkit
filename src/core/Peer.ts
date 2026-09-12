import type { PeerId } from "../utils/types.js"
import type { Signer } from "../auth/signer.js"
import type { Transport } from "../transports/types.js"
import type { SignallingChannel } from "../signalling/types.js"
import type { RTCBackend, RTCBackendSource } from "../backends/index.js"
import type { Frame, MsgFrame, HelloFrame, AckFrame, ReqFrame, ResFrame } from "../wire/index.js"
import { WIRE_VERSION, FrameCodec, validateFrame } from "../wire/index.js"
import type { API } from "../rpc/api.js"
import type { APISchema, RPCContext } from "../rpc/schema.js"
import type { Router, DispatchResult } from "../rpc/router.js"
import type { Client } from "../rpc/client.js"
import { createClient } from "../rpc/client.js"
import { RPCError } from "../rpc/error.js"
import { Emitter } from "../utils/emitter.js"
import { randomId } from "../utils/id.js"
import { NoopSigner } from "../auth/signer.js"
import { ECDSASigner } from "../auth/ecdsa.js"
import { encrypt, decrypt } from "../auth/crypto.js"
import { RTCTransport } from "../transports/rtc.js"
import { getRTC } from "../backends/index.js"

/** Events emitted by a {@link Peer}. */
export type PeerEvents<Msg> = {
  /** Fires once the remote's identity is proven (or accepted, without a signer). */
  connect: () => void
  /** A direct application message from this peer. */
  message: (msg: Msg) => void
  disconnect: () => void
  error: (err: Error) => void
  /** Internal: mesh/RPC frames P2PKit and the RPC layer consume. */
  frame: (frame: Frame) => void
}

export interface PeerOptions<Msg = unknown> {
  /** This side's id. Derived from `signer` when one is given; required otherwise. */
  self?: PeerId
  /** The peer to connect to. */
  remote: PeerId
  /** Signalling channel for the WebRTC handshake. */
  signalling?: SignallingChannel
  /** Identity signer; enables verified handshakes and encryption. */
  signer?: Signer
  /** WebRTC backend override; auto-detected when omitted. */
  backend?: RTCBackend | RTCBackendSource
  iceServers?: RTCIceServer[]
  /** Encrypt all post-handshake frames on this connection (requires an `ECDSASigner`). */
  encrypted?: boolean
  /** Capabilities to advertise in the handshake. */
  caps?: string[]
  /** Serve this peer's RPC calls (README §2.1). Built from `api.router({...})`. */
  router?: Router
  /** Per-call RPC timeout in ms. Default 30000. */
  rpcTimeout?: number
  /** Inject a ready transport instead of building an `RTCTransport` (advanced/testing). */
  transport?: Transport<Frame>
}

const PING_INTERVAL_MS = 5000
const DEFAULT_RPC_TIMEOUT = 30_000

/**
 * One authenticated connection to a single peer. Wraps a {@link Transport} with
 * a hello/ack identity handshake (README §5), direct messaging with optional
 * connection encryption, and latency probing. `P2PKit` builds these for you;
 * construct one directly for a single connection with no mesh (README §7).
 */
export class Peer<Msg = unknown> {
  private readonly options: PeerOptions<Msg>
  private readonly signer: Signer | undefined
  private readonly emitter = new Emitter<PeerEvents<Msg>>()

  private self?: PeerId
  private _remote: PeerId
  private transport?: Transport<Frame>
  private _transportName = "rtc"

  private readonly selfNonce = randomId(16)
  private open = false
  private closed = false
  private helloSent = false
  private helloReceived = false
  private ackSent = false
  private identityVerified = false

  private sharedKey?: Uint8Array
  private _latency?: number
  private pingTimer?: ReturnType<typeof setInterval>
  private readonly pendingPings = new Map<string, number>()
  private readonly pendingRPC = new Map<
    string,
    {
      method: string
      finish: (result: DispatchResult | RPCError) => void
    }
  >()
  private incoming = Promise.resolve()

  /** Resolves once the connection is open (identity proven) or rejects on failure. */
  readonly ready: Promise<void>

  constructor(options: PeerOptions<Msg>) {
    this.options = options
    this.signer = options.signer
    this._remote = options.remote
    this.ready = new Promise<void>((resolve, reject) => {
      this.emitter.on("connect", () => resolve())
      this.emitter.on("error", reject)
      this.emitter.on("disconnect", () => reject(new Error("peer disconnected")))
    })
    // Keep `ready` "handled" so an unawaited connection failure never surfaces
    // as an unhandled rejection; a caller awaiting `ready` still sees the error.
    this.ready.catch(() => {})
    void this.start()
  }

  /** The remote peer's id; cryptographically proven when `authenticated` is true. */
  get remote(): PeerId {
    return this._remote
  }

  /** Name of the active transport, e.g. `"rtc"`. */
  get transportName(): string {
    return this._transportName
  }

  /** Last measured round-trip latency in milliseconds, or `undefined` before the first probe. */
  get latency(): number | undefined {
    return this._latency
  }

  /** Whether the connection is currently open. */
  get connected(): boolean {
    return this.open
  }

  on<E extends keyof PeerEvents<Msg>>(event: E, handler: PeerEvents<Msg>[E]): void {
    this.emitter.on(event, handler)
  }

  /** Send a direct application message to this peer. */
  async send(msg: Msg): Promise<void> {
    await this.sendFrame({ v: WIRE_VERSION, k: "msg", body: msg })
  }

  /** A typed RPC client for calling this peer's methods (README §2.2). */
  client<S extends APISchema>(api: API<S>): Client<S> {
    return createClient(this, api, { timeout: this.options.rpcTimeout ?? DEFAULT_RPC_TIMEOUT })
  }

  /**
   * Issue one RPC request and await the correlated response (satisfies
   * `RPCRequester`). Resolves to the raw {@link DispatchResult}, or an
   * {@link RPCError} on timeout, send failure, or disconnect.
   */
  async requestRPC(
    method: string,
    body: unknown,
    timeoutMs: number,
  ): Promise<DispatchResult | RPCError> {
    if (this.closed) return new RPCError("disconnected", method)
    const id = randomId(12)
    return new Promise<DispatchResult | RPCError>(resolve => {
      const timer = setTimeout(() => finish(new RPCError("timeout", method)), timeoutMs)
      const finish = (result: DispatchResult | RPCError) => {
        if (!this.pendingRPC.delete(id)) return
        clearTimeout(timer)
        resolve(result)
      }
      this.pendingRPC.set(id, { method, finish })
      void (async () => {
        await this.ready
        if (this.pendingRPC.has(id)) {
          await this.sendFrame({ v: WIRE_VERSION, k: "req", id, method, body })
        }
      })().catch(err =>
        finish(new RPCError(this.closed ? "disconnected" : "send_failed", method, String(err))),
      )
    })
  }

  /** Send a post-handshake frame through the common encryption boundary. */
  async sendFrame(frame: Frame): Promise<void> {
    if (this.closed) throw new Error("peer disconnected")
    if (frame.k === "hello" || frame.k === "ack" || frame.k === "sealed") {
      throw new Error("handshake and sealed frames are managed by Peer")
    }
    await this.ready
    if (this.closed) throw new Error("peer disconnected")
    const wire: Frame = this.options.encrypted
      ? { v: WIRE_VERSION, k: "sealed", body: encrypt(this.sharedKey!, FrameCodec.encode(frame)) }
      : frame
    await this.transport!.send(wire)
  }

  /** Whether the handshake verified the remote using a non-noop signer. */
  get authenticated(): boolean {
    return this.open && !!this.signer && !(this.signer instanceof NoopSigner)
  }

  /** Close the connection. */
  disconnect(): void {
    if (this.closed) return
    this.closed = true
    if (this.pingTimer) clearInterval(this.pingTimer)
    this.open = false
    this.sharedKey = undefined
    this.pendingPings.clear()
    for (const pending of this.pendingRPC.values()) {
      pending.finish(new RPCError("disconnected", pending.method))
    }
    this.transport?.disconnect()
    this.emitter.emit("disconnect")
  }

  private async start(): Promise<void> {
    try {
      await (this.signer as { ready?: Promise<void> } | undefined)?.ready
      this.self = this.signer?.id ?? this.options.self
      if (!this.self) throw new Error("Peer requires `self` or a `signer`")
      if (this.options.encrypted && !(this.signer instanceof ECDSASigner)) {
        throw new Error("`encrypted` requires an ECDSASigner")
      }

      if (this.closed) return
      this.transport = this.options.transport ?? (await this.buildRTCTransport())
      if (this.closed) {
        this.transport.disconnect()
        return
      }
      this._transportName = (this.transport as { name?: string }).name ?? "custom"

      this.transport.on(
        "connect",
        () => void this.onTransportConnect().catch(err => this.fail(err)),
      )
      this.transport.on("message", frame => {
        // Preserve wire order while asynchronous identity verification completes.
        this.incoming = this.incoming.then(() => this.onFrame(frame)).catch(err => this.fail(err))
      })
      this.transport.on("disconnect", () => this.disconnect())
      this.transport.on("error", err => this.fail(err))
    } catch (err) {
      this.fail(err)
    }
  }

  private fail(err: unknown): void {
    if (this.closed) return
    this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
    this.disconnect()
  }

  private async buildRTCTransport(): Promise<Transport<Frame>> {
    const { signalling } = this.options
    if (!signalling)
      throw new Error("Peer requires a `signalling` channel (or an injected transport)")
    const backend = await getRTC(this.options.backend as RTCBackendSource | undefined)
    return new RTCTransport<Frame>({
      self: this.self!,
      remote: this._remote,
      signalling,
      backend,
      iceServers: this.options.iceServers,
      // Deterministic initiator: the lexicographically-smaller id offers, avoiding glare.
      initiator: this.self! < this._remote,
    })
  }

  private async onTransportConnect(): Promise<void> {
    if (this.closed || this.helloSent) return
    this.helloSent = true
    const hello: HelloFrame = {
      v: WIRE_VERSION,
      k: "hello",
      from: this.self!,
      caps: this.options.caps ?? ["rtc"],
      nonce: this.selfNonce,
    }
    await this.transport!.send(hello)
  }

  private async onFrame(frame: Frame): Promise<void> {
    if (this.closed) return
    try {
      validateFrame(frame)
    } catch {
      return
    }
    if (frame.k === "hello" || frame.k === "ack") {
      if (frame.k === "hello") await this.onHello(frame)
      else if (!this.open) await this.onAck(frame)
      return
    }
    if (!this.open) return
    if (frame.k === "sealed") {
      if (!this.options.encrypted || !this.sharedKey) return
      try {
        frame = FrameCodec.decode(decrypt(this.sharedKey, frame.body))
      } catch {
        return
      }
      if (frame.k === "sealed" || frame.k === "hello" || frame.k === "ack") return
    } else if (this.options.encrypted) return
    switch (frame.k) {
      case "msg":
        this.onMessage(frame)
        return
      case "req":
        void this.onReq(frame).catch(err => this.fail(err))
        return
      case "res":
        this.onRes(frame)
        return
      case "ping":
        void this.sendFrame({ v: WIRE_VERSION, k: "pong", id: frame.id }).catch(err =>
          this.fail(err),
        )
        return
      case "pong": {
        const sent = this.pendingPings.get(frame.id)
        if (sent !== undefined) {
          this._latency = Date.now() - sent
          this.pendingPings.delete(frame.id)
        }
        return
      }
      default:
        // Mesh (bcast/sub/pub/gossip) and RPC (req/res) frames — consumed upstream.
        this.emitter.emit("frame", frame)
    }
  }

  private async onHello(frame: HelloFrame): Promise<void> {
    if (this.helloReceived) return
    this.helloReceived = true
    const sig = this.signer ? await this.signer.sign(frame.nonce) : ""
    if (this.closed) return
    const ack: AckFrame = { v: WIRE_VERSION, k: "ack", from: this.self!, nonce: frame.nonce, sig }
    await this.transport!.send(ack)
    this.ackSent = true
    this.markOpen()
  }

  private async onAck(frame: AckFrame): Promise<void> {
    if (frame.nonce !== this.selfNonce) return // not a reply to our challenge
    const signer = this.signer ?? new NoopSigner(this._remote)
    const proven = await signer.verify(frame.sig ?? "", this.selfNonce, frame.from)
    if (this.closed) return
    if (!proven) {
      this.emitter.emit("error", new Error(`identity proof failed for ${frame.from}`))
      this.disconnect()
      return
    }
    if (frame.from.toLowerCase() !== this._remote.toLowerCase()) {
      this.emitter.emit(
        "error",
        new Error(`peer claims id ${frame.from}, expected ${this._remote}`),
      )
      this.disconnect()
      return
    }
    this._remote = frame.from

    if (this.options.encrypted && this.signer instanceof ECDSASigner && frame.sig) {
      const remotePub = ECDSASigner.recoverPublicKey(frame.sig, this.selfNonce)
      this.sharedKey = this.signer.sharedKeyWith(remotePub)
    }

    this.identityVerified = true
    this.markOpen()
  }

  private markOpen(): void {
    if (this.closed || this.open || !this.ackSent || !this.identityVerified) return
    this.open = true
    this.emitter.emit("connect")
    if (!this.closed) this.startPings()
  }

  private async onReq(frame: ReqFrame): Promise<void> {
    const router = this.options.router
    const ctx: RPCContext = { from: this._remote }
    const result: DispatchResult = router
      ? await router.dispatch(frame.method, frame.body, ctx)
      : { ok: false, err: { code: "no_handler", method: frame.method } }
    const res: ResFrame = result.ok
      ? { v: WIRE_VERSION, k: "res", id: frame.id, ok: true, body: result.body }
      : { v: WIRE_VERSION, k: "res", id: frame.id, ok: false, err: result.err }
    if (!this.closed) await this.sendFrame(res)
  }

  private onRes(frame: ResFrame): void {
    const pending = this.pendingRPC.get(frame.id)
    if (!pending) return
    pending.finish(
      frame.ok
        ? { ok: true, body: frame.body }
        : { ok: false, err: frame.err ?? { code: "unknown", method: "" } },
    )
  }

  private onMessage(frame: MsgFrame): void {
    this.emitter.emit("message", frame.body as Msg)
  }

  private startPings(): void {
    const ping = () => {
      const id = randomId(6)
      this.pendingPings.set(id, Date.now())
      void this.sendFrame({ v: WIRE_VERSION, k: "ping", id }).catch(err => this.fail(err))
    }
    ping()
    this.pingTimer = setInterval(ping, PING_INTERVAL_MS)
    if (typeof this.pingTimer === "object" && "unref" in this.pingTimer) {
      ;(this.pingTimer as { unref: () => void }).unref()
    }
  }
}

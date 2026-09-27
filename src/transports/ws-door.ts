import type { PeerId } from "../utils/types.js"
import type { Transport, TransportEvents } from "./types.js"
import { Emitter } from "../utils/emitter.js"
import type { Frame, WelcomeFrame } from "../wire/index.js"
import { WIRE_VERSION, FrameCodec } from "../wire/index.js"

/**
 * The WebSocket subset we rely on. The native constructor (browser, Node 22+,
 * Bun, Deno) and the `ws` package both satisfy it, so one transport serves a
 * browser client and a Node door alike.
 */
export interface DoorSocket {
  send(data: string): void
  close(): void
  readyState: number
  readonly bufferedAmount: number
  onopen: ((ev: unknown) => void) | null
  onmessage: ((ev: { data: unknown }) => void) | null
  onclose: ((ev: unknown) => void) | null
  onerror: ((ev: unknown) => void) | null
  /** Present on `ws` sockets, absent in browsers; enables the keepalive probe. */
  ping?(data?: unknown): void
}

type DoorSocketCtor = new (url: string) => DoorSocket

/** `ws` reports its pong outside the `on*` property convention, so it is injected. */
export type PongRegistrar = (handler: () => void) => void

export interface WSTransportOptions {
  /**
   * Dial this door URL. Omit when `socket` wraps an already-accepted
   * connection (the door side).
   */
  url?: string
  /** An already-accepted socket to wrap instead of dialing. */
  socket?: DoorSocket
  /** Override the WebSocket constructor (defaults to native, then the `ws` package). */
  WebSocket?: DoorSocketCtor
  /** Register the WS-pong listener (see {@link DoorSocket.ping}). */
  onPong?: PongRegistrar
  /** Our own id, sent in the welcome exchange. Both ends send one. */
  self: PeerId
  /** Capabilities advertised in the welcome exchange. Default `["ws"]`. */
  caps?: string[]
  /** Keepalive probe interval in ms; `0` disables. Default 15000. */
  keepAliveMs?: number
  /** Silence window after which the link is treated as dead. Default 10000. */
  keepAliveTimeoutMs?: number
  /** How long to wait for the counterpart's welcome. Default 10000. */
  identifyTimeoutMs?: number
  /** Bound on frames buffered before a consumer subscribes. Default 256. */
  maxBuffered?: number
}

const OPEN = 1

const DEFAULT_KEEPALIVE_MS = 15_000
const DEFAULT_KEEPALIVE_TIMEOUT_MS = 10_000
const DEFAULT_IDENTIFY_TIMEOUT_MS = 10_000
const DEFAULT_MAX_BUFFERED = 256
const DEFAULT_CAPS = ["ws"]

/**
 * The `ws` fallback, for runtimes with no native WebSocket. The specifier is
 * indirected so a bundler cannot resolve it: resolving it would drag Node's
 * http/crypto stack into a browser graph that can never reach this line (a
 * browser always has a native WebSocket, which is tried first).
 */
async function loadWs(): Promise<DoorSocketCtor> {
  const spec = "ws"
  return ((await import(/* @vite-ignore */ spec)).default as unknown) as DoorSocketCtor
}

/** First door reconnect delay in ms; doubles each attempt. */
export const DOOR_MIN_BACKOFF_MS = 500
/** Cap on the door reconnect delay in ms. */
export const DOOR_MAX_BACKOFF_MS = 15_000

/**
 * What `P2PKit` lets a caller tune about the door it attaches to. `self` is
 * filled in from the kit; reconnect backoff is the kit's job, not the
 * transport's, so it is an option here rather than on {@link WSTransport}.
 */
export interface DoorOptions {
  /** Override the WebSocket constructor (defaults to native, then the `ws` package). */
  WebSocket?: DoorSocketCtor
  /** Capabilities advertised in the welcome exchange. Default `["ws"]`. */
  caps?: string[]
  /** Keepalive probe interval in ms; `0` disables. Default 15000. */
  keepAliveMs?: number
  /** Silence window after which the link is treated as dead. Default 10000. */
  keepAliveTimeoutMs?: number
  /** How long to wait for the door's welcome. Default 10000. */
  identifyTimeoutMs?: number
  /** First reconnect delay in ms; doubles each attempt. Default 500. */
  minBackoffMs?: number
  /** Cap on the reconnect delay in ms. Default 15000. */
  maxBackoffMs?: number
}

/**
 * A peer link that *is* a WebSocket connection. This is what makes a door server
 * a peer rather than a relay: the frames crossing it are the standard mesh
 * {@link Frame} set, both ways, and the link sits in `peers` like any other.
 *
 * Each end sends a `welcome` frame naming itself as soon as the socket opens,
 * and waits for the counterpart's, so both sides know who they are talking to
 * before the hello/ack identity handshake runs. Frames that turn up in between
 * are buffered and replayed to the first `message` subscriber, preserving wire
 * order — the same construction gap `Peer` already bridges for WebRTC.
 *
 * Reconnecting is not this transport's job: a dead socket is reported through
 * `disconnect` and the caller decides what to do about it (the kit's door
 * attach retries with backoff).
 */
export class WSTransport implements Transport<Frame> {
  readonly name = "ws"

  /**
   * Resolves with the counterpart's id once its `welcome` arrives, and rejects
   * if the socket dies first. The door side needs this before it can key the
   * link; {@link WSTransport.open} awaits it so callers get a usable transport.
   */
  readonly identified: Promise<PeerId>

  private readonly emitter = new Emitter<TransportEvents<Frame>>()
  private readonly options: WSTransportOptions
  private socket?: DoorSocket
  private _remote?: PeerId
  private open = false
  private closed = false
  private welcomed = false
  /** Frames that arrived before a `message` handler registered. */
  private readonly buffered: Frame[] = []
  private hasConsumer = false
  private keepAliveTimer?: ReturnType<typeof setInterval>
  private keepAliveWatchdog?: ReturnType<typeof setTimeout>
  private identifyTimer?: ReturnType<typeof setTimeout>
  private readonly resolveIdentified: (remote: PeerId) => void
  private readonly rejectIdentified: (err: Error) => void

  constructor(options: WSTransportOptions) {
    this.options = options
    let resolve!: (remote: PeerId) => void
    let reject!: (err: Error) => void
    this.identified = new Promise<PeerId>((res, rej) => {
      resolve = res
      reject = rej
    })
    // A door that never identifies itself must not surface as an unhandled
    // rejection; `open()` is what awaits this.
    this.identified.catch(() => {})
    this.resolveIdentified = resolve
    this.rejectIdentified = reject
    this.identifyTimer = setTimeout(() => {
      this.identifyTimer = undefined
      if (!this.welcomed) this.fail(new Error("door link never identified itself"))
    }, options.identifyTimeoutMs ?? DEFAULT_IDENTIFY_TIMEOUT_MS)
    this.unref(this.identifyTimer)

    if (options.socket) this.attach(options.socket)
    else void this.dial()
  }

  /** The peer on the other end. Unavailable before its `welcome` arrived. */
  get remote(): PeerId {
    if (this._remote === undefined) throw new Error("door link has not identified itself yet")
    return this._remote
  }

  /** The counterpart's id, or `undefined` before its `welcome` arrived. */
  get identifiedPeer(): PeerId | undefined {
    return this._remote
  }

  get bufferedAmount(): number {
    return this.socket?.bufferedAmount ?? 0
  }

  /** True once the welcome exchange completed and the socket is still up. */
  get connected(): boolean {
    return this.open && !this.closed
  }

  /**
   * Build a transport that is already identified — the shape a kit needs before
   * it can key a peer. Rejects if the door never answers.
   */
  static async open(options: WSTransportOptions): Promise<WSTransport> {
    const transport = new WSTransport(options)
    await transport.identified
    return transport
  }

  on<E extends keyof TransportEvents<Frame>>(event: E, handler: TransportEvents<Frame>[E]): void {
    this.emitter.on(event, handler)
    // A consumer attaches after the transport has already done its work — `Peer`
    // resolves a signer, builds its state, and only then subscribes. The events
    // it missed are replayed, or it would wait for a connect that already fired.
    if (event === "message") {
      this.hasConsumer = true
      // Anything that arrived between identification and subscription is
      // delivered now, in wire order.
      for (const frame of this.buffered.splice(0)) this.emitter.emit("message", frame)
    } else if (event === "connect" && this.connected) {
      queueMicrotask(() => (handler as () => void)())
    } else if (event === "disconnect" && this.closed) {
      queueMicrotask(() => (handler as () => void)())
    }
  }

  async send(frame: Frame): Promise<void> {
    const socket = this.socket
    if (this.closed || !this.open || !socket || socket.readyState !== OPEN) {
      throw new Error("door link is not open")
    }
    socket.send(FrameCodec.encode(frame))
  }

  /** Close the socket. Idempotent. */
  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.stopTimers()
    const socket = this.socket
    this.socket = undefined
    try {
      socket?.close()
    } catch {
      /* already gone */
    }
    if (this.open) {
      this.open = false
      this.emitter.emit("disconnect")
    } else {
      this.rejectIdentified(new Error("door link closed before it opened"))
    }
    this.emitter.removeAll()
  }

  // ---- wiring -----------------------------------------------------------

  private async dial(): Promise<void> {
    const WS: DoorSocketCtor =
      this.options.WebSocket ??
      (globalThis as { WebSocket?: DoorSocketCtor }).WebSocket ??
      (await loadWs())
    if (this.closed) return
    try {
      this.attach(new WS(this.options.url!))
    } catch (err) {
      this.fail(err instanceof Error ? err : new Error(String(err)))
    }
  }

  /** Take ownership of a socket; dialing and accepting meet here. */
  private attach(socket: DoorSocket): void {
    if (this.closed) {
      try {
        socket.close()
      } catch {
        /* already gone */
      }
      return
    }
    this.socket = socket

    let stale = false
    const mine = () => !stale && !this.closed && this.socket === socket
    const goDown = (err?: unknown) => {
      if (stale || this.closed || this.socket !== socket) return
      stale = true
      if (err !== undefined) {
        this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
      }
      this.fail(new Error("door socket closed"))
    }

    socket.onopen = () => {
      if (!mine()) return
      this.open = true
      this.sendWelcome()
      this.startKeepalive()
    }
    socket.onmessage = ({ data }) => {
      if (!mine()) return
      this.noteActivity()
      const raw = typeof data === "string" ? data : String(data)
      let frame: Frame
      try {
        frame = FrameCodec.decode(raw)
      } catch {
        return // unsupported version or malformed: dropped, as everywhere else
      }
      this.onFrame(frame)
    }
    socket.onerror = err => goDown(err)
    socket.onclose = () => goDown()
    this.options.onPong?.(() => {
      if (mine()) this.noteActivity()
    })

    // An accepted socket is already open by the time it is handed over.
    if (socket.readyState === OPEN && !this.open) {
      this.open = true
      this.sendWelcome()
      this.startKeepalive()
    }
  }

  private sendWelcome(): void {
    const frame: WelcomeFrame = {
      v: WIRE_VERSION,
      k: "welcome",
      from: this.options.self,
      caps: this.options.caps ?? DEFAULT_CAPS,
    }
    try {
      this.socket?.send(FrameCodec.encode(frame))
    } catch (err) {
      this.fail(err instanceof Error ? err : new Error(String(err)))
    }
  }

  private onFrame(frame: Frame): void {
    if (frame.k === "welcome") {
      if (this.welcomed) return
      this.welcomed = true
      if (this.identifyTimer !== undefined) {
        clearTimeout(this.identifyTimer)
        this.identifyTimer = undefined
      }
      this._remote = frame.from
      this.resolveIdentified(frame.from)
      this.emitter.emit("connect")
      return
    }
    // A frame from an unidentified counterpart cannot be attributed; drop it.
    if (!this.welcomed) return
    if (!this.hasConsumer) {
      const max = this.options.maxBuffered ?? DEFAULT_MAX_BUFFERED
      if (this.buffered.length >= max) this.buffered.shift()
      this.buffered.push(frame)
      return
    }
    this.emitter.emit("message", frame)
  }

  // ---- keepalive --------------------------------------------------------
  // A socket whose counterpart vanished without a close frame stays half open
  // forever; a WS-level ping is the only probe that crosses it. Sockets without
  // `ping()` (browsers) cannot probe, and there they lean on the mesh's own 5s
  // ping/pong traffic to notice a dead link.

  private startKeepalive(): void {
    const interval = this.options.keepAliveMs ?? DEFAULT_KEEPALIVE_MS
    if (interval <= 0 || typeof this.socket?.ping !== "function") return
    this.keepAliveTimer = setInterval(() => {
      if (!this.closed) this.probe()
    }, interval)
    this.unref(this.keepAliveTimer)
    this.probe()
  }

  private probe(): void {
    try {
      this.socket?.ping?.()
    } catch {
      return
    }
    const timeout = this.options.keepAliveTimeoutMs ?? DEFAULT_KEEPALIVE_TIMEOUT_MS
    if (this.keepAliveWatchdog !== undefined) clearTimeout(this.keepAliveWatchdog)
    this.keepAliveWatchdog = setTimeout(() => {
      if (this.open && !this.closed) this.fail(new Error("door link went quiet"))
    }, timeout)
    this.unref(this.keepAliveWatchdog)
  }

  private noteActivity(): void {
    if (this.keepAliveWatchdog !== undefined) {
      clearTimeout(this.keepAliveWatchdog)
      this.keepAliveWatchdog = undefined
    }
  }

  private stopTimers(): void {
    if (this.keepAliveTimer !== undefined) {
      clearInterval(this.keepAliveTimer)
      this.keepAliveTimer = undefined
    }
    if (this.keepAliveWatchdog !== undefined) {
      clearTimeout(this.keepAliveWatchdog)
      this.keepAliveWatchdog = undefined
    }
    if (this.identifyTimer !== undefined) {
      clearTimeout(this.identifyTimer)
      this.identifyTimer = undefined
    }
  }

  /** Report a failure, then tear the link down. The rejection wins over the
   * generic one `disconnect` would raise, so a caller awaiting `identified`
   * learns what actually went wrong. */
  private fail(err: Error): void {
    this.rejectIdentified(err)
    this.disconnect()
  }

  private unref(timer: unknown): void {
    if (typeof timer === "object" && timer !== null && "unref" in timer) {
      ;(timer as { unref: () => void }).unref()
    }
  }
}

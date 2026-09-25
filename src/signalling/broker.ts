import type { PeerId } from "../utils/types.js"
import type { Frame, SigRelayFrame } from "../wire/index.js"
import { WIRE_VERSION } from "../wire/index.js"
import { randomId } from "../utils/id.js"
import { SeenCache } from "../core/envelope.js"
import type { SignallingChannel, SignallingMessage } from "./types.js"

/**
 * The slice of `P2PKit` a {@link SignalBroker} drives: who we are linked to and
 * how to push a frame onto one of those links. A link is the only thing that can
 * carry a relay, so `peerIds` doubles as the set of usable carriers.
 */
export interface SignalBrokerHost {
  /** This node's id. */
  readonly self: PeerId
  /** Ids of the peers we know about, links still being dialed included. */
  peerIds(): PeerId[]
  /** Ids of the peers a frame can be pushed onto right now. */
  linkedPeers(): PeerId[]
  /** Send one frame to a directly-connected peer (a no-op if it has dropped). */
  sendTo(peer: PeerId, frame: Frame): void
  /** Subscribe to a peer connecting; relays held for that peer flush on this. */
  onPeerConnected(handler: (peer: PeerId) => void): () => void
}

export interface SignalBrokerOptions {
  /**
   * How long (ms) a relay may sit waiting for a link to its destination before
   * it is dropped. Default 15000.
   */
  relayTimeoutMs?: number
  /** Maximum relays held at once, so a hostile peer cannot grow the queue. Default 128. */
  maxPending?: number
}

const DEFAULT_RELAY_TIMEOUT_MS = 15_000
const DEFAULT_MAX_PENDING = 128
/** Signals held for a link whose transport has not registered yet. */
const MAX_QUEUED_PER_PEER = 64
/**
 * A relayed SDP is a few kilobytes; this caps a carried envelope well above any
 * real one while keeping a single relay a handful of chunks.
 */
const MAX_SIGNAL_CHARS = 64_000

/** Signals for one remote: the handler its transport registered, plus a queue. */
interface RemoteChannel {
  handlers: Set<(message: SignallingMessage) => void>
  queued: SignallingMessage[]
}

/**
 * {@link SignallingChannel} that keeps working once the lobby is gone (README
 * §4). While the lobby is up it is a pure pass-through, so brokered signalling
 * cannot disturb a normal handshake. Once it is down, an outbound signal
 * addressed to a peer we are not linked to is wrapped in a `sig-relay` frame and
 * handed to one connected peer to carry; inbound relays are unwrapped and
 * delivered as if the lobby had sent them.
 *
 * The broker is a postman: the carried envelope is the lobby's, byte for byte,
 * so the no-TURN validation in `RTCTransport` is untouched. Only direct
 * neighbours carry, and a relay is never forwarded to more than one peer.
 *
 * Each link gets its own {@link channelFor} view, so a signal only reaches the
 * transport it is addressed to — and one that arrives just before that
 * transport registers is queued rather than dropped.
 */
export class SignalBroker implements SignallingChannel {
  private readonly upstream: SignallingChannel
  private readonly host: SignalBrokerHost
  private readonly kitHandlers = new Set<(message: SignallingMessage) => void>()
  private readonly remotes = new Map<PeerId, RemoteChannel>()
  private readonly pending = new Map<
    string,
    { frame: SigRelayFrame; timer: ReturnType<typeof setTimeout> }
  >()
  private readonly seen: SeenCache
  /**
   * The neighbour each remote's signals last arrived from. `pickCarrier` sends
   * a signal back that way, which is the only routing this broker knows — and
   * the only one it needs, since a relay travels one hop at a time.
   */
  private readonly routes = new Map<PeerId, PeerId>()
  private readonly relayTimeoutMs: number
  private readonly maxPending: number
  private offConnected?: () => void
  private lobbyUp = true
  private lobbySettled = false
  private readonly settleReady: () => void

  /**
   * Resolves once the lobby has answered, or as soon as we learn it never
   * will. A lobby that refuses us must not hang `start()`, because the mesh is
   * still reachable through peers learned another way — and an upstream whose
   * `ready` only ever retries (a reconnecting channel, say) reports "never"
   * through {@link markLobbyDown}, not through a rejected promise.
   */
  readonly ready: Promise<void>

  constructor(
    upstream: SignallingChannel,
    host: SignalBrokerHost,
    options: SignalBrokerOptions = {},
  ) {
    this.upstream = upstream
    this.host = host
    this.relayTimeoutMs = options.relayTimeoutMs ?? DEFAULT_RELAY_TIMEOUT_MS
    this.maxPending = options.maxPending ?? DEFAULT_MAX_PENDING
    this.seen = new SeenCache(this.relayTimeoutMs * 2)
    // Registered before `ready` is derived, so the flag is already down by the
    // time a caller resumed from `await ready` gets to send anything.
    upstream.ready.catch(() => {
      this.lobbySettled = true
      this.lobbyUp = false
    })
    let settle!: () => void
    this.ready = new Promise<void>(resolve => {
      settle = resolve
    }).then(() => {
      this.lobbySettled = true
    })
    this.settleReady = settle
    upstream.ready.then(() => settle(), () => settle())
    upstream.onMessage(message => this.deliver(message))
    this.offConnected = host.onPeerConnected(peer => this.flushPending(peer))
  }

  /**
   * A {@link SignallingChannel} carrying just this one link's traffic, for
   * handing to a `Peer`. Signals from any other peer never reach it.
   */
  channelFor(remote: PeerId): SignallingChannel {
    const entry = this.remember(remote)
    return {
      ready: this.ready,
      send: message => this.send(message),
      onMessage: handler => {
        entry.handlers.add(handler)
        // Anything that arrived between the slot opening and the transport
        // registering is delivered now, in order.
        for (const message of entry.queued.splice(0)) handler(message)
        return () => {
          entry.handlers.delete(handler)
        }
      },
    }
  }

  /** Stop relaying through the lobby and use peer links instead. */
  markLobbyDown(): void {
    this.lobbyUp = false
    // An upstream that will never answer must not hold `ready` open.
    this.settleReady()
  }

  /** Whether the lobby is still believed reachable. */
  get lobbyAlive(): boolean {
    return this.lobbyUp
  }

  /** The signalling channel is reachable again; go back to passing through. */
  markLobbyUp(): void {
    this.lobbyUp = true
  }

  send(message: SignallingMessage): void {
    // Before the lobby has answered, let it buffer the send as it would without
    // a broker; afterwards we know which path to take.
    if (!this.lobbySettled || this.lobbyUp) {
      this.upstream.send(message)
      return
    }
    // An announce is addressed to the whole room, so there is nobody to carry
    // it; discovery is what supplies peers once the lobby is gone.
    if (!("to" in message) || message.to === undefined) return
    if (JSON.stringify(message).length > MAX_SIGNAL_CHARS) return
    const carrier = this.pickCarrier(message.to)
    if (!carrier) return
    this.host.sendTo(carrier, {
      v: WIRE_VERSION,
      k: "sig-relay",
      id: randomId(12),
      ttl: 1,
      from: this.host.self,
      to: message.to,
      signal: message,
    })
  }

  /** Kit-level handler: everything, announces included. */
  onMessage(handler: (message: SignallingMessage) => void): () => void {
    this.kitHandlers.add(handler)
    return () => {
      this.kitHandlers.delete(handler)
    }
  }

  /**
   * A `sig-relay` frame arrived over a link: deliver it, or carry it one hop.
   * `via` is the peer the frame arrived from, when the host knows it — a relay
   * is sent back along the path it came in on, since that peer could reach the
   * destination a moment ago and the rest of our neighbours may not be able to.
   */
  ingest(frame: SigRelayFrame, via?: PeerId): void {
    if (frame.from === this.host.self) return
    if (this.seen.seen(frame.id)) return
    if (frame.to === this.host.self) {
      // Remember the link this peer's signals arrive on: the answer has to go
      // back the same way, and whoever carried it here can reach them.
      if (via !== undefined) this.routes.set(frame.from, via)
      this.deliver(frame.signal)
      return
    }
    // One forward hop, and only straight to the destination: never flooded to
    // the rest of our neighbours, and never carried past `ttl` 0.
    if (frame.ttl < 1) return
    this.relay({ ...frame, ttl: frame.ttl - 1 }, via)
  }

  /** Release timers and per-link state; the host is going away. */
  stop(): void {
    for (const { timer } of this.pending.values()) clearTimeout(timer)
    this.pending.clear()
    this.remotes.clear()
    this.routes.clear()
    this.offConnected?.()
    this.offConnected = undefined
  }

  /** Relays currently awaiting a link to their destination. */
  get pendingCount(): number {
    return this.pending.size
  }

  private remember(remote: PeerId): RemoteChannel {
    let entry = this.remotes.get(remote)
    if (!entry) this.remotes.set(remote, (entry = { handlers: new Set(), queued: [] }))
    return entry
  }

  private deliver(message: SignallingMessage): void {
    for (const handler of [...this.kitHandlers]) handler(message)
    // Announces are room-scoped, not addressed to one link.
    if ("announce" in message) return
    const entry = this.remember(message.from)
    if (entry.handlers.size === 0) {
      if (entry.queued.length < MAX_QUEUED_PER_PEER) entry.queued.push(message)
      return
    }
    for (const handler of [...entry.handlers]) handler(message)
  }

  /**
   * Hand a relay onward: straight to the destination if we are linked to it,
   * else back along the link it arrived on, else held briefly in case such a
   * link comes up — the peer we are relaying to may be dialing too.
   */
  private relay(frame: SigRelayFrame, via?: PeerId): void {
    const linked = this.host.linkedPeers()
    if (linked.includes(frame.to)) {
      this.host.sendTo(frame.to, frame)
      return
    }
    if (via !== undefined && linked.includes(via)) {
      this.host.sendTo(via, frame)
      return
    }
    if (this.pending.size >= this.maxPending) {
      const oldest = this.pending.keys().next().value
      if (oldest === undefined) return
      this.drop(oldest)
    }
    const timer = setTimeout(() => this.drop(frame.id), this.relayTimeoutMs)
    if (typeof timer === "object" && "unref" in timer) (timer as { unref: () => void }).unref()
    this.pending.set(frame.id, { frame, timer })
  }

  private flushPending(peer: PeerId): void {
    for (const [id, entry] of [...this.pending]) {
      if (entry.frame.to !== peer) continue
      this.pending.delete(id)
      clearTimeout(entry.timer)
      this.host.sendTo(peer, entry.frame)
    }
  }

  private drop(id: string): void {
    const entry = this.pending.get(id)
    if (!entry) return
    clearTimeout(entry.timer)
    this.pending.delete(id)
  }

  /**
   * The neighbour to hand a signal for `to` through: the peer itself if we are
   * linked to it, else the link its last signal arrived on (a reply goes back
   * the way the request came), else whichever peer we are linked to.
   */
  private pickCarrier(to: PeerId): PeerId | undefined {
    const linked = this.host.linkedPeers()
    if (linked.includes(to)) return to
    const known = this.routes.get(to)
    if (known !== undefined && linked.includes(known)) return known
    return linked[0]
  }
}

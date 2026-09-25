import type { PeerId } from "../utils/types.js"
import { NoopSigner, type Signer } from "../auth/signer.js"
import type { SignallingChannel, SignallingMessage } from "../signalling/types.js"
import { SignalBroker, type SignalBrokerHost, type SignalBrokerOptions } from "../signalling/broker.js"
import { WebSocketSignalling, type WebSocketSignallingOptions } from "../signalling/websocket.js"
import type { SigRelayFrame } from "../wire/index.js"
import type { RTCBackend, RTCBackendSource } from "../backends/index.js"
import type { Transport } from "../transports/types.js"
import type { Frame, BcastFrame, SubFrame, PubFrame, GossipFrame } from "../wire/index.js"
import { WIRE_VERSION } from "../wire/index.js"
import type { Discovery, DiscoveryHost } from "../discovery/types.js"
import { Emitter } from "../utils/emitter.js"
import { randomId } from "../utils/id.js"
import type { Router } from "../rpc/router.js"
import type { API } from "../rpc/api.js"
import type { APISchema } from "../rpc/schema.js"
import type { AnyProtocol } from "../rpc/protocol.js"
import { Peer } from "./Peer.js"
import { Topic, type TopicOptions, type TopicHost } from "./topic.js"
import { SeenCache, broadcastSignPayload, pubSignPayload } from "./envelope.js"

export interface BroadcastOptions {
  /** Hop limit for a flooded broadcast. Default 7. */
  ttl?: number
  /** How long (ms) a message id is remembered for dedup. Default 30000. */
  dedupWindow?: number
}

/**
 * How to reach the other peers before a link exists. Only the lobby is built in;
 * anything else is a {@link SignallingChannel} you supply yourself.
 */
export type BootstrapSource = { kind: "lobby"; url: string }

export interface P2PKitOptions {
  /** This node's id. Derived from `signer` when given; required otherwise. */
  self?: PeerId
  /** Signalling channel peers use to find and connect to each other. */
  signalling?: SignallingChannel
  /**
   * Bootstrap without building a signalling channel yourself (README §2). Give
   * this **or** `signalling`. Currently only the WebSocket lobby is built in.
   */
  bootstrap?: BootstrapSource
  /** Identity signer; enables verified handshakes, encryption and signed broadcasts. */
  signer?: Signer
  backend?: RTCBackend | RTCBackendSource
  iceServers?: RTCIceServer[]
  /** Cap on direct connections; excess reachability is maintained by relaying. */
  maxPeers?: number
  /** Encrypt all post-handshake frames on each direct connection (requires an `ECDSASigner`). */
  encrypted?: boolean
  /** Sign every broadcast so relays can't spoof the origin (requires a signer). */
  signedBroadcasts?: boolean
  broadcast?: BroadcastOptions
  caps?: string[]
  /** The API this node serves (README §2.1); its schema is reused for validation. */
  api?: API<APISchema>
  /** Handlers for incoming RPC calls, from `api.router({...})` (README §2.1). */
  router?: Router
  /** Type + validate one-way messages (README §2.3); invalid inbound messages are dropped. */
  protocol?: AnyProtocol
  /** Per-call RPC timeout in ms. Default 30000. */
  rpcTimeout?: number
  /** Discovery channels to grow/heal the mesh beyond the signalling room (README §4). */
  discovery?: Discovery | Discovery[]
  /**
   * Carry handshake signals over existing links when the signalling channel is
   * unreachable (README §4). Default `false`; with it off, a dead lobby means no
   * new connections.
   */
  brokeredSignalling?: boolean
  /** Options for the lobby built by `bootstrap`. */
  signallingOptions?: WebSocketSignallingOptions
  /** Options for the peer-brokered relay path enabled by `brokeredSignalling`. */
  brokerOptions?: SignalBrokerOptions
  /**
   * Advanced: supply a transport for a peer instead of the default WebRTC one.
   * Return `undefined` to fall back to WebRTC. Enables custom link types and
   * deterministic testing.
   */
  createTransport?: (info: {
    self: PeerId
    remote: PeerId
    initiator: boolean
  }) => Transport<Frame> | undefined
}

/** Authorship is distinct from the immediate connection that delivered a message. */
export interface MessageMetadata {
  /** Claimed original sender; trust for authorization only when originVerified is true. */
  readonly origin: PeerId
  /** Immediate peer that delivered the message. */
  readonly via: PeerId
  /** Origin verified using the configured signer (handshake for direct messages). */
  readonly originVerified: boolean
}

/** Events emitted by {@link P2PKit}. */
export type P2PKitEvents<Msg> = {
  /** A new peer joined; attach handlers before it connects. */
  peer: (peer: Peer<Msg>) => void
  /** A direct or broadcast message, delivering peer, and original-sender metadata. */
  message: (msg: Msg, peer: Peer<Msg>, metadata: MessageMetadata) => void
  error: (err: Error) => void
}

const DEFAULT_TTL = 7
const DEFAULT_DEDUP_WINDOW = 30_000
const BROADCAST_FRESHNESS_MS = 60_000
const REPLAY_WINDOW = 1024

/**
 * Connects you to a mesh of peers and hands you each one as it joins,
 * using WebRTC or an injected transport (README §1). Provides
 * whole-mesh {@link P2PKit.broadcast} flooding and subscription-scoped
 * {@link P2PKit.topic} pub/sub.
 */
export class P2PKit<Msg = unknown> implements TopicHost, DiscoveryHost, SignalBrokerHost {
  /** Directly-connected peers, keyed by id. */
  readonly peers = new Map<PeerId, Peer<Msg>>()

  private readonly options: P2PKitOptions
  private readonly signer: Signer | undefined
  private readonly signalling: SignallingChannel
  private readonly broker?: SignalBroker
  private readonly lobby?: WebSocketSignalling
  private readonly emitter = new Emitter<P2PKitEvents<Msg>>()
  private readonly ttl: number

  // Discovery channels + their event fan-out (peer connected / inbound gossip).
  private readonly discoveries: Discovery[]
  private readonly discoveryEvents = new Emitter<{
    peerConnected: (peer: PeerId) => void
    gossip: (from: PeerId, peers: PeerId[]) => void
  }>()

  private _self?: PeerId
  private started = false

  // Broadcast dedup + signed-broadcast replay tracking.
  private readonly bcastSeen: SeenCache
  private readonly bcastNonces: SeenCache

  // Topic state.
  private readonly topics = new Map<string, Topic<unknown>>()
  private readonly subGossipSeen: SeenCache
  private readonly pubDedup: SeenCache
  private readonly topicSubs = new Map<string, Map<PeerId, number>>()
  private readonly pubSeq = new Map<string, number>()
  private readonly pubReceived = new Map<string, Set<number>>()
  private readonly pubHighest = new Map<string, number>()

  constructor(options: P2PKitOptions) {
    this.options = options
    this.signer = options.signer
    // Assigned below, before the lobby's `onDown` can possibly fire; `this` is
    // safe as a broker host because every field it reads is initialised ahead
    // of this constructor body.
    let broker: SignalBroker | undefined
    let lobby: WebSocketSignalling | undefined
    const source = options.bootstrap
    if (!options.signalling && source) {
      if (source.kind !== "lobby") throw new Error(`unsupported bootstrap kind: ${source.kind}`)
      lobby = new WebSocketSignalling(source.url, {
        ...options.signallingOptions,
        onDown: () => broker?.markLobbyDown(),
      })
      this.lobby = lobby
    }
    const upstream = options.signalling ?? lobby
    if (!upstream) throw new Error("P2PKit requires `signalling` or a `bootstrap` source")
    if (options.brokeredSignalling) {
      broker = new SignalBroker(upstream, this, options.brokerOptions)
      this.broker = broker
      this.signalling = broker
    } else {
      this.signalling = upstream
    }
    this.ttl = options.broadcast?.ttl ?? DEFAULT_TTL
    const dedupWindow = options.broadcast?.dedupWindow ?? DEFAULT_DEDUP_WINDOW
    this.bcastSeen = new SeenCache(dedupWindow)
    this.bcastNonces = new SeenCache(Math.max(dedupWindow, BROADCAST_FRESHNESS_MS))
    this.subGossipSeen = new SeenCache(dedupWindow)
    this.pubDedup = new SeenCache(dedupWindow)
    const d = options.discovery
    this.discoveries = d === undefined ? [] : Array.isArray(d) ? d : [d]
  }

  get selfId(): PeerId {
    if (!this._self) throw new Error("P2PKit not started — call start() first")
    return this._self
  }

  /** This node's id (satisfies {@link DiscoveryHost}); alias of {@link selfId}. */
  get self(): PeerId {
    return this.selfId
  }

  on<E extends keyof P2PKitEvents<Msg>>(event: E, handler: P2PKitEvents<Msg>[E]): void {
    this.emitter.on(event, handler)
  }

  /** Join the mesh: peers start connecting. */
  async start(): Promise<void> {
    if (this.started) return
    this.started = true
    await (this.signer as { ready?: Promise<void> } | undefined)?.ready
    this._self = this.signer?.id ?? this.options.self
    if (!this._self) throw new Error("P2PKit requires `self` or a `signer`")
    if (this.options.signedBroadcasts && (!this.signer || this.signer instanceof NoopSigner)) {
      throw new Error("`signedBroadcasts` requires a `signer`")
    }
    await this.signalling.ready
    this.signalling.onMessage(message => this.onSignal(message))
    this.signalling.send({ announce: true, from: this._self })
    for (const discovery of this.discoveries) await discovery.start(this)
  }

  /** Stop the node and close every connection. */
  stop(): void {
    for (const discovery of this.discoveries) discovery.stop()
    for (const peer of this.peers.values()) peer.disconnect()
    this.peers.clear()
    this.broker?.stop()
    this.lobby?.close()
  }

  /** Flood a message across the whole mesh (README §1). */
  broadcast(msg: Msg): void {
    void this.doBroadcast(msg).catch(err => this.reportError(err))
  }

  /** Get (or create) a subscription-scoped topic (README §3). */
  topic<T = Msg>(name: string, opts: TopicOptions = {}): Topic<T> {
    const existing = this.topics.get(name)
    if (existing) {
      if (opts.signed !== undefined && opts.signed !== existing.signed) {
        throw new Error(`topic ${name} already exists with a different signing policy`)
      }
      return existing as Topic<T>
    }
    if (opts.signed && (!this.signer || this.signer instanceof NoopSigner))
      throw new Error("signed topics require a `signer`")
    const topic = new Topic<T>(this, name, opts)
    this.topics.set(name, topic as Topic<unknown>)
    return topic
  }

  // ---- peer lifecycle ---------------------------------------------------

  private onSignal(message: SignallingMessage): void {
    if ("announce" in message && message.from !== this._self) {
      const isNew = !this.peers.has(message.from)
      this.ensurePeer(message.from)
      // Re-announce so the newcomer learns about us too.
      if (isNew && this._self) this.signalling.send({ announce: true, from: this._self })
    }
  }

  private ensurePeer(remote: PeerId): void {
    if (!this._self || remote === this._self || this.peers.has(remote)) return
    if (this.options.maxPeers !== undefined && this.peers.size >= this.options.maxPeers) return

    const injected = this.options.createTransport?.({
      self: this._self,
      remote,
      initiator: this._self < remote,
    })
    const peer = new Peer<Msg>({
      self: this._self,
      remote,
      signalling: this.broker ? this.broker.channelFor(remote) : this.signalling,
      signer: this.signer,
      backend: this.options.backend,
      iceServers: this.options.iceServers,
      encrypted: this.options.encrypted,
      caps: this.options.caps,
      router: this.options.router,
      rpcTimeout: this.options.rpcTimeout,
      transport: injected,
    })
    this.peers.set(remote, peer)
    peer.on("message", msg => this.deliverMessage(msg, peer))
    peer.on("frame", frame => {
      void this.onMeshFrame(frame, peer).catch(err => this.reportError(err))
    })
    peer.on("connect", () => this.discoveryEvents.emit("peerConnected", peer.remote))
    peer.on("disconnect", () => this.peers.delete(remote))
    peer.on("error", err => this.emitter.emit("error", err))
    this.emitter.emit("peer", peer)
  }

  // ---- discovery (DiscoveryHost) ---------------------------------------

  /** Connected peer ids (satisfies {@link DiscoveryHost}). */
  peerIds(): PeerId[] {
    return [...this.peers.keys()]
  }

  connect(remote: PeerId): void {
    this.ensurePeer(remote)
  }

  sendGossip(to: PeerId, peers: PeerId[]): void {
    const peer = this.peers.get(to)
    if (peer?.connected)
      void peer
        .sendFrame({ v: WIRE_VERSION, k: "gossip", peers })
        .catch(err => this.reportError(err))
  }

  onPeerConnected(handler: (peer: PeerId) => void): () => void {
    return this.discoveryEvents.on("peerConnected", handler)
  }

  onGossip(handler: (from: PeerId, peers: PeerId[]) => void): () => void {
    return this.discoveryEvents.on("gossip", handler)
  }

  // ---- peer-brokered signalling (SignalBrokerHost) -----------------------

  /**
   * Peers a frame can be pushed onto right now — the handshake is complete.
   * Narrower than {@link peerIds}, which deliberately counts a peer we are
   * still dialing so discovery can share it.
   */
  linkedPeers(): PeerId[] {
    return [...this.peers.values()].filter(p => p.connected).map(p => p.remote)
  }

  /**
   * Report the signalling channel unreachable, so outbound signals switch to
   * the relay path (README §4.1). `bootstrap` reports this itself; a channel
   * you supplied has to, since `SignallingChannel` carries no liveness signal.
   * No-op without `brokeredSignalling`.
   */
  markSignallingDown(): void {
    this.broker?.markLobbyDown()
  }

  /** Report the signalling channel reachable again (see {@link markSignallingDown}). */
  markSignallingUp(): void {
    this.broker?.markLobbyUp()
  }

  /** Send one frame onto a direct link (satisfies {@link SignalBrokerHost}). */
  sendTo(peer: PeerId, frame: Frame): void {
    const target = this.peers.get(peer)
    if (target?.connected) void target.sendFrame(frame).catch(err => this.reportError(err))
  }

  /**
   * Emit a `message` to app listeners, dropping it when a `protocol` is set and
   * the message fails validation — so a malformed or spoofed message never
   * reaches application code (README §2.3).
   */
  private deliverMessage(
    msg: Msg,
    peer: Peer<Msg>,
    metadata: MessageMetadata = {
      origin: peer.remote,
      via: peer.remote,
      originVerified: peer.authenticated,
    },
  ): void {
    const protocol = this.options.protocol
    if (protocol) {
      const valid = protocol.validate(msg)
      if (valid === undefined) return
      this.emitter.emit("message", valid as Msg, peer, metadata)
      return
    }
    this.emitter.emit("message", msg, peer, metadata)
  }

  private reportError(err: unknown): void {
    this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
  }

  private sendToAll(frame: Frame, except?: Peer<Msg>): void {
    for (const peer of this.peers.values()) {
      if (peer !== except && peer.connected)
        void peer.sendFrame(frame).catch(err => this.reportError(err))
    }
  }

  // ---- broadcast --------------------------------------------------------

  private async doBroadcast(msg: Msg): Promise<void> {
    const id = randomId(12)
    this.bcastSeen.seen(id)
    let frame: BcastFrame = {
      v: WIRE_VERSION,
      k: "bcast",
      id,
      ttl: this.ttl,
      from: this.selfId,
      body: msg,
    }
    if (this.options.signedBroadcasts && this.signer) {
      const ts = Date.now()
      const nonce = randomId(8)
      const sig = await this.signer.sign(
        broadcastSignPayload({ from: this.selfId, id, ts, nonce, body: msg }),
      )
      frame = { ...frame, ts, nonce, sig }
    }
    this.sendToAll(frame)
  }

  private async onBroadcast(frame: BcastFrame, from: Peer<Msg>): Promise<void> {
    if (this.bcastSeen.has(frame.id)) return
    const signed = frame.sig !== undefined
    if (this.options.signedBroadcasts || signed) {
      if (!(await this.verifyBroadcast(frame))) return
    }
    // Commit dedup/replay state only after verification, rechecking after await.
    if (this.bcastSeen.seen(frame.id)) return
    if (signed && this.bcastNonces.seen(JSON.stringify([frame.from, frame.nonce]))) return
    this.deliverMessage(frame.body as Msg, from, {
      origin: frame.from,
      via: from.remote,
      originVerified: signed && !!this.signer && !(this.signer instanceof NoopSigner),
    })
    if (frame.ttl > 1) this.sendToAll({ ...frame, ttl: frame.ttl - 1 }, from)
  }

  private async verifyBroadcast(frame: BcastFrame): Promise<boolean> {
    if (frame.sig === undefined || frame.ts === undefined || frame.nonce === undefined) return false
    if (Math.abs(Date.now() - frame.ts) > BROADCAST_FRESHNESS_MS) return false
    if (this.bcastNonces.has(JSON.stringify([frame.from, frame.nonce]))) return false
    if (!this.signer) return true // Relay without claiming origin verification.
    return this.signer.verify(
      frame.sig,
      broadcastSignPayload({
        from: frame.from,
        id: frame.id,
        ts: frame.ts,
        nonce: frame.nonce,
        body: frame.body,
      }),
      frame.from,
    )
  }

  // ---- topics (TopicHost) ----------------------------------------------

  floodSubscription(kind: "sub" | "unsub", topic: string): void {
    const frame: SubFrame = { v: WIRE_VERSION, k: kind, topic, from: this.selfId, id: randomId(10) }
    this.subGossipSeen.seen(frame.id)
    this.sendToAll(frame)
  }

  publishToTopic(topic: string, body: unknown, signed: boolean): void {
    void this.doPublish(topic, body, signed).catch(err => this.reportError(err))
  }

  subscribersOf(topic: string): Set<PeerId> {
    const map = this.topicSubs.get(topic)
    return new Set(map ? map.keys() : [])
  }

  dropTopic(topic: string): void {
    this.topics.delete(topic)
  }

  private async doPublish(topic: string, body: unknown, signed: boolean): Promise<void> {
    const seq = (this.pubSeq.get(topic) ?? 0) + 1
    this.pubSeq.set(topic, seq)
    const nonce = randomId(8)
    let frame: PubFrame = {
      v: WIRE_VERSION,
      k: "pub",
      topic,
      from: this.selfId,
      seq,
      nonce,
      ttl: this.ttl,
      body,
    }
    if (signed && this.signer) {
      const sig = await this.signer.sign(
        pubSignPayload({ topic, from: this.selfId, seq, nonce, body }),
      )
      frame = { ...frame, sig }
    }
    this.pubDedup.seen(JSON.stringify([topic, this.selfId, seq]))
    // Publishes do not echo to the local publisher.
    this.sendToAll(frame)
  }

  private async onMeshFrame(frame: Frame, from: Peer<Msg>): Promise<void> {
    switch (frame.k) {
      case "bcast":
        await this.onBroadcast(frame, from)
        return
      case "sub":
      case "unsub":
        this.onSubscription(frame, from)
        return
      case "pub":
        await this.onPublish(frame, from)
        return
      case "gossip":
        this.onGossipFrame(frame, from)
        return
      case "sig-relay":
        this.onSigRelay(frame, from.remote)
        return
      default:
        // req/res (RPC) are consumed inside Peer, before the frame event.
        return
    }
  }

  private onGossipFrame(frame: GossipFrame, from: Peer<Msg>): void {
    this.discoveryEvents.emit("gossip", from.remote, frame.peers)
  }

  /**
   * A handshake signal carried over a link because the lobby is unreachable.
   * Opening the slot for an unknown origin is what `announce` does on the lobby:
   * without it the signal has no transport to arrive at.
   */
  private onSigRelay(frame: SigRelayFrame, via: PeerId): void {
    if (!this._self || !this.broker) return
    if (frame.to === this._self) this.ensurePeer(frame.from)
    this.broker.ingest(frame, via)
  }

  private onSubscription(frame: SubFrame, from: Peer<Msg>): void {
    if (this.subGossipSeen.seen(frame.id)) return
    let map = this.topicSubs.get(frame.topic)
    if (!map) this.topicSubs.set(frame.topic, (map = new Map()))
    if (frame.k === "sub") map.set(frame.from, Date.now())
    else map.delete(frame.from)
    this.sendToAll(frame, from) // gossip onward
  }

  private async onPublish(frame: PubFrame, from: Peer<Msg>): Promise<void> {
    const topic = this.topics.get(frame.topic)
    if (topic?.signed && !frame.sig) return
    let originVerified = false
    if (frame.sig !== undefined && this.signer) {
      if (!(await this.signer.verify(frame.sig, pubSignPayload(frame), frame.from))) return
      originVerified = !(this.signer instanceof NoopSigner)
    }
    if (topic?.signed && !originVerified) return

    // No replay state is touched until policy and authenticity checks pass.
    const dedupKey = JSON.stringify([frame.topic, frame.from, frame.seq])
    const highestKey = JSON.stringify([frame.topic, frame.from])
    const highest = this.pubHighest.get(highestKey) ?? 0
    const received = this.pubReceived.get(highestKey) ?? new Set<number>()
    if (frame.seq <= highest - REPLAY_WINDOW || received.has(frame.seq)) return
    if (this.pubDedup.seen(dedupKey)) return
    const nextHighest = Math.max(highest, frame.seq)
    this.pubHighest.set(highestKey, nextHighest)
    received.add(frame.seq)
    for (const seq of received) if (seq <= nextHighest - REPLAY_WINDOW) received.delete(seq)
    this.pubReceived.set(highestKey, received)

    if (topic)
      topic.deliver(frame.body, frame.from, {
        origin: frame.from,
        via: from.remote,
        originVerified,
      })
    if (frame.ttl > 1) this.sendToAll({ ...frame, ttl: frame.ttl - 1 }, from)
  }
}

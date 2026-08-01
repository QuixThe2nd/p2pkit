import type { PeerId } from "../utils/types.js"
import type { Signer } from "../auth/signer.js"
import type { SignallingChannel, SignallingMessage } from "../signalling/types.js"
import type { RTCBackend, RTCBackendSource } from "../backends/index.js"
import type { Transport } from "../transports/types.js"
import type { Frame, BcastFrame, SubFrame, PubFrame } from "../wire/index.js"
import { WIRE_VERSION } from "../wire/index.js"
import { Emitter } from "../utils/emitter.js"
import { randomId } from "../utils/id.js"
import { Peer } from "./Peer.js"
import { Topic, type TopicOptions, type TopicHost } from "./topic.js"
import { SeenCache, broadcastSignPayload, pubSignPayload } from "./envelope.js"

export interface BroadcastOptions {
  /** Hop limit for a flooded broadcast. Default 7. */
  ttl?: number
  /** How long (ms) a message id is remembered for dedup. Default 30000. */
  dedupWindow?: number
}

export interface P2PKitOptions {
  /** This node's id. Derived from `signer` when given; required otherwise. */
  self?: PeerId
  /** Signalling channel peers use to find and connect to each other. */
  signalling: SignallingChannel
  /** Identity signer; enables verified handshakes, encryption and signed broadcasts. */
  signer?: Signer
  backend?: RTCBackend | RTCBackendSource
  iceServers?: RTCIceServer[]
  /** Cap on direct connections; excess reachability is maintained by relaying. */
  maxPeers?: number
  /** End-to-end encrypt direct messages (requires an `ECDSASigner`). */
  encrypted?: boolean
  /** Sign every broadcast so relays can't spoof the origin (requires a signer). */
  signedBroadcasts?: boolean
  broadcast?: BroadcastOptions
  caps?: string[]
  /**
   * Advanced: supply a transport for a peer instead of the default WebRTC one.
   * Return `undefined` to fall back to WebRTC. Enables custom link types and
   * deterministic testing.
   */
  createTransport?: (info: { self: PeerId; remote: PeerId; initiator: boolean }) => Transport<Frame> | undefined
}

/** Events emitted by {@link P2PKit}. */
export type P2PKitEvents<Msg> = {
  /** A new peer joined; attach handlers before it connects. */
  peer: (peer: Peer<Msg>) => void
  /** A direct or broadcast message arrived, with the delivering peer. */
  message: (msg: Msg, peer: Peer<Msg>) => void
  error: (err: Error) => void
}

const DEFAULT_TTL = 7
const DEFAULT_DEDUP_WINDOW = 30_000
const BROADCAST_FRESHNESS_MS = 60_000
const REPLAY_WINDOW = 1024

/**
 * Connects you to a mesh of peers and hands you each one as it joins,
 * negotiating the best transport automatically (README §1). Provides
 * whole-mesh {@link P2PKit.broadcast} flooding and subscription-scoped
 * {@link P2PKit.topic} pub/sub.
 */
export class P2PKit<Msg = unknown> implements TopicHost {
  /** Directly-connected peers, keyed by id. */
  readonly peers = new Map<PeerId, Peer<Msg>>()

  private readonly options: P2PKitOptions
  private readonly signer: Signer | undefined
  private readonly signalling: SignallingChannel
  private readonly emitter = new Emitter<P2PKitEvents<Msg>>()
  private readonly ttl: number

  private self?: PeerId
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
  private readonly pubHighest = new Map<string, number>()

  constructor(options: P2PKitOptions) {
    this.options = options
    this.signer = options.signer
    this.signalling = options.signalling
    this.ttl = options.broadcast?.ttl ?? DEFAULT_TTL
    const dedupWindow = options.broadcast?.dedupWindow ?? DEFAULT_DEDUP_WINDOW
    this.bcastSeen = new SeenCache(dedupWindow)
    this.bcastNonces = new SeenCache(Math.max(dedupWindow, BROADCAST_FRESHNESS_MS))
    this.subGossipSeen = new SeenCache(dedupWindow)
    this.pubDedup = new SeenCache(dedupWindow)
  }

  get selfId(): PeerId {
    if (!this.self) throw new Error("P2PKit not started — call start() first")
    return this.self
  }

  on<E extends keyof P2PKitEvents<Msg>>(event: E, handler: P2PKitEvents<Msg>[E]): void {
    this.emitter.on(event, handler)
  }

  /** Join the mesh: peers start connecting. */
  async start(): Promise<void> {
    if (this.started) return
    this.started = true
    await (this.signer as { ready?: Promise<void> } | undefined)?.ready
    this.self = this.signer?.id ?? this.options.self
    if (!this.self) throw new Error("P2PKit requires `self` or a `signer`")
    if (this.options.signedBroadcasts && !this.signer) {
      throw new Error("`signedBroadcasts` requires a `signer`")
    }
    await this.signalling.ready
    this.signalling.onMessage(message => this.onSignal(message))
    this.signalling.send({ announce: true, from: this.self })
  }

  /** Stop the node and close every connection. */
  stop(): void {
    for (const peer of this.peers.values()) peer.disconnect()
    this.peers.clear()
  }

  /** Flood a message across the whole mesh (README §1). */
  broadcast(msg: Msg): void {
    void this.doBroadcast(msg)
  }

  /** Get (or create) a subscription-scoped topic (README §3). */
  topic<T = Msg>(name: string, opts: TopicOptions = {}): Topic<T> {
    const existing = this.topics.get(name)
    if (existing) return existing as Topic<T>
    if (opts.signed && !this.signer) throw new Error("signed topics require a `signer`")
    const topic = new Topic<T>(this, name, opts)
    this.topics.set(name, topic as Topic<unknown>)
    return topic
  }

  // ---- peer lifecycle ---------------------------------------------------

  private onSignal(message: SignallingMessage): void {
    if ("announce" in message && message.from !== this.self) {
      const isNew = !this.peers.has(message.from)
      this.ensurePeer(message.from)
      // Re-announce so the newcomer learns about us too.
      if (isNew && this.self) this.signalling.send({ announce: true, from: this.self })
    }
  }

  private ensurePeer(remote: PeerId): void {
    if (!this.self || remote === this.self || this.peers.has(remote)) return
    if (this.options.maxPeers !== undefined && this.peers.size >= this.options.maxPeers) return

    const injected = this.options.createTransport?.({
      self: this.self,
      remote,
      initiator: this.self < remote,
    })
    const peer = new Peer<Msg>({
      self: this.self,
      remote,
      signalling: this.signalling,
      signer: this.signer,
      backend: this.options.backend,
      iceServers: this.options.iceServers,
      encrypted: this.options.encrypted,
      caps: this.options.caps,
      transport: injected,
    })
    this.peers.set(remote, peer)
    peer.on("message", msg => this.emitter.emit("message", msg, peer))
    peer.on("frame", frame => this.onMeshFrame(frame, peer))
    peer.on("disconnect", () => this.peers.delete(remote))
    peer.on("error", err => this.emitter.emit("error", err))
    this.emitter.emit("peer", peer)
  }

  private sendToAll(frame: Frame, except?: Peer<Msg>): void {
    for (const peer of this.peers.values()) {
      if (peer !== except && peer.connected) void peer.sendFrame(frame)
    }
  }

  // ---- broadcast --------------------------------------------------------

  private async doBroadcast(msg: Msg): Promise<void> {
    const id = randomId(12)
    this.bcastSeen.seen(id)
    let frame: BcastFrame = { v: WIRE_VERSION, k: "bcast", id, ttl: this.ttl, from: this.selfId, body: msg }
    if (this.options.signedBroadcasts && this.signer) {
      const ts = Date.now()
      const nonce = randomId(8)
      const sig = await this.signer.sign(broadcastSignPayload({ from: this.selfId, id, ts, nonce, body: msg }))
      frame = { ...frame, ts, nonce, sig }
    }
    this.sendToAll(frame)
  }

  private async onBroadcast(frame: BcastFrame, from: Peer<Msg>): Promise<void> {
    if (this.bcastSeen.seen(frame.id)) return
    if (this.options.signedBroadcasts || frame.sig !== undefined) {
      if (!(await this.verifyBroadcast(frame))) return
    }
    this.emitter.emit("message", frame.body as Msg, from)
    if (frame.ttl > 1) this.sendToAll({ ...frame, ttl: frame.ttl - 1 }, from)
  }

  private async verifyBroadcast(frame: BcastFrame): Promise<boolean> {
    if (frame.sig === undefined || frame.ts === undefined || frame.nonce === undefined) return false
    if (Math.abs(Date.now() - frame.ts) > BROADCAST_FRESHNESS_MS) return false
    if (this.bcastNonces.seen(`${frame.from}|${frame.nonce}`)) return false
    if (!this.signer) return true // cannot verify without a signer; accept and relay
    const payload = broadcastSignPayload({
      from: frame.from,
      id: frame.id,
      ts: frame.ts,
      nonce: frame.nonce,
      body: frame.body,
    })
    return this.signer.verify(frame.sig, payload, frame.from)
  }

  // ---- topics (TopicHost) ----------------------------------------------

  floodSubscription(kind: "sub" | "unsub", topic: string): void {
    const frame: SubFrame = { v: WIRE_VERSION, k: kind, topic, from: this.selfId, id: randomId(10) }
    this.subGossipSeen.seen(frame.id)
    this.sendToAll(frame)
  }

  publishToTopic(topic: string, body: unknown, signed: boolean): void {
    void this.doPublish(topic, body, signed)
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
      const sig = await this.signer.sign(pubSignPayload({ topic, from: this.selfId, seq, nonce, body }))
      frame = { ...frame, sig }
    }
    this.pubDedup.seen(`${topic}|${this.selfId}|${seq}`)
    // Local subscribers receive their own publishes' echoes? No — deliver only to
    // remote-facing flooding; local emit happens on the publisher's own Topic here.
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
      default:
        // gossip (discovery, Phase 7) and req/res (RPC, Phase 6) handled elsewhere.
        return
    }
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
    const dedupKey = `${frame.topic}|${frame.from}|${frame.seq}`
    if (this.pubDedup.seen(dedupKey)) return

    // Sliding-window replay protection: drop stale sequence numbers.
    const highestKey = `${frame.topic}|${frame.from}`
    const highest = this.pubHighest.get(highestKey) ?? 0
    if (frame.seq <= highest - REPLAY_WINDOW) return
    if (frame.seq > highest) this.pubHighest.set(highestKey, frame.seq)

    if (frame.sig !== undefined && this.signer) {
      const payload = pubSignPayload({
        topic: frame.topic,
        from: frame.from,
        seq: frame.seq,
        nonce: frame.nonce,
        body: frame.body,
      })
      if (!(await this.signer.verify(frame.sig, payload, frame.from))) return
    }

    const topic = this.topics.get(frame.topic)
    if (topic) topic.deliver(frame.body, frame.from)
    if (frame.ttl > 1) this.sendToAll({ ...frame, ttl: frame.ttl - 1 }, from)
  }
}

import type { MessageMetadata } from "./P2PKit.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"

export interface TopicOptions {
  /** Require verified signatures on incoming publishes and sign outgoing publishes. */
  signed?: boolean
}

/** The slice of `P2PKit` a {@link Topic} drives. Implemented internally by `P2PKit`. */
export interface TopicHost {
  readonly selfId: PeerId
  floodSubscription(kind: "sub" | "unsub", topic: string): void
  publishToTopic(topic: string, body: unknown, signed: boolean): void
  subscribersOf(topic: string): Set<PeerId>
  dropTopic(topic: string): void
}

/**
 * A subscription-scoped channel within the mesh. Publishes flood the network;
 * only local subscribers receive events. Pass `{ signed: true }` to require
 * verified origin signatures before delivery or relay at this node.
 */
export class Topic<T = unknown> {
  readonly name: string
  private readonly host: TopicHost
  private readonly opts: TopicOptions
  private readonly emitter = new Emitter<{
    message: (msg: T, from: PeerId, metadata: MessageMetadata) => void
  }>()
  private left = false

  constructor(host: TopicHost, name: string, opts: TopicOptions = {}) {
    this.host = host
    this.name = name
    this.opts = { ...opts }
    host.floodSubscription("sub", name)
  }

  on(event: "message", handler: (msg: T, from: PeerId, metadata: MessageMetadata) => void): void {
    this.emitter.on(event, handler)
  }

  /** Whether this subscription requires verified origin signatures. */
  get signed(): boolean {
    return this.opts.signed ?? false
  }

  /** Subscribers we know about (learned via gossip). */
  get peers(): Set<PeerId> {
    return this.host.subscribersOf(this.name)
  }

  /** Publish to this topic's subscribers. */
  publish(msg: T): void {
    if (this.left) throw new Error(`topic ${this.name} has been left`)
    this.host.publishToTopic(this.name, msg, this.opts.signed ?? false)
  }

  /** Unsubscribe and stop receiving. */
  leave(): void {
    if (this.left) return
    this.left = true
    this.host.floodSubscription("unsub", this.name)
    this.host.dropTopic(this.name)
  }

  /** Internal: deliver an incoming publish to local listeners. */
  deliver(msg: T, from: PeerId, metadata: MessageMetadata): void {
    if (!this.left) this.emitter.emit("message", msg, from, metadata)
  }
}

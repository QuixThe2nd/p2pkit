import type { PeerId } from "../../src/utils/types.js"
import type { Frame } from "../../src/wire/index.js"
import type { Transport, TransportEvents } from "../../src/transports/types.js"
import type { SignallingChannel, SignallingMessage } from "../../src/signalling/types.js"
import type { Signer } from "../../src/auth/index.js"
import { Emitter } from "../../src/utils/emitter.js"
import { P2PKit } from "../../src/index.js"

// A linked pair of in-memory transports, used to wire mesh nodes without WebRTC.
class MemoryTransport implements Transport<Frame> {
  readonly name = "memory"
  bufferedAmount = 0
  private readonly emitter = new Emitter<TransportEvents<Frame>>()
  partner?: MemoryTransport
  private opened = false
  private closed = false
  constructor(readonly remote: PeerId) {}
  open(): void {
    if (this.opened) return
    this.opened = true
    setTimeout(() => this.emitter.emit("connect"), 0)
  }
  on<E extends keyof TransportEvents<Frame>>(event: E, handler: TransportEvents<Frame>[E]): void {
    this.emitter.on(event, handler)
    if (event === "connect" && this.opened) setTimeout(() => (handler as () => void)(), 0)
  }
  send(frame: Frame): Promise<void> {
    if (!this.closed && this.partner) {
      const p = this.partner
      queueMicrotask(() => p.emitter.emit("message", frame))
    }
    return Promise.resolve()
  }
  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.emitter.emit("disconnect")
  }
}

/** Shared registry so both ends of an edge get the two ends of one linked pair. */
class TransportRegistry {
  private readonly pairs = new Map<string, { a: MemoryTransport; b: MemoryTransport }>()
  private key(x: PeerId, y: PeerId): string {
    return x < y ? `${x}::${y}` : `${y}::${x}`
  }
  end(self: PeerId, remote: PeerId): Transport<Frame> {
    const key = this.key(self, remote)
    let pair = this.pairs.get(key)
    if (!pair) {
      const [lo, hi] = self < remote ? [self, remote] : [remote, self]
      const a = new MemoryTransport(hi) // owned by lo, remote = hi
      const b = new MemoryTransport(lo)
      a.partner = b
      b.partner = a
      a.open()
      b.open()
      this.pairs.set(key, { a, b })
      pair = this.pairs.get(key)!
    }
    return self < remote ? pair.a : pair.b
  }
}

/** In-memory signalling bus that delivers announces only along configured edges. */
class MeshBus {
  private readonly channels = new Map<PeerId, TestSignalling>()
  private readonly neighbours = new Map<PeerId, Set<PeerId>>()
  register(id: PeerId, channel: TestSignalling): void {
    this.channels.set(id, channel)
  }
  connect(a: PeerId, b: PeerId): void {
    ;(this.neighbours.get(a) ?? this.neighbours.set(a, new Set()).get(a)!).add(b)
    ;(this.neighbours.get(b) ?? this.neighbours.set(b, new Set()).get(b)!).add(a)
  }
  deliver(from: PeerId, message: SignallingMessage): void {
    for (const neighbour of this.neighbours.get(from) ?? []) {
      this.channels.get(neighbour)?.receive(message)
    }
  }
}

class TestSignalling implements SignallingChannel {
  readonly ready = Promise.resolve()
  private handler?: (m: SignallingMessage) => void
  constructor(
    private readonly id: PeerId,
    private readonly bus: MeshBus,
  ) {}
  send(message: SignallingMessage): void {
    this.bus.deliver(this.id, message)
  }
  onMessage(handler: (message: SignallingMessage) => void): void {
    this.handler = handler
  }
  receive(message: SignallingMessage): void {
    this.handler?.(message)
  }
}

export interface MeshNodeOptions {
  signer?: Signer
  signedBroadcasts?: boolean
  broadcast?: { ttl?: number; dedupWindow?: number }
  protocol?: import("../../src/rpc/index.js").AnyProtocol
  router?: import("../../src/rpc/index.js").Router
  discovery?: import("../../src/discovery/index.js").Discovery | import("../../src/discovery/index.js").Discovery[]
}

/**
 * Build an in-memory mesh with an explicit topology. `edges` control who can
 * connect to whom, letting tests exercise multi-hop relay/routing (e.g. a line
 * A–B–C where A and C are not directly connected).
 */
export async function buildMesh<Msg = unknown>(
  ids: PeerId[],
  edges: [PeerId, PeerId][],
  perNode: (id: PeerId) => MeshNodeOptions = () => ({}),
): Promise<Map<PeerId, P2PKit<Msg>>> {
  const bus = new MeshBus()
  const registry = new TransportRegistry()
  const nodes = new Map<PeerId, P2PKit<Msg>>()

  for (const id of ids) {
    const signalling = new TestSignalling(id, bus)
    bus.register(id, signalling)
    const opts = perNode(id)
    const kit = new P2PKit<Msg>({
      self: id,
      signalling,
      createTransport: ({ self, remote }) => registry.end(self, remote),
      ...opts,
    })
    nodes.set(id, kit)
  }
  for (const [a, b] of edges) bus.connect(a, b)

  await Promise.all([...nodes.values()].map(k => k.start()))
  // Let announces propagate and transports open.
  await new Promise(r => setTimeout(r, 30))
  return nodes
}

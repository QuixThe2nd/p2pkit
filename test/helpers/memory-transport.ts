import type { Transport, TransportEvents } from "../../src/transports/types.js"
import type { Frame } from "../../src/wire/index.js"
import type { PeerId } from "../../src/utils/types.js"
import { Emitter } from "../../src/utils/emitter.js"

/**
 * An in-process transport for testing `Peer`/`P2PKit` logic without WebRTC.
 * Delivers frames to its linked partner on a microtask. `connect` is emitted on
 * a macrotask (after both `Peer`s have had a chance to subscribe) and replayed
 * to late subscribers.
 */
class MemoryTransport implements Transport<Frame> {
  readonly name = "memory"
  readonly remote: PeerId
  bufferedAmount = 0
  private readonly emitter = new Emitter<TransportEvents<Frame>>()
  private partner?: MemoryTransport
  private opened = false
  private closed = false

  constructor(remote: PeerId) {
    this.remote = remote
  }

  link(partner: MemoryTransport): void {
    this.partner = partner
  }

  open(): void {
    if (this.opened) return
    this.opened = true
    setTimeout(() => this.emitter.emit("connect"), 0)
  }

  on<E extends keyof TransportEvents<Frame>>(event: E, handler: TransportEvents<Frame>[E]): void {
    this.emitter.on(event, handler)
    if (event === "connect" && this.opened) {
      setTimeout(() => (handler as () => void)(), 0)
    }
  }

  send(frame: Frame): Promise<void> {
    if (this.closed || !this.partner) return Promise.resolve()
    const partner = this.partner
    queueMicrotask(() => partner.emitter.emit("message", frame))
    return Promise.resolve()
  }

  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.emitter.emit("disconnect")
    if (this.partner && !this.partner.closed) this.partner.disconnect()
  }
}

/** Create two linked in-memory transports and open them. */
export function memoryTransportPair(
  a: PeerId,
  b: PeerId,
): [Transport<Frame>, Transport<Frame>] {
  const ta = new MemoryTransport(b) // a's transport, remote = b
  const tb = new MemoryTransport(a)
  ta.link(tb)
  tb.link(ta)
  ta.open()
  tb.open()
  return [ta, tb]
}

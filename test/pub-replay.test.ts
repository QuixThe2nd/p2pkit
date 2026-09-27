import { describe, it, expect } from "vitest"
import { P2PKit } from "../src/index.js"
import { ECDSASigner, generatePrivateKey } from "../src/auth/index.js"
import type { Frame } from "../src/wire/index.js"
import type { Transport, TransportEvents } from "../src/transports/types.js"
import type { PeerId } from "../src/utils/types.js"
import { Emitter } from "../src/utils/emitter.js"

/**
 * Regression coverage for signed-publish replay protection across publisher
 * incarnations: a sender that restarts with the same key must reach
 * still-running receivers, while replay/duplicate/forgery rejection and
 * legacy (session-less) wire compatibility are preserved.
 *
 * Links are in-memory tap transports so tests can capture, duplicate,
 * reorder, mutate, strip, and replay frames exactly as a malicious or
 * careless carrier could.
 */

type TapHook = (frame: Frame, forward: (f?: Frame) => void) => void

/** Memory transport with an outbound frame hook (default: forward unchanged). */
class TapTransport implements Transport<Frame> {
  readonly name = "tap"
  bufferedAmount = 0
  hook: TapHook = (_f, fwd) => fwd()
  private readonly emitter = new Emitter<TransportEvents<Frame>>()
  partner?: TapTransport
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
    if (this.closed) return Promise.resolve()
    this.hook(frame, (f = frame) => {
      const p = this.partner
      if (p && !p.closed) queueMicrotask(() => p.emitter.emit("message", f))
    })
    return Promise.resolve()
  }
  /** Deliver a frame to the partner out of band (test-injected carrier traffic). */
  inject(frame: Frame): void {
    const p = this.partner
    if (p && !p.closed) queueMicrotask(() => p.emitter.emit("message", frame))
  }
  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.emitter.emit("disconnect")
    if (this.partner && !this.partner.closed) this.partner.disconnect()
  }
}

function tapPair(a: PeerId, b: PeerId): [TapTransport, TapTransport] {
  const ta = new TapTransport(b)
  const tb = new TapTransport(a)
  ta.partner = tb
  tb.partner = ta
  ta.open()
  tb.open()
  return [ta, tb]
}

interface Node {
  kit: P2PKit<unknown>
  signer: ECDSASigner
  key: Uint8Array
  /** The transport THIS node sends on for its current link to a given peer. */
  taps: Map<PeerId, TapTransport>
}

async function makeNode(key?: Uint8Array): Promise<Node> {
  const k = key ?? generatePrivateKey()
  const signer = new ECDSASigner({ privateKey: k })
  await signer.ready
  const kit = new P2PKit({ signer })
  await kit.start()
  return { kit, signer, key: k, taps: new Map() }
}

/** Link two nodes; returns after both handshakes complete. */
async function link(x: Node, y: Node): Promise<void> {
  const [tx, ty] = tapPair(x.signer.id, y.signer.id)
  x.taps.set(y.signer.id, tx)
  y.taps.set(x.signer.id, ty)
  x.kit.acceptLink(tx)
  y.kit.acceptLink(ty)
  await Promise.all([x.kit.peers.get(y.signer.id)!.ready, y.kit.peers.get(x.signer.id)!.ready])
}

/** Restart a node in place: stop the kit, start a fresh one with the same key. */
async function restart(node: Node): Promise<void> {
  node.kit.stop()
  node.taps.clear()
  node.signer = new ECDSASigner({ privateKey: node.key })
  await node.signer.ready
  node.kit = new P2PKit({ signer: node.signer })
  await node.kit.start()
}

const TOPIC = "test/replay"
const settle = (ms = 40) => new Promise(r => setTimeout(r, ms))

/** Collect chat bodies delivered to a node's signed topic subscription. */
function subscribe(node: Node): { bodies: string[]; verified: boolean[] } {
  const topic = node.kit.topic<{ body: string }>(TOPIC, { signed: true })
  const out = { bodies: [] as string[], verified: [] as boolean[] }
  topic.on("message", (m, _from, meta) => {
    out.bodies.push((m as { body: string }).body)
    out.verified.push(meta.originVerified)
  })
  return out
}

function publish(node: Node, body: string): void {
  node.kit.topic(TOPIC, { signed: true }).publish({ body })
}

/** Flip one hex character in a 0x-prefixed signature. */
function corrupt(sig: string): string {
  const i = 4
  const c = sig[i] === "0" ? "1" : "0"
  return sig.slice(0, i) + c + sig.slice(i + 1)
}

describe("signed publish replay protection across incarnations", () => {
  it("delivers a restarted same-key sender's publishes to a still-running receiver", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()

    publish(a, "before restart 1")
    publish(a, "before restart 2")
    await settle()
    expect(atB.bodies).toEqual(["before restart 1", "before restart 2"])

    await restart(a)
    await link(a, b)
    subscribe(a)
    await settle()

    // seq restarts at 1; the still-running receiver must accept the new
    // incarnation's sequence space.
    publish(a, "after restart 1")
    publish(a, "after restart 2")
    await settle()
    expect(atB.bodies).toEqual([
      "before restart 1",
      "before restart 2",
      "after restart 1",
      "after restart 2",
    ])
    expect(atB.verified).toEqual([true, true, true, true])
    b.kit.stop()
    a.kit.stop()
  })

  it("delivers from two concurrent same-key sender sessions (direct and relayed)", async () => {
    const key = generatePrivateKey()
    const a1 = await makeNode(key)
    const a2 = await makeNode(key) // same identity, concurrent incarnation
    const relay = await makeNode()
    const b = await makeNode()
    await link(a1, b)
    await link(a2, relay)
    await link(relay, b)
    const atB = subscribe(b)
    subscribe(a1)
    subscribe(a2)
    await settle()

    publish(a1, "from session one")
    publish(a2, "from session two") // reaches B relayed through `relay`
    await settle(80)
    expect(atB.bodies.sort()).toEqual(["from session one", "from session two"])
    for (const n of [a1, a2, relay, b]) n.kit.stop()
  })

  it("still rejects replays of an earlier incarnation's frames", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()

    // Capture the first incarnation's frames as they cross the link.
    const captured: Frame[] = []
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub") captured.push(structuredClone(f))
      fwd()
    }
    publish(a, "incarnation one")
    await settle()
    expect(atB.bodies).toEqual(["incarnation one"])
    expect(captured.filter(f => f.k === "pub")).toHaveLength(1)

    await restart(a)
    await link(a, b)
    subscribe(a)
    await settle()
    publish(a, "incarnation two")
    await settle()
    expect(atB.bodies).toEqual(["incarnation one", "incarnation two"])

    // A malicious carrier replays the old incarnation's frame alongside.
    const tap = a.taps.get(b.signer.id)!
    const old = captured.find(f => f.k === "pub")!
    tap.hook = (f, fwd) => {
      fwd()
      if (f.k === "pub") fwd(structuredClone(old)) // replay rides the next publish
    }
    publish(a, "trigger")
    await settle()
    expect(atB.bodies).toEqual(["incarnation one", "incarnation two", "trigger"])
    b.kit.stop()
    a.kit.stop()
  })

  it("dedupes the same frame delivered twice", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      fwd()
      if (f.k === "pub") fwd(structuredClone(f))
    }
    publish(a, "once only")
    await settle()
    expect(atB.bodies).toEqual(["once only"])
    b.kit.stop()
    a.kit.stop()
  })

  it("accepts out-of-order delivery within the replay window", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()
    // Hold back the first publish, release it after the second has landed.
    const held: Frame[] = []
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub" && held.length === 0) {
        held.push(structuredClone(f))
        return
      }
      fwd()
    }
    publish(a, "first")
    publish(a, "second")
    await settle()
    expect(atB.bodies).toEqual(["second"])
    // Release the held frame late — still inside the window, so it is
    // delivered rather than rejected as stale or duplicate.
    a.taps.get(b.signer.id)!.inject(held[0]!)
    await settle()
    expect(atB.bodies).toEqual(["second", "first"])
    b.kit.stop()
    a.kit.stop()
  })

  it("rejects signature mutation, session-signature mutation, and session stripping", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()

    // 1. Corrupt the per-frame signature.
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub" && f.sig) fwd({ ...f, sig: corrupt(f.sig) })
      else fwd()
    }
    publish(a, "bad sig")
    await settle()
    expect(atB.bodies).toEqual([])

    // 2. Corrupt only the incarnation signature.
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub" && f.sessionSig) fwd({ ...f, sessionSig: corrupt(f.sessionSig) })
      else fwd()
    }
    publish(a, "bad session sig")
    await settle()
    expect(atB.bodies).toEqual([])

    // Clean link again: the genuine publish must flow.
    a.taps.get(b.signer.id)!.hook = (_f, fwd) => fwd()
    publish(a, "genuine")
    await settle()
    expect(atB.bodies).toEqual(["genuine"])

    // 3. Strip session + sessionSig off a copy of a genuine frame and replay
    // it: the signature still verifies over the unchanged payload, but the
    // receiver already knows this origin's incarnation and must not reopen
    // the legacy sequence space (which would duplicate the delivery).
    let stripped: Frame | undefined
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub") {
        const { session: _s, sessionSig: _ss, ...rest } = f
        stripped = structuredClone(rest) as Frame
      }
      fwd()
    }
    publish(a, "strip me")
    await settle()
    expect(atB.bodies).toEqual(["genuine", "strip me"])
    expect(stripped).toBeDefined()
    const tap = a.taps.get(b.signer.id)!
    tap.hook = (f, fwd) => {
      fwd()
      if (f.k === "pub") fwd(structuredClone(stripped!))
    }
    publish(a, "carrier")
    await settle()
    expect(atB.bodies).toEqual(["genuine", "strip me", "carrier"])
    b.kit.stop()
    a.kit.stop()
  })

  it("keeps legacy session-less publishers working (old wire behavior)", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()
    // Simulate a pre-incarnation publisher: strip session from every frame.
    a.taps.get(b.signer.id)!.hook = (f, fwd) => {
      if (f.k === "pub") {
        const { session: _s, sessionSig: _ss, ...rest } = f
        fwd(rest as Frame)
        return
      }
      fwd()
    }
    publish(a, "legacy one")
    publish(a, "legacy two")
    await settle()
    expect(atB.bodies).toEqual(["legacy one", "legacy two"])
    expect(atB.verified).toEqual([true, true])
    b.kit.stop()
    a.kit.stop()
  })

  it("bounds per-origin incarnation state while still delivering new restarts", async () => {
    const a = await makeNode()
    const b = await makeNode()
    await link(a, b)
    const atB = subscribe(b)
    subscribe(a)
    await settle()
    publish(a, "incarnation 0")
    await settle()

    const restarts = 20 // beyond MAX_PUB_SESSIONS_PER_ORIGIN (16)
    for (let i = 1; i <= restarts; i++) {
      await restart(a)
      await link(a, b)
      subscribe(a)
      publish(a, `incarnation ${i}`)
      await settle(20)
    }
    // Every fresh incarnation was delivered, including those past the bound.
    expect(atB.bodies).toHaveLength(restarts + 1)
    expect(atB.bodies[restarts]).toBe(`incarnation ${restarts}`)
    // State stayed bounded: at most 16 incarnations retained for this origin.
    const replay = (b.kit as unknown as { pubReplay: Map<string, unknown> }).pubReplay
    expect(replay.size).toBeLessThanOrEqual(16)
    b.kit.stop()
    a.kit.stop()
  })
})

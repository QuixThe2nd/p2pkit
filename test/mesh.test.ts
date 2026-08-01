import { describe, it, expect } from "vitest"
import type { P2PKit } from "../src/index.js"
import { ECDSASigner } from "../src/auth/index.js"
import { buildMesh } from "./helpers/mesh.js"

type Chat = { type: "chat"; body: string }

const collect = <Msg>(kit: P2PKit<Msg>) => {
  const msgs: Msg[] = []
  kit.on("message", m => msgs.push(m))
  return msgs
}
const settle = () => new Promise(r => setTimeout(r, 40))

describe("broadcast flooding", () => {
  it("relays across a line A–B–C to a non-adjacent peer, exactly once", async () => {
    const nodes = await buildMesh<Chat>(["A", "B", "C"], [["A", "B"], ["B", "C"]])
    const a = nodes.get("A")!
    const c = nodes.get("C")!
    const atC = collect(c)

    a.broadcast({ type: "chat", body: "hello mesh" })
    await settle()

    expect(atC).toEqual([{ type: "chat", body: "hello mesh" }])
  })

  it("dedups across multiple paths in a triangle", async () => {
    const nodes = await buildMesh<Chat>(
      ["A", "B", "C"],
      [["A", "B"], ["B", "C"], ["A", "C"]],
    )
    const atC = collect(nodes.get("C")!)
    nodes.get("A")!.broadcast({ type: "chat", body: "once" })
    await settle()
    expect(atC).toEqual([{ type: "chat", body: "once" }])
  })

  it("drops messages past the TTL hop limit", async () => {
    // Line A–B–C–D with ttl=2: A's broadcast reaches B and C but not D.
    const nodes = await buildMesh<Chat>(
      ["A", "B", "C", "D"],
      [["A", "B"], ["B", "C"], ["C", "D"]],
      () => ({ broadcast: { ttl: 2 } }),
    )
    const atC = collect(nodes.get("C")!)
    const atD = collect(nodes.get("D")!)
    nodes.get("A")!.broadcast({ type: "chat", body: "limited" })
    await settle()
    expect(atC).toHaveLength(1)
    expect(atD).toHaveLength(0)
  })
})

describe("topics (pub/sub)", () => {
  it("scopes delivery to subscribers and relays through non-subscribers", async () => {
    const nodes = await buildMesh(["A", "B", "C"], [["A", "B"], ["B", "C"]])
    const roomA = nodes.get("A")!.topic<{ body: string }>("chat/general")
    const roomC = nodes.get("C")!.topic<{ body: string }>("chat/general")
    // B relays but does not subscribe.
    const bDirect = collect(nodes.get("B")!)
    await settle() // let subscriptions gossip

    const atC: string[] = []
    roomC.on("message", m => atC.push(m.body))
    roomA.publish({ body: "hi room" })
    await settle()

    expect(atC).toEqual(["hi room"])
    expect(bDirect).toHaveLength(0) // B got no direct/topic message surfaced
    expect(roomA.peers.has("C")).toBe(true) // learned C's subscription via gossip
  })

  it("leave() stops delivery", async () => {
    const nodes = await buildMesh(["A", "B"], [["A", "B"]])
    const roomA = nodes.get("A")!.topic<{ body: string }>("t")
    const roomB = nodes.get("B")!.topic<{ body: string }>("t")
    await settle()
    const got: string[] = []
    roomB.on("message", m => got.push(m.body))

    roomA.publish({ body: "first" })
    await settle()
    roomB.leave()
    roomA.publish({ body: "second" })
    await settle()

    expect(got).toEqual(["first"])
  })
})

describe("signed broadcasts", () => {
  it("delivers signed broadcasts across a line", async () => {
    const signers = new Map<string, ECDSASigner>()
    for (const id of ["A", "B", "C"]) signers.set(id, new ECDSASigner())
    await Promise.all([...signers.values()].map(s => s.ready))
    const ids = [...signers.values()].map(s => s.id)
    const [A, B, C] = ids as [string, string, string]

    const nodes = await buildMesh<Chat>(
      [A, B, C],
      [[A, B], [B, C]],
      id => ({ signer: [...signers.values()].find(s => s.id === id)!, signedBroadcasts: true }),
    )
    const atC = collect(nodes.get(C)!)
    nodes.get(A)!.broadcast({ type: "chat", body: "signed" })
    await settle()
    expect(atC).toEqual([{ type: "chat", body: "signed" }])
  })
})

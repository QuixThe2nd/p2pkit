import { describe, it, expect } from "vitest"
import type { DiscoveryHost } from "../src/discovery/index.js"
import { GossipDiscovery } from "../src/discovery/index.js"
import { buildMesh } from "./helpers/mesh.js"

const settle = (ms = 60) => new Promise(r => setTimeout(r, ms))

describe("GossipDiscovery (unit)", () => {
  it("shares its peer list on connect and connects to unknown ids", () => {
    const connected: string[] = []
    const sent: Array<{ to: string; peers: string[] }> = []
    let onConn!: (p: string) => void
    let onGoss!: (from: string, peers: string[]) => void

    const host: DiscoveryHost = {
      self: "self",
      peerIds: () => ["self", "known"],
      connect: p => connected.push(p),
      sendGossip: (to, peers) => sent.push({ to, peers }),
      onPeerConnected: h => ((onConn = h), () => {}),
      onGossip: h => ((onGoss = h), () => {}),
    }

    const gossip = new GossipDiscovery()
    gossip.start(host)

    // A peer connects → we share everyone else we know (excluding that peer).
    onConn("known")
    expect(sent).toEqual([{ to: "known", peers: ["self"] }])

    // Inbound gossip → connect to ids we don't already know (skip self + known).
    onGoss("known", ["self", "known", "newA", "newB"])
    expect(connected).toEqual(["newA", "newB"])
  })

  it("introduces a newly connected peer to the peers it was already linked to", () => {
    // Without this half of the trade a peer joining through one link stays
    // invisible to the rest of the mesh: nobody dials an id they never heard,
    // and a peer that has never heard of us certainly never offers to us.
    const sent: Array<{ to: string; peers: string[] }> = []
    let onConn!: (p: string) => void

    const host: DiscoveryHost = {
      self: "self",
      peerIds: () => ["known", "newcomer"],
      connect: () => {},
      sendGossip: (to, peers) => sent.push({ to, peers }),
      onPeerConnected: h => ((onConn = h), () => {}),
      onGossip: () => () => {},
    }

    new GossipDiscovery().start(host)

    onConn("newcomer")

    // "known" hears about the newcomer; the newcomer gets our full list.
    expect(sent).toEqual([
      { to: "known", peers: ["newcomer"] },
      { to: "newcomer", peers: ["known"] },
    ])
  })
})

describe("GossipDiscovery (mesh fan-out)", () => {
  it("bridges non-adjacent peers into a direct connection", async () => {
    // A–B–C share signalling only along the edges; A and C never see each
    // other's announce. Gossip should still connect A directly to C.
    const nodes = await buildMesh(
      ["A", "B", "C"],
      [
        ["A", "B"],
        ["B", "C"],
      ],
      () => ({ discovery: new GossipDiscovery() }),
    )
    await settle(120) // allow a couple of exchange rounds

    const a = nodes.get("A")!
    const c = nodes.get("C")!
    expect(a.peers.has("C")).toBe(true)
    expect(c.peers.has("A")).toBe(true)
  })

  it("tells the mesh about a peer that joined through a single link", async () => {
    // A–B are up; C joins late through B only. C is the largest id of the
    // three, so C is never the one to offer — unless A learns C exists and
    // dials it, the link cannot form at all.
    const nodes = await buildMesh(
      ["A", "B", "C"],
      [
        ["A", "B"],
        ["B", "C"],
      ],
      () => ({ discovery: new GossipDiscovery() }),
      { late: ["C"] },
    )
    await settle(120)

    const a = nodes.get("A")!
    const c = nodes.get("C")!
    expect(a.peers.has("C")).toBe(false) // nobody has heard of C yet

    await c.start()
    await settle(120)

    expect(a.peers.has("C")).toBe(true)
    expect(c.peers.has("A")).toBe(true)
  })
})

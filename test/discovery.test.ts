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
})

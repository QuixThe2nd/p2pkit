import { describe, it, expect, afterEach } from "vitest"
import wrtc from "@roamhq/wrtc"
import type { P2PKitOptions } from "../src/index.js"
import { P2PKit } from "../src/index.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import {
  createEdgeSignallingServer,
  type EdgeSignallingServer,
} from "./helpers/signalling-server.js"
import type { Frame } from "../src/wire/index.js"
import type { PeerId } from "../src/utils/types.js"

const settle = (ms = 200) => new Promise(r => setTimeout(r, ms))

/**
 * The out-of-band proof for library-owned mesh peering: nodes are built with
 * `bootstrap` and nothing else — no gossip or broker switches, no manual
 * connect — over real WebRTC data channels. The lobby fixture decides which
 * peers can ever signal directly; once it dies, the library must grow the mesh
 * over the links that remain.
 */
describe("bootstrap mesh heals without the lobby (real WebRTC)", () => {
  let server: EdgeSignallingServer
  let backend: RTCBackend
  const kits: P2PKit<{ body: string }>[] = []
  /** sig-relay frames each node was asked to carry, for the evidence log. */
  const relayLog: Array<{ via: string; carrier: string; to: string; signal: string }> = []

  afterEach(async () => {
    for (const kit of kits.splice(0)) kit.stop()
    await server?.close()
    relayLog.length = 0
  })

  const setup = async (edges: Array<[string, string]>) => {
    server = await createEdgeSignallingServer(edges)
    backend = await getRTC(wrtc as never)
  }

  const start = async (id: string, options: Partial<P2PKitOptions> = {}) => {
    const kit = new P2PKit<{ body: string }>({
      self: id,
      bootstrap: { kind: "lobby", url: server.url },
      backend,
      iceServers: [],
      ...options,
    })
    // Tap the broker's carriage: what signal each relay frame carries, and to
    // whom it is addressed — the evidence that the handshake rode peer links.
    const sendTo = kit.sendTo.bind(kit)
    kit.sendTo = (peer: PeerId, frame: Frame) => {
      if (frame.k === "sig-relay") {
        const signal = frame.signal
        const kind =
          "description" in signal
            ? `description:${signal.description.type}`
            : "iceCandidate" in signal
              ? "iceCandidate"
              : "announce"
        relayLog.push({ via: id, carrier: peer, to: frame.to, signal: kind })
      }
      sendTo(peer, frame)
    }
    await kit.start()
    kits.push(kit)
    return kit
  }

  const waitFor = async (cond: () => boolean, timeoutMs = 20000) => {
    const deadline = Date.now() + timeoutMs
    while (!cond() && Date.now() < deadline) await settle(100)
    if (!cond()) {
      console.log(
        "waitFor state:",
        kits.map(k => `${k.selfId}: linked=${linked(k).join("+")} slots=${k.peerIds().join("+")}`).join(" | "),
      )
    }
    expect(cond()).toBe(true)
  }

  const linked = (kit: P2PKit<{ body: string }>) => kit.linkedPeers().sort()

  /**
   * The line A–B–C with the A–C introduction blocked by the fixture: A–B and
   * B–C form through the lobby, A–C cannot. Then the lobby dies and A–C must
   * form on its own, through B. Run for both start orders.
   */
  const lineHeals = async (order: [string, string, string]) => {
    await setup([
      ["a", "b"],
      ["b", "c"],
    ])
    const nodes = new Map<string, P2PKit<{ body: string }>>()
    for (const id of order) nodes.set(id, await start(id))
    const a = nodes.get("a")!
    const b = nodes.get("b")!
    const c = nodes.get("c")!

    // A–B and B–C connect; A–C is absent — gossip may have named C to A, but
    // no signal can cross the fixture, so no link exists.
    await waitFor(() => linked(a).length === 1 && linked(b).length === 2 && linked(c).length === 1)
    expect(linked(a)).toEqual(["b"])
    expect(linked(c)).toEqual(["b"])
    expect(a.peers.get("c")?.connected ?? false).toBe(false)
    expect(c.peers.get("a")?.connected ?? false).toBe(false)

    // The lobby dies. Nothing is re-seeded, restarted, or re-announced by the
    // test: the mesh itself must close the gap.
    relayLog.length = 0
    await server.close()

    const atC = new Promise<string>(resolve =>
      c.on("message", (msg, peer) => {
        if (peer.remote === "a") resolve(msg.body)
      }),
    )
    const atA = new Promise<string>(resolve =>
      a.on("message", (msg, peer) => {
        if (peer.remote === "c") resolve(msg.body)
      }),
    )
    await waitFor(() => Boolean(a.peers.get("c")?.connected && c.peers.get("a")?.connected))

    await a.peers.get("c")!.send({ body: `a to c [${order.join(",")}]` })
    await c.peers.get("a")!.send({ body: "c to a" })
    expect(await atC).toBe(`a to c [${order.join(",")}]`)
    expect(await atA).toBe("c to a")

    // The handshake rode B's links both ways: an offer and an answer carried,
    // with ICE alongside, and none of it through the (dead) lobby.
    const carried = relayLog.filter(e => e.via === "b")
    console.log(
      `[line ${order.join(",")}] brokered signals:`,
      relayLog.map(e => `${e.via}~${e.carrier}>${e.to}:${e.signal}`).join(" "),
    )
    expect(carried.some(e => e.carrier === "c" && e.signal === "description:offer")).toBe(true)
    expect(carried.some(e => e.carrier === "a" && e.signal === "description:answer")).toBe(true)
    expect(carried.some(e => e.signal === "iceCandidate")).toBe(true)
  }

  it("closes the A–C gap through B after the lobby dies (a,b,c order)", async () => {
    await lineHeals(["a", "b", "c"])
  }, 40000)

  it("closes the A–C gap through B after the lobby dies (c,b,a order)", async () => {
    await lineHeals(["c", "b", "a"])
  }, 40000)

  it("repeats the heal to catch timing-dependent failures", async () => {
    for (let round = 0; round < 3; round++) {
      await lineHeals(["a", "b", "c"])
      for (const kit of kits.splice(0)) kit.stop()
      await settle(200)
    }
  }, 120000)

  it("does not let an unrelated first neighbour blackhole the negotiation", async () => {
    // A's first link is D, which leads nowhere; the route to C is through B.
    // With no learned route to C, A's broker fans the offer out over every
    // link — D's copy dies, B's copy is carried into C.
    await setup([
      ["a", "d"],
      ["a", "b"],
      ["b", "c"],
    ])
    const a = await start("a")
    const d = await start("d")
    await waitFor(() => linked(a).includes("d"))
    const b = await start("b")
    const c = await start("c")
    await waitFor(() => linked(a).includes("b") && linked(b).includes("c"))
    expect(linked(d)).toEqual(["a"])
    expect(a.peers.get("c")?.connected ?? false).toBe(false)

    relayLog.length = 0
    await server.close()

    const atC = new Promise<string>(resolve =>
      c.on("message", (msg, peer) => {
        if (peer.remote === "a") resolve(msg.body)
      }),
    )
    await waitFor(() => Boolean(a.peers.get("c")?.connected && c.peers.get("a")?.connected))
    await a.peers.get("c")!.send({ body: "around the blackhole" })
    expect(await atC).toBe("around the blackhole")

    console.log(
      "[wrong-neighbour] brokered signals:",
      relayLog.map(e => `${e.via}~${e.carrier}>${e.to}:${e.signal}`).join(" "),
    )
    // The offer was fanned out from A over both links — D was handed a copy it
    // could do nothing with — and B carried the copy that reached C, then
    // carried the answer back.
    expect(
      relayLog.some(e => e.via === "a" && e.carrier === "d" && e.signal === "description:offer"),
    ).toBe(true)
    expect(
      relayLog.some(e => e.via === "a" && e.carrier === "b" && e.signal === "description:offer"),
    ).toBe(true)
    expect(
      relayLog.some(e => e.via === "b" && e.carrier === "c" && e.signal === "description:offer"),
    ).toBe(true)
    expect(
      relayLog.some(e => e.via === "b" && e.carrier === "a" && e.signal === "description:answer"),
    ).toBe(true)
  }, 40000)

  it("never closes the gap when brokered signalling is opted out", async () => {
    await setup([
      ["a", "b"],
      ["b", "c"],
    ])
    const a = await start("a", { brokeredSignalling: false })
    const b = await start("b", { brokeredSignalling: false })
    const c = await start("c", { brokeredSignalling: false })
    await waitFor(() => linked(a).length === 1 && linked(b).length === 2 && linked(c).length === 1)
    expect(a.peers.get("c")?.connected ?? false).toBe(false)

    await server.close()
    // Gossip still names C to A over the live links, but with the lobby dead
    // and no relay path, no signal can cross: the link must stay absent.
    await settle(5000)
    expect(a.peers.get("c")?.connected ?? false).toBe(false)
    expect(c.peers.get("a")?.connected ?? false).toBe(false)
  }, 30000)

  it("reports the lobby through bootstrap status, separately from peer links", async () => {
    await setup([["a", "b"]])
    const a = await start("a")
    const b = await start("b")
    const seen: string[] = []
    a.onBootstrapStatus(status => seen.push(status))

    await waitFor(() => linked(a).length === 1)
    expect(a.bootstrapStatus).toBe("up")

    await server.close()
    await waitFor(() => a.bootstrapStatus === "down", 10000)
    // The peer link outlives the lobby: bootstrap down says nothing about it.
    expect(linked(a)).toEqual(["b"])
    expect(seen).toContain("down")
  }, 30000)
})

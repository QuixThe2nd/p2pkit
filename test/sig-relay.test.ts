import { describe, it, expect, afterEach, vi } from "vitest"
import { WebSocketServer, type WebSocket } from "ws"
import type { AddressInfo } from "node:net"
import wrtc from "@roamhq/wrtc"
import type { P2PKitOptions } from "../src/index.js"
import { P2PKit } from "../src/index.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import { SignalBroker, type SignalBrokerHost } from "../src/signalling/index.js"
import { createMockSignallingServer, type MockSignallingServer } from "./helpers/signalling-server.js"
import { WIRE_VERSION, type Frame, type SigRelayFrame } from "../src/wire/index.js"

const settle = (ms = 200) => new Promise(r => setTimeout(r, ms))

interface EdgeSignallingServer {
  url: string
  close(): Promise<void>
}

/**
 * A signalling relay that only forwards along the given edges, so a test can
 * shape which peers ever hear each other's announce — a line A–B–C rather than
 * the full mesh the plain mock server builds.
 */
const createEdgeSignallingServer = async (edges: Array<[string, string]>): Promise<EdgeSignallingServer> => {
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 })
  const ids = new Map<WebSocket, string>()
  const linked = (x: string, y: string) =>
    edges.some(([p, q]) => (p === x && q === y) || (p === y && q === x))

  wss.on("connection", socket => {
    socket.on("message", data => {
      const raw = data.toString()
      let from = ids.get(socket)
      if (from === undefined) {
        try {
          const parsed = JSON.parse(raw) as { from?: string }
          if (typeof parsed.from !== "string") return
          from = parsed.from
        } catch {
          return
        }
        ids.set(socket, from)
      }
      for (const [peer, id] of [...ids]) {
        if (peer !== socket && linked(from, id) && peer.readyState === peer.OPEN) peer.send(raw)
      }
    })
    socket.on("close", () => ids.delete(socket))
  })

  await new Promise<void>(resolve => wss.on("listening", resolve))
  const { port } = wss.address() as AddressInfo
  return {
    url: `ws://127.0.0.1:${port}`,
    close: () =>
      new Promise<void>(resolve => {
        for (const socket of ids.keys()) socket.terminate()
        wss.close(() => resolve())
      }),
  }
}

/** A signalling envelope the way the lobby would carry it. */
const relayFrame = (over: Partial<SigRelayFrame> = {}): SigRelayFrame => ({
  v: WIRE_VERSION,
  k: "sig-relay",
  id: "relay-1",
  ttl: 1,
  from: "A",
  to: "C",
  signal: { description: { type: "offer", sdp: "v=0" }, from: "A", to: "C" },
  ...over,
})

interface Recorder {
  host: SignalBrokerHost
  sent: Array<{ to: string; frame: Frame }>
  linked: string[]
  arrive: (peer: string) => void
}

/** A broker host over an explicit list of links, recording every frame it is asked to send. */
const recorder = (self: string, linked: string[] = []): Recorder => {
  const rec: Recorder = {
    host: {
      self,
      peerIds: () => [...rec.linked],
      linkedPeers: () => [...rec.linked],
      sendTo: (to, frame) => rec.sent.push({ to, frame }),
      onPeerConnected: handler => {
        rec.arrive = handler
        return () => {}
      },
    },
    sent: [],
    linked,
    arrive: () => {},
  }
  return rec
}

/** Upstream standing in for a lobby: records sends, never relays anything back. */
const lobby = () => {
  const sent: unknown[] = []
  return {
    sent,
    channel: {
      ready: Promise.resolve(),
      send: (message: unknown) => sent.push(message),
      onMessage: () => () => {},
    },
  }
}

describe("SignalBroker (unit)", () => {
  it("delivers a relay addressed to this node", () => {
    const rec = recorder("C")
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    const seen: unknown[] = []
    broker.onMessage(m => seen.push(m))

    broker.ingest(relayFrame({ to: "C" }))

    expect(seen).toEqual([{ description: { type: "offer", sdp: "v=0" }, from: "A", to: "C" }])
    expect(rec.sent).toEqual([])
  })

  it("carries a relay one hop to a destination it is linked to", () => {
    const rec = recorder("B", ["C"])
    const broker = new SignalBroker(lobby().channel as never, rec.host)

    broker.ingest(relayFrame({ ttl: 1 }))

    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("C")
    expect(rec.sent[0]!.frame).toMatchObject({ k: "sig-relay", ttl: 0, from: "A", to: "C" })
  })

  it("does not carry a relay past its last hop", () => {
    const rec = recorder("B", ["X", "Y"])
    const broker = new SignalBroker(lobby().channel as never, rec.host)

    broker.ingest(relayFrame({ ttl: 0 }))

    expect(rec.sent).toEqual([])
  })

  it("does not flood a relay to neighbours it is not linked to", () => {
    const rec = recorder("B", ["X", "Y"]) // no link to C
    const broker = new SignalBroker(lobby().channel as never, rec.host)

    broker.ingest(relayFrame({ ttl: 1 }))

    expect(rec.sent).toEqual([])
    expect(broker.pendingCount).toBe(1)
  })

  it("sends a relay back along the link it arrived on", () => {
    // Linked to two peers, neither of them the destination: the answer has to
    // go back the way the offer came, or it lands on a peer that cannot
    // reach C either.
    const rec = recorder("B", ["A", "D"])
    const broker = new SignalBroker(lobby().channel as never, rec.host)

    broker.ingest(relayFrame({ ttl: 1 }), "A")

    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("A")
  })

  it("still prefers a direct link to the destination over the arrival link", () => {
    const rec = recorder("B", ["A", "C"])
    const broker = new SignalBroker(lobby().channel as never, rec.host)

    broker.ingest(relayFrame({ ttl: 1 }), "A")

    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("C")
  })

  it("sends a held relay once the destination links", () => {
    const rec = recorder("B", ["X"])
    const broker = new SignalBroker(lobby().channel as never, rec.host)
    broker.ingest(relayFrame({ ttl: 1 }))
    expect(rec.sent).toEqual([])

    rec.linked.push("C")
    rec.arrive("C")

    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("C")
    expect(broker.pendingCount).toBe(0)
  })

  it("drops a duplicate relay by id", () => {
    const rec = recorder("C")
    const broker = new SignalBroker(lobby().channel as never, rec.host)
    const seen: unknown[] = []
    broker.onMessage(m => seen.push(m))

    broker.ingest(relayFrame({ to: "C", id: "same" }))
    broker.ingest(relayFrame({ to: "C", id: "same" }))

    expect(seen).toHaveLength(1)
  })

  it("times out a relay whose destination never links", async () => {
    vi.useFakeTimers()
    try {
      const rec = recorder("B", ["X"])
      const broker = new SignalBroker(lobby().channel as never, rec.host, { relayTimeoutMs: 1000 })
      broker.ingest(relayFrame({ ttl: 1 }))
      expect(broker.pendingCount).toBe(1)

      vi.advanceTimersByTime(2000)

      expect(broker.pendingCount).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("passes signalling straight through while the lobby is up", async () => {
    const rec = recorder("A", ["B"])
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    await broker.ready
    const message = { description: { type: "offer", sdp: "v=0" }, from: "A", to: "B" } as const

    broker.send(message)

    expect(upstream.sent).toEqual([message])
    expect(rec.sent).toEqual([])
  })

  it("wraps an outbound signal in a relay once the lobby is down", async () => {
    const rec = recorder("A", ["B"])
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    await broker.ready
    broker.markLobbyDown()
    const message = { description: { type: "offer", sdp: "v=0" }, from: "A", to: "B" } as const

    broker.send(message)

    expect(upstream.sent).toEqual([])
    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("B")
    const frame = rec.sent[0]!.frame as SigRelayFrame
    expect(frame).toMatchObject({ k: "sig-relay", ttl: 1, from: "A", to: "B" })
    expect(frame.signal).toEqual(message)
  })

  it("sends a signal destined for a linked peer straight down that link", async () => {
    // A is linked to both B and C; an offer for B must not detour through C.
    const rec = recorder("A", ["B", "C"])
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    await broker.ready
    broker.markLobbyDown()

    broker.send({ description: { type: "offer", sdp: "v=0" }, from: "A", to: "C" })

    expect(rec.sent).toHaveLength(1)
    expect(rec.sent[0]!.to).toBe("C")
  })

  it("drops an announce when there is no lobby to address it to", async () => {
    const rec = recorder("A", ["B"])
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    await broker.ready
    broker.markLobbyDown()

    broker.send({ announce: true, from: "A" })

    expect(upstream.sent).toEqual([])
    expect(rec.sent).toEqual([])
  })

  it("settles ready when the lobby is reported down, even if it never answers", async () => {
    // A reconnecting channel retries forever rather than rejecting: without
    // this, `await ready` would hang and the node would never start.
    const rec = recorder("A", ["B"])
    const neverReady = { ready: new Promise<void>(() => {}), send: () => {}, onMessage: () => () => {} }
    const broker = new SignalBroker(neverReady as never, rec.host)
    broker.markLobbyDown()

    await broker.ready
    expect(broker.lobbyAlive).toBe(false)
  })

  it("goes back to passing through once the lobby is reported up again", async () => {    const rec = recorder("A", ["B"])
    const upstream = lobby()
    const broker = new SignalBroker(upstream.channel as never, rec.host)
    await broker.ready
    const message = { description: { type: "offer", sdp: "v=0" }, from: "A", to: "B" } as const

    broker.markLobbyDown()
    broker.send(message)
    expect(upstream.sent).toEqual([])
    expect(rec.sent).toHaveLength(1)

    broker.markLobbyUp()
    broker.send(message)

    expect(upstream.sent).toEqual([message])
    expect(rec.sent).toHaveLength(1) // nothing further relayed
  })
})

describe("brokered signalling over real WebRTC", () => {
  let server: MockSignallingServer | EdgeSignallingServer
  let backend: RTCBackend
  const kits: Array<{ dispose: () => Promise<void> }> = []

  afterEach(async () => {
    for (const kit of kits.splice(0)) await kit.dispose()
    await server?.close()
  })

  const setup = async (edges?: Array<[string, string]>) => {
    server = edges ? await createEdgeSignallingServer(edges) : await createMockSignallingServer()
    backend = await getRTC(wrtc as never)
  }

  const start = async <Msg>(id: string, options: Partial<P2PKitOptions> = {}) => {
    const kit = new P2PKit<Msg>({
      self: id,
      bootstrap: { kind: "lobby", url: server.url },
      brokeredSignalling: true,
      backend,
      iceServers: [],
      ...options,
    })
    await kit.start()
    kits.push({ dispose: () => Promise.resolve(kit.stop()) })
    return kit
  }

  const waitPeers = async <Msg>(kit: P2PKit<Msg>, count: number) => {
    const deadline = Date.now() + 15000
    while (kit.peerIds().length < count && Date.now() < deadline) await settle(50)
  }

  it("completes a handshake to a peer it is not linked to, relayed by a mutual neighbour", async () => {
    // A–B–C: the edges decide who ever hears whose announce, so A and C start
    // out knowing nothing of each other while B is linked to both.
    await setup([
      ["a", "b"],
      ["b", "c"],
    ])
    const a = await start<{ body: string }>("a")
    const b = await start<{ body: string }>("b")
    const c = await start<{ body: string }>("c")
    await waitPeers(a, 1)
    await waitPeers(b, 2)
    await waitPeers(c, 1)
    expect(a.peerIds()).toEqual(["b"])
    expect(b.peerIds().sort()).toEqual(["a", "c"])
    expect(c.peerIds()).toEqual(["b"])

    // The lobby goes away: from here on there is no room to announce into.
    await server.close()
    await settle(300)

    // A is seeded with C's id out of band — in the mesh that is gossip's job
    // (see the discovery tests). The offer can only travel over the A–B link,
    // so B has to carry it.
    const atC = new Promise<string>(resolve =>
      c.on("message", (msg, peer) => {
        if (peer.remote === "a") resolve(msg.body)
      }),
    )
    a.connect("c")
    const deadline = Date.now() + 20000
    while ((!a.peers.get("c")?.connected || !c.peers.get("a")?.connected) && Date.now() < deadline) {
      await settle(100)
    }
    expect(a.peers.get("c")?.connected).toBe(true)
    expect(c.peers.get("a")?.connected).toBe(true)

    await a.peers.get("c")!.send({ body: "relay works" })
    expect(await atC).toBe("relay works")
  }, 30000)

  it("leaves a live lobby in charge of signalling", async () => {
    await setup()
    const a = await start<{ body: string }>("a")
    const b = await start<{ body: string }>("b")
    await waitPeers(a, 1)
    await waitPeers(b, 1)

    expect(a.peerIds()).toEqual(["b"])
    expect(b.peerIds()).toEqual(["a"])

    const atB = new Promise<string>(resolve => b.on("message", msg => resolve(msg.body)))
    await a.peers.get("b")!.send({ body: "straight through" })
    expect(await atB).toBe("straight through")
  }, 30000)
})

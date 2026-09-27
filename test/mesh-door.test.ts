import { describe, it, expect, afterEach } from "vitest"
import wrtc from "@roamhq/wrtc"
import type { P2PKitOptions } from "../src/index.js"
import { P2PKit } from "../src/index.js"
import { DoorAcceptor } from "../src/transports/index.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import type { Frame, SigRelayFrame } from "../src/wire/index.js"
import type { PeerId } from "../src/utils/types.js"

const settle = (ms = 100) => new Promise(r => setTimeout(r, ms))

/** Structural, so it takes any `P2PKit<Msg>`. */
type AnyKit = { selfId: string; peers: Map<string, { connected: boolean }> }

const describeState = (kits: AnyKit[]) =>
  kits
    .map(k => {
      const peers = [...k.peers].map(([id, p]) => `${id}${p.connected ? "" : "*"}`).join(",")
      return `${k.selfId}=[${peers}]`
    })
    .join(" ")

/**
 * The proof the brief asks for: two cold browsers and a server that is only a
 * peer with a WebSocket it listens on. No relay process exists anywhere in this
 * file — the door socket is the server's own kit, the frames on it are the
 * standard mesh frame set, and everything past that first link is ordinary mesh.
 */
describe("door-only mesh (real WebRTC)", () => {
  let backend: RTCBackend
  const kits: P2PKit<{ body: string }>[] = []
  let door: DoorAcceptor
  /** sig-relay frames the server was asked to carry — the carriage evidence. */
  const relayLog: Array<{ carrier: string; to: string; signal: string }> = []

  const startDoorPeer = async (id: string, extra: Partial<P2PKitOptions> = {}) => {
    const kit = new P2PKit<{ body: string }>({ self: id, ...extra })
    // Tap the server's carriage: which signal it carries, and for whom.
    const sendTo = kit.sendTo.bind(kit)
    kit.sendTo = (peer: PeerId, frame: Frame) => {
      if (frame.k === "sig-relay") {
        const signal = (frame as SigRelayFrame).signal
        relayLog.push({
          carrier: peer,
          to: (frame as SigRelayFrame).to,
          signal:
            "description" in signal
              ? `description:${signal.description.type}`
              : "iceCandidate" in signal
                ? "iceCandidate"
                : "announce",
        })
      }
      sendTo(peer, frame)
    }
    door = new DoorAcceptor({ host: kit, port: 0 })
    await door.listen()
    await kit.start()
    kits.push(kit)
    return kit
  }

  const startClient = async (id: string, extra: Partial<P2PKitOptions> = {}) => {
    const kit = new P2PKit<{ body: string }>({
      self: id,
      bootstrap: { kind: "door", url: door.url },
      // Reconnect fast enough for a test, slow enough to stay deterministic.
      doorOptions: { minBackoffMs: 50, maxBackoffMs: 200 },
      backend,
      iceServers: [],
      ...extra,
    })
    await kit.start()
    kits.push(kit)
    return kit
  }

  const waitFor = async (cond: () => boolean, label: string, timeoutMs = 25_000) => {
    const deadline = Date.now() + timeoutMs
    while (!cond() && Date.now() < deadline) await settle(100)
    if (!cond()) console.log(`waitFor ${label}:`, describeState(kits))
    expect(cond()).toBe(true)
  }

  afterEach(async () => {
    for (const kit of kits.splice(0)) kit.stop()
    await door?.close()
    relayLog.length = 0
  })

  it("joins two clients through a door and lets them link directly", { timeout: 45_000 }, async () => {
    backend = await getRTC(wrtc as never)
    await startDoorPeer("server")
    const server = kits[0]!
    const a = await startClient("a")
    const c = await startClient("c")

    // Both are linked to the server over the door, and the link is a peer slot
    // on the server's own kit — not a room, not a relay.
    await waitFor(
      () => a.peers.get("server")?.connected === true && c.peers.get("server")?.connected === true,
      "door links",
    )
    expect(a.peers.get("server")!.transportName).toBe("ws")
    expect([...server.peers.keys()]).toEqual(["a", "c"])

    // Gossip through the door introduces a to c; WebRTC closes the rest.
    await waitFor(
      () => a.peers.get("c")?.connected === true && c.peers.get("a")?.connected === true,
      "direct a<->c",
    )
    expect(a.peers.get("c")!.transportName).toBe("rtc")

    // A message a sent reaches c without the server in between.
    const heard = new Promise<string>(resolve =>
      c.on("message", (msg, peer) => {
        if (peer.remote === "a") resolve(msg.body)
      }),
    )
    a.broadcast({ body: "straight across" })
    expect(await heard).toBe("straight across")

    // And the introduction really was carried by the server peer: it relayed
    // the a<->c handshake because no other carrier exists in this topology.
    expect(relayLog.some(r => r.signal === "description:offer")).toBe(true)
  })

  it("recovers the whole mesh after the door link bounces", { timeout: 45_000 }, async () => {
    backend = await getRTC(wrtc as never)
    const server = await startDoorPeer("server")
    const a = await startClient("a")
    const c = await startClient("c")

    await waitFor(
      () =>
        a.peers.get("c")?.connected === true &&
        a.peers.get("server")?.connected === true &&
        c.peers.get("server")?.connected === true,
      "initial mesh",
    )

    // The server drops a's socket out from under it. The drop is observed as a
    // transition, not by polling: with a 50ms backoff a is down and back up
    // inside one poll interval, and the interesting proof is that the
    // transition happened at all.
    const stale = server.peers.get("a")!
    const sawDrop = new Promise<void>(resolve =>
      a.onBootstrapStatus(status => {
        if (status === "down") resolve()
      }),
    )
    stale.disconnect()
    await sawDrop

    // Nothing re-seeds anything: a's own attach loop must restore the link — a
    // fresh peer slot on both ends, not the one that was dropped — and the mesh
    // must re-form around it.
    await waitFor(
      () => a.peers.get("server")?.connected === true && a.peers.get("server") !== undefined,
      "a re-attached",
    )
    await waitFor(
      () =>
        a.bootstrapStatus === "up" &&
        server.peers.get("a") !== stale &&
        server.peers.get("a")?.connected === true &&
        a.peers.get("c")?.connected === true,
      "mesh re-formed",
    )

    const heard = new Promise<string>(resolve =>
      c.on("message", (msg, peer) => {
        if (peer.remote === "a") resolve(msg.body)
      }),
    )
    a.broadcast({ body: "after the bounce" })
    expect(await heard).toBe("after the bounce")
  })
})

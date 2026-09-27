import { describe, it, expect, afterEach } from "vitest"
import { P2PKit, type P2PKitOptions } from "../src/index.js"
import { DoorAcceptor, WSTransport } from "../src/transports/index.js"
import type { Transport } from "../src/transports/types.js"
import type { Frame, MsgFrame } from "../src/wire/index.js"
import { WIRE_VERSION } from "../src/wire/index.js"

const settle = (ms = 25) => new Promise(r => setTimeout(r, ms))

const describeState = (label: string, kits: P2PKit<unknown>[]) =>
  `${label}: ${kits
    .map(k => {
      const id = k.bootstrapStatus === undefined ? `${safeSelf(k)}?` : safeSelf(k)
      const peers = [...k.peers].map(([pid, p]) => `${pid}${p.connected ? "" : "*"}`).join(",")
      return `${id}=[${peers}]${k.bootstrapStatus ? `{${k.bootstrapStatus}}` : ""}`
    })
    .join(" ")}`

const safeSelf = (kit: P2PKit<unknown>): string => {
  try {
    return kit.selfId
  } catch {
    return "unstarted"
  }
}

const waitFor = async (
  cond: () => boolean,
  state: () => string,
  timeoutMs = 10_000,
) => {
  const deadline = Date.now() + timeoutMs
  while (!cond() && Date.now() < deadline) await settle(25)
  if (!cond()) console.log("waitFor state:", state())
  expect(cond()).toBe(true)
}

/**
 * The out-of-band proof for the door: a server that is only a peer with a
 * WebSocket it listens on, and clients whose bootstrap is that URL alone. There
 * is no relay process anywhere in these tests — the frames on the socket are the
 * standard mesh frame set, and the socket sits in `peers` like any other link.
 */
describe("ws door", () => {
  // Doors, backoff and reconnects all take real wall-clock time.
  const TIMEOUT = 20_000
  const kits: P2PKit<unknown>[] = []
  const doors: DoorAcceptor[] = []
  const state = () => describeState("door", kits)

  afterEach(async () => {
    for (const kit of kits.splice(0)) kit.stop()
    for (const door of doors.splice(0)) await door.close()
  })

  /** A listening peer. No signalling channel, no bootstrap — it only accepts. */
  const startDoorPeer = async <Msg = unknown>(
    id: string,
    extra: Partial<P2PKitOptions> = {},
    doorExtra: Partial<{ path: string }> = {},
  ): Promise<{ kit: P2PKit<Msg>; door: DoorAcceptor }> => {
    const kit = new P2PKit<Msg>({ self: id, ...extra })
    const door = new DoorAcceptor({ host: kit, port: 0, ...doorExtra })
    await door.listen()
    await kit.start()
    kits.push(kit as P2PKit<unknown>)
    doors.push(door)
    return { kit, door }
  }

  /** A cold client: the door URL is the only thing it knows. */
  const startClient = async <Msg = unknown>(
    id: string,
    url: string,
    extra: Partial<P2PKitOptions> = {},
  ): Promise<P2PKit<Msg>> => {
    const kit = new P2PKit<Msg>({
      self: id,
      bootstrap: { kind: "door", url },
      // Reconnect fast enough to test, slow enough to stay deterministic.
      doorOptions: { minBackoffMs: 25, maxBackoffMs: 100 },
      ...extra,
    })
    kits.push(kit as P2PKit<unknown>)
    // `start` does not wait for the link (a door that is down must not hang it),
    // so the tests keep using `waitFor` on `bootstrapStatus` and `peers`.
    await kit.start()
    return kit
  }

  it("sends a welcome both ways and reports the counterpart's id", { timeout: TIMEOUT }, async () => {
    const { door } = await startDoorPeer("server")
    const client = new WSTransport({ url: door.url, self: "a", keepAliveMs: 0 })
    expect(await client.identified).toBe("server")
    expect(client.remote).toBe("server")
    expect(client.connected).toBe(true)
    client.disconnect()
  })

  it("carries standard frames both ways across the socket", { timeout: TIMEOUT }, async () => {
    const { door } = await startDoorPeer("server")
    const client = await WSTransport.open({ url: door.url, self: "a", keepAliveMs: 0 })
    await waitFor(() => door.linkCount > 0, state)

    const server = [...(door as unknown as { links: Set<WSTransport> }).links][0]!
    const fromServer = new Promise<Frame>(resolve => client.on("message", resolve))
    const fromClient = new Promise<Frame>(resolve => server.on("message", resolve))

    await client.send({ v: WIRE_VERSION, k: "msg", body: { hello: 1 } })
    await server.send({ v: WIRE_VERSION, k: "msg", body: { hello: 2 } })

    expect(((await fromClient) as MsgFrame).body).toEqual({ hello: 1 })
    expect(((await fromServer) as MsgFrame).body).toEqual({ hello: 2 })
    client.disconnect()
  })

  it("drops frames with an unsupported wire version instead of delivering them", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer("server")
    const client = await WSTransport.open({ url: door.url, self: "a", keepAliveMs: 0 })
    // The door has adopted the link; the mesh handshake cannot complete here
    // because a bare transport has no `Peer` to answer it, and the test does not
    // need it to.
    await waitFor(() => server.peers.has("a"), state)
    await settle(120)

    // Subscribe only once the link is quiet, so what arrives next is the frame
    // under test and not the handshake still in flight.
    const seen: Frame[] = []
    client.on("message", frame => seen.push(frame))
    await settle(120)
    seen.length = 0

    const socket = (client as unknown as { socket: { send(data: string): void } }).socket
    socket.send(JSON.stringify({ v: 99, k: "msg", body: 1 }))
    await settle(120)

    expect(seen).toEqual([])
    expect(client.connected).toBe(true) // the bad frame did not kill the link
    client.disconnect()
  })

  it("reports a drop through disconnect", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer("server")
    const client = await WSTransport.open({ url: door.url, self: "a", keepAliveMs: 0 })
    await waitFor(() => server.peers.has("a"), state)

    const dropped = new Promise<void>(resolve => client.on("disconnect", resolve))
    server.peers.get("a")!.disconnect()
    await dropped
    expect(client.connected).toBe(false)
  })

  it("turns an accepted connection into a peer of the listening kit", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer("server")
    await startClient("a", door.url)

    await waitFor(() => server.peers.get("a")?.connected === true, state)
    const client = kits[kits.length - 1]!
    await waitFor(() => client.peers.get("server")?.connected === true, state)
    expect([...server.peers.keys()]).toEqual(["a"])
    expect([...client.peers.keys()]).toEqual(["server"])
  })

  it("moves application traffic over the door link", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer<{ body: string }>("server")
    const client = await startClient<{ body: string }>("a", door.url)
    const heard = new Promise<string>(resolve =>
      server.on("message", msg => resolve(msg.body)),
    )
    await waitFor(() => client.peers.get("server")?.connected === true, state)
    await client.peers.get("server")!.send({ body: "over the door" })
    expect(await heard).toBe("over the door")
  })

  it("re-attaches when the door link drops", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer("server")
    const client = await startClient("a", door.url)
    await waitFor(() => client.bootstrapStatus === "up", state)
    await waitFor(() => server.peers.get("a")?.connected === true, state)

    // The server side drops the socket out from under the client.
    server.peers.get("a")!.disconnect()
    await waitFor(() => client.bootstrapStatus === "down", state)
    // Nothing re-seeds the client here: its own attach loop must bring the
    // link back on its own.
    await waitFor(() => client.bootstrapStatus === "up", state)
    await waitFor(() => server.peers.get("a")?.connected === true, state)
    expect(client.peers.get("server")?.connected).toBe(true)
  })

  it("replaces a stale link when the same peer dials twice", { timeout: TIMEOUT }, async () => {
    const { kit: server, door } = await startDoorPeer("server")
    await startClient("a", door.url)
    await waitFor(() => server.peers.get("a")?.connected === true, state)
    const first = server.peers.get("a")!

    // A second socket arrives for an id the door already holds.
    const second = await WSTransport.open({ url: door.url, self: "a", keepAliveMs: 0 })
    server.acceptLink(second)
    await waitFor(() => server.peers.get("a") !== first, state)
    expect(first.connected).toBe(false)
    await settle()
  })

  it("rejects a door that never identifies itself", { timeout: TIMEOUT }, async () => {
    const transport = new WSTransport({
      url: "ws://127.0.0.1:1/no-door",
      self: "a",
      identifyTimeoutMs: 60,
      keepAliveMs: 0,
    })
    await expect(transport.identified).rejects.toThrow()
    expect(transport.connected).toBe(false)
  })

  it("scopes an acceptor to its URL path", { timeout: TIMEOUT }, async () => {
    const { door } = await startDoorPeer("server")
    const scoped = new DoorAcceptor({ host: kits[0]!, port: 0, path: "/mesh" })
    await scoped.listen()
    doors.push(scoped)
    void door

    const outsider = new WSTransport({ url: `${scoped.url}/other`, self: "x", keepAliveMs: 0 })
    await expect(outsider.identified).rejects.toThrow()

    const member = new WSTransport({ url: `${scoped.url}/mesh`, self: "a", keepAliveMs: 0 })
    expect(await member.identified).toBe("server")
    member.disconnect()
  })

  it("refuses a bootstrap kind it does not know", { timeout: TIMEOUT }, () => {
    expect(
      () =>
        new P2PKit({
          bootstrap: { kind: "lobbyx", url: "ws://x" } as unknown as P2PKitOptions["bootstrap"],
        }),
    ).toThrow(/unsupported bootstrap kind/)
  })

  it("fans one door link out into a mesh over an injected transport", { timeout: TIMEOUT }, async () => {
    // Server plus two door-only clients. The clients reach each other through
    // transports the test injects, which keeps this suite off WebRTC while
    // still proving that a single door link is enough to learn the whole mesh.
    const pipes = new Map<string, Map<string, Pipe>>()
    const dial = (self: string) => (remote: string) => {
      const key = [self, remote].sort().join("::")
      let pair = pipes.get(key)
      if (!pair) {
        pair = new Map()
        pipes.set(key, pair)
        const [lo, hi] = self < remote ? [self, remote] : [remote, self]
        const toHi = new Pipe(hi)
        const toLo = new Pipe(lo)
        toHi.partner = toLo
        toLo.partner = toHi
        pair.set(lo, toHi)
        pair.set(hi, toLo)
      }
      return pair.get(self)!
    }

    const { door } = await startDoorPeer("server")
    const a = await startClient<{ body: string }>("a", door.url, {
      createTransport: ({ self, remote }) => dial(self)(remote),
    })
    const c = await startClient<{ body: string }>("c", door.url, {
      createTransport: ({ self, remote }) => dial(self)(remote),
    })

    await waitFor(() => a.peers.get("server")?.connected === true, state)
    await waitFor(() => c.peers.get("server")?.connected === true, state)
    // Gossip through the door introduces a to c; the injected pipe carries it.
    await waitFor(
      () => a.peers.get("c")?.connected === true && c.peers.get("a")?.connected === true,
      state,
    )

    const heard = new Promise<string>(resolve =>
      c.on("message", msg => resolve(msg.body)),
    )
    a.broadcast({ body: "one link was enough" })
    expect(await heard).toBe("one link was enough")
  })
})

/** An always-open transport a test hands to both ends of one edge. */
class Pipe implements Transport<Frame> {
  readonly name = "pipe"
  bufferedAmount = 0
  partner?: Pipe
  private readonly handlers = new Map<string, Set<(...args: unknown[]) => void>>()
  private readonly buffered: Frame[] = []
  private closed = false
  constructor(readonly remote: string) {}
  on(event: string, handler: (...args: unknown[]) => void): void {
    const set = this.handlers.get(event) ?? this.handlers.set(event, new Set()).get(event)!
    set.add(handler)
    if (event === "connect") setTimeout(() => handler(), 0)
    // A frame that outruns the consumer's subscription is held, not dropped —
    // the same contract WSTransport keeps.
    if (event === "message") {
      for (const frame of this.buffered.splice(0)) handler(frame)
    }
  }
  async send(frame: Frame): Promise<void> {
    const partner = this.partner
    if (this.closed || !partner) return
    queueMicrotask(() => {
      const handlers = partner.handlers.get("message")
      if (handlers && handlers.size > 0) {
        for (const handler of handlers) handler(frame)
      } else if (partner.buffered.length < 256) {
        partner.buffered.push(frame)
      }
    })
  }
  disconnect(): void {
    if (this.closed) return
    this.closed = true
    for (const handler of this.handlers.get("disconnect") ?? []) handler()
  }
}

import { describe, it, expect } from "vitest"
import { HTTPTransport } from "../src/transports/http.js"
import { Peer } from "../src/index.js"

const collect = (t: HTTPTransport<unknown>) => {
  const msgs: unknown[] = []
  t.on("message", m => msgs.push(m))
  return msgs
}
const settle = (ms = 50) => new Promise(r => setTimeout(r, ms))

describe("HTTPTransport", () => {
  it("exchanges frames both directions between a listener and a dialer", async () => {
    const server = new HTTPTransport({ self: "S", remote: "C", listen: { port: 0 } })
    const port = await server.listening!
    const client = new HTTPTransport({
      self: "C",
      remote: "S",
      url: `http://127.0.0.1:${port}`,
      pollInterval: 20,
    })

    const atServer = collect(server)
    const atClient = collect(client)

    const serverConnected = new Promise<void>(r => server.on("connect", r))
    const clientConnected = new Promise<void>(r => client.on("connect", r))

    // Client → server (delivered on the request).
    await client.send({ hello: "from client" })
    await Promise.all([serverConnected, clientConnected])
    await settle()
    expect(atServer).toEqual([{ hello: "from client" }])

    // Server → client (queued, drained on the client's next poll).
    await server.send({ hello: "from server" })
    await settle(60)
    expect(atClient).toEqual([{ hello: "from server" }])

    server.disconnect()
    client.disconnect()
  })

  it("requires either listen or url", () => {
    expect(() => new HTTPTransport({ self: "a", remote: "b" })).toThrow(/listen.*url|url.*listen/)
  })

  it("carries a full Peer handshake and message", async () => {
    const st = new HTTPTransport({ self: "S", remote: "C", listen: { port: 0 } })
    const port = await st.listening!
    const ct = new HTTPTransport({
      self: "C",
      remote: "S",
      url: `http://127.0.0.1:${port}`,
      pollInterval: 15,
    })

    const serverPeer = new Peer<string>({ self: "S", remote: "C", transport: st })
    const clientPeer = new Peer<string>({ self: "C", remote: "S", transport: ct })

    const got = new Promise<string>(resolve => serverPeer.on("message", resolve))
    await Promise.all([serverPeer.ready, clientPeer.ready])
    await clientPeer.send("hi over http")

    expect(await got).toBe("hi over http")
    expect(clientPeer.transportName).toBe("http")

    serverPeer.disconnect()
    clientPeer.disconnect()
  })
})

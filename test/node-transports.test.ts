import { describe, it, expect } from "vitest"
import { createServer, connect, type Socket } from "node:net"
import { UTPTransport } from "../src/transports/utp.js"
import { DHTTransport, type DHTRPCSocket } from "../src/transports/dht.js"
import { mapPort } from "../src/nat/index.js"

const settle = (ms = 30) => new Promise(r => setTimeout(r, ms))

/** A loopback TCP socket pair — a Duplex stream that behaves like a µTP socket. */
function tcpPair(): Promise<[Socket, Socket]> {
  return new Promise((resolve, reject) => {
    const server = createServer(sock => {
      resolve([client, sock])
      server.close()
    })
    server.on("error", reject)
    let client: Socket
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as { port: number }
      client = connect(port, "127.0.0.1")
      client.on("error", reject)
    })
  })
}

describe("UTPTransport (over a duplex stream)", () => {
  it("frames and exchanges messages over a real socket", async () => {
    const [a, b] = await tcpPair()
    await settle() // both ends connected
    const ta = new UTPTransport({ self: "A", remote: "B", socket: a, connected: true })
    const tb = new UTPTransport({ self: "B", remote: "A", socket: b, connected: true })

    const atB: unknown[] = []
    tb.on("message", m => atB.push(m))
    const bConnected = new Promise<void>(r => tb.on("connect", r))
    await bConnected

    await ta.send({ hello: "utp" })
    await ta.send({ n: 2 })
    await settle()
    expect(atB).toEqual([{ hello: "utp" }, { n: 2 }])
    expect(tb.name).toBe("utp")

    const closed = new Promise<void>(r => tb.on("disconnect", r))
    ta.disconnect()
    await closed // remote close propagates
  })

  it("requires socket or connect", () => {
    expect(() => new UTPTransport({ self: "a", remote: "b" })).toThrow(/socket.*connect/)
  })
})

/** Two in-memory DHT-RPC sockets wired to each other, standing in for `bonana`. */
function rpcSocketPair(): [DHTRPCSocket, DHTRPCSocket] {
  const msg: [Array<(d: string) => void>, Array<(d: string) => void>] = [[], []]
  const closeH: [Array<() => void>, Array<() => void>] = [[], []]
  const make = (self: 0 | 1): DHTRPCSocket => {
    const peer = (self === 0 ? 1 : 0) as 0 | 1
    return {
      send: data => void queueMicrotask(() => msg[peer].forEach(h => h(data))),
      on: (event, handler) => {
        if (event === "message") msg[self].push(handler as (d: string) => void)
        else if (event === "close") closeH[self].push(handler as () => void)
        // connect/error are unused by the in-memory pair
      },
      close: () => closeH[self].forEach(h => h()),
    }
  }
  return [make(0), make(1)]
}

describe("DHTTransport (injected RPC socket)", () => {
  it("connects and exchanges frames through the socket adapter", async () => {
    const [sa, sb] = rpcSocketPair()
    const ta = new DHTTransport({ self: "A", remote: "B", openSocket: async () => sa })
    const tb = new DHTTransport({ self: "B", remote: "A", openSocket: async () => sb })

    const atB: unknown[] = []
    tb.on("message", m => atB.push(m))
    await new Promise<void>(r => ta.on("connect", r))

    await ta.send({ hello: "dht" })
    await settle()
    expect(atB).toEqual([{ hello: "dht" }])
    expect(tb.name).toBe("dht")
  })

  it("errors clearly when bonana is absent (default adapter)", async () => {
    const t = new DHTTransport({ self: "A", remote: "B" })
    const err = await new Promise<Error>(r => t.on("error", r))
    expect(err.message).toMatch(/bonana/)
  })
})

describe("mapPort", () => {
  it("errors clearly when nat-upnp is not installed", async () => {
    await expect(mapPort({ port: 20000, protocol: "udp" })).rejects.toThrow(/nat-upnp/)
  })
})

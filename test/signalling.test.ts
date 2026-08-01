import { describe, it, expect, afterEach } from "vitest"
import { WebSocketSignalling, type SignallingMessage } from "../src/signalling/index.js"
import { createMockSignallingServer, type MockSignallingServer } from "./helpers/signalling-server.js"

describe("WebSocketSignalling", () => {
  let server: MockSignallingServer
  const channels: WebSocketSignalling[] = []

  afterEach(async () => {
    for (const c of channels.splice(0)) c.close()
    await server?.close()
  })

  const connect = async () => {
    const c = new WebSocketSignalling(server.url)
    channels.push(c)
    await c.ready
    return c
  }

  it("relays a message from one peer to another", async () => {
    server = await createMockSignallingServer()
    const a = await connect()
    const b = await connect()

    const received = new Promise<SignallingMessage>(resolve => b.onMessage(resolve))
    a.send({ announce: true, from: "peer-a" })

    expect(await received).toEqual({ announce: true, from: "peer-a" })
  })

  it("does not echo a message back to its sender", async () => {
    server = await createMockSignallingServer()
    const a = await connect()
    await connect()

    let echoed = false
    a.onMessage(() => (echoed = true))
    a.send({ announce: true, from: "peer-a" })
    await new Promise(r => setTimeout(r, 50))
    expect(echoed).toBe(false)
  })

  it("queues sends made before the socket opens", async () => {
    server = await createMockSignallingServer()
    const b = await connect()
    const received = new Promise<SignallingMessage>(resolve => b.onMessage(resolve))

    // Send immediately, before `ready` resolves.
    const a = new WebSocketSignalling(server.url)
    channels.push(a)
    a.send({ announce: true, from: "early" })

    expect(await received).toEqual({ announce: true, from: "early" })
  })
})

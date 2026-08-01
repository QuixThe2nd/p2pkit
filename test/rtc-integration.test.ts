import { describe, it, expect, afterEach } from "vitest"
import wrtc from "@roamhq/wrtc"
import { Peer } from "../src/index.js"
import { ECDSASigner } from "../src/auth/index.js"
import { WebSocketSignalling } from "../src/signalling/index.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import { createMockSignallingServer, type MockSignallingServer } from "./helpers/signalling-server.js"

// The Phase 4 milestone gate: two Peers exchange a message end-to-end over a
// real WebRTC data channel, negotiated through a signalling server.
describe("RTCTransport + Peer over real WebRTC", () => {
  let server: MockSignallingServer
  const cleanup: Array<() => void> = []
  let backend: RTCBackend

  afterEach(async () => {
    for (const fn of cleanup.splice(0)) fn()
    await server?.close()
  })

  const setup = async () => {
    server = await createMockSignallingServer()
    backend = await getRTC(wrtc as never)
  }

  it("connects two peers and delivers a direct message", async () => {
    await setup()
    const sigA = new WebSocketSignalling(server.url)
    const sigB = new WebSocketSignalling(server.url)
    await Promise.all([sigA.ready, sigB.ready])

    const alice = new Peer<string>({
      self: "alice",
      remote: "bob",
      signalling: sigA,
      backend,
      iceServers: [],
    })
    const bob = new Peer<string>({
      self: "bob",
      remote: "alice",
      signalling: sigB,
      backend,
      iceServers: [],
    })
    cleanup.push(() => alice.disconnect(), () => bob.disconnect(), () => sigA.close(), () => sigB.close())

    const got = new Promise<string>(resolve => bob.on("message", resolve))
    await Promise.all([alice.ready, bob.ready])
    await alice.send("hello over webrtc")

    expect(await got).toBe("hello over webrtc")
  }, 20000)

  it("proves identity and reports latency with ECDSA signers", async () => {
    await setup()
    const sa = new ECDSASigner()
    const sb = new ECDSASigner()
    await Promise.all([sa.ready, sb.ready])

    const sigA = new WebSocketSignalling(server.url)
    const sigB = new WebSocketSignalling(server.url)
    await Promise.all([sigA.ready, sigB.ready])

    const alice = new Peer<string>({ remote: sb.id, signer: sa, signalling: sigA, backend, iceServers: [], encrypted: true })
    const bob = new Peer<string>({ remote: sa.id, signer: sb, signalling: sigB, backend, iceServers: [], encrypted: true })
    cleanup.push(() => alice.disconnect(), () => bob.disconnect(), () => sigA.close(), () => sigB.close())

    const got = new Promise<string>(resolve => bob.on("message", resolve))
    await Promise.all([alice.ready, bob.ready])
    expect(alice.remote).toBe(sb.id)

    await alice.send("encrypted hi")
    expect(await got).toBe("encrypted hi")

    // Latency probe should complete shortly after connect.
    await new Promise(r => setTimeout(r, 200))
    expect(typeof alice.latency).toBe("number")
  }, 20000)
})

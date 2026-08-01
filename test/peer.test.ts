import { describe, it, expect } from "vitest"
import { Peer } from "../src/index.js"
import { ECDSASigner } from "../src/auth/index.js"
import { memoryTransportPair } from "./helpers/memory-transport.js"

// Fast handshake/messaging tests over an in-memory transport (no WebRTC).
describe("Peer handshake (memory transport)", () => {
  it("completes a no-signer handshake and exchanges messages", async () => {
    const [ta, tb] = memoryTransportPair("alice", "bob")
    const alice = new Peer<string>({ self: "alice", remote: "bob", transport: ta })
    const bob = new Peer<string>({ self: "bob", remote: "alice", transport: tb })

    const got = new Promise<string>(resolve => bob.on("message", resolve))
    await Promise.all([alice.ready, bob.ready])
    await alice.send("hi bob")

    expect(await got).toBe("hi bob")
    expect(alice.remote).toBe("bob")
    expect(alice.transportName).toBe("memory")
  })

  it("verifies identity and encrypts messages with ECDSASigner", async () => {
    const sa = new ECDSASigner()
    const sb = new ECDSASigner()
    await Promise.all([sa.ready, sb.ready])
    const [ta, tb] = memoryTransportPair(sa.id, sb.id)

    const alice = new Peer<{ n: number }>({
      remote: sb.id,
      signer: sa,
      encrypted: true,
      transport: ta,
    })
    const bob = new Peer<{ n: number }>({
      remote: sa.id,
      signer: sb,
      encrypted: true,
      transport: tb,
    })

    const got = new Promise<{ n: number }>(resolve => bob.on("message", resolve))
    await Promise.all([alice.ready, bob.ready])

    // Proven identities.
    expect(alice.remote).toBe(sb.id)
    expect(bob.remote).toBe(sa.id)

    await alice.send({ n: 7 })
    expect(await got).toEqual({ n: 7 })
  })

  it("rejects a peer whose proven id differs from the expected remote", async () => {
    const sa = new ECDSASigner()
    const sb = new ECDSASigner()
    await Promise.all([sa.ready, sb.ready])
    const [ta, tb] = memoryTransportPair(sa.id, sb.id)

    // Alice expects a different remote id than Bob actually proves.
    const alice = new Peer({ self: sa.id, remote: "0xWRONG", signer: sa, transport: ta })
    const bob = new Peer({ remote: sa.id, signer: sb, transport: tb })
    void bob

    const err = await new Promise<Error>(resolve => alice.on("error", resolve))
    expect(err.message).toMatch(/claims id|proof failed/)
  })
})

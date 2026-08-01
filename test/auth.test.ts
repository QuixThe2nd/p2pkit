import { describe, it, expect, afterAll } from "vitest"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm } from "node:fs/promises"
import { ECDSASigner, NoopSigner, KeyManager, encrypt, decrypt } from "../src/auth/index.js"

describe("ECDSASigner", () => {
  it("derives a checksummed 0x address and signs/verifies", async () => {
    const signer = new ECDSASigner()
    await signer.ready
    expect(signer.id).toMatch(/^0x[0-9a-fA-F]{40}$/)

    const sig = await signer.sign("hello")
    expect(await signer.verify(sig, "hello", signer.id)).toBe(true)
  })

  it("rejects a signature over different data or from a different peer", async () => {
    const a = new ECDSASigner()
    const b = new ECDSASigner()
    await Promise.all([a.ready, b.ready])
    const sig = await a.sign("payload")
    expect(await a.verify(sig, "tampered", a.id)).toBe(false)
    expect(await a.verify(sig, "payload", b.id)).toBe(false)
    expect(await b.verify(sig, "payload", a.id)).toBe(true) // any verifier, correct signer
  })

  it("recovers the public key from a signature (for ECDH)", async () => {
    const a = new ECDSASigner()
    await a.ready
    const sig = await a.sign("nonce-123")
    const recovered = ECDSASigner.recoverPublicKey(sig, "nonce-123")
    expect(recovered).toEqual(a.publicKey)
  })

  it("two peers derive the same ECDH shared key", async () => {
    const a = new ECDSASigner()
    const b = new ECDSASigner()
    await Promise.all([a.ready, b.ready])
    const ka = a.sharedKeyWith(b.publicKey)
    const kb = b.sharedKeyWith(a.publicKey)
    expect(ka).toEqual(kb)
  })

  it("throws if id is read before an async keypair load resolves", async () => {
    const km = new KeyManager("not-ready-probe", { dir: join(tmpdir(), `p2pkit-nr-${Date.now()}`) })
    const s = new ECDSASigner(km)
    expect(() => s.id).toThrow(/not ready/) // keypair still loading
    await s.ready
    expect(s.id).toMatch(/^0x/)
  })
})

describe("NoopSigner", () => {
  it("uses the supplied id and trusts everything", async () => {
    const s = new NoopSigner("peer-x")
    expect(s.id).toBe("peer-x")
    expect(await s.verify("", "anything", "whoever")).toBe(true)
  })
})

describe("encrypt/decrypt", () => {
  it("round-trips under a shared key and fails on a wrong key", () => {
    const key = crypto.getRandomValues(new Uint8Array(32))
    const wrong = crypto.getRandomValues(new Uint8Array(32))
    const ct = encrypt(key, "secret message")
    expect(decrypt(key, ct)).toBe("secret message")
    expect(() => decrypt(wrong, ct)).toThrow()
  })
})

describe("KeyManager", () => {
  const dir = join(tmpdir(), `p2pkit-test-${Date.now()}`)
  afterAll(() => rm(dir, { recursive: true, force: true }))

  it("persists and reloads the same keypair by name", async () => {
    const km1 = new KeyManager("node-1", { dir })
    const addr1 = await km1.getAddress()
    const km2 = new KeyManager("node-1", { dir })
    const addr2 = await km2.getAddress()
    expect(addr2).toBe(addr1)
  })

  it("gives different names different keypairs", async () => {
    const a = await new KeyManager("a", { dir }).getAddress()
    const b = await new KeyManager("b", { dir }).getAddress()
    expect(a).not.toBe(b)
  })

  it("drives an ECDSASigner whose id matches the manager address", async () => {
    const km = new KeyManager("signer-key", { dir })
    const signer = new ECDSASigner(km)
    await signer.ready
    expect(signer.id).toBe(await km.getAddress())
  })
})

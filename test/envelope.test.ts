import { describe, it, expect } from "vitest"
import { SeenCache, broadcastSignPayload, pubSignPayload } from "../src/core/envelope.js"

describe("SeenCache", () => {
  it("reports duplicates and expires entries", async () => {
    const cache = new SeenCache(20)
    expect(cache.seen("a")).toBe(false)
    expect(cache.seen("a")).toBe(true) // duplicate
    await new Promise(r => setTimeout(r, 40))
    expect(cache.has("a")).toBe(false) // expired
    expect(cache.seen("a")).toBe(false) // fresh again
  })
})

describe("sign payloads", () => {
  it("are deterministic for equal fields", () => {
    const a = broadcastSignPayload({ from: "x", id: "1", ts: 5, nonce: "n", body: { m: 1 } })
    const b = broadcastSignPayload({ from: "x", id: "1", ts: 5, nonce: "n", body: { m: 1 } })
    expect(a).toBe(b)
  })
  it("differ when any field differs", () => {
    const base = { topic: "t", from: "x", seq: 1, nonce: "n", body: 1 }
    expect(pubSignPayload(base)).not.toBe(pubSignPayload({ ...base, seq: 2 }))
  })
})

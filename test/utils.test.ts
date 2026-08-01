import { describe, it, expect } from "vitest"
import {
  Emitter,
  ErrorTimeout,
  promiseWithTimeout,
  extractIP,
  DEFAULT_ICE_SERVERS,
  randomId,
} from "../src/utils/index.js"

describe("Emitter", () => {
  type Events = { hi: (name: string) => void }

  it("delivers events to handlers and supports unsubscribe", () => {
    const e = new Emitter<Events>()
    const seen: string[] = []
    const off = e.on("hi", n => seen.push(n))
    e.emit("hi", "a")
    off()
    e.emit("hi", "b")
    expect(seen).toEqual(["a"])
    expect(e.listenerCount("hi")).toBe(0)
  })

  it("once fires exactly once", () => {
    const e = new Emitter<Events>()
    let count = 0
    e.once("hi", () => count++)
    e.emit("hi", "x")
    e.emit("hi", "y")
    expect(count).toBe(1)
  })

  it("tolerates handlers that unsubscribe during emit", () => {
    const e = new Emitter<Events>()
    const seen: string[] = []
    const off1 = e.on("hi", () => off2())
    const off2 = e.on("hi", n => seen.push(n))
    e.emit("hi", "z")
    expect(seen).toEqual(["z"])
    off1()
  })
})

describe("promiseWithTimeout", () => {
  it("resolves with the value when in time", async () => {
    const r = await promiseWithTimeout(Promise.resolve(42), 50)
    expect(r).toBe(42)
  })

  it("resolves with ErrorTimeout when the deadline wins", async () => {
    const slow = new Promise<number>(res => setTimeout(() => res(1), 100))
    const r = await promiseWithTimeout(slow, 10)
    expect(r).toBeInstanceOf(ErrorTimeout)
    expect((r as ErrorTimeout).ms).toBe(10)
  })

  it("propagates a genuine rejection", async () => {
    await expect(promiseWithTimeout(Promise.reject(new Error("boom")), 50)).rejects.toThrow("boom")
  })
})

describe("extractIP", () => {
  it("returns the first non-placeholder connection address", () => {
    const sdp = ["v=0", "c=IN IP4 0.0.0.0", "a=foo", "c=IN IP4 203.0.113.7", "c=IN IP4 198.51.100.1"].join(
      "\r\n",
    )
    expect(extractIP(sdp)).toBe("203.0.113.7")
  })

  it("returns undefined when no usable address exists", () => {
    expect(extractIP("v=0\r\nc=IN IP4 0.0.0.0")).toBeUndefined()
  })
})

describe("misc utils", () => {
  it("ships public STUN defaults", () => {
    expect(DEFAULT_ICE_SERVERS.length).toBeGreaterThan(0)
    expect(DEFAULT_ICE_SERVERS[0]?.urls).toContain("stun:")
  })

  it("randomId is hex of the requested byte length and unique", () => {
    const a = randomId(16)
    const b = randomId(16)
    expect(a).toMatch(/^[0-9a-f]{32}$/)
    expect(a).not.toBe(b)
  })
})

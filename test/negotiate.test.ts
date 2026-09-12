import { describe, it, expect } from "vitest"
import {
  chooseTransport,
  capsFor,
  isInitiator,
  DEFAULT_TRANSPORT_ORDER,
} from "../src/transports/negotiate.js"

describe("transport negotiation", () => {
  it("picks the most-preferred transport both peers advertise", () => {
    expect(chooseTransport(["rtc", "http"], ["rtc", "utp"])).toBe("rtc")
    expect(chooseTransport(["http", "utp"], ["utp", "http"])).toBe("utp") // utp precedes http
    expect(chooseTransport(["http"], ["http", "dht"])).toBe("http")
  })

  it("returns undefined when there is no shared transport", () => {
    expect(chooseTransport(["rtc"], ["http"])).toBeUndefined()
    expect(chooseTransport([], ["rtc"])).toBeUndefined()
  })

  it("respects a custom preference order", () => {
    expect(chooseTransport(["rtc", "http"], ["rtc", "http"], ["http", "rtc"])).toBe("http")
  })

  it("derives caps from enabled transports in preference order", () => {
    expect(capsFor({ rtc: true, http: { port: 1 }, utp: false })).toEqual(["rtc", "http"])
    expect(capsFor({})).toEqual([])
  })

  it("chooses a deterministic initiator", () => {
    expect(isInitiator("a", "b")).toBe(true)
    expect(isInitiator("b", "a")).toBe(false)
    expect(DEFAULT_TRANSPORT_ORDER[0]).toBe("rtc")
  })
})

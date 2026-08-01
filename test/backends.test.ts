import { describe, it, expect } from "vitest"
import { getRTC } from "../src/backends/index.js"

describe("getRTC", () => {
  it("normalizes an explicit override", async () => {
    class FakePC {}
    const backend = await getRTC({ RTCPeerConnection: FakePC } as never)
    expect(backend.RTCPeerConnection).toBe(FakePC)
  })

  it("falls back to an installed backend (werift) under Node", async () => {
    // No global RTCPeerConnection and no @roamhq/wrtc in the test runtime, so
    // detection should resolve werift.
    const backend = await getRTC()
    expect(typeof backend.RTCPeerConnection).toBe("function")
  })
})

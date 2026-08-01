import { describe, it, expect } from "vitest"
import { FrameCodec, WireError, WIRE_VERSION, type Frame } from "../src/wire/index.js"

describe("FrameCodec", () => {
  it("round-trips a frame", () => {
    const frame: Frame = { v: WIRE_VERSION, k: "msg", body: { type: "chat", body: "hi" } }
    expect(FrameCodec.decode(FrameCodec.encode(frame))).toEqual(frame)
  })

  it("rejects invalid JSON", () => {
    expect(() => FrameCodec.decode("{not json")).toThrow(WireError)
  })

  it("rejects an unknown wire version", () => {
    expect(() => FrameCodec.decode(JSON.stringify({ v: 99, k: "msg" }))).toThrow(/wire version/)
  })

  it("rejects an unknown frame kind", () => {
    expect(() => FrameCodec.decode(JSON.stringify({ v: WIRE_VERSION, k: "nope" }))).toThrow(
      /frame kind/,
    )
  })
})

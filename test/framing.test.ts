import { describe, it, expect } from "vitest"
import { Chunker, type ChunkPacket } from "../src/framing/index.js"

describe("Chunker", () => {
  const roundTrip = (data: string, maxPacketSize: number) => {
    const tx = new Chunker({ maxPacketSize })
    const rx = new Chunker({ maxPacketSize })
    let result: string | undefined
    for (const wire of tx.split("g1", data)) {
      const packet = JSON.parse(wire) as ChunkPacket
      const out = rx.ingest(packet)
      if (out !== undefined) result = out
    }
    return result
  }

  it("round-trips a payload larger than the packet size", () => {
    const data = "x".repeat(100_000)
    expect(roundTrip(data, 16_000)).toBe(data)
  })

  it("passes a single-packet payload straight through", () => {
    expect(roundTrip("hello", 16_000)).toBe("hello")
  })

  it("handles the empty string", () => {
    expect(roundTrip("", 16_000)).toBe("")
  })

  it("reassembles out-of-order and ignores duplicates", () => {
    const tx = new Chunker({ maxPacketSize: 4 })
    const rx = new Chunker({ maxPacketSize: 4 })
    const packets = [...tx.split("g", "abcdefghij")].map(w => JSON.parse(w) as ChunkPacket)
    const shuffled = [packets[2]!, packets[0]!, packets[2]!, packets[1]!]
    let result: string | undefined
    for (const p of shuffled) {
      const out = rx.ingest(p)
      if (out !== undefined) result = out
    }
    expect(result).toBe("abcdefghij")
  })

  it("preserves multi-byte characters split across fragments", () => {
    const data = "😀".repeat(1000) // surrogate pairs, sliced by code unit
    expect(roundTrip(data, 3)).toBe(data)
  })
})

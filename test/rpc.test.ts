import { describe, it, expect } from "vitest"
import { z } from "zod"
import { Peer } from "../src/index.js"
import { defineAPI, defineProtocol, RPCError } from "../src/rpc/index.js"
import { memoryTransportPair } from "./helpers/memory-transport.js"
import { buildMesh } from "./helpers/mesh.js"

const api = defineAPI({
  getBlock: {
    request: z.object({ height: z.number().int().nonnegative() }),
    response: z.object({ hash: z.string(), txs: z.array(z.string()) }),
  },
  putBlock: {
    request: z.object({ hash: z.string(), txs: z.array(z.string()) }),
    response: z.object({ accepted: z.boolean() }),
  },
})

/** A connected pair where `bob` serves `router` and `alice` calls it. */
async function pair(
  router?: ReturnType<typeof api.router> | ReturnType<typeof api.router.partial>,
) {
  const [ta, tb] = memoryTransportPair("alice", "bob")
  const alice = new Peer({ self: "alice", remote: "bob", transport: ta })
  const bob = new Peer({ self: "bob", remote: "alice", transport: tb, router })
  await Promise.all([alice.ready, bob.ready])
  return { alice, bob }
}

describe("RPC request/response", () => {
  it("calls a method and returns the typed response", async () => {
    const router = api.router({
      getBlock: async ({ height }) => ({ hash: `h${height}`, txs: [`tx${height}`] }),
      putBlock: async ({ hash, txs }) => ({ accepted: txs.length > 0 && hash.length > 0 }),
    })
    const { alice } = await pair(router)
    const client = alice.client(api)

    const block = await client.getBlock({ height: 42 })
    expect(block).toEqual({ hash: "h42", txs: ["tx42"] })

    const put = await client.putBlock({ hash: "abc", txs: ["t1", "t2"] })
    expect(put).toEqual({ accepted: true })
  })

  it("passes the verified sender as ctx.from", async () => {
    let seen: string | undefined
    const router = api.router({
      getBlock: async ({ height }, ctx) => {
        seen = ctx.from
        return { hash: `h${height}`, txs: [] }
      },
      putBlock: async () => ({ accepted: false }),
    })
    const { alice } = await pair(router)
    await alice.client(api).getBlock({ height: 1 })
    expect(seen).toBe("alice")
  })

  it("answers unimplemented methods with a no_handler RPCError", async () => {
    const router = api.router.partial({
      getBlock: async ({ height }) => ({ hash: `h${height}`, txs: [] }),
    })
    const { alice } = await pair(router)
    const res = await alice.client(api).putBlock({ hash: "x", txs: [] })
    expect(res).toBeInstanceOf(RPCError)
    expect((res as RPCError).code).toBe("no_handler")
    expect((res as RPCError).method).toBe("putBlock")
  })

  it("returns no_handler when no router is configured at all", async () => {
    const { alice } = await pair(undefined)
    const res = await alice.client(api).getBlock({ height: 0 })
    expect(res).toBeInstanceOf(RPCError)
    expect((res as RPCError).code).toBe("no_handler")
  })

  it("rejects an invalid request locally without a round-trip", async () => {
    const router = api.router({
      getBlock: async () => ({ hash: "h", txs: [] }),
      putBlock: async () => ({ accepted: true }),
    })
    const { alice } = await pair(router)
    // height must be a non-negative integer.
    const res = await alice.client(api).getBlock({ height: -1 } as never)
    expect(res).toBeInstanceOf(RPCError)
    expect((res as RPCError).code).toBe("invalid_request")
  })

  it("surfaces a handler-thrown RPCError with its code", async () => {
    const router = api.router({
      getBlock: async () => {
        throw new RPCError("forbidden")
      },
      putBlock: async () => ({ accepted: true }),
    })
    const { alice } = await pair(router)
    const res = await alice.client(api).getBlock({ height: 5 })
    expect(res).toBeInstanceOf(RPCError)
    expect((res as RPCError).code).toBe("forbidden")
    expect((res as RPCError).method).toBe("getBlock")
  })

  it("times out when the peer never answers", async () => {
    const router = api.router({
      getBlock: () => new Promise(() => {}) as never, // never resolves
      putBlock: async () => ({ accepted: true }),
    })
    const [ta, tb] = memoryTransportPair("alice", "bob")
    const alice = new Peer({ self: "alice", remote: "bob", transport: ta, rpcTimeout: 30 })
    const bob = new Peer({ self: "bob", remote: "alice", transport: tb, router })
    await Promise.all([alice.ready, bob.ready])

    const res = await alice.client(api).getBlock({ height: 1 })
    expect(res).toBeInstanceOf(RPCError)
    expect((res as RPCError).code).toBe("timeout")
  })
})

describe("defineProtocol", () => {
  const protocol = defineProtocol({
    chat: z.object({ body: z.string() }),
    typing: z.object({ active: z.boolean() }),
  })

  it("parses and validates a well-formed message", () => {
    expect(protocol.parse({ type: "chat", body: "hi" })).toEqual({ type: "chat", body: "hi" })
    expect(protocol.validate({ type: "typing", active: true })).toEqual({
      type: "typing",
      active: true,
    })
  })

  it("rejects an unknown type or bad fields", () => {
    expect(protocol.validate({ type: "nope" })).toBeUndefined()
    expect(protocol.validate({ type: "chat", body: 123 })).toBeUndefined()
    expect(protocol.validate({ body: "no type" })).toBeUndefined()
    expect(() => protocol.parse({ type: "chat", body: 1 })).toThrow()
  })

  it("drops inbound mesh messages that fail the protocol", async () => {
    const nodes = await buildMesh(["A", "B"], [["A", "B"]], () => ({ protocol }))
    const got: unknown[] = []
    nodes.get("B")!.on("message", m => got.push(m))

    const a = nodes.get("A")!
    a.broadcast({ type: "chat", body: "ok" } as never)
    a.broadcast({ type: "chat", body: 123 } as never) // invalid fields
    a.broadcast({ type: "nope" } as never) // unknown type
    await new Promise(r => setTimeout(r, 40))

    expect(got).toEqual([{ type: "chat", body: "ok" }])
  })
})

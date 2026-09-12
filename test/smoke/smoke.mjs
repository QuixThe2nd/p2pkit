// Cross-runtime smoke test: plain ESM, no test framework, so it runs identically
// under Node, Bun (`bun test/smoke/smoke.mjs`) and Deno (`deno task smoke`).
// Imports only the dependency-free public entries from the built `dist/`, so no
// npm resolution or permissions are needed on any runtime.
import { Chunker } from "../../dist/framing.js"
import { randomId, promiseWithTimeout, ErrorTimeout } from "../../dist/utils.js"
import { defineAPI, defineProtocol, createRouter, RPCError } from "../../dist/rpc.js"

let checks = 0
function assert(cond, message) {
  checks++
  if (!cond) {
    console.error(`✗ ${message}`)
    // Non-zero exit fails the CI job on every runtime.
    if (typeof process !== "undefined") process.exit(1)
    throw new Error(message)
  }
}

// --- framing: split a large payload and reassemble it out of order ----------
{
  const chunker = new Chunker({ maxPacketSize: 8 })
  const data = "x".repeat(100) + "END"
  const packets = [...chunker.split("g1", data)].map(p => JSON.parse(p))
  assert(packets.length > 1, "framing: large payload splits into multiple packets")
  let full
  for (const p of packets.reverse()) full = chunker.ingest(p) ?? full
  assert(full === data, "framing: reassembles the original string")
}

// --- utils: randomId + promiseWithTimeout -----------------------------------
{
  const a = randomId(12)
  assert(a.length === 24 && a !== randomId(12), "utils: randomId is hex and unique")
  const timedOut = await promiseWithTimeout(new Promise(() => {}), 10)
  assert(timedOut instanceof ErrorTimeout, "utils: promiseWithTimeout returns ErrorTimeout")
  const value = await promiseWithTimeout(Promise.resolve(42), 10)
  assert(value === 42, "utils: promiseWithTimeout passes a resolved value through")
}

// --- rpc: defineProtocol validation (structural schema, no zod) -------------
{
  const str = { parse: v => { if (typeof v.body !== "string") throw new Error("bad"); return v } }
  const protocol = defineProtocol({ chat: str })
  assert(protocol.validate({ type: "chat", body: "hi" }) !== undefined, "rpc: protocol accepts valid")
  assert(protocol.validate({ type: "chat", body: 1 }) === undefined, "rpc: protocol rejects bad fields")
  assert(protocol.validate({ type: "nope" }) === undefined, "rpc: protocol rejects unknown type")
}

// --- rpc: defineAPI router dispatch -----------------------------------------
{
  const id = { parse: v => v }
  const api = defineAPI({ echo: { request: id, response: id } })
  const router = api.router({ echo: async (req, ctx) => ({ ...req, from: ctx.from }) })
  const ok = await router.dispatch("echo", { n: 1 }, { from: "peerA" })
  assert(ok.ok === true && ok.body.from === "peerA", "rpc: router dispatches and passes ctx.from")

  const missing = await createRouter(api.schema, {}).dispatch("echo", {}, { from: "x" })
  assert(missing.ok === false && missing.err.code === "no_handler", "rpc: missing handler → no_handler")
  assert(new RPCError("timeout", "echo") instanceof Error, "rpc: RPCError is an Error")
}

console.log(`✓ smoke: ${checks} checks passed on ${
  typeof Deno !== "undefined" ? "Deno" : typeof Bun !== "undefined" ? "Bun" : "Node"
}`)

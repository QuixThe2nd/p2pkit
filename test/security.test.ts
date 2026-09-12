import { afterEach, describe, expect, it, vi } from "vitest"
import { Peer, type MessageMetadata } from "../src/index.js"
import { ECDSASigner, NoopSigner } from "../src/auth/index.js"
import { encrypt, decrypt } from "../src/auth/crypto.js"
import { broadcastSignPayload, pubSignPayload } from "../src/core/envelope.js"
import {
  FrameCodec,
  WIRE_VERSION as v,
  type Frame,
  type PubFrame,
  type BcastFrame,
} from "../src/wire/index.js"
import type { Transport, TransportEvents } from "../src/transports/types.js"
import { Emitter } from "../src/utils/emitter.js"
import { memoryTransportPair } from "./helpers/memory-transport.js"
import { buildMesh } from "./helpers/mesh.js"

const cleanup: (() => void)[] = []
afterEach(() => {
  for (const stop of cleanup.splice(0)) stop()
  vi.restoreAllMocks()
})
const settle = () => new Promise(resolve => setTimeout(resolve, 20))

class ControlledTransport implements Transport<Frame> {
  readonly remote = "attacker"
  readonly bufferedAmount = 0
  readonly sent: Frame[] = []
  readonly events = new Emitter<TransportEvents<Frame>>()
  on<E extends keyof TransportEvents<Frame>>(event: E, handler: TransportEvents<Frame>[E]): void {
    this.events.on(event, handler)
  }
  async send(frame: Frame): Promise<void> {
    this.sent.push(frame)
  }
  disconnect(): void {
    this.events.emit("disconnect")
  }
}

async function controlled(signer?: {
  id: string
  sign(payload: string): Promise<string>
  verify(sig: string, payload: string, from: string): Promise<boolean>
}) {
  const transport = new ControlledTransport()
  const dispatch = vi.fn(async () => ({ ok: true as const, body: "response" }))
  const peer = new Peer({
    self: "victim",
    remote: "attacker",
    signer,
    transport,
    router: { dispatch, schema: {}, has: () => true },
  })
  cleanup.push(() => peer.disconnect())
  await settle()
  transport.events.emit("connect")
  await settle()
  const hello = transport.sent.find(f => f.k === "hello")!
  if (hello.k !== "hello") throw new Error("no hello")
  transport.events.emit("message", {
    v,
    k: "hello",
    from: "attacker",
    nonce: "challenge",
    caps: [],
  })
  await settle()
  return {
    peer,
    transport,
    dispatch,
    ack: { v, k: "ack" as const, from: "attacker", nonce: hello.nonce, sig: "proof" },
  }
}

describe("authentication boundary", () => {
  it("drops all application and control traffic before the handshake", async () => {
    const { peer, transport, dispatch, ack } = await controlled()
    const message = vi.fn(),
      frame = vi.fn()
    peer.on("message", message)
    peer.on("frame", frame)
    const frames: Frame[] = [
      { v, k: "req", id: "1", method: "steal", body: null },
      { v, k: "res", id: "1", ok: true, body: null },
      { v, k: "msg", body: "attack" },
      { v, k: "bcast", id: "1", from: "attacker", ttl: 2, body: "attack" },
      { v, k: "pub", topic: "t", from: "attacker", seq: 1, nonce: "n", ttl: 2, body: "attack" },
      { v, k: "sub", topic: "t", from: "attacker", id: "1" },
      { v, k: "gossip", peers: ["attacker"] },
      { v, k: "ping", id: "early" },
    ]
    for (const f of frames) transport.events.emit("message", f)
    await settle()
    expect(dispatch).not.toHaveBeenCalled()
    expect(message).not.toHaveBeenCalled()
    expect(frame).not.toHaveBeenCalled()
    expect(transport.sent.map(f => f.k)).toEqual(["hello", "ack"])
    transport.events.emit("message", ack)
    await peer.ready
    transport.events.emit("message", frames[0]!)
    await vi.waitFor(() => expect(dispatch).toHaveBeenCalledOnce())
    expect(dispatch.mock.calls[0]).toEqual(["steal", null, { from: "attacker" }])
  })

  it("never dispatches while verification is pending, or reopens after disconnect", async () => {
    let verify!: (ok: boolean) => void
    const { peer, transport, dispatch, ack } = await controlled({
      id: "victim",
      sign: async () => "sig",
      verify: () =>
        new Promise(resolve => {
          verify = resolve
        }),
    })
    const connected = vi.fn()
    peer.on("connect", connected)
    transport.events.emit("message", ack)
    transport.events.emit("message", { v, k: "req", id: "1", method: "steal", body: null })
    await settle()
    expect(dispatch).not.toHaveBeenCalled()
    peer.disconnect()
    verify(true)
    await expect(peer.ready).rejects.toThrow("disconnected")
    await settle()
    expect(connected).not.toHaveBeenCalled()
    expect(dispatch).not.toHaveBeenCalled()
    expect(peer.connected).toBe(false)
  })

  it("settles RPCs on disconnect before and after the handshake", async () => {
    for (const open of [false, true]) {
      const { peer, transport, ack } = await controlled()
      if (open) {
        transport.events.emit("message", ack)
        await peer.ready
      }
      const pending = peer.requestRPC("slow", null, 60_000)
      peer.disconnect()
      expect(await pending).toMatchObject({ code: "disconnected", method: "slow" })
      expect(await peer.requestRPC("again", null, 60_000)).toMatchObject({ code: "disconnected" })
      await expect(peer.send("late")).rejects.toThrow("disconnected")
    }
  })

  it("does not send a request that timed out waiting for authentication", async () => {
    const { peer, transport, ack } = await controlled()
    expect(await peer.requestRPC("expired", null, 5)).toMatchObject({ code: "timeout" })
    transport.events.emit("message", ack)
    await peer.ready
    await settle()
    expect(transport.sent.some(f => f.k === "req")).toBe(false)
  })

  it("converts transport send failures to RPC errors", async () => {
    const { peer, transport, ack } = await controlled()
    transport.events.emit("message", ack)
    await peer.ready
    await settle()
    transport.send = async () => {
      throw new Error("broken link")
    }
    expect(await peer.requestRPC("call", null, 60_000)).toMatchObject({
      code: "send_failed",
      method: "call",
    })
  })
})

async function encryptedPair() {
  const sa = new ECDSASigner(),
    sb = new ECDSASigner()
  await Promise.all([sa.ready, sb.ready])
  const [ta, tb] = memoryTransportPair(sa.id, sb.id)
  const sentA: Frame[] = [],
    sentB: Frame[] = []
  const rawA = ta.send.bind(ta),
    rawB = tb.send.bind(tb)
  ta.send = async f => {
    sentA.push(f)
    await rawA(f)
  }
  tb.send = async f => {
    sentB.push(f)
    await rawB(f)
  }
  const dispatch = vi.fn(async (method: string, body: unknown) =>
    method === "fail"
      ? { ok: false as const, err: { code: "secret_error", method, message: "private error" } }
      : { ok: true as const, body },
  )
  const alice = new Peer({ signer: sa, remote: sb.id, encrypted: true, transport: ta })
  const bob = new Peer({
    signer: sb,
    remote: sa.id,
    encrypted: true,
    transport: tb,
    router: { dispatch, schema: {}, has: () => true },
  })
  cleanup.push(
    () => alice.disconnect(),
    () => bob.disconnect(),
  )
  return { alice, bob, sa, sb, sentA, sentB, rawA, rawB, dispatch }
}

describe("common application encryption", () => {
  it("encrypts queued messages, RPC requests, successes, errors, and mesh frames", async () => {
    const { alice, bob, sentA, sentB } = await encryptedPair()
    const message = vi.fn(),
      frame = vi.fn()
    bob.on("message", message)
    bob.on("frame", frame)
    const queued = alice.send("private message")
    const response = alice.requestRPC("secret_method", "private body", 1000)
    await Promise.all([alice.ready, bob.ready, queued])
    expect(await response).toEqual({ ok: true, body: "private body" })
    expect(await alice.requestRPC("fail", null, 1000)).toMatchObject({
      ok: false,
      err: { code: "secret_error" },
    })
    await alice.sendFrame({ v, k: "gossip", peers: ["private peer"] })
    await settle()
    expect(message).toHaveBeenCalledWith("private message")
    expect(frame).toHaveBeenCalledWith({ v, k: "gossip", peers: ["private peer"] })
    for (const sent of [sentA, sentB]) {
      expect(sent.some(f => f.k === "sealed")).toBe(true)
      expect(sent.every(f => ["hello", "ack", "sealed"].includes(f.k))).toBe(true)
      expect(JSON.stringify(sent)).not.toMatch(/private|secret_method|secret_error/)
    }
  })

  it("drops plaintext, tampered ciphertext, nested envelopes, and encrypted handshakes", async () => {
    const { alice, bob, sa, sb, rawA, dispatch } = await encryptedPair()
    await Promise.all([alice.ready, bob.ready])
    const message = vi.fn(),
      frame = vi.fn()
    bob.on("message", message)
    bob.on("frame", frame)
    await rawA({ v, k: "msg", body: "plaintext" })
    await rawA({ v, k: "req", id: "bad", method: "bad", body: "plaintext" })
    await rawA({ v, k: "gossip", peers: ["bad"] })
    await rawA({ v, k: "sealed", body: "tampered" })
    const signature = await sb.sign("key")
    const key = sa.sharedKeyWith(ECDSASigner.recoverPublicKey(signature, "key"))
    for (const body of [
      { v, k: "sealed", body: "nested" },
      { v, k: "ack", from: sb.id, nonce: "bad", sig: "bad" },
      { v, k: "req", id: 42 },
    ])
      await rawA({ v, k: "sealed", body: encrypt(key, JSON.stringify(body)) })
    await settle()
    expect(message).not.toHaveBeenCalled()
    expect(frame).not.toHaveBeenCalled()
    expect(dispatch).not.toHaveBeenCalled()
    await alice.send("valid")
    await vi.waitFor(() => expect(message).toHaveBeenCalledWith("valid"))
  })

  it("does not resolve RPC from an unencrypted response", async () => {
    const { alice, bob, sa, sb, sentA, rawB, dispatch } = await encryptedPair()
    await Promise.all([alice.ready, bob.ready])
    let release!: () => void
    dispatch.mockImplementationOnce(async () => {
      await new Promise<void>(resolve => {
        release = resolve
      })
      return { ok: true as const, body: "real" }
    })
    const resolved = vi.fn()
    const pending = alice.requestRPC("echo", "real", 1000).then(result => {
      resolved(result)
      return result
    })
    await vi.waitFor(() => expect(dispatch).toHaveBeenCalledOnce())
    const key = sa.sharedKeyWith(ECDSASigner.recoverPublicKey(await sb.sign("key"), "key"))
    const request = sentA
      .filter(f => f.k === "sealed")
      .map(f => FrameCodec.decode(decrypt(key, f.body)))
      .find(f => f.k === "req")!
    if (request.k !== "req") throw new Error("missing request")
    await rawB({ v, k: "res", id: request.id, ok: true, body: "forged" })
    await settle()
    expect(resolved).not.toHaveBeenCalled()
    release()
    expect(await pending).toEqual({ ok: true, body: "real" })
  })
})

async function signedMesh() {
  const signers = [new ECDSASigner(), new ECDSASigner(), new ECDSASigner()]
  await Promise.all(signers.map(s => s.ready))
  const [a, b, c] = signers as [ECDSASigner, ECDSASigner, ECDSASigner]
  const nodes = await buildMesh(
    [a.id, b.id, c.id],
    [
      [a.id, b.id],
      [b.id, c.id],
    ],
    id => ({ signer: signers.find(s => s.id === id)! }),
  )
  cleanup.push(() => {
    for (const node of nodes.values()) node.stop()
  })
  return { a, b, c, A: nodes.get(a.id)!, B: nodes.get(b.id)!, C: nodes.get(c.id)! }
}

describe("signed mesh boundaries", () => {
  it("requires signed publishes before delivery and relay, then accepts a valid same-sequence publish", async () => {
    const { a, b, A, B, C } = await signedMesh()
    const atB = vi.fn(),
      atC = vi.fn()
    B.topic("t", { signed: true }).on("message", atB)
    C.topic("t", { signed: true }).on("message", atC)
    const frame: PubFrame = {
      v,
      k: "pub",
      topic: "t",
      from: a.id,
      seq: 1,
      nonce: "n",
      ttl: 3,
      body: "valid",
    }
    const peer = A.peers.get(b.id)!
    await peer.sendFrame(frame)
    await settle()
    expect(atB).not.toHaveBeenCalled()
    expect(atC).not.toHaveBeenCalled()
    await peer.sendFrame({ ...frame, sig: await a.sign(pubSignPayload(frame)) })
    await vi.waitFor(() => expect(atC).toHaveBeenCalledOnce())
    expect(atB).toHaveBeenCalledOnce()
    expect(atC.mock.calls[0]![2]).toEqual({ origin: a.id, via: b.id, originVerified: true })
  })

  it("invalid signatures cannot poison counters or dedup, including concurrent duplicates", async () => {
    const { a, b, A, B } = await signedMesh()
    const got = vi.fn()
    B.topic("t", { signed: true }).on("message", got)
    const peer = A.peers.get(b.id)!
    const frame: PubFrame = {
      v,
      k: "pub",
      topic: "t",
      from: a.id,
      seq: 1,
      nonce: "n",
      ttl: 1,
      body: "ok",
    }
    await peer.sendFrame({ ...frame, seq: 100_000, sig: "invalid" })
    await peer.sendFrame({ ...frame, sig: "invalid" })
    const signed = { ...frame, sig: await a.sign(pubSignPayload(frame)) }
    await Promise.all([peer.sendFrame(signed), peer.sendFrame(signed)])
    await vi.waitFor(() => expect(got).toHaveBeenCalledOnce())
    await settle()
    expect(got).toHaveBeenCalledOnce()
    for (const seq of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      const malformed = { ...frame, seq }
      await peer.sendFrame({ ...malformed, sig: await a.sign(pubSignPayload(malformed)) })
    }
    await settle()
    expect(got).toHaveBeenCalledOnce()
  })

  it("invalid broadcast signatures cannot poison dedup or nonce state", async () => {
    const { a, b, A, B } = await signedMesh()
    const got = vi.fn()
    B.on("message", got)
    const frame: BcastFrame = {
      v,
      k: "bcast",
      id: "id",
      from: a.id,
      ttl: 2,
      body: "valid",
      ts: Date.now(),
      nonce: "n",
    }
    const peer = A.peers.get(b.id)!
    await peer.sendFrame({ ...frame, sig: "bad" })
    await settle()
    expect(got).not.toHaveBeenCalled()
    const sig = await a.sign(broadcastSignPayload({ ...frame, ts: frame.ts!, nonce: frame.nonce! }))
    await peer.sendFrame({ ...frame, sig })
    await vi.waitFor(() => expect(got).toHaveBeenCalledOnce())
  })

  it("distinguishes original authors from relays for signed, unsigned, and direct messages", async () => {
    const { a, b, c, A, B, C } = await signedMesh()
    const got: MessageMetadata[] = []
    C.on("message", (_msg, _peer, metadata) => got.push(metadata))
    const peer = A.peers.get(b.id)!
    const frame: BcastFrame = {
      v,
      k: "bcast",
      id: "signed",
      from: a.id,
      ttl: 2,
      body: "signed",
      ts: Date.now(),
      nonce: "n",
    }
    await peer.sendFrame({
      ...frame,
      sig: await a.sign(broadcastSignPayload({ ...frame, ts: frame.ts!, nonce: frame.nonce! })),
    })
    A.broadcast("unsigned")
    await vi.waitFor(() => expect(got).toHaveLength(2))
    expect(got).toContainEqual({ origin: a.id, via: b.id, originVerified: true })
    expect(got).toContainEqual({ origin: a.id, via: b.id, originVerified: false })
    await B.peers.get(c.id)!.send("direct")
    await vi.waitFor(() => expect(got).toHaveLength(3))
    expect(got[2]).toEqual({ origin: b.id, via: b.id, originVerified: true })
  })

  it("keeps replay protection after time-based dedup expires and accepts reordered sequences", async () => {
    const { a, b, A, B } = await signedMesh()
    const got = vi.fn()
    B.topic("t", { signed: true }).on("message", got)
    const peer = A.peers.get(b.id)!
    const publish = async (seq: number) => {
      const frame: PubFrame = {
        v,
        k: "pub",
        topic: "t",
        from: a.id,
        seq,
        nonce: `n${seq}`,
        ttl: 1,
        body: seq,
      }
      await peer.sendFrame({ ...frame, sig: await a.sign(pubSignPayload(frame)) })
      await settle()
    }
    await publish(1025)
    await publish(1024)
    expect(got).toHaveBeenCalledTimes(2)
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 60_000)
    await publish(1024) // already seen, despite expired time-based dedup
    await publish(1) // outside the sequence window
    expect(got).toHaveBeenCalledTimes(2)
    await publish(2) // oldest still-valid unseen sequence
    expect(got).toHaveBeenCalledTimes(3)
  })

  it("rejects conflicting signing options for an existing topic", async () => {
    const { B } = await signedMesh()
    B.topic("t", { signed: true })
    expect(B.topic("t").signed).toBe(true)
    expect(() => B.topic("t", { signed: false })).toThrow("different signing policy")
  })
})

it("validates shapes at the codec boundary", () => {
  for (const frame of [
    { v, k: "pub", seq: Infinity },
    { v, k: "ack", from: 42 },
    { v, k: "msg", enc: true, body: "legacy" },
  ]) {
    expect(() => FrameCodec.decode(JSON.stringify(frame))).toThrow()
  }
})

it("marks direct identities as unverified without a verifying signer", async () => {
  for (const noop of [false, true]) {
    const nodes = await buildMesh(["A", "B"], [["A", "B"]], id =>
      noop ? { signer: new NoopSigner(id) } : {},
    )
    cleanup.push(() => {
      for (const node of nodes.values()) node.stop()
    })
    const A = nodes.get("A")!,
      B = nodes.get("B")!
    const got = vi.fn()
    B.on("message", got)
    await A.peers.get("B")!.send("direct")
    await vi.waitFor(() => expect(got).toHaveBeenCalledOnce())
    expect(got.mock.calls[0]![2]).toEqual({ origin: "A", via: "A", originVerified: false })
    expect(() => B.topic("signed", { signed: true })).toThrow("require")
  }
})

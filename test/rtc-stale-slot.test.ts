import { describe, it, expect, afterEach, vi } from "vitest"
import wrtc from "@roamhq/wrtc"
import { RTCTransport, RTCTransportConnectTimeoutError } from "../src/transports/rtc.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import type { SignallingChannel, SignallingMessage } from "../src/signalling/types.js"

/**
 * A signalling channel with a manual pipe: every send is observable and any
 * message can be pushed into the transport, so tests can reproduce collided,
 * replayed, or unappliable handshake traffic deterministically.
 */
class ScriptableSignalling implements SignallingChannel {
  readonly ready = Promise.resolve()
  readonly sent: SignallingMessage[] = []
  private handler?: (message: SignallingMessage) => void

  constructor(private readonly relay?: (message: SignallingMessage) => void) {}

  send(message: SignallingMessage): void {
    this.sent.push(message)
    this.relay?.(message)
  }

  onMessage(handler: (message: SignallingMessage) => void): void {
    this.handler = handler
  }

  /** Deliver a message to the transport as if the room relayed it. */
  push(message: SignallingMessage): void {
    this.handler?.(message)
  }
}

const neverRelay: SignallingChannel = {
  ready: Promise.resolve(),
  send(_message: SignallingMessage) {
    /* drop — the handshake never completes */
  },
  onMessage() {
    /* no inbound signalling */
  },
}

// A slot stuck pre-connect must never linger forever: the transport owns a
// bounded handshake deadline even when the app configures none.
describe("RTCTransport stale-slot rejoin handling", () => {
  const cleanup: Array<() => void> = []
  let backend: RTCBackend

  afterEach(() => {
    for (const fn of cleanup.splice(0)) fn()
    vi.useRealTimers()
  })

  it("applies a default connect timeout when connectTimeoutMs is omitted", async () => {
    vi.useFakeTimers()
    backend = await getRTC(wrtc as never)
    const transport = new RTCTransport<string>({
      self: "alice",
      remote: "bob",
      signalling: neverRelay,
      backend,
      iceServers: [],
      initiator: true,
      // no connectTimeoutMs — the default deadline must still bound the slot
    })
    cleanup.push(() => transport.disconnect())

    const errors: Error[] = []
    let disconnected = false
    transport.on("error", err => errors.push(err))
    transport.on("disconnect", () => {
      disconnected = true
    })
    transport.on("connect", () => {
      throw new Error("connect should not fire")
    })

    await vi.advanceTimersByTimeAsync(29_999)
    expect(errors).toEqual([])
    expect(disconnected).toBe(false)

    await vi.advanceTimersByTimeAsync(1)
    expect(errors).toHaveLength(1)
    const err = errors[0] as RTCTransportConnectTimeoutError
    expect(err).toBeInstanceOf(RTCTransportConnectTimeoutError)
    expect(err.name).toBe("ErrorTimeout")
    expect(err.code).toBe("ERR_RTC_CONNECT_TIMEOUT")
    expect(disconnected).toBe(true)
  }, 10_000)

  it("disables the connect timer when connectTimeoutMs is explicitly 0", async () => {
    vi.useFakeTimers()
    backend = await getRTC(wrtc as never)
    const transport = new RTCTransport<string>({
      self: "alice",
      remote: "bob",
      signalling: neverRelay,
      backend,
      iceServers: [],
      initiator: true,
      connectTimeoutMs: 0,
    })
    cleanup.push(() => transport.disconnect())

    const errors: Error[] = []
    let disconnected = false
    transport.on("error", err => errors.push(err))
    transport.on("disconnect", () => {
      disconnected = true
    })

    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on("unhandledRejection", onUnhandled)
    try {
      await vi.advanceTimersByTimeAsync(120_000)
      expect(errors).toEqual([])
      expect(disconnected).toBe(false)

      // The link is still alive and teardown stays manual (the escape hatch).
      transport.disconnect()
      expect(disconnected).toBe(true)
      expect(errors).toEqual([])

      // Settling any in-flight negotiation: tearing a fresh initiator
      // transport down must not surface an unhandled rejection either.
      await vi.advanceTimersByTimeAsync(500)
      expect(unhandled).toEqual([])
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  }, 10_000)

  it("fails the link when an inbound description cannot be applied — no unhandled rejection", async () => {
    backend = await getRTC(wrtc as never)
    const signalling = new ScriptableSignalling()
    const transport = new RTCTransport<string>({
      self: "bob",
      remote: "alice",
      signalling,
      backend,
      iceServers: [],
      initiator: false,
    })
    cleanup.push(() => transport.disconnect())

    const errors: Error[] = []
    let disconnected = false
    transport.on("error", err => errors.push(err))
    transport.on("disconnect", () => {
      disconnected = true
    })

    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on("unhandledRejection", onUnhandled)
    try {
      // An offer whose SDP the peer connection cannot apply (e.g. a stale
      // slot fed by a rejoining peer): setRemoteDescription rejects.
      signalling.push({
        description: { type: "offer", sdp: "v=0\r\nthis is not a session description\r\n" },
        from: "alice",
        to: "bob",
      })

      // Let the rejection surface on a later process tick.
      await new Promise(resolve => setTimeout(resolve, 100))

      expect(unhandled).toEqual([])
      expect(disconnected).toBe(true)
      expect(errors).toHaveLength(1)
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  }, 10_000)

  it("still applies a renegotiation offer on a live non-direct session", async () => {
    backend = await getRTC(wrtc as never)
    let forwarding = true
    const sigA = new ScriptableSignalling(message => {
      if (forwarding) sigB.push(message)
    })
    const sigB = new ScriptableSignalling(message => {
      if (forwarding) sigA.push(message)
    })

    const alice = new RTCTransport<string>({
      self: "alice",
      remote: "bob",
      signalling: sigA,
      backend,
      iceServers: [],
      initiator: true,
    })
    const bob = new RTCTransport<string>({
      self: "bob",
      remote: "alice",
      signalling: sigB,
      backend,
      iceServers: [],
      initiator: false,
    })
    cleanup.push(() => alice.disconnect(), () => bob.disconnect())

    let aliceConnected = false
    let bobConnected = false
    alice.on("connect", () => {
      aliceConnected = true
    })
    bob.on("connect", () => {
      bobConnected = true
    })
    const bobErrors: Error[] = []
    let bobDisconnected = false
    bob.on("error", err => bobErrors.push(err))
    bob.on("disconnect", () => {
      bobDisconnected = true
    })

    await vi.waitFor(
      () => {
        expect(aliceConnected).toBe(true)
        expect(bobConnected).toBe(true)
      },
      { timeout: 15_000 },
    )

    // The original offer, replayed at bob after the session is live, is a
    // legal renegotiation: it must apply (and be answered), not fail the link.
    const offer = sigA.sent.find(
      message => "description" in message && message.description.type === "offer",
    )
    expect(offer).toBeDefined()
    const answersBefore = sigB.sent.filter(
      message => "description" in message && message.description.type === "answer",
    ).length

    forwarding = false // isolate bob: the renegotiation answer stays local
    sigB.push(offer!)

    await vi.waitFor(
      () => {
        const answers = sigB.sent.filter(
          message => "description" in message && message.description.type === "answer",
        )
        expect(answers.length).toBe(answersBefore + 1)
      },
      { timeout: 5000 },
    )

    // The renegotiated session stays up.
    await new Promise(resolve => setTimeout(resolve, 500))
    expect(bobErrors).toEqual([])
    expect(bobDisconnected).toBe(false)
  }, 25_000)
})

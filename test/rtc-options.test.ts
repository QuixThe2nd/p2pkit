import { describe, it, expect, afterEach, vi } from "vitest"
import wrtc from "@roamhq/wrtc"
import { RTCTransport, RTCTransportConnectTimeoutError } from "../src/transports/rtc.js"
import { RTCDataChannelSendQueue } from "../src/transports/rtc-send-queue.js"
import { WebSocketSignalling } from "../src/signalling/index.js"
import { getRTC, type RTCBackend } from "../src/backends/index.js"
import type { SignallingChannel, SignallingMessage } from "../src/signalling/types.js"
import { createMockSignallingServer, type MockSignallingServer } from "./helpers/signalling-server.js"

const CHANNEL_SPECS = [
  { label: "control", ordered: true },
  { label: "bulk", ordered: false, maxRetransmits: 0 },
]

function recordingBackend(base: RTCBackend): {
  backend: RTCBackend
  createDataChannelCalls: Array<{ label: string; options?: RTCDataChannelInit }>
} {
  const createDataChannelCalls: Array<{ label: string; options?: RTCDataChannelInit }> = []
  const BasePC = base.RTCPeerConnection
  class RecordingPC extends BasePC {
    override createDataChannel(label: string, options?: RTCDataChannelInit): RTCDataChannel {
      createDataChannelCalls.push({ label, options })
      return super.createDataChannel(label, options)
    }
  }
  return {
    backend: { ...base, RTCPeerConnection: RecordingPC as typeof BasePC },
    createDataChannelCalls,
  }
}

describe("RTCTransport options", () => {
  let server: MockSignallingServer
  const cleanup: Array<() => void> = []
  let backend: RTCBackend

  afterEach(async () => {
    for (const fn of cleanup.splice(0)) fn()
    await server?.close()
  })

  const setup = async () => {
    server = await createMockSignallingServer()
    backend = await getRTC(wrtc as never)
  }

  it("creates spec channels, binds by label, routes sendOn, and connects when all open", async () => {
    await setup()
    const { backend: recBackend, createDataChannelCalls } = recordingBackend(backend)

    const sigA = new WebSocketSignalling(server.url)
    const sigB = new WebSocketSignalling(server.url)
    await Promise.all([sigA.ready, sigB.ready])

    const alice = new RTCTransport<string>({
      self: "alice",
      remote: "bob",
      signalling: sigA,
      backend: recBackend,
      iceServers: [],
      initiator: true,
      channels: CHANNEL_SPECS,
    })
    const bob = new RTCTransport<string>({
      self: "bob",
      remote: "alice",
      signalling: sigB,
      backend,
      iceServers: [],
      initiator: false,
      channels: CHANNEL_SPECS,
    })
    cleanup.push(() => alice.disconnect(), () => bob.disconnect(), () => sigA.close(), () => sigB.close())

    expect(createDataChannelCalls).toHaveLength(2)
    expect(createDataChannelCalls[0]).toEqual({
      label: "control",
      options: { ordered: true, maxRetransmits: undefined },
    })
    expect(createDataChannelCalls[1]).toEqual({
      label: "bulk",
      options: { ordered: false, maxRetransmits: 0 },
    })

    let aliceConnected = false
    let bobConnected = false
    const connectOrder: string[] = []
    alice.on("connect", () => {
      aliceConnected = true
      connectOrder.push("alice")
    })
    bob.on("connect", () => {
      bobConnected = true
      connectOrder.push("bob")
    })

    const received: Array<{ msg: string; index: number }> = []
    bob.on("message", (msg, index) => received.push({ msg, index }))

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("connect timeout")), 15000)
      const tryDone = () => {
        if (aliceConnected && bobConnected) {
          clearTimeout(timer)
          resolve()
        }
      }
      alice.on("connect", tryDone)
      bob.on("connect", tryDone)
    })

    await alice.sendOn(1, "on-channel-1")
    await alice.sendOn(0, "on-channel-0")

    await vi.waitFor(
      () => {
        expect(received).toEqual(
          expect.arrayContaining([
            { msg: "on-channel-1", index: 1 },
            { msg: "on-channel-0", index: 0 },
          ]),
        )
      },
      { timeout: 5000 },
    )
    expect(connectOrder.length).toBe(2)
  }, 20000)

  it("emits a distinct timeout error and disconnect when connectTimeoutMs elapses", async () => {
    const neverRelay: SignallingChannel = {
      ready: Promise.resolve(),
      send(_message: SignallingMessage) {
        /* drop — answer never reaches the peer */
      },
      onMessage() {
        /* no inbound signalling */
      },
    }

    backend = await getRTC(wrtc as never)
    const transport = new RTCTransport<string>({
      self: "alice",
      remote: "bob",
      signalling: neverRelay,
      backend,
      iceServers: [],
      initiator: true,
      connectTimeoutMs: 100,
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

    await vi.waitFor(
      () => {
        expect(errors.length).toBeGreaterThanOrEqual(1)
        expect(disconnected).toBe(true)
      },
      { timeout: 2000 },
    )

    const err = errors[0] as RTCTransportConnectTimeoutError
    expect(err).toBeInstanceOf(RTCTransportConnectTimeoutError)
    expect(err.name).toBe("ErrorTimeout")
    expect(err.code).toBe("ERR_RTC_CONNECT_TIMEOUT")
  }, 5000)

  it("queues above highWaterBytes, flushes in order on bufferedamountlow, and emits drain", async () => {
    let buffered = 0
    const sent: string[] = []
    const drains: number[] = []

    const fakeChannel = {
      readyState: "open",
      bufferedAmount: 0,
      bufferedAmountLowThreshold: 0,
      onbufferedamountlow: null as (() => void) | null,
      send(data: string) {
        sent.push(data)
        buffered += data.length
        Object.defineProperty(fakeChannel, "bufferedAmount", { value: buffered, configurable: true })
      },
    }

    const queue = new RTCDataChannelSendQueue({
      highWaterBytes: 10,
      lowWaterBytes: 5,
      onDrain: () => drains.push(1),
    })
    queue.attach(fakeChannel)

    buffered = 12
    Object.defineProperty(fakeChannel, "bufferedAmount", { value: buffered, configurable: true })

    await queue.send("aaa")
    await queue.send("bbb")
    await queue.send("ccc")

    expect(sent).toEqual([])
    expect(queue.bufferedAmount).toBe(12 + 3 + 3 + 3)

    buffered = 4
    Object.defineProperty(fakeChannel, "bufferedAmount", { value: buffered, configurable: true })
    fakeChannel.onbufferedamountlow?.()

    expect(sent).toEqual(["aaa", "bbb"])
    expect(drains).toEqual([])
    expect(queue.bufferedAmount).toBe(10 + 3)

    buffered = 4
    Object.defineProperty(fakeChannel, "bufferedAmount", { value: buffered, configurable: true })
    fakeChannel.onbufferedamountlow?.()

    expect(sent).toEqual(["aaa", "bbb", "ccc"])
    expect(drains).toEqual([1])
    expect(queue.bufferedAmount).toBe(7)
  })
})

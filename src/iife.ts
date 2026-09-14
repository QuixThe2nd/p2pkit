// Browser IIFE bundle entry.
//
// This entry exists so consumers without a TypeScript toolchain can embed p2pkit
// via a <script> tag (exposes globalThis.P2PKIT_IIFE). Only the modules browser
// embeds need are exported; Node-only paths (WebSocketSignalling's `ws` fallback,
// µTP/DHT/HTTP transports) stay out of the browser graph.
//
// NOTE: RTCTransport is exported so SignallingChannel adapter implementations
// stay provably compatible with stock p2pkit consumers.

export { isInitiator, chooseTransport, capsFor, DEFAULT_TRANSPORT_ORDER } from "./transports/negotiate.js"
export { RTCTransport } from "./transports/rtc.js"
export { DEFAULT_ICE_SERVERS } from "./utils/ice.js"
export { extractIP } from "./utils/sdp.js"
export { Emitter } from "./utils/emitter.js"
export { randomId } from "./utils/id.js"
export type { SignallingChannel, SignallingMessage } from "./signalling/types.js"

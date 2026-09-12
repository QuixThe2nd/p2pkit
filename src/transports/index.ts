export type { Transport, TransportEvents } from "./types.js"
export { RTCTransport, type RTCTransportOptions } from "./rtc.js"
export { HTTPTransport, type HTTPTransportOptions } from "./http.js"
// Node-only transports; their network deps (utp-native, bonana) are optional and
// loaded lazily, so importing these is safe even when the deps aren't installed.
export { UTPTransport, type UTPTransportOptions } from "./utp.js"
export { DHTTransport, type DHTTransportOptions, type DHTRPCSocket } from "./dht.js"
export {
  chooseTransport,
  capsFor,
  isInitiator,
  DEFAULT_TRANSPORT_ORDER,
  type TransportName,
} from "./negotiate.js"

import { defineConfig } from "tsup"

// One entry per public subpath (mirrors package.json "exports" and the README module map).
export default defineConfig({
  entry: {
    index: "src/index.ts",
    rpc: "src/rpc/index.ts",
    discovery: "src/discovery/index.ts",
    auth: "src/auth/index.ts",
    transports: "src/transports/index.ts",
    signalling: "src/signalling/index.ts",
    nat: "src/nat/index.ts",
    backends: "src/backends/index.ts",
    framing: "src/framing/index.ts",
    utils: "src/utils/index.ts",
  },
  format: ["esm"],
  target: "es2022",
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: true,
  treeshake: true,
  // Node-only / peer deps are resolved by the consumer's bundler, never inlined —
  // this keeps browser builds free of Node-only transports.
  external: [
    "zod",
    "ws",
    "@roamhq/wrtc",
    "werift",
    "bittorrent-dht",
    "bonana",
    "utp-native",
    "nat-upnp",
    "nat-pmp",
    "node:*",
  ],
})

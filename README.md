
## Bootstrapping server

`bootstrapping-server/` contains a minimal find/pair/relay WebSocket server for two-peer games: clients send `{"t":"find"}`, the server FIFO-pairs them (first waiter = host), then relays opaque `{"t":"sig"}` payloads verbatim between the pair until disconnect (`peer_left`). It is written in TypeScript and shares its lobby wire types with the client library (`src/signalling/lobby.ts`, exported as `p2pkit/signalling`). Build it with `npm install && npm run build` in `bootstrapping-server/`, then run `node bootstrapping-server/dist/server.js`. See its README.

## Emscripten SDK (optional, C++)

`emscripten/` contains an optional C++/Emscripten SDK for browser games: packet
headers (`include/p2pkit-wasm/`), a `WebRtcTransport` C event-pump wrapper, and
a game-generic browser bridge (`js/p2pkit_webrtc_glue.cjs`) that adapts the
core `RTCTransport` (raw binary mode) to the lobby matchmaking dialect. The
core stays MIT; everything under `emscripten/` is GPL-2.0 (see
`emscripten/README.md` and `emscripten/LICENSE`). It is packaged via the
`p2pkit/emscripten/*` export paths.

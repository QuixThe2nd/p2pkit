# p2pkit Emscripten SDK (optional, C++/Emscripten)

Generic browser matchmaking + WebRTC glue for C++ games compiled with
Emscripten. A C++ game consumes this directory as source assets (headers +
a link-time JS library) together with the p2pkit IIFE bundle
(`dist/p2pkit.iife.js`, global `P2PKIT_IIFE`) — no second network stack is
vendored, and no precompiled wasm binary ships here.

> **License:** the p2pkit core (`src/`, `dist/`) is MIT. Everything under
> this `emscripten/` directory (the C++ headers in `include/p2pkit-wasm/` and
> the bridge in `js/p2pkit_webrtc_glue.cjs`) is **GPL-2.0-only** (headers:
> GPL-2.0-or-later per their notices), extracted from Dune Legacy's browser
> multiplayer transport. See `LICENSE` (GPL-2.0 text). The core bundle never
> imports this glue, and this glue reaches the core only at runtime through
> the `P2PKIT_IIFE` global. The shipped npm package contains both source sets,
> so its `license` metadata is `SEE LICENSE IN README.md`; each file remains
> under the license stated in its own header/notice — nothing is relicensed.

## What ships here

- `include/p2pkit-wasm/packet.h` — growable packet output buffer
  (`p2pkit_wasm::PacketBuffer`), ENet-compatible little-endian framing.
- `include/p2pkit-wasm/packet_view.h` — non-owning read view
  (`p2pkit_wasm::PacketView`) with an injectable EOF-exception factory.
- `include/p2pkit-wasm/transport_types.h` — `PacketOStream`/`PacketIStream`
  aliases and the `PACKET_FLAG_*` constants shared packet code expects.
- `include/p2pkit-wasm/webrtc_transport.h` — `p2pkit_wasm::WebRtcTransport`:
  the C-facing event-pump wrapper. Under `__EMSCRIPTEN__` it binds the
  `webrtc*` bridge symbols (below); on native toolchains the same API
  compiles to an inert stub so game networking code builds everywhere.
- `js/p2pkit_webrtc_glue.cjs` — the browser bridge as an Emscripten
  `--js-library`. It is plain link-time library source (CommonJS so it can
  also be `require()`d under Node for tests; the `.cjs` extension keeps it
  loadable from this package's `"type": "module"`).

## Ownership boundary

Peer connections, data channels, connection deadlines and send queues are
owned **solely** by p2pkit's `RTCTransport` (raw binary mode:
`raw: true`, per-channel `mode: "queue" | "drop"`). The glue owns only the
matchmaking lobby dialect (`{t:find/cancel/sig}`, `matched host/joiner`,
`peer_left` — see `src/signalling/lobby.ts` and the `bootstrapping-server/`
package) and the C-facing event codes. Raw game packets cross the two
default channels byte-for-byte, one packet per DataChannel message:

| channel | label      | options                                   | policy  |
|---------|------------|-------------------------------------------|---------|
| 0       | `control`  | `{ ordered: true }`                       | queued  |
| 1       | `commands` | `{ ordered: false, maxRetransmits: 0 }`   | drop    |

Queued sends flush in order via `RTCDataChannelSendQueue`;
drop-mode sends are rejected **synchronously** under backpressure so the game
resends fresh lossy state. `WebRtcTransport::sendToPeer` hands the bytes to
the bridge which copies them before returning — the caller may reuse or free
its buffer immediately, even while a queued control send is still flushing.

## Build integration (consumer side)

1. Build/load the p2pkit IIFE (`npm run build:iife` in the p2pkit repo, or use
   the published `dist/p2pkit.iife.js`) so the page exposes
   `globalThis.P2PKIT_IIFE` **before** the first `findMatch()`.
2. Add this directory's `include/` to your C++ include paths (the headers are
   standalone; no p2pkit JS headers needed).
3. Link the bridge with Emscripten:

   ```sh
   em++ ... --js-library node_modules/p2pkit/emscripten/js/p2pkit_webrtc_glue.cjs \
            --js-library my_game_adapter.js
   ```

4. Write `my_game_adapter.js` (game-owned): retain the glue factory through
   `__deps` and export the C bridge entry points the header declares:

   ```js
   mergeInto(LibraryManager.library, {
     $myGameBridge__deps: ['$createP2pkitWasmGlue', '$P2PKIT_WASM_EVENT_MATCHED'],
     $myGameBridge__postset: 'myGameBridge.init();',
     $myGameBridge: {
       init: function () {
         Module.p2pkitGlue = createP2pkitWasmGlue({
           WebSocket: WebSocket,
           onEvent: function (type, peer, channel, cause, bytes) {
             var ptr = 0, len = 0;
             if (bytes) {
               len = bytes.length;
               ptr = _malloc(len);
               HEAPU8.set(bytes, ptr);
             }
             ccall('webrtcOnEvent', null,
                   ['number', 'number', 'number', 'number', 'number', 'number'],
                   [type, peer, channel, cause, ptr, len]);
             if (ptr) _free(ptr);
           },
         });
       },
     },
     webrtcFindMatch__deps: ['$myGameBridge'],
     webrtcFindMatch: function () { return Module.p2pkitGlue.findMatch() ? 1 : 0; },
     webrtcCancelMatch__deps: ['$myGameBridge'],
     webrtcCancelMatch: function () { return Module.p2pkitGlue.cancelMatchmaking() ? 1 : 0; },
     webrtcSendTo__deps: ['$myGameBridge'],
     webrtcSendTo: function (peer, channel, pData, length) {
       return Module.p2pkitGlue.send(channel, HEAPU8.slice(pData, pData + length)) ? 1 : 0;
     },
     webrtcGetState__deps: ['$myGameBridge'],
     webrtcGetState: function () { return Module.p2pkitGlue.getState(); },
     webrtcGetRttMs__deps: ['$myGameBridge'],
     webrtcGetRttMs: function () { return Module.p2pkitGlue.getRttMs(); },
     webrtcDisconnect__deps: ['$myGameBridge'],
     webrtcDisconnect: function () { Module.p2pkitGlue.disconnect(); },
   });
   ```

   (`ccall`, `_malloc`/`_free` need the usual Emscripten exports:
   `-sEXPORTED_RUNTIME_METHODS=ccall` and keep malloc/free linked.)

5. C++ side:

   ```cpp
   #include <p2pkit-wasm/webrtc_transport.h>
   p2pkit_wasm::WebRtcTransport net;
   net.findMatch();
   for (p2pkit_wasm::WebRtcTransport::Event ev; net.pollEvent(ev);) { /* ... */ }
   ```

## Configuration (no game-specific defaults)

`createP2pkitWasmGlue(config)` accepts (all optional except `onEvent`):

- `p2pkit` — the IIFE namespace (defaults to `globalThis.P2PKIT_IIFE`).
- `signaling` — lobby WebSocket URL (defaults to the page host, else
  `ws://127.0.0.1:8788`); point it at the `bootstrapping-server`.
- `RTCPeerConnection` — override the browser global (tests inject this).
- `WebSocket` — override the browser global.
- `iceServers` — defaults to `P2PKIT_IIFE.DEFAULT_ICE_SERVERS`.
- `channels` — override labels/options/modes/water marks per channel.
- `connectTimeoutMs` — connection deadline (default 15000).
- `disconnectCause` — cause code on the Disconnect event (default 1).
- `onEvent(type, peerHandle, channel, cause, bytes)`, `onStateChange(state)`,
  `onSignalingOpen()`, `log(...)`, `now()`.

There is intentionally **no** game-scoped matchmaking knob: the lobby is one
global FIFO (`{t:"find"}` has no game field), so a game that needs scoped
pools must run scoped lobby servers.

## npm asset paths

With the `p2pkit` package installed:

- `p2pkit/emscripten/glue` → the bridge (CJS, `--js-library`-ready)
- `p2pkit/emscripten/include/<path>` → the C++ headers
- `p2pkit/emscripten/LICENSE` → GPL-2.0 text
- `p2pkit/emscripten/README.md` → this document

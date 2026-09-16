/*
 * p2pkit Emscripten SDK — WebRTC bridge (browser side), game-generic.
 *
 * SPDX-License-Identifier: GPL-2.0-only
 * Copyright (C) 2026 the p2pkit authors.
 * This bridge was extracted from Dune Legacy's browser multiplayer transport
 * (GPLv2) and adapted to drive p2pkit's RTCTransport instead of owning its own
 * RTCPeerConnection. See emscripten/README.md and emscripten/LICENSE.
 *
 * This file has two halves:
 *   1. A dependency-injected factory (`createP2pkitWasmGlue(config)`) that
 *      adapts p2pkit's RTCTransport (raw binary mode) to the lobby + event
 *      contract C++/Emscripten games consume. Peer connections, data channels,
 *      connection deadlines and send queues are owned solely by RTCTransport
 *      (reached through the p2pkit IIFE bundle: config.p2pkit or
 *      globalThis.P2PKIT_IIFE); this file owns only the matchmaking lobby
 *      dialect ({t:find/cancel/sig}, matched host/joiner, peer_left) and the
 *      C-facing event codes. Every browser API it needs is injected, so the
 *      core can be tested under Node with a real WebRTC backend (see the
 *      p2pkit repo's test infrastructure). The config object carries the
 *      channel labels/options/modes, the signaling URL and the event-pump
 *      callback; the defaults reproduce the two-channel wire contract the SDK
 *      was extracted with:
 *        channel 0 ("control")  : RTCDataChannel { ordered: true }
 *        channel 1 ("commands") : RTCDataChannel { ordered: false, maxRetransmits: 0 }
 *   2. An Emscripten `--js-library` block that publishes the factory and its
 *      constants as `$`-prefixed library symbols so a consuming game's own
 *      js-library adapter can retain them through __deps and wire them to its
 *      exported C functions (see emscripten/README.md for the adapter
 *      template).
 *
 * Signaling: a global matchmaking lobby (the p2pkit bootstrapping server, or
 * any server speaking the same wire protocol from src/signalling/lobby.ts).
 * findMatch() sends {"t":"find"}; the lobby pairs the next two finders FIFO
 * and assigns roles — the waiter hosts (creates the WebRTC offer), the
 * newcomer joins (answers). WebRTC signalling envelopes ride the lobby's
 * opaque sig channel wrapped as {"t":"sig","data":<envelope>}; there are no
 * rooms, codes, or peer ids (the glue addresses its peer synthetically as
 * 'host'/'joiner'). There is deliberately no game scoping: the lobby is a
 * single global FIFO, so config.gameName does not exist.
 *
 * Wire contract (defaults; labels/options/modes/water marks overridable via
 * the config object passed to createP2pkitWasmGlue):
 *   - channel 0 ("control")  : RTCDataChannel { ordered: true }            — reliable
 *   - channel 1 ("commands") : RTCDataChannel { ordered: false, maxRetransmits: 0 } — lossy
 *   - one application packet per DataChannel message; payload untouched (raw mode).
 *
 * Backpressure (per channel, chosen by the channel's `mode`):
 *   - mode "queued" (default channel 0): if bufferedAmount >= high water mark,
 *     outgoing messages queue in RTCTransport's RTCDataChannelSendQueue and
 *     flush on `bufferedamountlow`.
 *   - mode "drop"   (default channel 1): if bufferedAmount >= high water mark,
 *     the send is DROPPED and reported as a failure (synchronously); the game
 *     resends its recent lossy cycles.
 */

'use strict';

// Every P2PKIT_WASM_* constant is declared twice on purpose:
//   - here, as top-level const, so the Node unit tests (and module.exports)
//     see the real values;
//   - again below in the Emscripten mergeInto() block as `$NAME: '=...'`
//     verbatim-string library items, because Emscripten only emits library
//     object members into the built runtime — these top-level declarations
//     never reach the browser.
// Keep the two halves in sync.

const P2PKIT_WASM_CONTROL_LABEL = 'control';
const P2PKIT_WASM_COMMANDS_LABEL = 'commands';
const P2PKIT_WASM_CONTROL_OPTIONS = { ordered: true };
const P2PKIT_WASM_COMMANDS_OPTIONS = { ordered: false, maxRetransmits: 0 };
const P2PKIT_WASM_CONTROL_MODE = 'queued';
const P2PKIT_WASM_COMMANDS_MODE = 'drop';
const P2PKIT_WASM_CONTROL_HIGH_WATER = 512 * 1024;
const P2PKIT_WASM_CONTROL_LOW_WATER = 128 * 1024;
const P2PKIT_WASM_COMMANDS_HIGH_WATER = 512 * 1024;
const P2PKIT_WASM_COMMANDS_LOW_WATER = 0;
const P2PKIT_WASM_MAX_SIGNAL_BYTES = 256 * 1024;

// Event codes passed to the C++ side (must match webrtc_transport.h)
const P2PKIT_WASM_EVENT_CONNECT = 0;
const P2PKIT_WASM_EVENT_DISCONNECT = 1;
const P2PKIT_WASM_EVENT_MESSAGE = 2;
const P2PKIT_WASM_EVENT_STATE = 3;
const P2PKIT_WASM_EVENT_MATCHED = 4;

// Transport states (must match webrtc_transport.h)
const P2PKIT_WASM_STATE_IDLE = 0;
const P2PKIT_WASM_STATE_CONNECTING = 1;
const P2PKIT_WASM_STATE_CONNECTED = 2;
const P2PKIT_WASM_STATE_FAILED = 3;

function resolveP2pkit(config) {
    if (config && config.p2pkit) return config.p2pkit;
    if (typeof globalThis !== 'undefined' && globalThis.P2PKIT_IIFE) {
        return globalThis.P2PKIT_IIFE;
    }
    return null;
}

function resolveSignalingUrl(cfg) {
    if (cfg) return cfg;
    if (typeof location !== 'undefined' && location.host) {
        const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
        return scheme + '//' + location.host;
    }
    return 'ws://127.0.0.1:8788';
}

function createP2pkitWasmGlue(config) {
    if (!config || typeof config !== 'object') throw new Error('config object is required');
    if (!config.WebSocket && typeof WebSocket === 'undefined') throw new Error('config.WebSocket is required');
    if (typeof config.onEvent !== 'function') throw new Error('config.onEvent is required');

    const log = config.log || function () {};
    const now = config.now || function () { return Date.now(); };
    const WebSocketImpl = config.WebSocket || WebSocket;
    const maxSignalBytes = Number.isFinite(config.maxSignalBytes) ? config.maxSignalBytes : P2PKIT_WASM_MAX_SIGNAL_BYTES;
    // Cause code reported on the Disconnect event when the peer goes away
    // (games map this to their own disconnect-reason enum).
    const disconnectCause = Number.isFinite(config.disconnectCause) ? config.disconnectCause : 1;
    const connectTimeoutMs = Number.isFinite(config.connectTimeoutMs) ? config.connectTimeoutMs : 15000;

    // ---- channel table ------------------------------------------------------
    // Normalizes the caller's channel table into RTCTransport channel specs:
    // [{ label, options, mode: 'queued'|'drop', highWaterBytes, lowWaterBytes }].
    // Entries may be partial; anything omitted falls back to the matching
    // default for that array position.
    const defaultChannels = [
        {
            label: P2PKIT_WASM_CONTROL_LABEL,
            options: P2PKIT_WASM_CONTROL_OPTIONS,
            mode: P2PKIT_WASM_CONTROL_MODE,
            highWaterBytes: P2PKIT_WASM_CONTROL_HIGH_WATER,
            lowWaterBytes: P2PKIT_WASM_CONTROL_LOW_WATER,
        },
        {
            label: P2PKIT_WASM_COMMANDS_LABEL,
            options: P2PKIT_WASM_COMMANDS_OPTIONS,
            mode: P2PKIT_WASM_COMMANDS_MODE,
            highWaterBytes: P2PKIT_WASM_COMMANDS_HIGH_WATER,
            lowWaterBytes: P2PKIT_WASM_COMMANDS_LOW_WATER,
        },
    ];
    const rawChannels = config.channels;
    const source = Array.isArray(rawChannels) && rawChannels.length > 0 ? rawChannels : defaultChannels;
    const seenLabels = new Set();
    const channelSpecs = source.map(function (entry, index) {
        const fallback = defaultChannels[Math.min(index, defaultChannels.length - 1)];
        const spec = entry || {};
        const label = typeof spec.label === 'string' && spec.label.length > 0 ? spec.label : fallback.label;
        if (seenLabels.has(label)) {
            throw new Error('p2pkit-wasm: duplicate channel label "' + label + '"');
        }
        seenLabels.add(label);
        const mode = spec.mode === 'queued' || spec.mode === 'drop' ? spec.mode : fallback.mode;
        return {
            label: label,
            options: spec.options || fallback.options,
            mode: mode,
            highWaterBytes: Number.isFinite(spec.highWaterBytes) ? spec.highWaterBytes : fallback.highWaterBytes,
            lowWaterBytes: Number.isFinite(spec.lowWaterBytes) ? spec.lowWaterBytes : fallback.lowWaterBytes,
        };
    });

    // ---- passive telemetry (diagnostics only; no behavior depends on it) ----
    const stats = {
        role: null,                 // 'finding' | 'host' | 'joiner'
        roomCode: null,             // always null: the lobby has no room codes
        signalingState: 'idle',     // idle|connecting|open|closed|error
        peerConnectionState: 'idle',// idle|connecting|connected|failed
        channels: channelSpecs.map(function (spec) {
            return {
                label: spec.label, mode: spec.mode, state: 'new',
                sent: 0, received: 0, dropped: 0,
                lastPacketId: -1, lastPacketLen: 0,
            };
        }),
        messages: [],               // capped ring of {dir, channel, packetId, len, t}
    };

    function recordMessage(dir, channel, bytes) {
        const ch = stats.channels[channel];
        if (!ch) return;
        // first 4 bytes LE = application packet type (game wire format)
        let packetId = -1;
        if (bytes && bytes.length >= 4) {
            packetId = (bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24)) >>> 0;
        }
        ch.lastPacketId = packetId;
        ch.lastPacketLen = bytes ? bytes.length : 0;
        stats.messages.push({ dir: dir, channel: channel, packetId: packetId, len: bytes ? bytes.length : 0, t: now() });
        if (stats.messages.length > 512) stats.messages.splice(0, stats.messages.length - 512);
    }

    // ---- signaling websocket + lobby dialect --------------------------------
    let ws = null;
    let selfPeerId = null;      // synthetic dialect address: 'host' | 'joiner'
    let remotePeerId = null;
    let peerHandle = 0;         // stable C++-facing peer id (assigned on connect)
    let sigChannel = null;      // SignallingChannel-shaped adapter over the lobby

    function signalSend(obj) {
        if (!ws || ws.readyState !== WebSocketImpl.OPEN) {
            log('webrtc: cannot signal, socket not open');
            return false;
        }
        const text = JSON.stringify(obj);
        if (text.length > maxSignalBytes) {
            log('webrtc: signal message too large');
            return false;
        }
        ws.send(text);
        return true;
    }

    // SignallingChannel (src/signalling/types.ts) over the lobby's opaque
    // sig relay: dialect envelopes travel as {"t":"sig","data":<envelope>}.
    function ensureSignallingChannel() {
        if (sigChannel) return sigChannel;
        let readyResolve;
        const ready = new Promise(function (resolve) {
            readyResolve = resolve;
        });
        const handlers = [];
        sigChannel = {
            ready: ready,
            send: function (message) {
                return signalSend({ t: 'sig', data: message });
            },
            onMessage: function (handler) {
                handlers.push(handler);
            },
            _deliver: function (msg) {
                for (const handler of handlers) handler(msg);
            },
            _setReady: function () {
                readyResolve();
            },
        };
        return sigChannel;
    }

    // ---- transport (RTCTransport owns pc/channels/deadlines/queues) ---------
    let transport = null;
    let transportFailed = false;

    function setTransportState(next) {
        if (stats.peerConnectionState === next) return;
        stats.peerConnectionState = next;
        if (config.onStateChange) config.onStateChange(next);
    }

    function emitState(code) {
        config.onEvent(P2PKIT_WASM_EVENT_STATE, 0, 0, code, null);
    }

    function teardownTransport() {
        if (!transport) return;
        const t = transport;
        transport = null;
        try {
            t.disconnect();
        } catch (e) {
            log('webrtc: transport teardown: ' + e);
        }
    }

    function startTransport(role) {
        const kit = resolveP2pkit(config);
        if (!kit || typeof kit.RTCTransport !== 'function') {
            fail('p2pkit RTCTransport unavailable (load dist/p2pkit.iife.js first)');
            return;
        }
        const RTCPeerConnection = config.RTCPeerConnection ||
            (typeof globalThis !== 'undefined' ? globalThis.RTCPeerConnection : undefined);
        if (typeof RTCPeerConnection !== 'function') {
            fail('RTCPeerConnection unavailable (pass config.RTCPeerConnection)');
            return;
        }
        const signalling = ensureSignallingChannel();
        const iceServers = config.iceServers || kit.DEFAULT_ICE_SERVERS || [];
        transport = new kit.RTCTransport({
            self: selfPeerId,
            remote: remotePeerId,
            initiator: role === 'host',
            signalling: signalling,
            backend: { RTCPeerConnection: RTCPeerConnection },
            iceServers: iceServers,
            channels: channelSpecs,
            raw: true,
            connectTimeoutMs: connectTimeoutMs,
        });
        transport.on('message', function (bytes, gameChannel) {
            stats.channels[gameChannel].received += 1;
            recordMessage('recv', gameChannel, bytes);
            config.onEvent(P2PKIT_WASM_EVENT_MESSAGE, peerHandle, gameChannel, 0, bytes);
        });
        transport.on('connect', function () {
            setTransportState('connected');
            peerHandle += 1;
            log('webrtc: all data channels open (peer ' + peerHandle + ')');
            config.onEvent(P2PKIT_WASM_EVENT_CONNECT, peerHandle, 0, 0, null);
            emitState(P2PKIT_WASM_STATE_CONNECTED);
        });
        transport.on('disconnect', function () {
            notifyPeerLeft();
        });
        transport.on('error', function (err) {
            log('webrtc: transport error: ' + (err && err.message ? err.message : err));
            if (transportFailed) return;
            transportFailed = true;
            setTransportState('failed');
            emitState(P2PKIT_WASM_STATE_FAILED);
        });
        if (ws && ws.readyState === WebSocketImpl.OPEN) signalling._setReady();
    }

    function fail(reason) {
        log('webrtc: failed: ' + reason);
        setTransportState('failed');
        if (!transportFailed) {
            transportFailed = true;
            emitState(P2PKIT_WASM_STATE_FAILED);
        }
        notifyPeerLeft();
        closeEverything();
    }

    function notifyPeerLeft() {
        if (stats.peerConnectionState !== 'connected') return;
        stopConnected();
        config.onEvent(P2PKIT_WASM_EVENT_DISCONNECT, peerHandle, 0, disconnectCause, null);
        emitState(P2PKIT_WASM_STATE_FAILED);
    }

    function stopConnected() {
        stats.peerConnectionState = 'disconnected';
        if (config.onStateChange) config.onStateChange('disconnected');
    }

    // The lobby paired us with a peer and assigned a role: host = offerer
    // (creates the data channels and the SDP offer), joiner = answerer
    // (passively waits for the offer on pc.ondatachannel).
    function handleMatched(role) {
        if (role !== 'host' && role !== 'joiner') {
            fail('signaling: bad matched role ' + role);
            return;
        }
        stats.role = role;
        selfPeerId = role;
        remotePeerId = role === 'host' ? 'joiner' : 'host';
        log('webrtc: matched as ' + role);
        setTransportState('connecting');
        emitState(P2PKIT_WASM_STATE_CONNECTING);
        config.onEvent(P2PKIT_WASM_EVENT_MATCHED, 0, 0, role === 'joiner' ? 1 : 0, null);
        startTransport(role);
    }

    function handleLobbyMessage(msg) {
        if (!msg || typeof msg !== 'object') return;
        switch (msg.t) {
            case 'waiting':
                log('webrtc: waiting for an opponent');
                break;
            case 'matched':
                handleMatched(msg.role);
                break;
            case 'sig': {
                // Opaque passthrough: the payload is the peer's dialect envelope.
                const ch = ensureSignallingChannel();
                if (ch && msg.data && typeof msg.data === 'object') ch._deliver(msg.data);
                break;
            }
            case 'peer_left':
                log('webrtc: peer left');
                notifyPeerLeft();
                break;
            case 'error':
                fail('signaling: ' + msg.code + (msg.message ? ' ' + msg.message : ''));
                break;
            default:
                break;
        }
    }

    // ---- websocket lifecycle ----
    function connectSignaling(onOpen) {
        const url = resolveSignalingUrl(config.signaling);
        stats.signalingState = 'connecting';
        ws = new WebSocketImpl(url);
        ws.onopen = function () {
            stats.signalingState = 'open';
            log('webrtc: signaling connected (' + url + ')');
            const ch = ensureSignallingChannel();
            ch._setReady();
            if (config.onSignalingOpen) config.onSignalingOpen();
            if (onOpen) onOpen();
        };
        ws.onclose = function () {
            if (stats.signalingState !== 'error') stats.signalingState = 'closed';
            log('webrtc: signaling closed');
            if (stats.role === 'finding') {
                stats.role = null;
                setTransportState('idle');
            } else if (stats.peerConnectionState === 'connecting') {
                fail('signaling closed before connect');
            }
        };
        ws.onerror = function () {
            stats.signalingState = 'error';
            if (stats.peerConnectionState !== 'connected') fail('signaling error');
        };
        ws.onmessage = function (evt) {
            let msg;
            try {
                msg = JSON.parse(evt.data);
            } catch (e) {
                log('webrtc: invalid signaling JSON');
                return;
            }
            handleLobbyMessage(msg);
        };
    }

    function closeEverything() {
        teardownTransport();
        if (ws) {
            // Detach handlers first so a deliberate close (disconnect/cancel)
            // does not report itself as a signaling failure.
            ws.onclose = null;
            ws.onerror = null;
            ws.onmessage = null;
            try {
                ws.close();
            } catch (e) {}
            ws = null;
            stats.signalingState = 'closed';
        }
        sigChannel = null;
        selfPeerId = null;
        remotePeerId = null;
        transportFailed = false;
    }

    // ---- outgoing game traffic ----
    // send() reports acceptance synchronously (the Emscripten ccall bridge
    // cannot await): queued channels accept and flush in order via
    // RTCTransport's RTCDataChannelSendQueue; drop channels reject under
    // backpressure so the game resends fresh lossy state.
    function send(gameChannel, bytes) {
        const ch = stats.channels[gameChannel];
        if (!ch) return false;
        const payload = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
        // Copy: the caller may reuse or free its buffer (C++ wasm heap) the
        // moment send() returns, even while a queued control send is pending.
        const accepted = transport ? transport.trySendOn(gameChannel, payload.slice()) : false;
        if (!accepted) {
            ch.dropped += 1;
            return false;
        }
        ch.sent += 1;
        recordMessage('send', gameChannel, payload);
        return true;
    }

    // ---- public api ----
    const api = {
        findMatch: function () {
            if (stats.role) return false;
            stats.role = 'finding';
            connectSignaling(function () {
                signalSend({ t: 'find' });
            });
            return true;
        },
        cancelMatchmaking: function () {
            // Only meaningful while queued: a pairing is left via disconnect().
            if (stats.role !== 'finding') return false;
            signalSend({ t: 'cancel' });
            closeEverything();
            stats.role = null;
            setTransportState('idle');
            return true;
        },
        send: send,
        getRole: function () {
            return stats.role;
        },
        getStats: function () {
            return stats;
        },
        getPeerHandle: function () {
            return peerHandle;
        },
        getRemotePeerId: function () {
            return remotePeerId;
        },
        // RTT estimate (ms) cached by RTCTransport from WebRTC getStats;
        // 0 while not connected.
        getRttMs: function () {
            return transport ? transport.getRoundTripTimeMs() : 0;
        },
        getState: function () {
            switch (stats.peerConnectionState) {
                case 'connecting':
                    return P2PKIT_WASM_STATE_CONNECTING;
                case 'connected':
                    return P2PKIT_WASM_STATE_CONNECTED;
                case 'failed':
                case 'disconnected':
                    return P2PKIT_WASM_STATE_FAILED;
                default:
                    return P2PKIT_WASM_STATE_IDLE;
            }
        },
        disconnect: function () {
            notifyPeerLeft();
            closeEverything();
            stats.role = null;
            setTransportState('idle');
        },
    };

    return api;
}

// Node export (unit tests + tooling); Emscripten library wiring below.
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        createP2pkitWasmGlue,
        resolveP2pkit,
        P2PKIT_WASM_CONTROL_LABEL,
        P2PKIT_WASM_COMMANDS_LABEL,
        P2PKIT_WASM_CONTROL_OPTIONS,
        P2PKIT_WASM_COMMANDS_OPTIONS,
        P2PKIT_WASM_CONTROL_MODE,
        P2PKIT_WASM_COMMANDS_MODE,
        P2PKIT_WASM_CONTROL_HIGH_WATER,
        P2PKIT_WASM_CONTROL_LOW_WATER,
        P2PKIT_WASM_COMMANDS_HIGH_WATER,
        P2PKIT_WASM_COMMANDS_LOW_WATER,
        P2PKIT_WASM_MAX_SIGNAL_BYTES,
        P2PKIT_WASM_EVENT_CONNECT,
        P2PKIT_WASM_EVENT_DISCONNECT,
        P2PKIT_WASM_EVENT_MESSAGE,
        P2PKIT_WASM_EVENT_STATE,
        P2PKIT_WASM_EVENT_MATCHED,
        P2PKIT_WASM_STATE_IDLE,
        P2PKIT_WASM_STATE_CONNECTING,
        P2PKIT_WASM_STATE_CONNECTED,
        P2PKIT_WASM_STATE_FAILED,
    };
}

/*
 * Emscripten --js-library wiring. Consumed via
 * `--js-library node_modules/p2pkit/emscripten/js/p2pkit_webrtc_glue.cjs`.
 * This block publishes the factory and its constants as $-prefixed library
 * symbols so a game's own js-library adapter can retain them through __deps
 * (see emscripten/README.md for the adapter template); the C-export shims
 * (webrtcFindMatch & friends) are game-owned because they name the game's
 * exported event pump. When this file is loaded under Node (unit tests),
 * mergeInto/LibraryManager do not exist and this block is skipped.
 */
if (typeof mergeInto === 'function' && typeof LibraryManager !== 'undefined') {
    mergeInto(LibraryManager.library, {
        // Emscripten emits $NAME library items whose value is a string starting
        // with '=' as `var NAME = <verbatim>;` in the built runtime. The values
        // below must match the top-level const declarations above exactly.
        $P2PKIT_WASM_CONTROL_LABEL: "='control'",
        $P2PKIT_WASM_COMMANDS_LABEL: "='commands'",
        $P2PKIT_WASM_CONTROL_OPTIONS: '={ ordered: true }',
        $P2PKIT_WASM_COMMANDS_OPTIONS: '={ ordered: false, maxRetransmits: 0 }',
        $P2PKIT_WASM_CONTROL_MODE: "='queued'",
        $P2PKIT_WASM_COMMANDS_MODE: "='drop'",
        $P2PKIT_WASM_CONTROL_HIGH_WATER: '=(512 * 1024)',
        $P2PKIT_WASM_CONTROL_LOW_WATER: '=(128 * 1024)',
        $P2PKIT_WASM_COMMANDS_HIGH_WATER: '=(512 * 1024)',
        $P2PKIT_WASM_COMMANDS_LOW_WATER: '=0',
        $P2PKIT_WASM_MAX_SIGNAL_BYTES: '=(256 * 1024)',
        $P2PKIT_WASM_EVENT_CONNECT: '=0',
        $P2PKIT_WASM_EVENT_DISCONNECT: '=1',
        $P2PKIT_WASM_EVENT_MESSAGE: '=2',
        $P2PKIT_WASM_EVENT_STATE: '=3',
        $P2PKIT_WASM_EVENT_MATCHED: '=4',
        $P2PKIT_WASM_STATE_IDLE: '=0',
        $P2PKIT_WASM_STATE_CONNECTING: '=1',
        $P2PKIT_WASM_STATE_CONNECTED: '=2',
        $P2PKIT_WASM_STATE_FAILED: '=3',

        $resolveP2pkit: resolveP2pkit,
        $resolveSignalingUrl: resolveSignalingUrl,

        // Retain the factory in emitted JS; Emscripten only keeps $-prefixed
        // library symbols. __deps recursively retains every $P2PKIT_WASM_*
        // constant above, so the emitted factory has no free missing
        // identifiers. RTCTransport itself is resolved at runtime from the
        // p2pkit IIFE bundle (config.p2pkit / globalThis.P2PKIT_IIFE), never
        // bundled into the link — the core MIT bundle must not absorb this
        // GPL-derived glue.
        $createP2pkitWasmGlue__deps: [
            '$P2PKIT_WASM_CONTROL_LABEL', '$P2PKIT_WASM_COMMANDS_LABEL',
            '$P2PKIT_WASM_CONTROL_OPTIONS', '$P2PKIT_WASM_COMMANDS_OPTIONS',
            '$P2PKIT_WASM_CONTROL_MODE', '$P2PKIT_WASM_COMMANDS_MODE',
            '$P2PKIT_WASM_CONTROL_HIGH_WATER', '$P2PKIT_WASM_CONTROL_LOW_WATER',
            '$P2PKIT_WASM_COMMANDS_HIGH_WATER', '$P2PKIT_WASM_COMMANDS_LOW_WATER',
            '$P2PKIT_WASM_MAX_SIGNAL_BYTES',
            '$P2PKIT_WASM_EVENT_CONNECT', '$P2PKIT_WASM_EVENT_DISCONNECT',
            '$P2PKIT_WASM_EVENT_MESSAGE', '$P2PKIT_WASM_EVENT_STATE',
            '$P2PKIT_WASM_EVENT_MATCHED',
            '$P2PKIT_WASM_STATE_IDLE', '$P2PKIT_WASM_STATE_CONNECTING',
            '$P2PKIT_WASM_STATE_CONNECTED', '$P2PKIT_WASM_STATE_FAILED',
            '$resolveP2pkit', '$resolveSignalingUrl',
        ],
        $createP2pkitWasmGlue: createP2pkitWasmGlue,
    });
}

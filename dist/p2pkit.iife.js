var P2PKIT_IIFE = (function (exports) {
  'use strict';

  // src/transports/negotiate.ts
  var DEFAULT_TRANSPORT_ORDER = ["rtc", "utp", "http", "dht"];
  function chooseTransport(localCaps, remoteCaps, order = DEFAULT_TRANSPORT_ORDER) {
    const remote = new Set(remoteCaps);
    const local = new Set(localCaps);
    for (const name of order) {
      if (local.has(name) && remote.has(name)) return name;
    }
    return void 0;
  }
  function capsFor(enabled) {
    return DEFAULT_TRANSPORT_ORDER.filter((name) => Boolean(enabled[name]));
  }
  function isInitiator(self, remote) {
    return self < remote;
  }

  // src/utils/emitter.ts
  var Emitter = class {
    handlers = {};
    /** Subscribe to `event`. Returns an unsubscribe function. */
    on(event, handler) {
      (this.handlers[event] ??= /* @__PURE__ */ new Set()).add(handler);
      return () => this.off(event, handler);
    }
    /** Subscribe once; the handler is removed after its first invocation. */
    once(event, handler) {
      const wrap = ((...args) => {
        this.off(event, wrap);
        handler(...args);
      });
      return this.on(event, wrap);
    }
    /** Remove a previously registered handler. */
    off(event, handler) {
      this.handlers[event]?.delete(handler);
    }
    /** Synchronously invoke every handler registered for `event`. */
    emit(event, ...args) {
      const set = this.handlers[event];
      if (!set) return;
      for (const handler of [...set]) handler(...args);
    }
    /** Remove all handlers for one event, or all events when `event` is omitted. */
    removeAll(event) {
      if (event !== void 0) this.handlers[event]?.clear();
      else for (const key of Object.keys(this.handlers)) delete this.handlers[key];
    }
    /** Number of handlers registered for `event`. */
    listenerCount(event) {
      return this.handlers[event]?.size ?? 0;
    }
  };

  // src/framing/index.ts
  var DEFAULT_MAX_PACKET_SIZE = 16e3;
  var Chunker = class {
    maxPacketSize;
    inbox = /* @__PURE__ */ new Map();
    constructor(options = {}) {
      this.maxPacketSize = options.maxPacketSize ?? DEFAULT_MAX_PACKET_SIZE;
    }
    /**
     * Split `data` into JSON-encoded {@link ChunkPacket}s under `id`. Always yields
     * at least one packet (even for the empty string). Send each yielded string as
     * one transport message.
     */
    *split(id, data) {
      const size = this.maxPacketSize;
      const n = data.length === 0 ? 1 : Math.ceil(data.length / size);
      for (let i = 0; i < n; i++) {
        const packet = { id, i, n, part: data.slice(i * size, (i + 1) * size) };
        yield JSON.stringify(packet);
      }
    }
    /**
     * Feed one received {@link ChunkPacket}. Returns the fully reassembled string
     * once the final missing fragment arrives, otherwise `undefined`. Duplicate
     * fragments are ignored; fragments may arrive in any order.
     */
    ingest(packet) {
      const { id, i, n, part } = packet;
      if (n <= 1) return part;
      let entry = this.inbox.get(id);
      if (!entry) {
        entry = { parts: new Array(n).fill(void 0), received: 0 };
        this.inbox.set(id, entry);
      }
      if (i < 0 || i >= n) return void 0;
      if (entry.parts[i] === void 0) {
        entry.parts[i] = part;
        entry.received++;
      }
      if (entry.received === n) {
        this.inbox.delete(id);
        return entry.parts.join("");
      }
      return void 0;
    }
    /** Drop any partially-received payloads (e.g. on disconnect). */
    reset() {
      this.inbox.clear();
    }
  };

  // src/utils/id.ts
  function randomId(bytes = 16) {
    const buf = new Uint8Array(bytes);
    globalThis.crypto.getRandomValues(buf);
    let out = "";
    for (const b of buf) out += b.toString(16).padStart(2, "0");
    return out;
  }

  // src/utils/ice.ts
  var DEFAULT_ICE_SERVERS = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" },
    { urls: "stun:stun2.l.google.com:19302" },
    { urls: "stun:global.stun.twilio.com:3478" }
  ];

  // src/transports/rtc-send-queue.ts
  var RTC_SEND_QUEUE_FLUSH_THRESHOLD = 1 << 20;
  function payloadByteLength(data) {
    if (typeof data === "string") return data.length;
    if (data instanceof ArrayBuffer) return data.byteLength;
    return data.byteLength;
  }
  function snapshotPayload(data) {
    if (typeof data === "string") return data;
    const view = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    const copy = new Uint8Array(view.byteLength);
    copy.set(view);
    return copy;
  }
  var RTCDataChannelSendQueue = class {
    channel;
    highWaterBytes;
    lowWaterBytes;
    onDrain;
    queue = [];
    lowHandler;
    flushing = false;
    backpressured = false;
    constructor(options = {}) {
      this.highWaterBytes = options.highWaterBytes;
      if (options.highWaterBytes !== void 0) {
        this.lowWaterBytes = options.lowWaterBytes ?? Math.floor(options.highWaterBytes / 2);
      } else {
        this.lowWaterBytes = options.lowWaterBytes;
      }
      this.onDrain = options.onDrain;
    }
    /** Bind (or re-bind) this queue to a live data channel. */
    attach(channel) {
      this.detach();
      this.channel = channel;
      if (this.highWaterBytes === void 0) return;
      channel.bufferedAmountLowThreshold = this.lowWaterBytes;
      this.lowHandler = () => this.flushQueue();
      if (channel.addEventListener) channel.addEventListener("bufferedamountlow", this.lowHandler);
      else channel.onbufferedamountlow = this.lowHandler;
    }
    /** Detach from the current channel and clear any pending sends. */
    detach() {
      const channel = this.channel;
      if (channel && this.lowHandler) {
        if (channel.removeEventListener) channel.removeEventListener("bufferedamountlow", this.lowHandler);
        else channel.onbufferedamountlow = null;
      }
      this.channel = void 0;
      this.lowHandler = void 0;
      this.queue.length = 0;
      this.flushing = false;
      this.backpressured = false;
    }
    /** Channel `bufferedAmount` plus bytes still waiting in this queue. */
    get bufferedAmount() {
      let pending = 0;
      for (const item of this.queue) pending += payloadByteLength(item);
      return (this.channel?.bufferedAmount ?? 0) + pending;
    }
    /**
     * Synchronous, lossy send attempt: returns `false` (dropping the payload)
     * when the channel is at or above the high-water mark, instead of queueing.
     *
     * Why this exists beside the async {@link send}: Emscripten `ccall` exports
     * (and other synchronous producers of lossy, time-sensitive traffic such as
     * per-tick game state) must learn acceptance in the same tick — a promise
     * would answer after the moment to resend has passed. Only meaningful with
     * {@link highWaterBytes} configured; without it, always sends and returns
     * true (legacy polling path has no drop policy).
     *
     * Never overtakes {@link send}: when older payloads are still queued, this
     * appends behind them (snapshot included) and reports `true` — the payload
     * was accepted and will flush in order.
     */
    trySend(data) {
      const channel = this.channel;
      if (!channel || channel.readyState !== "open") return false;
      if (this.highWaterBytes !== void 0 && channel.bufferedAmount >= this.highWaterBytes) {
        return false;
      }
      if (this.highWaterBytes !== void 0 && this.queue.length > 0) {
        this.queue.push(snapshotPayload(data));
        this.tryFlushAfterSend();
        return true;
      }
      channel.send(data);
      if (this.highWaterBytes === void 0) return true;
      if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true;
      else this.tryFlushAfterSend();
      return true;
    }
    /**
     * Send one payload. With {@link highWaterBytes} configured, payloads are queued
     * (never dropped) while the channel is at or above the high-water mark;
     * otherwise uses the legacy 1 MB polling flush (same as stock RTCTransport).
     * Queued payloads always flush in FIFO order: while any older payload is
     * still queued, new payloads append behind it rather than sending directly.
     */
    async send(data) {
      const channel = this.channel;
      if (!channel || channel.readyState !== "open") throw new Error("RTC data channel is not open");
      if (this.highWaterBytes === void 0) {
        channel.send(data);
        await this.pollFlush(channel);
        return;
      }
      if (channel.bufferedAmount >= this.highWaterBytes) {
        this.backpressured = true;
        this.queue.push(snapshotPayload(data));
        return;
      }
      if (this.queue.length > 0) {
        this.queue.push(snapshotPayload(data));
        this.tryFlushAfterSend();
        return;
      }
      channel.send(data);
      if (channel.bufferedAmount >= this.highWaterBytes) this.backpressured = true;
      else this.tryFlushAfterSend();
    }
    async pollFlush(channel) {
      while (channel.readyState === "open" && channel.bufferedAmount > RTC_SEND_QUEUE_FLUSH_THRESHOLD) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
    tryFlushAfterSend() {
      if (!this.channel || this.highWaterBytes === void 0) return;
      if (this.channel.bufferedAmount < this.highWaterBytes) this.flushQueue();
    }
    flushQueue() {
      if (this.flushing || !this.channel || this.highWaterBytes === void 0) return;
      this.flushing = true;
      try {
        const channel = this.channel;
        while (this.queue.length > 0 && channel.readyState === "open" && channel.bufferedAmount < this.highWaterBytes) {
          channel.send(this.queue.shift());
        }
        if (this.queue.length === 0 && this.backpressured) {
          this.backpressured = false;
          this.onDrain?.();
        }
      } finally {
        this.flushing = false;
      }
    }
  };

  // src/transports/rtc.ts
  function toUint8Array(value) {
    if (value instanceof Uint8Array) return value;
    if (value instanceof ArrayBuffer) return new Uint8Array(value);
    if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    throw new Error("raw RTC payloads must be an ArrayBuffer or ArrayBufferView");
  }
  var RTCTransportConnectTimeoutError = class extends Error {
    name = "ErrorTimeout";
    code = "ERR_RTC_CONNECT_TIMEOUT";
    constructor(timeoutMs) {
      super(`RTC transport connect timed out after ${timeoutMs}ms`);
    }
  };
  var RTCTransportBackpressureDropError = class extends Error {
    name = "ErrorBackpressureDrop";
    code = "ERR_RTC_BACKPRESSURE_DROP";
    constructor(channelIndex) {
      super(`RTC channel ${channelIndex} dropped raw payload under backpressure (drop mode)`);
    }
  };
  var RTCTransport = class {
    remote;
    name = "rtc";
    self;
    signalling;
    emitter = new Emitter();
    channelStates = [];
    channelSpecs;
    expectedChannelCount;
    connectTimeoutMs;
    connectTimer;
    connectEmitted = false;
    openChannels = /* @__PURE__ */ new Set();
    pc;
    remoteDescriptionSet = false;
    pendingCandidates = [];
    closed = false;
    emittedClose = false;
    raw;
    rttMs = 0;
    rttTimer;
    constructor(options) {
      this.self = options.self;
      this.remote = options.remote;
      this.signalling = options.signalling;
      this.channelSpecs = options.channels;
      this.expectedChannelCount = options.channels?.length ?? 1;
      this.connectTimeoutMs = options.connectTimeoutMs;
      this.raw = options.raw === true;
      const iceServers = options.iceServers ?? DEFAULT_ICE_SERVERS;
      this.pc = new options.backend.RTCPeerConnection({ iceServers });
      if (this.connectTimeoutMs !== void 0) {
        this.connectTimer = setTimeout(() => this.handleConnectTimeout(), this.connectTimeoutMs);
      }
      this.pc.onicecandidate = (ev) => {
        if (ev.candidate) {
          this.signalling.send({
            iceCandidate: ev.candidate.toJSON(),
            from: this.self,
            to: this.remote
          });
        }
      };
      this.pc.onconnectionstatechange = () => {
        const state = this.pc.connectionState;
        if (state === "failed" || state === "closed" || state === "disconnected") this.handleClose();
      };
      if (options.initiator) {
        if (options.channels) {
          for (let i = 0; i < options.channels.length; i++) {
            const spec = options.channels[i];
            this.setupChannel(
              i,
              this.pc.createDataChannel(spec.label, {
                ordered: spec.ordered,
                maxRetransmits: spec.maxRetransmits
              }),
              options
            );
          }
        } else {
          this.setupChannel(
            0,
            this.pc.createDataChannel(options.label ?? "p2pkit"),
            options
          );
        }
        void this.negotiate();
      } else {
        this.pc.ondatachannel = (ev) => {
          const label = ev.channel.label;
          if (options.channels) {
            const index = options.channels.findIndex((spec) => spec.label === label);
            if (index === -1) return;
            this.setupChannel(index, ev.channel, options);
          } else {
            this.setupChannel(0, ev.channel, options);
          }
        };
      }
      this.signalling.onMessage(this.onSignal);
    }
    get bufferedAmount() {
      let total = 0;
      for (const state of this.channelStates) total += state.queue.bufferedAmount;
      return total;
    }
    /** Bytes queued on one channel (spec index, or `0` in single-channel mode). */
    bufferedAmountOn(channelIndex) {
      return this.channelStates[channelIndex]?.queue.bufferedAmount ?? 0;
    }
    on(event, handler) {
      this.emitter.on(event, handler);
    }
    async send(value) {
      await this.sendOn(0, value);
    }
    /**
     * Send one value on the given channel; resolves `undefined` once the payload
     * has been handed to the send queue (default JSON mode, preserving the
     * historical `Promise<void>` contract). In raw mode (`raw: true`) binary
     * payloads cross byte-for-byte: on `"drop"`-policy channels the promise
     * rejects with {@link RTCTransportBackpressureDropError} when the payload is
     * rejected under backpressure (use {@link trySendOn} for synchronous
     * acceptance); on `"queue"`-policy channels it never rejects for that reason.
     */
    async sendOn(channelIndex, value) {
      const state = this.channelStates[channelIndex];
      if (!state || state.channel.readyState !== "open") {
        throw new Error("RTC transport is not open");
      }
      if (this.raw) {
        const bytes = toUint8Array(value);
        if (state.spec?.mode === "drop") {
          if (!state.queue.trySend(bytes)) throw new RTCTransportBackpressureDropError(channelIndex);
          return;
        }
        await state.queue.send(bytes);
        return;
      }
      const groupId = randomId(8);
      const data = JSON.stringify(value);
      for (const packet of state.chunker.split(groupId, data)) {
        await state.queue.send(packet);
      }
    }
    /**
     * Synchronous acceptance for raw mode: queues on `"queue"`-policy channels
     * (fire-and-forget) and returns whether the payload was accepted; on
     * `"drop"`-policy channels returns `false` when it was rejected under
     * backpressure instead of queueing stale lossy state.
     *
     * Why this exists beside the async {@link sendOn}: Emscripten `ccall`
     * exports and other synchronous producers must learn send acceptance in the
     * same tick; a promise resolves too late to substitute fresh state.
     */
    trySendOn(channelIndex, value) {
      if (!this.raw) throw new Error("trySendOn requires raw mode (options.raw)");
      const state = this.channelStates[channelIndex];
      if (!state || state.channel.readyState !== "open") return false;
      const bytes = toUint8Array(value);
      if (state.spec?.mode === "drop") return state.queue.trySend(bytes);
      state.queue.send(bytes).catch((err) => {
        this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)));
      });
      return true;
    }
    /**
     * Last known round-trip time estimate for this link in milliseconds, from
     * polling `pc.getStats()` while connected; 0 when not connected.
     *
     * Why cached instead of exposing `getStats()` directly: synchronous C
     * exports (Emscripten `ccall`) cannot await a promise, so the transport
     * keeps a freshest-known value.
     */
    getRoundTripTimeMs() {
      return this.rttMs;
    }
    disconnect() {
      this.closed = true;
      this.clearConnectTimer();
      this.stopRttPolling();
      for (const state of this.channelStates) {
        state.queue.detach();
        try {
          state.channel.close();
        } catch {
        }
      }
      try {
        this.pc.close();
      } catch {
      }
      this.handleClose();
    }
    setupChannel(index, channel, options) {
      const chunker = new Chunker({ maxPacketSize: options.chunkSize });
      const queue = new RTCDataChannelSendQueue({
        highWaterBytes: options.channels?.[index]?.highWaterBytes ?? options.highWaterBytes,
        lowWaterBytes: options.channels?.[index]?.lowWaterBytes ?? options.lowWaterBytes,
        onDrain: () => this.emitter.emit("drain", index)
      });
      queue.attach(channel);
      this.channelStates[index] = { channel, queue, chunker, spec: this.channelSpecs?.[index] };
      try {
        channel.binaryType = "arraybuffer";
      } catch {
      }
      channel.onopen = () => this.onChannelOpen(index);
      channel.onmessage = (ev) => this.onData(index, ev.data);
      channel.onclose = () => this.handleClose();
      channel.onerror = () => this.emitter.emit("error", new Error("RTC data channel error"));
      if (channel.readyState === "open") this.onChannelOpen(index);
    }
    onChannelOpen(index) {
      if (this.closed || this.connectEmitted) return;
      this.openChannels.add(index);
      if (this.openChannels.size >= this.expectedChannelCount) this.emitConnect();
    }
    emitConnect() {
      if (this.connectEmitted || this.closed) return;
      this.connectEmitted = true;
      this.clearConnectTimer();
      this.startRttPolling();
      this.emitter.emit("connect");
    }
    startRttPolling() {
      if (this.rttTimer || typeof this.pc.getStats !== "function") return;
      this.rttTimer = setInterval(() => {
        if (this.closed) {
          this.stopRttPolling();
          return;
        }
        this.pc.getStats().then((report) => {
          if (this.closed || this.rttTimer === void 0) return;
          let best = 0;
          report.forEach((entry) => {
            if (entry.type === "candidate-pair" && entry.state === "succeeded" && typeof entry.currentRoundTripTime === "number") {
              const ms = entry.currentRoundTripTime * 1e3;
              if (best === 0 || ms < best) best = ms;
            }
          });
          if (best > 0) this.rttMs = Math.round(best);
        }).catch(() => {
        });
      }, 2e3);
      if (typeof this.rttTimer === "object" && this.rttTimer && typeof this.rttTimer.unref === "function") {
        this.rttTimer.unref();
      }
    }
    stopRttPolling() {
      if (this.rttTimer !== void 0) {
        clearInterval(this.rttTimer);
        this.rttTimer = void 0;
      }
      this.rttMs = 0;
    }
    clearConnectTimer() {
      if (this.connectTimer !== void 0) {
        clearTimeout(this.connectTimer);
        this.connectTimer = void 0;
      }
    }
    handleConnectTimeout() {
      if (this.closed || this.connectEmitted || this.emittedClose) return;
      this.closed = true;
      this.clearConnectTimer();
      this.stopRttPolling();
      this.emitter.emit("error", new RTCTransportConnectTimeoutError(this.connectTimeoutMs));
      for (const state of this.channelStates) {
        state.queue.detach();
        try {
          state.channel.close();
        } catch {
        }
      }
      try {
        this.pc.close();
      } catch {
      }
      this.handleClose();
    }
    async negotiate() {
      await this.signalling.ready;
      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      this.sendDescription();
    }
    sendDescription() {
      const description = this.pc.localDescription;
      if (!description) return;
      this.signalling.send({
        description: { type: description.type, sdp: description.sdp },
        from: this.self,
        to: this.remote
      });
    }
    onSignal = (message) => {
      void this.handleSignal(message);
    };
    async handleSignal(message) {
      if (this.closed) return;
      if ("description" in message) {
        if (message.from !== this.remote || message.to !== this.self) return;
        await this.pc.setRemoteDescription(message.description);
        this.remoteDescriptionSet = true;
        await this.flushCandidates();
        if (message.description.type === "offer") {
          const answer = await this.pc.createAnswer();
          await this.pc.setLocalDescription(answer);
          this.sendDescription();
        }
      } else if ("iceCandidate" in message) {
        if (message.from !== this.remote || message.to !== this.self) return;
        if (this.remoteDescriptionSet) await this.pc.addIceCandidate(message.iceCandidate);
        else this.pendingCandidates.push(message.iceCandidate);
      }
    }
    async flushCandidates() {
      for (const candidate of this.pendingCandidates.splice(0)) {
        try {
          await this.pc.addIceCandidate(candidate);
        } catch {
        }
      }
    }
    onData(channelIndex, data) {
      const state = this.channelStates[channelIndex];
      if (!state) return;
      if (this.raw) {
        if (typeof data === "string") return;
        const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
        this.emitter.emit("message", bytes, channelIndex);
        return;
      }
      const raw = typeof data === "string" ? data : new TextDecoder().decode(data);
      let packet;
      try {
        packet = JSON.parse(raw);
      } catch {
        return;
      }
      const full = state.chunker.ingest(packet);
      if (full === void 0) return;
      try {
        this.emitter.emit("message", JSON.parse(full), channelIndex);
      } catch {
      }
    }
    handleClose() {
      if (this.emittedClose) return;
      this.emittedClose = true;
      this.clearConnectTimer();
      this.stopRttPolling();
      for (const state of this.channelStates) {
        state.chunker.reset();
        state.queue.detach();
      }
      this.emitter.emit("disconnect");
    }
  };

  // src/utils/sdp.ts
  function extractIP(sdp) {
    for (const raw of sdp.split(/\r?\n/)) {
      const match = /^c=IN IP[46] (\S+)/.exec(raw.trim());
      const ip = match?.[1];
      if (ip && ip !== "0.0.0.0" && ip !== "::") return ip;
    }
    return void 0;
  }
  globalThis.P2PKIT_IIFE = P2PKIT_IIFE;

  exports.DEFAULT_ICE_SERVERS = DEFAULT_ICE_SERVERS;
  exports.DEFAULT_TRANSPORT_ORDER = DEFAULT_TRANSPORT_ORDER;
  exports.Emitter = Emitter;
  exports.RTCDataChannelSendQueue = RTCDataChannelSendQueue;
  exports.RTCTransport = RTCTransport;
  exports.RTCTransportConnectTimeoutError = RTCTransportConnectTimeoutError;
  exports.RTC_SEND_QUEUE_FLUSH_THRESHOLD = RTC_SEND_QUEUE_FLUSH_THRESHOLD;
  exports.capsFor = capsFor;
  exports.chooseTransport = chooseTransport;
  exports.extractIP = extractIP;
  exports.isInitiator = isInitiator;
  exports.randomId = randomId;

  return exports;

})({});

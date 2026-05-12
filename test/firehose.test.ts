import { describe, it, expect, vi } from "vitest";
import { Mesh0, ConfigurationError, NetworkError } from "../src/index.js";
import type { EventRow, FirehoseFrame } from "../src/index.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

type Listener = (ev: unknown) => void;

class FakeWS {
  static lastUrl = "";
  static lastProtocols: string | string[] | undefined;
  static instances: FakeWS[] = [];
  listeners = new Map<string, Listener[]>();
  closeCalls: { code?: number; reason?: string }[] = [];
  constructor(url: string, protocols?: string | string[]) {
    FakeWS.lastUrl = url;
    FakeWS.lastProtocols = protocols;
    FakeWS.instances.push(this);
  }
  addEventListener(type: string, listener: Listener) {
    const arr = this.listeners.get(type) ?? [];
    arr.push(listener);
    this.listeners.set(type, arr);
  }
  emit(type: string, ev: unknown) {
    for (const fn of this.listeners.get(type) ?? []) fn(ev);
  }
  send() {}
  close(code = 1000, reason = "") {
    this.closeCalls.push({ code, reason });
    this.emit("close", { code, reason });
  }
}

function newMesh(baseUrl?: string) {
  FakeWS.instances = [];
  return new Mesh0({
    apiKey: KEY,
    ...(baseUrl ? { baseUrl } : {}),
    fetch: (async () => new Response("{}")) as typeof fetch,
    WebSocket: FakeWS as unknown as typeof WebSocket,
  });
}

const ws = { transport: "ws" } as const;

describe("WebSocket firehose transport", () => {
  describe("URL + params", () => {
    it("connects with token subprotocol and rewrites http(s) to ws(s)", () => {
      const m = newMesh();
      m.firehose({ ...ws, since: "latest" });
      expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?since=latest");
      expect(FakeWS.lastProtocols).toEqual([`mesh0.token.${KEY}`]);
    });

    it("omits ?since when not supplied", () => {
      const m = newMesh();
      m.firehose(ws);
      expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose");
    });

    it("threads ?root=1 when root-only is requested", () => {
      const m = newMesh();
      m.firehose({ ...ws, root: true });
      expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?root=1");
    });

    it("combines since and root in the query string", () => {
      const m = newMesh();
      m.firehose({ ...ws, since: "earliest", root: true });
      expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?since=earliest&root=1");
    });

    it("rewrites http://localhost baseUrl to ws://", () => {
      const m = newMesh("http://localhost:8080");
      m.firehose({ ...ws, since: "earliest" });
      expect(FakeWS.lastUrl).toBe("ws://localhost:8080/v1/firehose?since=earliest");
    });
  });

  describe("transport selection", () => {
    it("defaults transport to ws when omitted", () => {
      const m = newMesh();
      m.firehose();
      expect(FakeWS.instances).toHaveLength(1);
    });

    it("throws ConfigurationError when no WebSocket is available", () => {
      vi.stubGlobal("WebSocket", undefined);
      try {
        const m = new Mesh0({
          apiKey: KEY,
          fetch: (async () => new Response("{}")) as typeof fetch,
        });
        expect(() => m.firehose(ws)).toThrow(ConfigurationError);
      } finally {
        vi.unstubAllGlobals();
      }
    });
  });

  describe("framing", () => {
    it("dispatches hello/event/ping and resolves closed cleanly on 1000", async () => {
      const m = newMesh();
      const rows: EventRow[] = [];
      const frames: FirehoseFrame[] = [];
      let ping = 0;
      let hello = { topic: "", since: "", root: false };
      const handle = m.firehose(ws, {
        onHello: (h) => {
          hello = h;
        },
        onEvent: (r) => rows.push(r),
        onPing: (t) => {
          ping = t;
        },
        onMessage: (f) => frames.push(f),
      });
      const sock = FakeWS.instances[0]!;
      sock.emit("message", {
        data: JSON.stringify({ type: "hello", topic: "events.org.r1", since: "latest", root: true }),
      });
      sock.emit("message", {
        data: JSON.stringify({
          type: "event",
          partition: 0,
          offset: "42",
          row: {
            event_id: "e1",
            trace_id: "t",
            span_id: "s",
            parent_span_id: "",
            timestamp: "x",
            project_id: "p",
            attributes: {},
          },
        }),
      });
      sock.emit("message", { data: JSON.stringify({ type: "ping", ts: 123 }) });

      expect(hello).toEqual({ topic: "events.org.r1", since: "latest", root: true });
      expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
      expect(ping).toBe(123);
      expect(frames.map((f) => f.kind)).toEqual(["hello", "event", "ping"]);

      // Server closes normally (no user-initiated close).
      sock.emit("close", { code: 1000, reason: "" });
      const info = await handle.closed;
      expect(info.kind).toBe("ok");
      expect(info.code).toBe(1000);
    });

    it("surfaces hello.root=true through the callback", () => {
      const m = newMesh();
      let root = false;
      m.firehose(ws, {
        onHello: (h) => {
          root = h.root;
        },
      });
      FakeWS.instances[0]!.emit("message", {
        data: JSON.stringify({ type: "hello", topic: "t", since: "latest", root: true }),
      });
      expect(root).toBe(true);
    });
  });

  describe("terminal frames", () => {
    it("resync frame resolves closed with kind=resync", async () => {
      const m = newMesh();
      const resyncs: { reason: string; dropped: number }[] = [];
      const handle = m.firehose(ws, { onResync: (r) => resyncs.push(r) });
      const sock = FakeWS.instances[0]!;
      sock.emit("message", {
        data: JSON.stringify({ type: "resync", reason: "overflow", dropped: 5 }),
      });
      sock.emit("close", { code: 1000, reason: "" });
      const info = await handle.closed;
      expect(resyncs).toEqual([{ reason: "overflow", dropped: 5 }]);
      expect(info.kind).toBe("resync");
      expect(info.resync).toEqual({ reason: "overflow", dropped: 5 });
    });

    it("error frame resolves closed with kind=error and serverError", async () => {
      const m = newMesh();
      const handle = m.firehose(ws);
      const sock = FakeWS.instances[0]!;
      sock.emit("message", {
        data: JSON.stringify({ type: "error", reason: "project_disabled", errorId: "tr-9" }),
      });
      sock.emit("close", { code: 1000, reason: "" });
      const info = await handle.closed;
      expect(info.kind).toBe("error");
      expect(info.serverError).toEqual({ reason: "project_disabled", errorId: "tr-9" });
    });
  });

  describe("error surface", () => {
    it("marks closed.kind=transport after malformed JSON frame", async () => {
      const m = newMesh();
      const errors: NetworkError[] = [];
      const handle = m.firehose(ws, { onError: (e) => errors.push(e) });
      const sock = FakeWS.instances[0]!;
      sock.emit("message", { data: "not-json{" });
      expect(errors).toHaveLength(1);
      expect(errors[0]).toBeInstanceOf(NetworkError);
      sock.emit("close", { code: 1000, reason: "" });
      const info = await handle.closed;
      expect(info.kind).toBe("transport");
    });

    it("surfaces unknown message types via onError", async () => {
      const m = newMesh();
      const errors: NetworkError[] = [];
      const handle = m.firehose(ws, { onError: (e) => errors.push(e) });
      const sock = FakeWS.instances[0]!;
      sock.emit("message", { data: JSON.stringify({ type: "future_kind" }) });
      expect(errors).toHaveLength(1);
      expect(errors[0]!.message).toContain("future_kind");
      sock.emit("close", { code: 1000, reason: "" });
      await handle.closed;
    });

    it("surfaces unsupported binary frame shapes via onError", () => {
      const m = newMesh();
      const errors: NetworkError[] = [];
      m.firehose(ws, { onError: (e) => errors.push(e) });
      // Blob is not handled; the SDK should report rather than silently drop.
      const blob = { /* not Uint8Array, not ArrayBuffer, not string */ } as unknown;
      FakeWS.instances[0]!.emit("message", { data: blob });
      expect(errors).toHaveLength(1);
      expect(errors[0]!.message).toContain("unsupported frame shape");
    });

    it("decodes Uint8Array frames", () => {
      const m = newMesh();
      const frames: FirehoseFrame[] = [];
      m.firehose(ws, { onMessage: (f) => frames.push(f) });
      const json = JSON.stringify({ type: "ping", ts: 7 });
      const data = new TextEncoder().encode(json);
      FakeWS.instances[0]!.emit("message", { data });
      expect(frames).toHaveLength(1);
      expect(frames[0]!.kind).toBe("ping");
    });

    it("surfaces socket-level errors via onError with cause and kind=transport", async () => {
      const m = newMesh();
      const errors: NetworkError[] = [];
      const handle = m.firehose(ws, { onError: (e) => errors.push(e) });
      const sock = FakeWS.instances[0]!;
      const underlying = new Error("ECONNRESET");
      sock.emit("error", { message: "socket hung up", error: underlying });
      expect(errors[0]!.message).toBe("socket hung up");
      expect(errors[0]!.cause).toBe(underlying);
      sock.emit("close", { code: 1006, reason: "" });
      const info = await handle.closed;
      expect(info.kind).toBe("transport");
      expect(info.code).toBe(1006);
    });

    it("surfaces typed error events without a message via the event type", () => {
      const m = newMesh();
      const errors: NetworkError[] = [];
      m.firehose(ws, { onError: (e) => errors.push(e) });
      FakeWS.instances[0]!.emit("error", { type: "error" });
      expect(errors[0]!.message).toContain("error");
    });
  });

  describe("lifecycle", () => {
    it("abnormal close (no error frame) resolves kind=transport", async () => {
      const m = newMesh();
      const handle = m.firehose(ws);
      FakeWS.instances[0]!.emit("close", { code: 1006, reason: "abnormal" });
      const info = await handle.closed;
      expect(info.kind).toBe("transport");
      expect(info.code).toBe(1006);
      expect(info.reason).toBe("abnormal");
    });

    it("caller close() resolves kind=aborted and forwards code/reason", async () => {
      const m = newMesh();
      const handle = m.firehose(ws);
      handle.close(4000, "bye");
      const info = await handle.closed;
      expect(info.kind).toBe("aborted");
      expect(info.code).toBe(4000);
      expect(info.reason).toBe("bye");
    });
  });
});

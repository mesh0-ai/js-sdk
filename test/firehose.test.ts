import { describe, it, expect, vi } from "vitest";
import { Mesh0, ConfigurationError, NetworkError } from "../src/index.js";
import type { EventRow, FirehoseMessage } from "../src/types.js";

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

function newMesh(extra: { since?: "earliest" | "latest" | string } = {}) {
  FakeWS.instances = [];
  const m = new Mesh0({
    apiKey: KEY,
    fetch: (async () => new Response("{}")) as typeof fetch,
    WebSocket: FakeWS as unknown as typeof WebSocket,
  });
  return { m, extra };
}

describe("WebSocket firehose", () => {
  it("connects with token subprotocol and rewrites http(s) to ws(s)", () => {
    const { m } = newMesh();
    m.firehose({ since: "latest" });
    expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?since=latest");
    expect(FakeWS.lastProtocols).toEqual([`mesh0.token.${KEY}`]);
  });

  it("omits ?since when not supplied", () => {
    const { m } = newMesh();
    m.firehose();
    expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose");
  });

  it("threads ?root=1 when root-only is requested", () => {
    const { m } = newMesh();
    m.firehose({ root: true });
    expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?root=1");
  });

  it("combines since and root in the query string", () => {
    const { m } = newMesh();
    m.firehose({ since: "earliest", root: true });
    expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?since=earliest&root=1");
  });

  it("rewrites http://localhost baseUrl to ws://", () => {
    FakeWS.instances = [];
    const m = new Mesh0({
      apiKey: KEY,
      baseUrl: "http://localhost:8080",
      fetch: (async () => new Response("{}")) as typeof fetch,
      WebSocket: FakeWS as unknown as typeof WebSocket,
    });
    m.firehose({ since: "earliest" });
    expect(FakeWS.lastUrl).toBe("ws://localhost:8080/v1/firehose?since=earliest");
  });

  it("throws ConfigurationError when no WebSocket is available", () => {
    // Node 22+ provides globalThis.WebSocket; simulate Node <22 by stubbing
    // it to undefined for the duration of the test.
    vi.stubGlobal("WebSocket", undefined);
    try {
      const m = new Mesh0({
        apiKey: KEY,
        fetch: (async () => new Response("{}")) as typeof fetch,
      });
      expect(() => m.firehose()).toThrow(ConfigurationError);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("dispatches hello/event/ping and resolves closed cleanly on 1000", async () => {
    const { m } = newMesh();
    const rows: EventRow[] = [];
    const messages: FirehoseMessage[] = [];
    let ping = 0;
    let hello = { topic: "", since: "", root: false };
    const handle = m.firehose(
      {},
      {
        onHello: (h) => {
          hello = h;
        },
        onEvent: (r) => rows.push(r),
        onPing: (t) => {
          ping = t;
        },
        onMessage: (msg) => messages.push(msg),
      },
    );
    const ws = FakeWS.instances[0]!;
    ws.emit("message", {
      data: JSON.stringify({ type: "hello", topic: "events.org.r1", since: "latest", root: true }),
    });
    ws.emit("message", {
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
    ws.emit("message", { data: JSON.stringify({ type: "ping", ts: 123 }) });

    expect(hello).toEqual({ topic: "events.org.r1", since: "latest", root: true });
    expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
    expect(ping).toBe(123);
    expect(messages).toHaveLength(3);

    handle.close();
    const info = await handle.closed;
    expect(info.code).toBe(1000);
    expect(info.clean).toBe(true);
  });

  it("marks closed.clean=false after malformed JSON frame", async () => {
    const { m } = newMesh();
    const errors: NetworkError[] = [];
    const handle = m.firehose({}, { onError: (e) => errors.push(e) });
    const ws = FakeWS.instances[0]!;
    ws.emit("message", { data: "not-json{" });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(NetworkError);
    handle.close();
    const info = await handle.closed;
    expect(info.clean).toBe(false);
  });

  it("surfaces unknown message types via onError", async () => {
    const { m } = newMesh();
    const errors: NetworkError[] = [];
    const handle = m.firehose({}, { onError: (e) => errors.push(e) });
    const ws = FakeWS.instances[0]!;
    ws.emit("message", { data: JSON.stringify({ type: "future_kind" }) });
    expect(errors).toHaveLength(1);
    expect(errors[0]!.message).toContain("future_kind");
    handle.close();
    await handle.closed;
  });

  it("surfaces socket-level errors via onError with cause", () => {
    const { m } = newMesh();
    const errors: NetworkError[] = [];
    m.firehose({}, { onError: (e) => errors.push(e) });
    const ws = FakeWS.instances[0]!;
    const underlying = new Error("ECONNRESET");
    ws.emit("error", { message: "socket hung up", error: underlying });
    expect(errors).toHaveLength(1);
    expect(errors[0]!.message).toBe("socket hung up");
    expect(errors[0]!.cause).toBe(underlying);
  });
});

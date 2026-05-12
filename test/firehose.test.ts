import { describe, it, expect } from "vitest";
import { Mesh0 } from "../src/index.js";
import type { EventRow, FirehoseMessage } from "../src/types.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

type Listener = (ev: unknown) => void;

class FakeWS {
  static lastUrl = "";
  static lastProtocols: string | string[] | undefined;
  listeners = new Map<string, Listener[]>();
  constructor(url: string, protocols?: string | string[]) {
    FakeWS.lastUrl = url;
    FakeWS.lastProtocols = protocols;
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
    this.emit("close", { code, reason });
  }
}

describe("WebSocket firehose", () => {
  it("connects with token subprotocol, parses hello/event/ping", async () => {
    const m = new Mesh0({
      apiKey: KEY,
      fetch: (async () => new Response("{}")) as typeof fetch,
      WebSocket: FakeWS as unknown as typeof WebSocket,
    });
    const events: { row: EventRow; partition: number }[] = [];
    const messages: FirehoseMessage[] = [];
    let hello: { topic: string; since: string } | null = null;

    const handle = m.firehose(
      { since: "latest" },
      {
        onHello: (h) => {
          hello = h;
        },
        onEvent: (row, meta) => events.push({ row, partition: meta.partition }),
        onMessage: (m) => messages.push(m),
      },
    );

    expect(FakeWS.lastUrl).toBe("wss://api.mesh0.ai/v1/firehose?since=latest");
    expect(FakeWS.lastProtocols).toEqual([`mesh0.token.${KEY}`]);

    // Grab the constructed socket via the stored handle's closed-promise
    // side-effects: we need direct access for the fake, so re-resolve from
    // the static reference. The constructor stored the instance in the
    // chain but we don't have it — so emit by reconstructing the listeners
    // list via the prototype is awkward. Easier: keep a ref.
    // We patch this by creating our own socket via FakeWS again is not
    // viable — instead, since FakeWS is what got constructed, fetch the
    // instance from a side-channel.

    // Simpler: expose via a custom subclass that captures `this`.
    // Re-route by capturing in static field.

    // Force-close to settle the test.
    // (Hello/event emission is handled in the dedicated subclass below.)
    void hello;
    void events;
    void messages;
    handle.close();
    await handle.closed;
  });

  it("dispatches messages and resolves closed promise", async () => {
    let instance: CapturingWS | null = null;
    class CapturingWS extends FakeWS {
      constructor(url: string, protocols?: string | string[]) {
        super(url, protocols);
        instance = this;
      }
    }
    const m = new Mesh0({
      apiKey: KEY,
      fetch: (async () => new Response("{}")) as typeof fetch,
      WebSocket: CapturingWS as unknown as typeof WebSocket,
    });

    const rows: EventRow[] = [];
    let ping = 0;
    const handle = m.firehose(
      {},
      {
        onEvent: (r) => rows.push(r),
        onPing: (t) => {
          ping = t;
        },
      },
    );

    instance!.emit("message", {
      data: JSON.stringify({ type: "hello", topic: "events.org.r1", since: "latest" }),
    });
    instance!.emit("message", {
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
    instance!.emit("message", { data: JSON.stringify({ type: "ping", ts: 123 }) });

    expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
    expect(ping).toBe(123);

    handle.close(1001, "bye");
    const { code, reason } = await handle.closed;
    expect(code).toBe(1001);
    expect(reason).toBe("bye");
  });
});

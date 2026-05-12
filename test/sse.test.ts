import { describe, it, expect } from "vitest";
import { Mesh0, AuthenticationError, NetworkError } from "../src/index.js";
import type { EventRow } from "../src/types.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

function sseStream(chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const c of chunks) {
        controller.enqueue(enc.encode(c));
      }
      controller.close();
    },
  });
}

describe("SSE stream", () => {
  it("parses hello/event/ping records and surfaces them via callbacks", async () => {
    const body = sseStream([
      "event: hello\ndata: {}\n\n",
      "event: ping\ndata: 1700000000000\n\n",
      'event: event\ndata: {"event_id":"e1","trace_id":"t","span_id":"s","parent_span_id":"","timestamp":"2026-01-01T00:00:00Z","project_id":"p","attributes":{"k":"v"}}\n\n',
      "event: ping\ndata: 1700000001000\n\n",
    ]);
    const fetchFn = (async () =>
      new Response(body, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });

    const rows: EventRow[] = [];
    const pings: number[] = [];
    let helloed = false;
    const handle = m.stream({
      onHello: () => {
        helloed = true;
      },
      onPing: (t) => pings.push(t),
      onEvent: (r) => rows.push(r),
    });
    await handle.done;
    expect(helloed).toBe(true);
    expect(pings).toEqual([1_700_000_000_000, 1_700_000_001_000]);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.event_id).toBe("e1");
    expect(rows[0]!.attributes).toEqual({ k: "v" });
  });

  it("handles records split across chunks", async () => {
    const body = sseStream([
      "event: event\ndata: ",
      '{"event_id":"e1","trace_id":"t","span_id":"s","parent_span_id":"","timestamp":"x","project_id":"p","attributes":{}}',
      "\n\n",
    ]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const rows: EventRow[] = [];
    await m.stream({ onEvent: (r) => rows.push(r) }).done;
    expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
  });

  it("accepts ping as {ts: n} as well as bare number", async () => {
    const body = sseStream(['event: ping\ndata: {"ts":42}\n\n']);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const pings: number[] = [];
    await m.stream({ onPing: (t) => pings.push(t) }).done;
    expect(pings).toEqual([42]);
  });

  it("surfaces resync via onResync callback", async () => {
    const body = sseStream(["event: resync\ndata: {}\n\n"]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    let resynced = false;
    await m.stream({
      onResync: () => {
        resynced = true;
      },
    }).done;
    expect(resynced).toBe(true);
  });

  it("rejects done when server sends an error frame", async () => {
    const body = sseStream([
      'event: error\ndata: {"reason":"project_disabled","errorId":"trace-9"}\n\n',
    ]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const seen: { reason: string; errorId?: string }[] = [];
    const err = await m
      .stream({ onError: (e) => seen.push(e) })
      .done.catch((e: unknown) => e);
    expect(seen).toEqual([{ reason: "project_disabled", errorId: "trace-9" }]);
    expect(err).toBeInstanceOf(NetworkError);
    expect((err as Error).message).toContain("project_disabled");
  });

  it("maps non-2xx connect response to an ApiError subtype with body detail", async () => {
    const fetchFn = (async () =>
      new Response(JSON.stringify({ error: "unauthorized", reason: "bad_token" }), {
        status: 401,
        headers: { "content-type": "application/json" },
      })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const err = await m.stream().done.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(AuthenticationError);
    expect((err as AuthenticationError).status).toBe(401);
  });

  it("close() resolves done cleanly mid-stream", async () => {
    let cancelled = false;
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          // When the SSE client aborts, end the body so reader.read()
          // unblocks. Real fetch implementations do this automatically.
          init?.signal?.addEventListener("abort", () => {
            cancelled = true;
            controller.close();
          });
        },
      });
      return new Response(body, { status: 200 });
    }) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const handle = m.stream();
    setTimeout(() => handle.close(), 5);
    await expect(handle.done).resolves.toBeUndefined();
    expect(cancelled).toBe(true);
  });
});

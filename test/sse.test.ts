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

const helloLine = (extra: Record<string, unknown> = {}) =>
  `event: hello\ndata: ${JSON.stringify({ topic: "events.org.r1", since: "latest", root: false, ...extra })}\n\n`;

const eventLine = (id: string, partition = 0, offset = "42"): string => {
  const row = {
    event_id: id,
    trace_id: "t",
    span_id: "s",
    parent_span_id: "",
    timestamp: "2026-01-01T00:00:00Z",
    project_id: "p",
    attributes: { k: "v" },
  };
  return `event: event\ndata: ${JSON.stringify({ partition, offset, row })}\n\n`;
};

describe("SSE firehose stream", () => {
  it("hits /v1/firehose and threads since/root params", async () => {
    let capturedUrl = "";
    const fetchFn = (async (url: string) => {
      capturedUrl = url;
      return new Response(sseStream([helloLine()]), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    }) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    await m.stream({ since: "earliest", root: true }).done;
    expect(capturedUrl).toBe("https://api.mesh0.ai/v1/firehose?since=earliest&root=1");
  });

  it("omits query when no opts are supplied", async () => {
    let capturedUrl = "";
    const fetchFn = (async (url: string) => {
      capturedUrl = url;
      return new Response(sseStream([helloLine()]), { status: 200 });
    }) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    await m.stream().done;
    expect(capturedUrl).toBe("https://api.mesh0.ai/v1/firehose");
  });

  it("parses hello/event/ping records and surfaces them via callbacks", async () => {
    const body = sseStream([
      helloLine({ root: true }),
      "event: ping\ndata: 1700000000000\n\n",
      eventLine("e1", 3, "100"),
      "event: ping\ndata: 1700000001000\n\n",
    ]);
    const fetchFn = (async () =>
      new Response(body, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });

    const rows: { row: EventRow; partition: number; offset: string }[] = [];
    const pings: number[] = [];
    let hello = { topic: "", since: "", root: false };
    const handle = m.stream(undefined, {
      onHello: (h) => {
        hello = h;
      },
      onPing: (t) => pings.push(t),
      onEvent: (row, meta) => rows.push({ row, partition: meta.partition, offset: meta.offset }),
    });
    await handle.done;
    expect(hello).toEqual({ topic: "events.org.r1", since: "latest", root: true });
    expect(pings).toEqual([1_700_000_000_000, 1_700_000_001_000]);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.row.event_id).toBe("e1");
    expect(rows[0]!.partition).toBe(3);
    expect(rows[0]!.offset).toBe("100");
    expect(rows[0]!.row.attributes).toEqual({ k: "v" });
  });

  it("handles records split across chunks", async () => {
    const eventFrame = eventLine("e1");
    const half = eventFrame.length >> 1;
    const body = sseStream([eventFrame.slice(0, half), eventFrame.slice(half)]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const rows: EventRow[] = [];
    await m.stream(undefined, { onEvent: (r) => rows.push(r) }).done;
    expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
  });

  it("accepts ping as {ts: n} as well as bare number", async () => {
    const body = sseStream(['event: ping\ndata: {"ts":42}\n\n']);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const pings: number[] = [];
    await m.stream(undefined, { onPing: (t) => pings.push(t) }).done;
    expect(pings).toEqual([42]);
  });

  it("surfaces resync.reason/dropped and rejects done", async () => {
    const body = sseStream([
      'event: resync\ndata: {"reason":"overflow","dropped":17}\n\n',
    ]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const resyncs: { reason: string; dropped: number }[] = [];
    const err = await m
      .stream(undefined, { onResync: (info) => resyncs.push(info) })
      .done.catch((e: unknown) => e);
    expect(resyncs).toEqual([{ reason: "overflow", dropped: 17 }]);
    expect(err).toBeInstanceOf(NetworkError);
    expect((err as Error).message).toContain("overflow");
    expect((err as Error).message).toContain("dropped=17");
  });

  it("rejects done when server sends an error frame", async () => {
    const body = sseStream([
      'event: error\ndata: {"reason":"project_disabled","errorId":"trace-9"}\n\n',
    ]);
    const fetchFn = (async () => new Response(body, { status: 200 })) as typeof fetch;
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn });
    const seen: { reason: string; errorId?: string }[] = [];
    const err = await m
      .stream(undefined, { onError: (e) => seen.push(e) })
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

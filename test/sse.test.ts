import { describe, it, expect } from "vitest";
import {
  Mesh0,
  AuthenticationError,
  BadRequestError,
  NotFoundError,
  RateLimitError,
  ServerError,
  NetworkError,
} from "../src/index.js";
import type { EventRow, FirehoseFrame } from "../src/index.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

function sseStream(chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const c of chunks) controller.enqueue(enc.encode(c));
      controller.close();
    },
  });
}

const helloLine = (extra: Record<string, unknown> = {}) =>
  `event: hello\ndata: ${JSON.stringify({
    topic: "events.org.r1",
    since: "latest",
    root: false,
    ...extra,
  })}\n\n`;

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

function mesh(handler: (url: string, init?: RequestInit) => Promise<Response>): Mesh0 {
  return new Mesh0({ apiKey: KEY, fetch: handler as typeof fetch });
}

const sse = { transport: "sse" } as const;

describe("SSE firehose transport", () => {
  describe("URL + params", () => {
    it("hits /v1/firehose and threads since/root", async () => {
      let capturedUrl = "";
      const m = mesh(async (url) => {
        capturedUrl = url;
        return new Response(sseStream([helloLine()]), { status: 200 });
      });
      await m.firehose({ ...sse, since: "earliest", root: true }).closed;
      expect(capturedUrl).toBe("https://api.mesh0.ai/v1/firehose?since=earliest&root=1");
    });

    it("omits query when no opts supplied", async () => {
      let capturedUrl = "";
      const m = mesh(async (url) => {
        capturedUrl = url;
        return new Response(sseStream([helloLine()]), { status: 200 });
      });
      await m.firehose(sse).closed;
      expect(capturedUrl).toBe("https://api.mesh0.ai/v1/firehose");
    });

    it("sends Authorization and Accept headers", async () => {
      let capturedHeaders: HeadersInit | undefined;
      const m = mesh(async (_url, init) => {
        capturedHeaders = init?.headers;
        return new Response(sseStream([helloLine()]), { status: 200 });
      });
      await m.firehose(sse).closed;
      const h = capturedHeaders as Record<string, string>;
      expect(h.Authorization).toBe(`Bearer ${KEY}`);
      expect(h.Accept).toBe("text/event-stream");
    });
  });

  describe("framing", () => {
    it("dispatches hello/event/ping with structured callbacks", async () => {
      const body = sseStream([
        helloLine({ root: true }),
        "event: ping\ndata: 1700000000000\n\n",
        eventLine("e1", 3, "100"),
        "event: ping\ndata: 1700000001000\n\n",
      ]);
      const m = mesh(async () => new Response(body, { status: 200 }));

      const rows: { row: EventRow; partition: number; offset: string }[] = [];
      const pings: number[] = [];
      let hello = { topic: "", since: "", root: false };
      const frames: FirehoseFrame[] = [];

      const info = await m.firehose(sse, {
        onHello: (h) => {
          hello = h;
        },
        onPing: (t) => pings.push(t),
        onEvent: (row, meta) => rows.push({ row, partition: meta.partition, offset: meta.offset }),
        onMessage: (f) => frames.push(f),
      }).closed;

      expect(info.kind).toBe("ok");
      expect(hello).toEqual({ topic: "events.org.r1", since: "latest", root: true });
      expect(pings).toEqual([1_700_000_000_000, 1_700_000_001_000]);
      expect(rows).toHaveLength(1);
      expect(rows[0]!.row.event_id).toBe("e1");
      expect(rows[0]!.partition).toBe(3);
      expect(rows[0]!.offset).toBe("100");
      expect(frames.map((f) => f.kind)).toEqual(["hello", "ping", "event", "ping"]);
    });

    it("handles records split across LF chunks", async () => {
      const frame = eventLine("e1");
      const half = frame.length >> 1;
      const body = sseStream([frame.slice(0, half), frame.slice(half)]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const rows: EventRow[] = [];
      await m.firehose(sse, { onEvent: (r) => rows.push(r) }).closed;
      expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
    });

    it("normalizes CRLF when chunks split a \\r\\n pair", async () => {
      const frame = `event: hello\r\ndata: ${JSON.stringify({
        topic: "t",
        since: "latest",
        root: false,
      })}\r\n\r\n`;
      // Split right inside the \r\n separator so each half is processed
      // before the pair is reassembled.
      const cut = frame.indexOf("\r\n") + 1;
      const body = sseStream([frame.slice(0, cut), frame.slice(cut)]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      let hello = { topic: "", since: "", root: false };
      await m.firehose(sse, {
        onHello: (h) => {
          hello = h;
        },
      }).closed;
      expect(hello.topic).toBe("t");
    });

    it("skips comment lines and strips one leading space from field values", async () => {
      const body = sseStream([
        ": this is a comment\n",
        `event: hello\ndata: ${JSON.stringify({
          topic: "  spaced",
          since: "latest",
          root: false,
        })}\n\n`,
      ]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      let topic = "";
      await m.firehose(sse, {
        onHello: (h) => {
          topic = h.topic;
        },
      }).closed;
      // The leading space-stripping is per the SSE spec on the field
      // value, not inside the JSON payload — so the spaces inside
      // "  spaced" survive.
      expect(topic).toBe("  spaced");
    });

    it("accepts ping as {ts: n} as well as bare number", async () => {
      const body = sseStream(['event: ping\ndata: {"ts":42}\n\n']);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const pings: number[] = [];
      await m.firehose(sse, { onPing: (t) => pings.push(t) }).closed;
      expect(pings).toEqual([42]);
    });
  });

  describe("terminal frames", () => {
    it("resync resolves closed with kind=resync and stops further dispatch", async () => {
      const body = sseStream([
        'event: resync\ndata: {"reason":"overflow","dropped":17}\n\n',
        // This event arrives after resync — must NOT be delivered.
        eventLine("ghost"),
      ]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const resyncs: { reason: string; dropped: number }[] = [];
      const rows: EventRow[] = [];
      const info = await m.firehose(sse, {
        onResync: (r) => resyncs.push(r),
        onEvent: (r) => rows.push(r),
      }).closed;
      expect(resyncs).toEqual([{ reason: "overflow", dropped: 17 }]);
      expect(rows).toHaveLength(0);
      expect(info.kind).toBe("resync");
      expect(info.resync).toEqual({ reason: "overflow", dropped: 17 });
    });

    it("resync defaults missing fields to reason=unknown / dropped=0", async () => {
      const body = sseStream(["event: resync\ndata: {}\n\n"]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const info = await m.firehose(sse).closed;
      expect(info.kind).toBe("resync");
      expect(info.resync).toEqual({ reason: "unknown", dropped: 0 });
    });

    it("error frame resolves closed with kind=error and serverError", async () => {
      const body = sseStream([
        'event: error\ndata: {"reason":"project_disabled","errorId":"trace-9"}\n\n',
      ]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const info = await m.firehose(sse).closed;
      expect(info.kind).toBe("error");
      expect(info.serverError).toEqual({ reason: "project_disabled", errorId: "trace-9" });
    });

    it("error frame omits errorId when absent", async () => {
      const body = sseStream(['event: error\ndata: {"reason":"unavailable"}\n\n']);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const info = await m.firehose(sse).closed;
      expect(info.kind).toBe("error");
      expect(info.serverError).toEqual({ reason: "unavailable" });
      expect(info.serverError?.errorId).toBeUndefined();
    });
  });

  describe("error surface", () => {
    it("surfaces malformed JSON event frames via onError without stopping the stream", async () => {
      const body = sseStream([
        "event: event\ndata: {not-json\n\n",
        eventLine("e1"),
      ]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const errors: NetworkError[] = [];
      const rows: EventRow[] = [];
      const info = await m.firehose(sse, {
        onError: (e) => errors.push(e),
        onEvent: (r) => rows.push(r),
      }).closed;
      expect(errors).toHaveLength(1);
      expect(errors[0]!.message).toContain("event");
      expect(rows.map((r) => r.event_id)).toEqual(["e1"]);
      expect(info.kind).toBe("ok");
    });

    it("surfaces unknown SSE event types via onError", async () => {
      const body = sseStream(['event: future_kind\ndata: {}\n\n']);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const errors: NetworkError[] = [];
      await m.firehose(sse, { onError: (e) => errors.push(e) }).closed;
      expect(errors).toHaveLength(1);
      expect(errors[0]!.message).toContain("future_kind");
    });

    it("surfaces empty ping payloads via onError (no onPing(0))", async () => {
      const body = sseStream(["event: ping\ndata: \n\n"]);
      const m = mesh(async () => new Response(body, { status: 200 }));
      const pings: number[] = [];
      const errors: NetworkError[] = [];
      await m.firehose(sse, {
        onPing: (t) => pings.push(t),
        onError: (e) => errors.push(e),
      }).closed;
      expect(pings).toEqual([]);
      expect(errors).toHaveLength(1);
    });
  });

  describe("connect errors", () => {
    const cases: Array<[number, new (...a: never[]) => Error]> = [
      [401, AuthenticationError],
      [403, AuthenticationError],
      [404, NotFoundError],
      [429, RateLimitError],
      [400, BadRequestError],
      [500, ServerError],
      [502, ServerError],
    ];
    for (const [status, ErrCtor] of cases) {
      it(`maps ${status} JSON body to ${ErrCtor.name} and surfaces via onError`, async () => {
        const m = mesh(async () =>
          new Response(JSON.stringify({ error: "x", reason: "y" }), {
            status,
            headers: { "content-type": "application/json" },
          }),
        );
        const errors: NetworkError[] = [];
        const info = await m.firehose(sse, { onError: (e) => errors.push(e) }).closed;
        expect(info.kind).toBe("transport");
        // We wrap the ApiError in a NetworkError for onError consistency,
        // but the wrapper's message carries the original.
        expect(errors).toHaveLength(1);
        expect(errors[0]!.message).toContain(String(status));
        // The wrapped error chain still includes the typed subclass.
        const cause = errors[0]!.cause ?? errors[0];
        expect(cause).toBeInstanceOf(ErrCtor);
      });
    }

    it("attaches raw body on non-JSON connect failure", async () => {
      const m = mesh(async () =>
        new Response("<html>Bad Gateway</html>", {
          status: 502,
          headers: { "content-type": "text/html" },
        }),
      );
      const errors: NetworkError[] = [];
      await m.firehose(sse, { onError: (e) => errors.push(e) }).closed;
      const cause = errors[0]!.cause as ServerError;
      expect(cause).toBeInstanceOf(ServerError);
      expect(cause.body).toEqual({ raw: "<html>Bad Gateway</html>" });
    });

    it("falls back to http_<status> code when body is non-JSON", async () => {
      const m = mesh(async () =>
        new Response("nope", { status: 503, headers: { "content-type": "text/plain" } }),
      );
      const errors: NetworkError[] = [];
      await m.firehose(sse, { onError: (e) => errors.push(e) }).closed;
      const cause = errors[0]!.cause as ServerError;
      expect(cause.code).toBe("http_503");
    });
  });

  describe("lifecycle", () => {
    it("close() resolves closed with kind=aborted", async () => {
      const m = mesh(async (_url, init) => {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            init?.signal?.addEventListener("abort", () => controller.close());
          },
        });
        return new Response(body, { status: 200 });
      });
      const handle = m.firehose(sse);
      setTimeout(() => handle.close(), 5);
      const info = await handle.closed;
      expect(info.kind).toBe("aborted");
    });

    it("transport failure during fetch surfaces as kind=transport", async () => {
      const m = mesh(async () => {
        throw new TypeError("network unreachable");
      });
      const errors: NetworkError[] = [];
      const info = await m.firehose(sse, { onError: (e) => errors.push(e) }).closed;
      expect(info.kind).toBe("transport");
      expect(info.error).toBeInstanceOf(NetworkError);
      expect(errors).toHaveLength(1);
    });
  });
});

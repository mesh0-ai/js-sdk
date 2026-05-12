import { describe, it, expect } from "vitest";
import { Mesh0 } from "../src/index.js";
import type { EventRow } from "../src/types.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

function sseStream(chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const c of chunks) {
        controller.enqueue(enc.encode(c));
        await new Promise((r) => setTimeout(r, 1));
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
});

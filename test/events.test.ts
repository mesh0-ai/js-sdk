import { describe, it, expect } from "vitest";
import { Mesh0 } from "../src/index.js";
import { fakeFetch } from "./helpers.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

describe("events.send", () => {
  it("posts to /v1/events with bearer auth and a wrapped events array", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: { accepted: 1 } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await m.events.send({ timestamp: 1_700_000_000_000, attributes: { "app.id": "x" } });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe("POST");
    expect(calls[0]!.url).toBe("https://api.mesh0.ai/v1/events");
    expect(calls[0]!.headers.authorization).toBe(`Bearer ${KEY}`);
    expect(calls[0]!.body).toMatchObject({
      events: [{ timestamp: 1_700_000_000_000, attributes: { "app.id": "x" } }],
    });
  });

  it("splits batches larger than 5000", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 200, body: {} },
      { status: 200, body: {} },
      { status: 200, body: {} },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    const events = Array.from({ length: 12_345 }, (_, i) => ({ timestamp: i }));
    await m.events.sendMany(events);
    expect(calls).toHaveLength(3);
    expect((calls[0]!.body as { events: unknown[] }).events).toHaveLength(5000);
    expect((calls[1]!.body as { events: unknown[] }).events).toHaveLength(5000);
    expect((calls[2]!.body as { events: unknown[] }).events).toHaveLength(2345);
  });
});

describe("events.list / iterate", () => {
  it("iterates through cursors transparently", async () => {
    const { fetchFn } = fakeFetch([
      {
        status: 200,
        body: {
          events: [{ event_id: "a" }, { event_id: "b" }],
          nextCursor: "cur1",
          hasMore: true,
        },
      },
      {
        status: 200,
        body: { events: [{ event_id: "c" }], nextCursor: null, hasMore: false },
      },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    const ids: string[] = [];
    for await (const row of m.events.iterate({ limit: 2 })) {
      ids.push(row.event_id as string);
    }
    expect(ids).toEqual(["a", "b", "c"]);
  });
});

describe("error mapping", () => {
  it("401 → AuthenticationError", async () => {
    const { fetchFn } = fakeFetch([
      { status: 401, body: { error: "unauthorized", reason: "missing_token" } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.send({ timestamp: 1 })).rejects.toMatchObject({
      name: "AuthenticationError",
      status: 401,
    });
  });

  it("429 carries retry-after", async () => {
    const { fetchFn } = fakeFetch([
      { status: 429, body: { error: "rate_limited" }, headers: { "retry-after": "7" } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.send({ timestamp: 1 })).rejects.toMatchObject({
      name: "RateLimitError",
      retryAfter: 7,
    });
  });

  it("retries 5xx then succeeds", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 500, body: { error: "internal_error" } },
      { status: 200, body: {} },
    ]);
    const m = new Mesh0({
      apiKey: KEY,
      fetch: fetchFn as typeof fetch,
      maxRetries: 2,
      retryBaseMs: 1,
    });
    await m.events.send({ timestamp: 1 });
    expect(calls).toHaveLength(2);
  });
});

import { describe, it, expect } from "vitest";
import { Mesh0, ValidationError, NetworkError } from "../src/index.js";
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

  it("rejects empty arrays with ValidationError (not BadRequestError)", async () => {
    const { fetchFn } = fakeFetch([{ status: 200, body: {} }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await expect(m.events.sendMany([])).rejects.toBeInstanceOf(ValidationError);
  });
});

describe("events.list / iterate", () => {
  it("iterates through cursors transparently and propagates cursor in URL", async () => {
    const { fetchFn, calls } = fakeFetch([
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
      ids.push(row.event_id);
    }
    expect(ids).toEqual(["a", "b", "c"]);
    expect(calls[0]!.url).toContain("limit=2");
    expect(calls[1]!.url).toContain("cursor=cur1");
  });

  it("encodes list options (from/to/limit) into the query string", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 200, body: { events: [], nextCursor: null, hasMore: false } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await m.events.list({ from: "2026-01-01", to: 1_700_000_000_000, limit: 50 });
    const url = calls[0]!.url;
    expect(url).toContain("from=2026-01-01");
    expect(url).toContain("to=1700000000000");
    expect(url).toContain("limit=50");
  });
});

describe("events.trace", () => {
  it("URL-encodes trace ids with special characters", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: { spans: [] } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await m.events.trace("ab/cd#ef");
    expect(calls[0]!.url).toBe("https://api.mesh0.ai/v1/traces/ab%2Fcd%23ef");
  });

  it("rejects empty traceId with ValidationError", async () => {
    const { fetchFn } = fakeFetch([{ status: 200, body: { spans: [] } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await expect(m.events.trace("")).rejects.toBeInstanceOf(ValidationError);
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

  it("403 → AuthenticationError", async () => {
    const { fetchFn } = fakeFetch([{ status: 403, body: { error: "forbidden" } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.send({ timestamp: 1 })).rejects.toMatchObject({
      name: "AuthenticationError",
      status: 403,
    });
  });

  it("400 → BadRequestError", async () => {
    const { fetchFn } = fakeFetch([{ status: 400, body: { error: "bad_input" } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.send({ timestamp: 1 })).rejects.toMatchObject({
      name: "BadRequestError",
      status: 400,
    });
  });

  it("404 → NotFoundError", async () => {
    const { fetchFn } = fakeFetch([{ status: 404, body: { error: "not_found" } }]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.trace("t1")).rejects.toMatchObject({
      name: "NotFoundError",
      status: 404,
    });
  });

  it("500 with retries disabled → ServerError carries errorId", async () => {
    const { fetchFn } = fakeFetch([
      { status: 500, body: { error: "internal_error", errorId: "trace-xyz" } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.events.send({ timestamp: 1 })).rejects.toMatchObject({
      name: "ServerError",
      status: 500,
      errorId: "trace-xyz",
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

  it("honors Retry-After on 429 and then succeeds", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 429, body: { error: "rate_limited" }, headers: { "retry-after": "0" } },
      { status: 200, body: {} },
    ]);
    const m = new Mesh0({
      apiKey: KEY,
      fetch: fetchFn as typeof fetch,
      maxRetries: 1,
      retryBaseMs: 1,
    });
    await m.events.send({ timestamp: 1 });
    expect(calls).toHaveLength(2);
  });

  it("malformed JSON success body → NetworkError", async () => {
    const { fetchFn } = fakeFetch([
      {
        status: 200,
        body: "{not-json",
        headers: { "content-type": "application/json" },
      },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch, maxRetries: 0 });
    await expect(m.identity.me()).rejects.toBeInstanceOf(NetworkError);
  });
});

describe("retry behavior", () => {
  it("retries a fetch rejection then surfaces NetworkError on exhaustion", async () => {
    const err = new Error("ECONNRESET");
    const { fetchFn, calls } = fakeFetch([{ throw: err }, { throw: err }]);
    const m = new Mesh0({
      apiKey: KEY,
      fetch: fetchFn as typeof fetch,
      maxRetries: 1,
      retryBaseMs: 1,
    });
    await expect(m.events.send({ timestamp: 1 })).rejects.toBeInstanceOf(NetworkError);
    expect(calls).toHaveLength(2);
  });

  it("retries a fetch rejection then succeeds", async () => {
    const { fetchFn, calls } = fakeFetch([
      { throw: new Error("ETIMEDOUT") },
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

  it("user-supplied AbortSignal cancels mid-flight without retry", async () => {
    const ac = new AbortController();
    const abortErr = new DOMException("aborted", "AbortError");
    let calls = 0;
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      calls++;
      ac.abort();
      // simulate fetch surfacing the abort
      throw init?.signal?.aborted ? abortErr : new Error("never");
    }) as typeof fetch;
    const m = new Mesh0({
      apiKey: KEY,
      fetch: fetchFn,
      maxRetries: 3,
      retryBaseMs: 1,
    });
    await expect(m.events.send({ timestamp: 1 }, { signal: ac.signal })).rejects.toBeDefined();
    expect(calls).toBe(1);
  });

  it("per-request timeout surfaces as NetworkError without retry", async () => {
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("aborted", "AbortError"));
        });
      });
    }) as typeof fetch;
    const m = new Mesh0({
      apiKey: KEY,
      fetch: fetchFn,
      maxRetries: 3,
      retryBaseMs: 1,
      timeoutMs: 5,
    });
    const err = await m.events.send({ timestamp: 1 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NetworkError);
    expect((err as NetworkError).message).toContain("timed out");
  });
});

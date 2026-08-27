# mesh0 JavaScript SDK

[![CI](https://github.com/mesh0-ai/js-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/mesh0-ai/js-sdk/actions/workflows/ci.yml)

Official JavaScript / TypeScript client for the [mesh0](https://mesh0.ai)
telemetry platform. Send events, query them with TQL, and tail the live
firehose — from browsers, Node, edge runtimes, and React Native.

- **Universal** — HTTP works on Node 18+, Bun, Deno, browsers, and edge
  runtimes (Cloudflare Workers, Vercel Edge). The WebSocket firehose
  needs Node ≥ 22 (or the `ws` package on older Node).
- **Zero required deps** — uses platform `fetch` and `WebSocket`. The
  optional `ws` peer dep covers Node < 22.
- **Fully typed** — strict TypeScript with no `any` on the public surface.
- **Built-in retries** — idempotent failures (`5xx`, `429`, transport)
  retry with exponential backoff and `Retry-After` honored.
- **Streaming** — one org-wide firehose at `/v1/firehose`, with two
  transports: WebSocket (default) or SSE (for runtimes that can't
  upgrade). Same auth, same params, same payload either way.

---

## Install

```sh
npm install @mesh0/sdk
# or
bun add @mesh0/sdk
# or
pnpm add @mesh0/sdk
```

On Node < 22 the WebSocket firehose needs the `ws` package:

```sh
npm install ws
```

---

## Quick start

```ts
import { Mesh0 } from "@mesh0/sdk";

const mesh0 = Mesh0.create("m0_abcde_xxxxxxxxxxxxxxxxxxxxxxxx");

// Send a single event. The wire shape is intentionally narrow — identity,
// time, plus two open bins (`attributes` queryable, `data` opaque).
await mesh0.events.send({
  timestamp: Date.now(),
  attributes: {
    "app.id": "checkout",
    "app.environment": "prod",
    "span.name": "charge.captured",
    "user.id": "user_42",
    order_id: "ord_123",
    amount_usd: 19.99,
  },
});
```

Or configure from `MESH0_API_KEY` / `MESH0_BASE_URL` env vars:

```ts
const mesh0 = new Mesh0(); // reads MESH0_API_KEY automatically
```

---

## Sending events

```ts
// Single event
await mesh0.events.send({
  timestamp: new Date().toISOString(),
  trace_id: traceId,
  attributes: {
    "app.id": "agents",
    "span.name": "agent.run",
    duration_ms: 820,
    status: "success",
    "gen_ai.system": "anthropic",
    "gen_ai.request.model": "claude-opus-4-7",
    "gen_ai.usage.input_tokens": 1240,
    "gen_ai.usage.output_tokens": 380,
  },
  // Big payloads (LLM message arrays, raw req/resp) → `data`. Opaque,
  // not TQL-queryable, only shown on single-event drilldown.
  data: { messages },
});

// Bulk — batches larger than MAX_EVENTS_PER_REQUEST auto-split into
// sequential POSTs (the server enforces the same cap).
await mesh0.events.sendMany(events);
```

The accepted top-level fields are `event_id`, `timestamp`, `trace_id`,
`span_id`, `parent_span_id`, `attributes`, `data`. Anything else
(`status`, `duration_ms`, `model.*`, …) goes inside `attributes` for
queryable data or `data` for the opaque slow blob.

---

## Querying (TQL)

```ts
const result = await mesh0.query.run({
  range: { from: "now-7d", to: "now" },
  filter: "status = 'error'",
  groupBy: ["status"],
  metrics: [{ fn: "count", alias: "n" }],
  orderBy: [{ key: "n", dir: "desc" }],
  limit: 25,
});
```

Only the identity/time TQL builtins resolve at the top level
(`timestamp`, `project.id`, `trace.id`, `span.id`, `parent_span.id`).
Anything else (`status`, `duration_ms`, `gen_ai.*`, …) must be exposed
via a per-project alias or promoted column — set those up in the
dashboard, then reference them by their alias name.

---

## Listing events

```ts
// Single page
const page = await mesh0.events.list({ limit: 100 });
for (const row of page.events) {
  /* … */
}

// Or stream every event, transparently following cursors:
for await (const row of mesh0.events.iterate()) {
  /* … */
}

// Fetch a whole trace
const { spans } = await mesh0.events.trace(traceId);
```

---

## Real-time

The firehose is a single org-wide endpoint at `GET /v1/firehose` with
two transports — WebSocket (default) and SSE — selected by
`opts.transport`. Both share the same auth, query params (`?since=`,
`?root=`), payload shape, and `FirehoseHandle` contract. Pick SSE for
runtimes that can't open a WebSocket (some serverless platforms,
proxies that strip `Upgrade`).

```ts
const fh = mesh0.firehose(
  {
    transport: "ws",   // "ws" (default) or "sse"
    since: "latest",   // "earliest" | "latest" | numeric offset string; default "latest"
    root: false,       // when true, deliver only root-trace events
  },
  {
    onHello: ({ topic, since, root }) => console.log({ topic, since, root }),
    onEvent: (row, { partition, offset }) => console.log(row),
    onPing: (ts) => {},
    // Terminal frames — server's per-connection queue overflowed or an
    // event failed to marshal. The stream ends right after; refetch
    // from /v1/events to recover, then reopen.
    onResync: ({ reason, dropped }) => console.warn("resync", reason, dropped),
    // Transport errors AND malformed/unknown server frames. Always a
    // NetworkError. `closed.kind` carries the same signal if you only
    // care about lifecycle.
    onError: (err) => console.error(err),
    // Catch-all in receive order. Malformed frames go to onError, not here.
    onMessage: (frame) => {},
  },
);

// Close when you're done. close() never throws and is idempotent.
fh.close();
// Always-resolving Promise — never rejects. Inspect `kind` to discriminate:
//   "ok"        | server closed cleanly
//   "aborted"   | caller invoked close()
//   "resync"    | terminal resync frame; .resync is populated
//   "error"     | terminal server error frame; .serverError is populated
//   "transport" | network drop / non-2xx connect / abnormal WS close; .error is populated
const info = await fh.closed;
if (info.kind === "resync") {
  // refetch from /v1/events, then re-open
}
```

### Switching to SSE

```ts
const fh = mesh0.firehose({ transport: "sse", since: "latest" }, callbacks);
```

The SSE transport is implemented with `fetch` + `ReadableStream` so it
works in every runtime — including ones without `EventSource`, and
without the browser `EventSource` limitation of not being able to set
`Authorization`.

On Node ≥ 22 the WebSocket transport uses the global `WebSocket`
automatically. On Node < 22, install `ws` and pass it in:

```ts
import WebSocket from "ws";
const mesh0 = new Mesh0({
  apiKey: process.env.MESH0_API_KEY!,
  WebSocket: WebSocket as unknown as typeof globalThis.WebSocket,
});
```

---

## OTLP traces

mesh0 accepts OTLP/HTTP JSON at `<baseUrl>/v1/traces`. Point any
OpenTelemetry exporter at it with the same Bearer token — there's no
SDK-side wrapper for this on purpose; OTel SDKs do it better than we
would.

```ts
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

new OTLPTraceExporter({
  url: "https://api.mesh0.ai/v1/traces",
  headers: { Authorization: `Bearer ${process.env.MESH0_API_KEY}` },
});
```

The SDK exposes the read side via `events.trace(traceId)`.

---

## Configuration

```ts
const mesh0 = new Mesh0({
  apiKey: "m0_…",
  baseUrl: "https://api.mesh0.ai",
  timeoutMs: 30_000,
  maxRetries: 2,
  retryBaseMs: 250,
  userAgent: "my-app/1.0",
  defaultHeaders: { "X-Tenant": "acme" },
  // Bring your own:
  fetch: customFetch,
  WebSocket: WebSocketCtor,
});
```

| Env var          | Description                                          |
| ---------------- | ---------------------------------------------------- |
| `MESH0_API_KEY`  | Credential. **Required.** A project key (`m0_<routing>_<secret>`), a user key (`m0u_<secret>`), or an open-mode instance token (JWT). |
| `MESH0_BASE_URL` | Override base URL (self-hosted deployments).         |

---

## Errors

All errors extend `Mesh0Error`:

| Error                   | Status   | When                                       |
| ----------------------- | -------- | ------------------------------------------ |
| `AuthenticationError`   | 401, 403 | Missing, malformed, or revoked API key.    |
| `BadRequestError`       | 4xx      | Payload rejected by validation.            |
| `NotFoundError`         | 404      | Resource doesn't exist.                    |
| `RateLimitError`        | 429      | Inspect `.retryAfter` (seconds).           |
| `ServerError`           | 5xx      | mesh0 internal error; `.errorId` set.      |
| `NetworkError`          | —        | Transport-level failure or timeout.        |
| `ValidationError`       | —        | Caller-side input rejected before request. |
| `ConfigurationError`    | —        | Bad SDK config (missing key, no fetch, …). |

Every error carries a `kind` literal (`"api.auth"`, `"network"`, etc.)
for exhaustive `switch` without `instanceof` chains.

The HTTP transport retries `5xx`, `429`, and network failures up to
`maxRetries` with exponential backoff + jitter; `Retry-After` is honored.

---

## Development

```sh
bun install        # or npm install
bun run build
bun run test
bun run typecheck
```

---

## License

MIT — see [LICENSE](./LICENSE).

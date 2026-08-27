# Changelog

All notable changes to `@mesh0/sdk` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [SemVer](https://semver.org/).

## 0.2.1

### Fixed

- **Accept open-mode instance tokens as a credential.** `resolveConfig`
  rejected any `apiKey` not starting with `m0_`, throwing
  `ConfigurationError` before any network call. mesh0's admission layer
  routes a non-`m0_` bearer to open mode, where a workspace-scoped instance
  token (a JWT) is the ONLY credential in existence — it authenticates
  ingest, the firehose, the management API and `/mcp` alike. The prefix check
  therefore made every one of those unreachable from this SDK for a cluster
  running open mode.

  Observed in production as a firehose that never connected: the consumer
  held a valid instance token, and `new Mesh0({ apiKey })` threw
  client-side on construction, so the reconnect loop retried a
  `ConfigurationError` every 30s indefinitely — no request was ever made and
  nothing server-side could report why.

  `m0u_` user keys are now accepted for the same reason: they were already a
  documented credential shape that this check refused.

  The instance-token arm is a SHAPE test, not a validation: three unpadded
  base64url segments whose first decodes to a JOSE header object carrying
  `alg`. Signature, claims and expiry are the server's business and it holds
  the key. Decoding the header is what makes it narrow enough to be worth
  having — segment-counting alone accepts `api.mesh0.ai`, which is exactly
  the kind of mistake the check exists to catch. Exposed as `isInstanceToken`
  for callers that need to tell the shapes apart.

  Mirrors `mesh0/php-sdk`, which made the same change in its 1.4.0.

## 0.2.0 — 2026-05-12

This release tracks the server-side unification of the realtime API:
the per-project SSE endpoint `/v1/events/stream` has been folded into
the org-wide firehose at `/v1/firehose`, which now serves **both**
WebSocket and SSE callers from the same path. The SDK shape has been
revised to match: a single `mesh0.firehose()` entrypoint dispatches on
`transport`, and both transports share one handle and lifecycle
contract.

### Breaking changes

- **`mesh0.stream()` has been removed.** Use
  `mesh0.firehose({ transport: "sse" }, callbacks)` instead. Both
  transports now share the same options, callbacks, and handle.
- **`mesh0.firehose()` signature unified.** Single entrypoint:
  ```ts
  mesh0.firehose(
    { transport: "ws" | "sse", since?, root? },
    callbacks,
  ): FirehoseHandle;
  ```
  `transport` defaults to `"ws"`. The org-wide stream now lives at
  `/v1/firehose` for both transports — the old per-project SSE endpoint
  `/v1/events/stream` no longer exists server-side.
- **`FirehoseHandle.closed` never rejects.** Both transports now
  resolve to a discriminated `FirehoseCloseInfo`:
  ```ts
  type FirehoseCloseKind = "ok" | "aborted" | "resync" | "error" | "transport";
  interface FirehoseCloseInfo {
    kind: FirehoseCloseKind;
    code?: number;           // WS close code, when known
    reason?: string;
    resync?: FirehoseResync;  // when kind === "resync"
    serverError?: FirehoseServerError; // when kind === "error"
    error?: NetworkError;     // when kind === "error" | "transport"
  }
  ```
  Previously the SSE handle rejected `done` on resync/error frames and
  the WS handle resolved `closed` with a separate shape. They are now
  identical.
- **Callbacks unified across transports.** Both WS and SSE call
  `onHello`, `onEvent(row, { partition, offset })`, `onPing(ts)`,
  `onResync({ reason, dropped })`, `onError(NetworkError)`, and
  `onMessage(FirehoseFrame)`. The old per-transport `onClose` is gone —
  inspect `closed.kind` instead. Server-sent `error` frames are
  surfaced through `closed.kind === "error"` rather than a callback.
- **`onMessage` payload is now a unified `FirehoseFrame`** with
  discriminator `kind: "hello" | "event" | "ping" | "resync" | "error"`,
  identical across both transports. The old per-transport
  `FirehoseMessage` and `StreamMessage` types are removed.
- **Malformed and unknown frames now route to `onError`** instead of
  being silently dropped. Empty `ping` payloads, JSON parse failures on
  `event`/`resync`/`error` frames, unknown SSE event types, unknown WS
  message types, and unsupported WS binary shapes all fire `onError`
  with a descriptive `NetworkError`.
- **Non-JSON HTTP connect failures now attach the raw body.** When the
  server returns HTML or plain text on a non-2xx, the original text is
  preserved on `error.body = { raw }` instead of being discarded.

### Added

- **`root` option on both firehose transports.** Pass `{ root: true }`
  to receive only root-trace events (rows where `parent_span_id` is
  empty). Server-side filter; non-root rows never cross the wire.
- **`transport` option** to pick WebSocket or SSE from a single
  `firehose()` call.
- **Unified `FirehoseFrame`** discriminated union exported from the
  package root for `onMessage` consumers and transport-agnostic
  handlers.
- New exported types: `FirehoseTransport`, `FirehoseOpts`,
  `FirehoseCallbacks`, `FirehoseHandle`, `FirehoseCloseKind`,
  `FirehoseCloseInfo`, `FirehoseHello`, `FirehoseEventMeta`,
  `FirehoseResync`, `FirehoseServerError`, `FirehoseFrame`.

### Migration

```ts
// Before:
const handle = mesh0.stream(
  { since: "latest" },
  {
    onHello: ({ topic, since, root }) => {},
    onEvent: (row, { partition, offset }) => {},
    onResync: ({ reason, dropped }) => {},
  },
);
try { await handle.done; } catch (e) { /* terminal frame */ }

// After:
const handle = mesh0.firehose(
  { transport: "sse", since: "latest" },
  {
    onHello: ({ topic, since, root }) => {},
    onEvent: (row, { partition, offset }) => {},
    onResync: ({ reason, dropped }) => {},
  },
);
const info = await handle.closed; // never throws
if (info.kind === "resync") { /* terminal */ }
```

```ts
// Before:
const fh = mesh0.firehose({ since: "latest" }, { onClose: (code, reason) => {} });

// After:
const fh = mesh0.firehose({ transport: "ws", since: "latest" });
const info = await fh.closed;
// info.kind / info.code / info.reason carry the same signal
```

## 0.1.0 — 2026-04-XX

- Initial release: event ingest, TQL queries, per-project SSE stream,
  org-wide WebSocket firehose, identity helpers, retries with backoff,
  and full TypeScript types.

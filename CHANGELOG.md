# Changelog

All notable changes to `@mesh0/sdk` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [SemVer](https://semver.org/).

## 0.2.0 — 2026-05-12

This release tracks the server-side unification of the realtime API:
the per-project SSE endpoint `/v1/events/stream` has been folded into
the org-wide firehose at `/v1/firehose`, which now serves **both**
WebSocket and SSE callers from the same path.

### Breaking changes

- **`mesh0.stream()` now targets `/v1/firehose` (SSE transport).** The
  old per-project SSE endpoint `/v1/events/stream` has been removed
  server-side. Consequences:
  - The stream is now **org-wide** (every project under the API key),
    not scoped to a single project.
  - `stream()` accepts a new `StreamOpts` argument:
    `mesh0.stream(opts?, callbacks?)`. Old call sites that passed
    callbacks as the first argument must add an opts object (or
    `undefined`) before the callbacks.
  - `onHello` now receives `{ topic, since, root }` (a `FirehoseHello`)
    instead of an unstructured `Record<string, unknown>`.
  - `onEvent` now receives `(row, { partition, offset })` instead of
    just `row`. Each frame carries the Kafka partition/offset of the
    underlying event.
  - `onResync` now receives `{ reason, dropped }` instead of being
    called with no arguments. `reason` is `"overflow" | "marshal" |
    <future>`; `dropped` is the best-effort count of events lost before
    resync (`0` for non-overflow reasons). `resync` is terminal — the
    server closes the stream after delivery and `handle.done` rejects
    with a `NetworkError`.
- **`mesh0.firehose()` (WebSocket) hello frame gains a `root` field.**
  `onHello` callbacks now receive `{ topic, since, root }`. The `since`
  value is the *effective* start point — a numeric offset without a
  project filter is silently downgraded to `"latest"` server-side, and
  the hello reflects what the server actually used.
- The `FirehoseMessage` and `StreamMessage` discriminated-union types
  changed shape — see above.

### Added

- **`root` option on both firehose transports.** Pass
  `{ root: true }` to `mesh0.firehose()` or `mesh0.stream()` to receive
  only root-trace events (rows where `parent_span_id` is empty).
  Server-side filter; non-root rows never cross the wire.
- New exported types: `FirehoseHello`, `FirehoseResync`, `StreamOpts`.

### Migration guide

```ts
// Before (0.1.x):
mesh0.stream({
  onHello: (data) => {},
  onEvent: (row) => {},
  onResync: () => {},
});

// After (0.2.x):
mesh0.stream(
  { since: "latest", root: false }, // new opts arg
  {
    onHello: ({ topic, since, root }) => {},
    onEvent: (row, { partition, offset }) => {},
    onResync: ({ reason, dropped }) => {},
  },
);
```

If you only need the WebSocket firehose, the only change is that
`onHello` now receives `root`.

## 0.1.0 — 2026-04-XX

- Initial release: event ingest, TQL queries, per-project SSE stream,
  org-wide WebSocket firehose, identity helpers, retries with backoff,
  and full TypeScript types.

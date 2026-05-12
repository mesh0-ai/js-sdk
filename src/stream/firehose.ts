// Public entrypoint for the org-wide firehose at GET /v1/firehose. The
// endpoint serves both WebSocket and SSE callers from the same path with
// identical auth, query params, and payload shape — `opts.transport`
// picks the wire framing. Both transports converge on the same handle,
// callbacks, and close semantics so consumers can swap freely.

import type { HttpClient } from "../http.js";
import type { NetworkError } from "../errors.js";
import type {
  EventRow,
  FirehoseEventMeta,
  FirehoseFrame,
  FirehoseHello,
  FirehoseResync,
  FirehoseServerError,
} from "../types.js";
import { openSseFirehose } from "./sse.js";
import { openWsFirehose } from "./ws.js";

export type FirehoseTransport = "ws" | "sse";

export interface FirehoseOpts {
  /** Wire transport. `"ws"` (default) uses the WebSocket upgrade;
   *  `"sse"` uses Server-Sent Events over plain HTTP — pick SSE for
   *  callers that can't open a WebSocket (some serverless runtimes,
   *  proxies that strip Upgrade, EventSource-style consumers). */
  transport?: FirehoseTransport;
  /** `"earliest"`, `"latest"`, or a numeric offset string. Server
   *  default is `"latest"` when omitted. A numeric offset on the
   *  org-wide firehose is silently downgraded to `"latest"` server-side
   *  (partitions have independent offset spaces) — the hello frame
   *  echoes what was actually applied. */
  since?: "earliest" | "latest" | (string & {});
  /** Server-side filter: when true, only root-trace events (rows with
   *  empty `parent_span_id`) cross the wire. */
  root?: boolean;
}

export interface FirehoseCallbacks {
  onHello?: (hello: FirehoseHello) => void;
  onEvent?: (row: EventRow, meta: FirehoseEventMeta) => void;
  onPing?: (tsMs: number) => void;
  /** Terminal on the transports that emit it (SSE today). The stream
   *  closes immediately after; `closed` resolves with
   *  `kind: "resync"`. */
  onResync?: (info: FirehoseResync) => void;
  /** Transport errors AND malformed/unknown server frames. Always a
   *  `NetworkError`. Callers that only care about visibility can rely
   *  on `closed.kind === "error" | "transport"`. */
  onError?: (err: NetworkError) => void;
  /** Catch-all in receive order. Frames that fail to parse are NOT
   *  delivered here — those route to `onError`. */
  onMessage?: (frame: FirehoseFrame) => void;
}

export type FirehoseCloseKind =
  /** Stream closed cleanly (WS code 1000 with no prior error, or SSE
   *  server-EOF / caller-close with no error). */
  | "ok"
  /** Server emitted a resync frame and closed the stream. `resync` is
   *  populated. */
  | "resync"
  /** Server emitted an error frame, or WS surfaced an unknown/malformed
   *  message before close. `error` is populated. */
  | "error"
  /** Lower-level transport failure (network drop, HTTP non-2xx connect,
   *  abnormal WS close). `error` is populated. */
  | "transport"
  /** Caller invoked `close()` while the stream was still open. */
  | "aborted";

export interface FirehoseCloseInfo {
  kind: FirehoseCloseKind;
  /** WS close code when known. Always present for the WS transport;
   *  absent on SSE. */
  code?: number;
  /** WS close reason, or a short tag for SSE close paths. */
  reason?: string;
  /** Present when `kind === "resync"`. */
  resync?: FirehoseResync;
  /** Present when `kind === "error"` (server-sent error frame). */
  serverError?: FirehoseServerError;
  /** Present when `kind === "error" | "transport"`. */
  error?: NetworkError;
}

export interface FirehoseHandle {
  /** Resolves when the connection closes, for any reason. Never
   *  rejects — inspect `kind` to discriminate clean vs failed. */
  closed: Promise<FirehoseCloseInfo>;
  /** Close the connection. `code` and `reason` are forwarded to the WS
   *  socket; they are ignored on SSE. */
  close(code?: number, reason?: string): void;
}

/** Open the org-wide firehose at GET /v1/firehose using `opts.transport`
 *  (default `"ws"`). See {@link FirehoseOpts} and
 *  {@link FirehoseCallbacks} for the shared contract. */
export function openFirehose(
  http: HttpClient,
  opts: FirehoseOpts = {},
  callbacks: FirehoseCallbacks = {},
): FirehoseHandle {
  if (opts.transport === "sse") return openSseFirehose(http, opts, callbacks);
  return openWsFirehose(http, opts, callbacks);
}

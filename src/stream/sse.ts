// fetch-based SSE parser for the SSE transport of the org-wide firehose
// (GET /v1/firehose without an Upgrade header). EventSource is not portable
// across runtimes (browsers + Node 22+ have it, edge runtimes vary, and
// EventSource can't set Authorization in browsers). Doing it ourselves with
// fetch + ReadableStream keeps one code path everywhere.
//
// The SSE transport is a sibling of the WebSocket firehose; both share the
// same endpoint, auth, query params (?since=, ?root=), and event payload
// shape. The only difference is the wire framing — SSE for callers that
// can't or won't upgrade (curl, EventSource, some serverless runtimes,
// proxies that strip Upgrade).
//
// Note: this client does not auto-reconnect. After a `resync` (server-side
// queue overflow or marshal failure) or an `error` frame, the stream
// terminates and consumers should re-call stream(). To recover the events
// they missed, refetch from /v1/events.

import type { HttpClient } from "../http.js";
import {
  ApiError,
  AuthenticationError,
  BadRequestError,
  Mesh0Error,
  NetworkError,
  NotFoundError,
  RateLimitError,
  ServerError,
} from "../errors.js";
import type {
  EventRow,
  FirehoseHello,
  FirehoseResync,
  StreamMessage,
} from "../types.js";

export interface StreamOpts {
  /** 'earliest', 'latest', or a numeric offset string. Server default is
   *  'latest' when omitted. A numeric offset is silently downgraded to
   *  'latest' on the org-wide firehose; the hello frame reflects the
   *  effective start point. */
  since?: "earliest" | "latest" | (string & {});
  /** When true, request only root-trace events (rows with empty
   *  parent_span_id). Server-side filter. */
  root?: boolean;
}

export interface StreamHandle {
  /** Resolves when the stream ends cleanly (server closes or caller calls
   *  `close()`). Rejects on transport error, a server-sent `error` frame,
   *  or a `resync` frame (terminal — refetch from /v1/events to recover). */
  done: Promise<void>;
  /** Close the underlying connection. */
  close(): void;
}

export interface StreamCallbacks {
  onEvent?: (row: EventRow, meta: { partition: number; offset: string }) => void;
  onHello?: (hello: FirehoseHello) => void;
  onPing?: (tsMs: number) => void;
  onResync?: (info: FirehoseResync) => void;
  onError?: (err: { reason: string; errorId?: string }) => void;
  /** Catch-all: every server-sent message, in receive order. */
  onMessage?: (msg: StreamMessage) => void;
}

interface RawSse {
  event: string;
  data: string;
}

function* parseSseBuffer(buf: string): Generator<RawSse> {
  // The wire spec is "\n\n" separated records; we accept "\r\n\r\n" too.
  const normalized = buf.replace(/\r\n/g, "\n");
  for (const block of normalized.split("\n\n")) {
    if (!block) continue;
    let event = "message";
    const dataLines: string[] = [];
    for (const line of block.split("\n")) {
      if (!line || line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      if (colon === -1) continue;
      const field = line.slice(0, colon);
      let value = line.slice(colon + 1);
      if (value.startsWith(" ")) value = value.slice(1);
      if (field === "event") event = value;
      else if (field === "data") dataLines.push(value);
    }
    yield { event, data: dataLines.join("\n") };
  }
}

function parseSseData(event: string, data: string): StreamMessage | null {
  switch (event) {
    case "hello": {
      const parsed = data ? safeJson(data) : {};
      const obj = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      const hello: FirehoseHello = {
        topic: typeof obj.topic === "string" ? obj.topic : "",
        since: typeof obj.since === "string" ? obj.since : "",
        root: Boolean(obj.root),
      };
      return { event: "hello", data: hello };
    }
    case "ping": {
      // Server may send either a bare number or `{"ts": <number>}`. We
      // accept both for forward compatibility.
      const trimmed = data.trim();
      const asNum = Number(trimmed);
      if (Number.isFinite(asNum)) return { event: "ping", data: asNum };
      const parsed = safeJson(trimmed);
      if (parsed && typeof parsed === "object") {
        const ts = (parsed as { ts?: unknown }).ts;
        if (typeof ts === "number" && Number.isFinite(ts)) {
          return { event: "ping", data: ts };
        }
      }
      return null;
    }
    case "event": {
      const parsed = safeJson(data);
      if (!parsed || typeof parsed !== "object") return null;
      const obj = parsed as { partition?: unknown; offset?: unknown; row?: unknown };
      if (!obj.row || typeof obj.row !== "object") return null;
      const partition = typeof obj.partition === "number" ? obj.partition : 0;
      const offset = typeof obj.offset === "string" ? obj.offset : String(obj.offset ?? "");
      return {
        event: "event",
        data: { partition, offset, row: obj.row as EventRow },
      };
    }
    case "resync": {
      const parsed = safeJson(data);
      const body = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      const reason = typeof body.reason === "string" ? body.reason : "unknown";
      const dropped = typeof body.dropped === "number" ? body.dropped : 0;
      return { event: "resync", data: { reason, dropped } };
    }
    case "error": {
      const parsed = safeJson(data);
      const body = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      const reason = typeof body.reason === "string" ? body.reason : "unknown";
      const out: { event: "error"; data: { reason: string; errorId?: string } } = {
        event: "error",
        data: { reason },
      };
      if (typeof body.errorId === "string") out.data.errorId = body.errorId;
      return out;
    }
    default:
      return null;
  }
}

function safeJson(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function isAbortError(err: unknown): boolean {
  return !!err && typeof err === "object" && (err as { name?: unknown }).name === "AbortError";
}

async function buildConnectError(res: Response): Promise<Mesh0Error> {
  const raw = await res.text().catch(() => "");
  let json: unknown = null;
  if (raw) {
    try {
      json = JSON.parse(raw);
    } catch {
      /* leave as raw string */
    }
  }
  const j = json && typeof json === "object" ? (json as Record<string, unknown>) : {};
  const code = typeof j.error === "string" ? j.error : `http_${res.status}`;
  const reason = typeof j.reason === "string" ? `: ${j.reason}` : "";
  const message = `mesh0: SSE connect failed — ${res.status} ${code}${reason}`;
  const errorId = typeof j.errorId === "string" ? j.errorId : undefined;
  if (res.status === 401 || res.status === 403)
    return new AuthenticationError(res.status, code, message, json);
  if (res.status === 404) return new NotFoundError(res.status, code, message, json);
  if (res.status === 429)
    return new RateLimitError(res.status, code, message, json);
  if (res.status >= 500)
    return new ServerError(res.status, code, message, json, errorId);
  if (res.status >= 400)
    return new BadRequestError(res.status, code, message, json);
  return new ApiError(res.status, code, message, json);
}

/** Open the SSE transport of the firehose at GET /v1/firehose. Org-wide
 *  stream of every event across every project. To request the WS transport
 *  of the same endpoint, use `Mesh0.firehose()` instead. */
export function streamEvents(
  http: HttpClient,
  opts: StreamOpts = {},
  callbacks: StreamCallbacks = {},
): StreamHandle {
  const ac = new AbortController();
  const query: Record<string, string | number | boolean> = {};
  if (opts.since) query.since = opts.since;
  if (opts.root) query.root = 1;
  const url = http.buildUrl("/v1/firehose", Object.keys(query).length ? query : undefined);
  const cfg = http.config;

  const done = (async () => {
    let res: Response;
    try {
      res = await cfg.fetch(url, {
        method: "GET",
        headers: {
          Accept: "text/event-stream",
          Authorization: `Bearer ${cfg.apiKey}`,
          "User-Agent": cfg.userAgent,
          ...cfg.defaultHeaders,
        },
        signal: ac.signal,
      });
    } catch (err) {
      if (isAbortError(err)) return;
      throw new NetworkError("mesh0: SSE connect failed", err);
    }
    if (!res.ok) throw await buildConnectError(res);
    if (!res.body) throw new NetworkError("mesh0: SSE response had no body");

    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";
    let serverError: { reason: string; errorId?: string } | null = null;
    let resyncInfo: FirehoseResync | null = null;
    try {
      for (;;) {
        const { value, done: streamDone } = await reader.read();
        if (streamDone) break;
        // Normalize CRLF up-front so boundary indices line up with the
        // string we actually slice. The SSE spec mandates LF after
        // normalization anyway.
        buf += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        const lastBreak = buf.lastIndexOf("\n\n");
        if (lastBreak === -1) continue;
        const complete = buf.slice(0, lastBreak + 2);
        buf = buf.slice(lastBreak + 2);
        for (const raw of parseSseBuffer(complete)) {
          const msg = parseSseData(raw.event, raw.data);
          if (!msg) continue;
          callbacks.onMessage?.(msg);
          switch (msg.event) {
            case "hello":
              callbacks.onHello?.(msg.data);
              break;
            case "ping":
              callbacks.onPing?.(msg.data);
              break;
            case "event":
              callbacks.onEvent?.(msg.data.row, {
                partition: msg.data.partition,
                offset: msg.data.offset,
              });
              break;
            case "resync":
              callbacks.onResync?.(msg.data);
              // resync is terminal on the firehose: the server closes the
              // stream right after. Capture and surface as a rejection so
              // callers don't silently lose events.
              resyncInfo = msg.data;
              break;
            case "error":
              callbacks.onError?.(msg.data);
              // Capture and reject `done` once the stream closes — a server
              // `error` frame is a terminal condition, never recoverable.
              serverError = msg.data;
              break;
          }
        }
      }
    } catch (err) {
      if (isAbortError(err)) return;
      throw err;
    } finally {
      // Lock may already be released if the stream closed naturally.
      try {
        reader.releaseLock();
      } catch {
        /* ignore */
      }
    }
    if (serverError) {
      const detail = serverError.errorId ? ` (errorId=${serverError.errorId})` : "";
      throw new NetworkError(`mesh0: SSE server error — ${serverError.reason}${detail}`);
    }
    if (resyncInfo) {
      throw new NetworkError(
        `mesh0: SSE resync — ${resyncInfo.reason} (dropped=${resyncInfo.dropped})`,
      );
    }
  })();

  return {
    done,
    close: () => ac.abort(),
  };
}

// fetch-based SSE parser for the SSE transport of the org-wide firehose
// (GET /v1/firehose without an Upgrade header). EventSource is not
// portable across runtimes (browsers + Node 22+ have it, edge runtimes
// vary, and EventSource can't set Authorization in browsers). Doing the
// parse ourselves with fetch + ReadableStream keeps one code path
// everywhere.
//
// Note: this client does not auto-reconnect. After a `resync` (server-
// side queue overflow or marshal failure) or an `error` frame, the
// stream terminates and `closed` resolves with the corresponding
// `kind`. Consumers should re-call `openFirehose()` and refetch from
// /v1/events to recover dropped rows.

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
  FirehoseFrame,
  FirehoseResync,
  FirehoseServerError,
} from "../types.js";
import type {
  FirehoseCallbacks,
  FirehoseCloseInfo,
  FirehoseHandle,
  FirehoseOpts,
} from "./firehose.js";

interface RawSse {
  event: string;
  data: string;
}

function* parseSseBuffer(buf: string): Generator<RawSse> {
  // The wire spec is "\n\n" separated records; CRLF is normalized before
  // we reach this function.
  for (const block of buf.split("\n\n")) {
    if (!block) continue;
    let event = "message";
    const dataLines: string[] = [];
    let sawField = false;
    for (const line of block.split("\n")) {
      if (!line || line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      // WHATWG spec: a line with no colon is treated as a field whose
      // value is the empty string. We honor that for `data` (rare but
      // forward-compatible).
      const field = colon === -1 ? line : line.slice(0, colon);
      let value = colon === -1 ? "" : line.slice(colon + 1);
      if (value.startsWith(" ")) value = value.slice(1);
      sawField = true;
      if (field === "event") event = value;
      else if (field === "data") dataLines.push(value);
    }
    if (sawField) yield { event, data: dataLines.join("\n") };
  }
}

type ParseResult =
  | { ok: true; frame: FirehoseFrame }
  | { ok: false; error: NetworkError };

function parseSseData(event: string, data: string): ParseResult | null {
  switch (event) {
    case "hello": {
      const parsed = data ? safeJson(data) : {};
      if (parsed === SAFE_JSON_FAIL) return parseFail("hello", data);
      const obj = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      return {
        ok: true,
        frame: {
          kind: "hello",
          hello: {
            topic: typeof obj.topic === "string" ? obj.topic : "",
            since: typeof obj.since === "string" ? obj.since : "",
            root: typeof obj.root === "boolean" ? obj.root : false,
          },
        },
      };
    }
    case "ping": {
      // Server may send a bare epoch-ms number or `{"ts": <number>}`. An
      // empty `data:` is malformed — treat as an error frame so it
      // surfaces instead of firing `onPing(0)`.
      const trimmed = data.trim();
      if (trimmed === "") return parseFail("ping", data);
      const asNum = Number(trimmed);
      if (Number.isFinite(asNum)) return { ok: true, frame: { kind: "ping", ts: asNum } };
      const parsed = safeJson(trimmed);
      if (parsed === SAFE_JSON_FAIL) return parseFail("ping", data);
      if (parsed && typeof parsed === "object") {
        const ts = (parsed as { ts?: unknown }).ts;
        if (typeof ts === "number" && Number.isFinite(ts)) {
          return { ok: true, frame: { kind: "ping", ts } };
        }
      }
      return parseFail("ping", data);
    }
    case "event": {
      const parsed = safeJson(data);
      if (parsed === SAFE_JSON_FAIL) return parseFail("event", data);
      if (!parsed || typeof parsed !== "object") return parseFail("event", data);
      const obj = parsed as { partition?: unknown; offset?: unknown; row?: unknown };
      if (!obj.row || typeof obj.row !== "object") return parseFail("event", data);
      if (typeof obj.partition !== "number") return parseFail("event", data);
      if (typeof obj.offset !== "string") return parseFail("event", data);
      return {
        ok: true,
        frame: {
          kind: "event",
          row: obj.row as EventRow,
          meta: { partition: obj.partition, offset: obj.offset },
        },
      };
    }
    case "resync": {
      const parsed = safeJson(data);
      if (parsed === SAFE_JSON_FAIL) return parseFail("resync", data);
      const body = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      const info: FirehoseResync = {
        reason: typeof body.reason === "string" ? body.reason : "unknown",
        dropped: typeof body.dropped === "number" ? body.dropped : 0,
      };
      return { ok: true, frame: { kind: "resync", info } };
    }
    case "error": {
      const parsed = safeJson(data);
      if (parsed === SAFE_JSON_FAIL) return parseFail("error", data);
      const body = (parsed && typeof parsed === "object" ? parsed : {}) as Record<string, unknown>;
      const error: FirehoseServerError = {
        reason: typeof body.reason === "string" ? body.reason : "unknown",
      };
      if (typeof body.errorId === "string") error.errorId = body.errorId;
      return { ok: true, frame: { kind: "error", error } };
    }
    default:
      return {
        ok: false,
        error: new NetworkError(`mesh0: SSE received unknown event type "${event}"`),
      };
  }
}

const SAFE_JSON_FAIL = Symbol("safeJsonFail");

function safeJson(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return SAFE_JSON_FAIL;
  }
}

function parseFail(event: string, data: string): ParseResult {
  const sample = data.length > 120 ? `${data.slice(0, 117)}...` : data;
  return {
    ok: false,
    error: new NetworkError(`mesh0: SSE received malformed "${event}" frame: ${sample}`),
  };
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
      /* leave json = null; we'll attach the raw text to the error body */
    }
  }
  const j = json && typeof json === "object" ? (json as Record<string, unknown>) : {};
  const code = typeof j.error === "string" ? j.error : `http_${res.status}`;
  const reason = typeof j.reason === "string" ? `: ${j.reason}` : "";
  const message = `mesh0: SSE connect failed — ${res.status} ${code}${reason}`;
  const errorId = typeof j.errorId === "string" ? j.errorId : undefined;
  // Always carry something on `.body` so consumers can inspect non-JSON
  // (HTML proxy responses, plain text) instead of losing it.
  const body: unknown = json ?? (raw ? { raw } : null);
  if (res.status === 401 || res.status === 403)
    return new AuthenticationError(res.status, code, message, body);
  if (res.status === 404) return new NotFoundError(res.status, code, message, body);
  if (res.status === 429) return new RateLimitError(res.status, code, message, body);
  if (res.status >= 500) return new ServerError(res.status, code, message, body, errorId);
  if (res.status >= 400) return new BadRequestError(res.status, code, message, body);
  return new ApiError(res.status, code, message, body);
}

export function openSseFirehose(
  http: HttpClient,
  opts: FirehoseOpts,
  callbacks: FirehoseCallbacks,
): FirehoseHandle {
  const ac = new AbortController();
  const query: Record<string, string | number | boolean> = {};
  if (opts.since) query.since = opts.since;
  if (opts.root) query.root = 1;
  const url = http.buildUrl("/v1/firehose", Object.keys(query).length ? query : undefined);
  const cfg = http.config;

  let userClosed = false;
  let pending: FirehoseCloseInfo | null = null;
  const setPending = (info: FirehoseCloseInfo): void => {
    if (!pending) pending = info;
  };

  const closed: Promise<FirehoseCloseInfo> = (async () => {
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
      if (isAbortError(err)) {
        return pending ?? { kind: "aborted" };
      }
      const wrapped = new NetworkError("mesh0: SSE connect failed", err);
      callbacks.onError?.(wrapped);
      return { kind: "transport", error: wrapped };
    }
    if (!res.ok) {
      const err = await buildConnectError(res);
      const wrapped = err instanceof NetworkError ? err : new NetworkError(err.message, err);
      callbacks.onError?.(wrapped);
      return { kind: "transport", error: wrapped };
    }
    if (!res.body) {
      const err = new NetworkError("mesh0: SSE response had no body");
      callbacks.onError?.(err);
      return { kind: "transport", error: err };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";
    // A trailing "\r" at a chunk boundary may pair with a "\n" at the
    // start of the next chunk. Stash it so the CRLF normalization
    // doesn't miss the seam.
    let pendingCR = false;
    try {
      readLoop: for (;;) {
        const { value, done: streamDone } = await reader.read();
        if (streamDone) break;
        let chunk = decoder.decode(value, { stream: true });
        if (pendingCR) {
          chunk = "\r" + chunk;
          pendingCR = false;
        }
        if (chunk.endsWith("\r")) {
          chunk = chunk.slice(0, -1);
          pendingCR = true;
        }
        buf += chunk.replace(/\r\n/g, "\n");
        const lastBreak = buf.lastIndexOf("\n\n");
        if (lastBreak === -1) continue;
        const complete = buf.slice(0, lastBreak + 2);
        buf = buf.slice(lastBreak + 2);
        for (const raw of parseSseBuffer(complete)) {
          const result = parseSseData(raw.event, raw.data);
          if (!result) continue;
          if (!result.ok) {
            callbacks.onError?.(result.error);
            // Malformed frames are advisory, not terminal — keep reading.
            continue;
          }
          const frame = result.frame;
          callbacks.onMessage?.(frame);
          switch (frame.kind) {
            case "hello":
              callbacks.onHello?.(frame.hello);
              break;
            case "ping":
              callbacks.onPing?.(frame.ts);
              break;
            case "event":
              callbacks.onEvent?.(frame.row, frame.meta);
              break;
            case "resync":
              callbacks.onResync?.(frame.info);
              setPending({ kind: "resync", resync: frame.info });
              // Terminal — stop dispatching further frames immediately.
              ac.abort();
              break readLoop;
            case "error":
              setPending({ kind: "error", serverError: frame.error });
              ac.abort();
              break readLoop;
          }
        }
      }
    } catch (err) {
      if (isAbortError(err)) {
        // Either the caller closed us, or we aborted ourselves after a
        // terminal frame. Either way, `pending` is authoritative.
        return pending ?? (userClosed ? { kind: "aborted" } : { kind: "ok" });
      }
      const wrapped = err instanceof NetworkError ? err : new NetworkError("mesh0: SSE stream failed", err);
      callbacks.onError?.(wrapped);
      return { kind: "transport", error: wrapped };
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* lock may already be released after natural EOF or abort */
      }
    }
    if (pending) return pending;
    if (userClosed) return { kind: "aborted" };
    return { kind: "ok" };
  })();

  return {
    closed,
    close: () => {
      userClosed = true;
      ac.abort();
    },
  };
}

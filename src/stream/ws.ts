// WebSocket transport for the org-wide firehose at /v1/firehose. Auth is
// sent via `Sec-WebSocket-Protocol` (the only header settable from
// browser WebSocket constructors) and additionally via an Authorization
// header on the `ws` package's `headers` option (no-op in browsers) so
// non-browser clients keep working if subprotocol auth is ever dropped
// server-side.

import type { HttpClient } from "../http.js";
import { ConfigurationError, NetworkError } from "../errors.js";
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

// Minimal structural type so we accept both browser WebSocket and the
// `ws` package WebSocket without taking a hard dep on either.
interface MinimalWebSocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (ev: unknown) => void): void;
}

interface MinimalWebSocketCtor {
  new (url: string, protocols?: string | string[], options?: unknown): MinimalWebSocket;
}

export function openWsFirehose(
  http: HttpClient,
  opts: FirehoseOpts,
  callbacks: FirehoseCallbacks,
): FirehoseHandle {
  const cfg = http.config;
  const WS = cfg.WebSocket as unknown as MinimalWebSocketCtor | undefined;
  if (!WS) {
    throw new ConfigurationError(
      "mesh0: no WebSocket implementation available — on Node <22, install the `ws` package and pass it via Mesh0({ WebSocket: WebSocket })",
    );
  }

  const query: Record<string, string | number | boolean> = {};
  if (opts.since) query.since = opts.since;
  if (opts.root) query.root = 1;
  const wsUrl = http
    .buildUrl("/v1/firehose", Object.keys(query).length ? query : undefined)
    .replace(/^http(s?):/, (_m, s: string) => `ws${s}:`);

  let sock: MinimalWebSocket;
  try {
    sock = new WS(wsUrl, [`mesh0.token.${cfg.apiKey}`], {
      // `ws` reads this; browsers ignore the third positional arg.
      headers: {
        Authorization: `Bearer ${cfg.apiKey}`,
        "User-Agent": cfg.userAgent,
      },
    });
  } catch (err) {
    throw new NetworkError("mesh0: firehose connect failed", err);
  }

  let resolve: (v: FirehoseCloseInfo) => void = () => {};
  const closed = new Promise<FirehoseCloseInfo>((res) => {
    resolve = res;
  });

  // The first definitive close cause wins. Subsequent frames or the
  // `close` event only fill in WS-level code/reason if not already set.
  let pending: FirehoseCloseInfo | null = null;
  let userClosed = false;

  const setPending = (info: FirehoseCloseInfo): void => {
    if (!pending) pending = info;
  };

  const fireError = (err: NetworkError): void => {
    callbacks.onError?.(err);
    setPending({ kind: "transport", error: err });
  };

  sock.addEventListener("message", (ev: unknown) => {
    const data = extractData(ev);
    if (data === null) {
      fireError(new NetworkError("mesh0: firehose received unsupported frame shape"));
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch (err) {
      fireError(new NetworkError("mesh0: firehose received malformed JSON frame", err));
      return;
    }
    if (!parsed || typeof parsed !== "object") {
      fireError(new NetworkError("mesh0: firehose received non-object frame"));
      return;
    }
    const obj = parsed as Record<string, unknown>;
    const frame = toFrame(obj);
    if (!frame) {
      const type = JSON.stringify(obj.type);
      fireError(new NetworkError(`mesh0: firehose received unknown message type ${type}`));
      return;
    }
    callbacks.onMessage?.(frame);
    switch (frame.kind) {
      case "hello":
        callbacks.onHello?.(frame.hello);
        break;
      case "event":
        callbacks.onEvent?.(frame.row, frame.meta);
        break;
      case "ping":
        callbacks.onPing?.(frame.ts);
        break;
      case "resync":
        callbacks.onResync?.(frame.info);
        setPending({ kind: "resync", resync: frame.info });
        break;
      case "error":
        setPending({ kind: "error", serverError: frame.error });
        break;
    }
  });

  sock.addEventListener("error", (ev: unknown) => {
    fireError(toNetworkError(ev));
  });

  sock.addEventListener("close", (ev: unknown) => {
    const code = readNumber(ev, "code") ?? 1000;
    const reason = readString(ev, "reason");
    if (!pending) {
      if (userClosed) pending = { kind: "aborted", code, reason };
      else if (code === 1000) pending = { kind: "ok", code, reason };
      else
        pending = {
          kind: "transport",
          code,
          reason,
          error: new NetworkError(`mesh0: firehose closed abnormally (code=${code})`),
        };
    } else {
      pending.code ??= code;
      if (!pending.reason && reason) pending.reason = reason;
    }
    resolve(pending);
  });

  return {
    closed,
    close: (code = 1000, reason = "") => {
      userClosed = true;
      sock.close(code, reason);
    },
  };
}

// --- frame normalization -------------------------------------------------

function toFrame(obj: Record<string, unknown>): FirehoseFrame | null {
  switch (obj.type) {
    case "hello":
      return {
        kind: "hello",
        hello: {
          topic: typeof obj.topic === "string" ? obj.topic : "",
          since: typeof obj.since === "string" ? obj.since : "",
          root: typeof obj.root === "boolean" ? obj.root : false,
        },
      };
    case "event": {
      const row = obj.row;
      if (!row || typeof row !== "object") return null;
      if (typeof obj.partition !== "number") return null;
      if (typeof obj.offset !== "string") return null;
      return {
        kind: "event",
        row: row as EventRow,
        meta: { partition: obj.partition, offset: obj.offset },
      };
    }
    case "ping": {
      const ts = typeof obj.ts === "number" ? obj.ts : NaN;
      if (!Number.isFinite(ts)) return null;
      return { kind: "ping", ts };
    }
    case "resync": {
      const info: FirehoseResync = {
        reason: typeof obj.reason === "string" ? obj.reason : "unknown",
        dropped: typeof obj.dropped === "number" ? obj.dropped : 0,
      };
      return { kind: "resync", info };
    }
    case "error": {
      const error: FirehoseServerError = {
        reason: typeof obj.reason === "string" ? obj.reason : "unknown",
      };
      if (typeof obj.errorId === "string") error.errorId = obj.errorId;
      return { kind: "error", error };
    }
    default:
      return null;
  }
}

// --- helpers -------------------------------------------------------------

function toNetworkError(ev: unknown): NetworkError {
  if (ev && typeof ev === "object") {
    const obj = ev as { message?: unknown; error?: unknown; type?: unknown };
    const msg = typeof obj.message === "string" && obj.message
      ? obj.message
      : typeof obj.type === "string"
        ? `mesh0: firehose socket error (${obj.type})`
        : "mesh0: firehose socket error";
    const cause = obj.error ?? ev;
    return new NetworkError(msg, cause);
  }
  return new NetworkError("mesh0: firehose socket error");
}

function extractData(ev: unknown): string | null {
  if (!ev || typeof ev !== "object") return null;
  const d = (ev as { data?: unknown }).data;
  if (typeof d === "string") return d;
  // Node `ws` may emit Buffer (a Uint8Array subclass); browsers emit
  // ArrayBuffer when binaryType="arraybuffer". Blob/anything else is
  // unsupported and routed to onError by the caller.
  if (d instanceof Uint8Array) return new TextDecoder("utf-8").decode(d);
  if (d instanceof ArrayBuffer) return new TextDecoder("utf-8").decode(new Uint8Array(d));
  return null;
}

function readNumber(ev: unknown, key: string): number | undefined {
  if (!ev || typeof ev !== "object" || !(key in ev)) return undefined;
  const n = Number((ev as Record<string, unknown>)[key]);
  return Number.isFinite(n) ? n : undefined;
}

function readString(ev: unknown, key: string): string {
  if (!ev || typeof ev !== "object" || !(key in ev)) return "";
  const v = (ev as Record<string, unknown>)[key];
  return v == null ? "" : String(v);
}

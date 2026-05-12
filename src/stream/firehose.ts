import type { HttpClient } from "../http.js";
import { ConfigurationError, NetworkError } from "../errors.js";
import type { EventRow, FirehoseMessage } from "../types.js";

export interface FirehoseOpts {
  /** Either 'earliest', 'latest' (default), or a numeric offset string. */
  since?: "earliest" | "latest" | string;
}

export interface FirehoseCallbacks {
  onHello?: (msg: { topic: string; since: string }) => void;
  onEvent?: (row: EventRow, meta: { partition: number; offset: string }) => void;
  onPing?: (tsMs: number) => void;
  onMessage?: (msg: FirehoseMessage) => void;
  onError?: (err: Error) => void;
  onClose?: (code: number, reason: string) => void;
}

export interface FirehoseHandle {
  /** Resolves when the socket closes for any reason. */
  closed: Promise<{ code: number; reason: string }>;
  close(code?: number, reason?: string): void;
}

// Minimal structural type so we can accept both browser WebSocket and the
// `ws` package WebSocket without taking a hard dep on either.
interface MinimalWebSocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (ev: unknown) => void): void;
}

interface MinimalWebSocketCtor {
  new (url: string, protocols?: string | string[], options?: unknown): MinimalWebSocket;
}

/**
 * Open the WebSocket firehose at GET /v1/firehose.
 *
 * Auth is sent via Sec-WebSocket-Protocol on browsers (Authorization
 * headers aren't settable from WebSocket) and the `ws` package's `headers`
 * option on Node when supported.
 */
export function openFirehose(
  http: HttpClient,
  opts: FirehoseOpts = {},
  callbacks: FirehoseCallbacks = {},
): FirehoseHandle {
  const cfg = http.config;
  const WS = cfg.WebSocket as unknown as MinimalWebSocketCtor | undefined;
  if (!WS) {
    throw new ConfigurationError(
      "mesh0: no WebSocket implementation available — on Node <22, install the `ws` package and pass it via Mesh0({ WebSocket: WebSocket })",
    );
  }

  const wsUrl = http
    .buildUrl("/v1/firehose", opts.since ? { since: opts.since } : undefined)
    .replace(/^http(s?):/, (_m, s: string) => `ws${s}:`);

  // Subprotocol token works for both browser and `ws` package — the server
  // accepts `mesh0.token.<key>` on Sec-WebSocket-Protocol. We still pass a
  // Bearer header through the `ws` `headers` option when it's available
  // (no-op in browsers) so non-browser clients work even if a future
  // server rejects subprotocol auth.
  const tokenProto = `mesh0.token.${cfg.apiKey}`;
  let sock: MinimalWebSocket;
  try {
    sock = new WS(wsUrl, [tokenProto], {
      // The `ws` Node package reads this; browsers ignore it.
      headers: {
        Authorization: `Bearer ${cfg.apiKey}`,
        "User-Agent": cfg.userAgent,
      },
    });
  } catch (err) {
    throw new NetworkError("mesh0: firehose connect failed", err);
  }

  let closeResolve: (v: { code: number; reason: string }) => void = () => {};
  const closed = new Promise<{ code: number; reason: string }>((res) => {
    closeResolve = res;
  });

  sock.addEventListener("message", (ev: unknown) => {
    const data = extractData(ev);
    if (!data) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch {
      return;
    }
    if (!parsed || typeof parsed !== "object") return;
    const msg = parsed as FirehoseMessage;
    callbacks.onMessage?.(msg);
    switch (msg.type) {
      case "hello":
        callbacks.onHello?.({ topic: msg.topic, since: msg.since });
        break;
      case "event":
        callbacks.onEvent?.(msg.row, { partition: msg.partition, offset: msg.offset });
        break;
      case "ping":
        callbacks.onPing?.(msg.ts);
        break;
    }
  });
  sock.addEventListener("error", (ev: unknown) => {
    const err =
      ev && typeof ev === "object" && "message" in ev && typeof (ev as { message: unknown }).message === "string"
        ? new NetworkError((ev as { message: string }).message)
        : new NetworkError("mesh0: firehose socket error");
    callbacks.onError?.(err);
  });
  sock.addEventListener("close", (ev: unknown) => {
    const code = ev && typeof ev === "object" && "code" in ev ? Number((ev as { code: unknown }).code) || 1000 : 1000;
    const reason =
      ev && typeof ev === "object" && "reason" in ev
        ? String((ev as { reason: unknown }).reason ?? "")
        : "";
    callbacks.onClose?.(code, reason);
    closeResolve({ code, reason });
  });

  return {
    closed,
    close: (code = 1000, reason = "") => sock.close(code, reason),
  };
}

function extractData(ev: unknown): string | null {
  if (!ev || typeof ev !== "object") return null;
  const d = (ev as { data?: unknown }).data;
  if (typeof d === "string") return d;
  // Node `ws` may emit Buffer; browsers emit Blob/ArrayBuffer when binaryType
  // is set. We only need text; coerce best-effort.
  if (d && typeof (d as { toString?: () => string }).toString === "function") {
    return (d as { toString(): string }).toString();
  }
  return null;
}

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
  /** Surface for transport errors AND malformed server frames. Always a
   *  NetworkError; subclassing was avoided to keep the contract simple. */
  onError?: (err: NetworkError) => void;
  onClose?: (code: number, reason: string) => void;
}

/** Resolved close state for {@link FirehoseHandle.closed}. `clean` is true
 *  only when the socket closed with a normal-closure code (1000) and no
 *  pending transport error was observed. */
export interface FirehoseCloseInfo {
  code: number;
  reason: string;
  clean: boolean;
}

export interface FirehoseHandle {
  /** Resolves when the socket closes. Inspect `clean`/`code` to detect
   *  abnormal closure (auth failure surfaces as code 1006/4401, not as a
   *  rejection — we never reject this promise so `await` is always safe). */
  closed: Promise<FirehoseCloseInfo>;
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

  let closeResolve: (v: FirehoseCloseInfo) => void = () => {};
  const closed = new Promise<FirehoseCloseInfo>((res) => {
    closeResolve = res;
  });
  let sawError = false;

  sock.addEventListener("message", (ev: unknown) => {
    const data = extractData(ev);
    if (data === null) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch (err) {
      sawError = true;
      callbacks.onError?.(new NetworkError("mesh0: firehose received malformed JSON frame", err));
      return;
    }
    if (!parsed || typeof parsed !== "object") {
      sawError = true;
      callbacks.onError?.(new NetworkError("mesh0: firehose received non-object frame"));
      return;
    }
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
      default:
        // Unknown message types are a protocol-version mismatch — surface
        // them so consumers can update the SDK or report upstream.
        sawError = true;
        callbacks.onError?.(
          new NetworkError(`mesh0: firehose received unknown message type ${JSON.stringify((msg as { type?: unknown }).type)}`),
        );
    }
  });
  sock.addEventListener("error", (ev: unknown) => {
    sawError = true;
    callbacks.onError?.(toNetworkError(ev));
  });
  sock.addEventListener("close", (ev: unknown) => {
    const code = ev && typeof ev === "object" && "code" in ev ? Number((ev as { code: unknown }).code) || 1000 : 1000;
    const reason =
      ev && typeof ev === "object" && "reason" in ev
        ? String((ev as { reason: unknown }).reason ?? "")
        : "";
    const clean = !sawError && code === 1000;
    callbacks.onClose?.(code, reason);
    closeResolve({ code, reason, clean });
  });

  return {
    closed,
    close: (code = 1000, reason = "") => sock.close(code, reason),
  };
}

function toNetworkError(ev: unknown): NetworkError {
  if (ev && typeof ev === "object") {
    // Browser ErrorEvent carries `.error` (the underlying Error) and `.message`.
    // The `ws` package emits an Error directly.
    const obj = ev as { message?: unknown; error?: unknown };
    const msg = typeof obj.message === "string" ? obj.message : "mesh0: firehose socket error";
    const cause = obj.error ?? ev;
    return new NetworkError(msg, cause);
  }
  return new NetworkError("mesh0: firehose socket error");
}

function extractData(ev: unknown): string | null {
  if (!ev || typeof ev !== "object") return null;
  const d = (ev as { data?: unknown }).data;
  if (typeof d === "string") return d;
  // Node `ws` may emit Buffer; browsers emit Blob/ArrayBuffer when binaryType
  // is set. Coerce the binary forms we can without pulling in Node's Buffer
  // typings, but ignore unknown shapes so we never feed `[object Object]`
  // into JSON.parse.
  if (d instanceof Uint8Array) return new TextDecoder("utf-8").decode(d);
  if (d instanceof ArrayBuffer) return new TextDecoder("utf-8").decode(new Uint8Array(d));
  return null;
}

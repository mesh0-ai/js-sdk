// fetch-based SSE parser. EventSource is not portable across runtimes
// (browsers + Node 22+ have it, edge runtimes vary, and EventSource can't
// set the Authorization header in browsers). Doing it ourselves with
// fetch + ReadableStream keeps one code path everywhere.

import type { HttpClient } from "../http.js";
import { NetworkError } from "../errors.js";
import type { EventRow, StreamMessage } from "../types.js";

export interface StreamHandle {
  /** Resolves when the stream ends cleanly (server closes). */
  done: Promise<void>;
  /** Close the underlying connection. */
  close(): void;
}

export interface StreamCallbacks {
  onEvent?: (row: EventRow) => void;
  onHello?: (data: Record<string, unknown>) => void;
  onPing?: (tsMs: number) => void;
  onResync?: () => void;
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
      return { event: "hello", data: (parsed as Record<string, unknown>) ?? {} };
    }
    case "ping": {
      const n = Number(data);
      return { event: "ping", data: Number.isFinite(n) ? n : Date.now() };
    }
    case "event": {
      const parsed = safeJson(data);
      if (!parsed || typeof parsed !== "object") return null;
      return { event: "event", data: parsed as EventRow };
    }
    case "resync":
      return { event: "resync", data: {} };
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

/** Open the SSE channel at GET /v1/events/stream. */
export function streamEvents(http: HttpClient, callbacks: StreamCallbacks = {}): StreamHandle {
  const ac = new AbortController();
  const url = http.buildUrl("/v1/events/stream");
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
      if (ac.signal.aborted) return;
      throw new NetworkError("mesh0: SSE connect failed", err);
    }
    if (!res.ok) {
      throw new NetworkError(`mesh0: SSE connect failed with status ${res.status}`);
    }
    if (!res.body) {
      throw new NetworkError("mesh0: SSE response had no body");
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";
    try {
      for (;;) {
        const { value, done: streamDone } = await reader.read();
        if (streamDone) break;
        buf += decoder.decode(value, { stream: true });
        let lastBreak = buf.lastIndexOf("\n\n");
        if (lastBreak === -1) lastBreak = buf.replace(/\r\n/g, "\n").lastIndexOf("\n\n");
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
              callbacks.onEvent?.(msg.data);
              break;
            case "resync":
              callbacks.onResync?.();
              break;
            case "error":
              callbacks.onError?.(msg.data);
              break;
          }
        }
      }
    } catch (err) {
      if (ac.signal.aborted) return;
      throw err;
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* ignore */
      }
    }
  })();

  return {
    done,
    close: () => ac.abort(),
  };
}

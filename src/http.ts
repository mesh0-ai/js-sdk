import type { Mesh0Config } from "./config.js";
import {
  ApiError,
  AuthenticationError,
  BadRequestError,
  NetworkError,
  NotFoundError,
  RateLimitError,
  ServerError,
} from "./errors.js";

export interface RequestOpts {
  method: "GET" | "POST" | "PATCH" | "DELETE" | "PUT";
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

function buildUrl(baseUrl: string, path: string, query?: RequestOpts["query"]): string {
  let url = baseUrl + (path.startsWith("/") ? path : `/${path}`);
  if (query) {
    const parts: string[] = [];
    for (const [k, v] of Object.entries(query)) {
      if (v === undefined || v === null) continue;
      parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
    }
    if (parts.length > 0) url += `?${parts.join("&")}`;
  }
  return url;
}

function parseRetryAfter(h: string | null): number | undefined {
  if (!h) return undefined;
  const n = Number(h);
  if (Number.isFinite(n) && n >= 0) return n;
  const t = Date.parse(h);
  if (Number.isFinite(t)) return Math.max(0, Math.round((t - Date.now()) / 1000));
  return undefined;
}

function isAbortError(err: unknown): boolean {
  return !!err && typeof err === "object" && (err as { name?: unknown }).name === "AbortError";
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const t = setTimeout(() => resolve(), ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}

function backoff(attempt: number, baseMs: number): number {
  // Exponential with full jitter.
  const cap = 10_000;
  const exp = Math.min(cap, baseMs * 2 ** attempt);
  return Math.floor(Math.random() * exp);
}

// Minimum wait before honoring a server-supplied Retry-After. A buggy server
// or skewed clock that returns 0/past-date would otherwise hot-loop us.
const MIN_RETRY_AFTER_MS = 100;

async function readBody(res: Response): Promise<{ raw: string; json: unknown }> {
  const raw = await res.text();
  if (!raw) return { raw: "", json: null };
  try {
    return { raw, json: JSON.parse(raw) };
  } catch {
    return { raw, json: null };
  }
}

function throwForStatus(res: Response, body: { raw: string; json: unknown }): never {
  const j =
    body.json && typeof body.json === "object" ? (body.json as Record<string, unknown>) : {};
  const code = typeof j.error === "string" ? j.error : `http_${res.status}`;
  const reason = typeof j.reason === "string" ? `: ${j.reason}` : "";
  const detail = typeof j.detail === "string" ? ` — ${j.detail}` : "";
  // If the body wasn't JSON, surface a snippet of it so the caller has
  // something to debug instead of an opaque `http_500`.
  const rawHint =
    body.json === null && body.raw ? ` — ${body.raw.slice(0, 200)}` : "";
  const message = `${res.status} ${code}${reason}${detail}${rawHint}`;
  const errorId = typeof j.errorId === "string" ? j.errorId : undefined;

  if (res.status === 401 || res.status === 403) {
    throw new AuthenticationError(res.status, code, message, body.json);
  }
  if (res.status === 404) {
    throw new NotFoundError(res.status, code, message, body.json);
  }
  if (res.status === 429) {
    throw new RateLimitError(
      res.status,
      code,
      message,
      body.json,
      parseRetryAfter(res.headers.get("retry-after")),
    );
  }
  if (res.status >= 500) {
    throw new ServerError(res.status, code, message, body.json, errorId);
  }
  if (res.status >= 400) {
    throw new BadRequestError(res.status, code, message, body.json);
  }
  // Fallback for status codes outside the standard 4xx/5xx buckets.
  throw new ApiError(res.status, code, message, body.json);
}

/** HTTP statuses considered transient/retryable. Network failures are handled
 *  separately in the request catch block. */
function isRetryable(status: number): boolean {
  return status === 429 || (status >= 500 && status <= 599);
}

export class HttpClient {
  constructor(private readonly cfg: Mesh0Config) {}

  async request<T = unknown>(opts: RequestOpts): Promise<T> {
    const url = buildUrl(this.cfg.baseUrl, opts.path, opts.query);
    const headers: Record<string, string> = {
      Accept: "application/json",
      Authorization: `Bearer ${this.cfg.apiKey}`,
      "User-Agent": this.cfg.userAgent,
      ...this.cfg.defaultHeaders,
      ...(opts.headers ?? {}),
    };
    let bodyInit: BodyInit | undefined;
    if (opts.body !== undefined && opts.body !== null) {
      bodyInit = JSON.stringify(opts.body);
      headers["Content-Type"] = "application/json";
    }

    const maxAttempts = this.cfg.maxRetries + 1;
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const ac = new AbortController();
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        ac.abort(new Error("mesh0: request timed out"));
      }, this.cfg.timeoutMs);
      const onUserAbort = () => ac.abort(opts.signal?.reason);
      opts.signal?.addEventListener("abort", onUserAbort, { once: true });

      let res: Response;
      try {
        res = await this.cfg.fetch(url, {
          method: opts.method,
          headers,
          body: bodyInit,
          signal: ac.signal,
        });
      } catch (err) {
        clearTimeout(timer);
        opts.signal?.removeEventListener("abort", onUserAbort);
        // User cancellation must surface immediately — never wrap or retry.
        if (opts.signal?.aborted) throw opts.signal.reason ?? err;
        // Timeout: surface as NetworkError but do not retry — the request
        // already exceeded the user's budget. Retrying compounds the wait.
        if (timedOut) {
          throw new NetworkError(`mesh0: ${opts.method} ${opts.path} timed out after ${this.cfg.timeoutMs}ms`, err);
        }
        // AbortError that isn't ours is a programming error — re-throw raw.
        if (isAbortError(err)) throw err;
        lastErr = new NetworkError(`mesh0: network error during ${opts.method} ${opts.path}`, err);
        if (attempt < maxAttempts - 1) {
          await sleep(backoff(attempt, this.cfg.retryBaseMs), opts.signal);
          continue;
        }
        throw lastErr;
      }
      clearTimeout(timer);
      opts.signal?.removeEventListener("abort", onUserAbort);

      if (res.ok) {
        if (res.status === 204) return undefined as T;
        const ct = res.headers.get("content-type") ?? "";
        if (ct.includes("application/json")) {
          const raw = await res.text();
          if (!raw) return undefined as T;
          try {
            return JSON.parse(raw) as T;
          } catch (err) {
            throw new NetworkError(
              `mesh0: ${opts.method} ${opts.path} returned malformed JSON`,
              err,
            );
          }
        }
        return (await res.text()) as unknown as T;
      }

      if (isRetryable(res.status) && attempt < maxAttempts - 1) {
        const ra = parseRetryAfter(res.headers.get("retry-after"));
        const wait =
          ra !== undefined
            ? Math.max(ra * 1000, MIN_RETRY_AFTER_MS)
            : backoff(attempt, this.cfg.retryBaseMs);
        await sleep(wait, opts.signal);
        continue;
      }

      const body = await readBody(res);
      throwForStatus(res, body);
    }
    // Unreachable: throwForStatus is `: never` and the catch block always
    // throws on the final attempt. Kept as a defensive sink for typing.
    throw lastErr ?? new NetworkError("mesh0: request failed after retries");
  }

  /** Build an absolute URL using this client's base URL. Used by streaming. */
  buildUrl(path: string, query?: RequestOpts["query"]): string {
    return buildUrl(this.cfg.baseUrl, path, query);
  }

  get config(): Mesh0Config {
    return this.cfg;
  }
}

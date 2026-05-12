import { vi } from "vitest";

export interface FakeCall {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: unknown;
}

export interface FakeResponseInit {
  status?: number;
  body?: unknown;
  headers?: Record<string, string>;
  /** If set, throw instead of resolving. */
  throw?: unknown;
}

/**
 * Build a fetch fake that returns the provided responses in order. After
 * the last response is consumed, subsequent calls reuse the final entry.
 */
export function fakeFetch(responses: FakeResponseInit[]) {
  const calls: FakeCall[] = [];
  let i = 0;
  const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
    const headers = headersToObject(init?.headers);
    let body: unknown = null;
    if (typeof init?.body === "string") {
      try {
        body = JSON.parse(init.body);
      } catch {
        body = init.body;
      }
    }
    calls.push({ url, method: init?.method ?? "GET", headers, body });
    const idx = Math.min(i, responses.length - 1);
    i++;
    const r = responses[idx] ?? { status: 200, body: {} };
    if (r.throw) throw r.throw;
    const status = r.status ?? 200;
    const bodyText =
      r.body === undefined ? "" : typeof r.body === "string" ? r.body : JSON.stringify(r.body);
    return new Response(bodyText, {
      status,
      headers: {
        "content-type": "application/json",
        ...(r.headers ?? {}),
      },
    });
  });
  return { fetchFn, calls };
}

function headersToObject(h: HeadersInit | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!h) return out;
  if (h instanceof Headers) {
    h.forEach((v, k) => {
      out[k.toLowerCase()] = v;
    });
    return out;
  }
  if (Array.isArray(h)) {
    for (const [k, v] of h) out[k.toLowerCase()] = v;
    return out;
  }
  for (const [k, v] of Object.entries(h)) out[k.toLowerCase()] = String(v);
  return out;
}

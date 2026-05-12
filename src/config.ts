import { ConfigurationError } from "./errors.js";

export interface Mesh0ConfigInput {
  apiKey?: string;
  baseUrl?: string;
  /** Per-request timeout in ms. Default 30_000. */
  timeoutMs?: number;
  /** Retry idempotent failures (5xx, 429, network). Default 2. */
  maxRetries?: number;
  /** Initial backoff before the first retry, in ms. Default 250. */
  retryBaseMs?: number;
  /** Custom User-Agent. */
  userAgent?: string;
  /** Extra headers attached to every request. */
  defaultHeaders?: Record<string, string>;
  /** Custom fetch implementation. Defaults to globalThis.fetch. */
  fetch?: typeof fetch;
  /** Custom WebSocket constructor. Defaults to globalThis.WebSocket. Only
   *  required if you call {@link Mesh0.firehose}; HTTP works without it.
   *  On Node <22 pass the `ws` package: `new Mesh0({ WebSocket: WebSocket })`. */
  WebSocket?: typeof WebSocket;
}

export interface Mesh0Config {
  apiKey: string;
  baseUrl: string;
  timeoutMs: number;
  maxRetries: number;
  retryBaseMs: number;
  userAgent: string;
  defaultHeaders: Record<string, string>;
  fetch: typeof fetch;
  WebSocket: typeof WebSocket | undefined;
}

const DEFAULT_BASE_URL = "https://api.mesh0.ai";
const DEFAULT_UA = `mesh0-js-sdk/0.1.0`;

function readEnv(name: string): string | undefined {
  // Cross-runtime env access: Node/Bun expose process.env; Deno exposes
  // Deno.env; browsers/edge typically have neither. We deliberately don't
  // assume any single one.
  const g = globalThis as unknown as {
    process?: { env?: Record<string, string | undefined> };
    Deno?: { env?: { get(name: string): string | undefined } };
  };
  if (g.process?.env?.[name]) return g.process.env[name];
  if (g.Deno?.env?.get) return g.Deno.env.get(name) ?? undefined;
  return undefined;
}

export function resolveConfig(input: Mesh0ConfigInput = {}): Mesh0Config {
  const apiKey = input.apiKey ?? readEnv("MESH0_API_KEY");
  if (!apiKey || !apiKey.startsWith("m0_")) {
    throw new ConfigurationError(
      "mesh0: apiKey is required and must start with 'm0_' (set MESH0_API_KEY or pass { apiKey }).",
    );
  }
  const baseUrl = (input.baseUrl ?? readEnv("MESH0_BASE_URL") ?? DEFAULT_BASE_URL).replace(
    /\/+$/,
    "",
  );
  // Only bind globalThis when falling back to the global fetch — a
  // user-supplied fetch may rely on its own `this`.
  const userFetch = input.fetch;
  const fetchImpl = userFetch ?? (globalThis.fetch ? globalThis.fetch.bind(globalThis) : undefined);
  if (!fetchImpl) {
    throw new ConfigurationError(
      "mesh0: no fetch implementation available — pass { fetch } or run on Node 18+/modern browser.",
    );
  }
  return {
    apiKey,
    baseUrl,
    timeoutMs: input.timeoutMs ?? 30_000,
    maxRetries: input.maxRetries ?? 2,
    retryBaseMs: input.retryBaseMs ?? 250,
    userAgent: input.userAgent ?? DEFAULT_UA,
    defaultHeaders: input.defaultHeaders ?? {},
    fetch: fetchImpl,
    WebSocket: input.WebSocket ?? (globalThis as { WebSocket?: typeof WebSocket }).WebSocket,
  };
}

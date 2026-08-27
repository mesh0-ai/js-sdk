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
const DEFAULT_UA = `mesh0-js-sdk/0.2.0`;

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

/**
 * Does this credential have the shape of an open-mode instance token?
 *
 * Three unpadded base64url segments whose first decodes to a JOSE header
 * object carrying `alg`. Deliberately says nothing about the signature, the
 * claims, or whether the token is expired — all three are the server's to
 * judge, and it is the only party holding the key.
 *
 * Decoding the header is what makes the check narrow enough to be worth
 * having. Segment-counting alone accepts `api.mesh0.ai`, which is three
 * dot-separated alphanumeric runs and exactly the kind of mistake this
 * exists to catch. mesh0 requires UNPADDED base64url on the wire, so `=` is
 * deliberately outside the character class.
 */
export function isInstanceToken(candidate: string): boolean {
  if (!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(candidate)) {
    return false;
  }
  const header = candidate.slice(0, candidate.indexOf("."));
  let decoded: string;
  try {
    decoded = decodeBase64Url(header);
  } catch {
    return false;
  }
  if (decoded === "") return false;
  try {
    const parsed: unknown = JSON.parse(decoded);
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) && "alg" in parsed;
  } catch {
    return false;
  }
}

/**
 * Decode unpadded base64url across runtimes. `atob` is the one decoder
 * present in browsers, Deno, Bun and Node 16+ alike; Buffer is not.
 */
function decodeBase64Url(segment: string): string {
  const b64 = segment.replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  const g = globalThis as unknown as { atob?: (data: string) => string };
  if (typeof g.atob !== "function") {
    throw new Error("no base64 decoder available");
  }
  return g.atob(padded);
}

/**
 * The credential shapes this SDK forwards as a bearer.
 *
 * `m0_` project keys and `m0u_` user keys are the classic pair. An open-mode
 * instance token is a third: mesh0's admission layer routes a non-`m0_`
 * bearer to open mode, where that JWT is the ONLY credential a workspace has
 * — it authenticates ingest, the firehose, the management API and `/mcp`
 * alike. Rejecting it here made every one of those unreachable from this SDK
 * for a cluster running open mode.
 */
function isAcceptedCredential(apiKey: string): boolean {
  return apiKey.startsWith("m0_") || apiKey.startsWith("m0u_") || isInstanceToken(apiKey);
}

export function resolveConfig(input: Mesh0ConfigInput = {}): Mesh0Config {
  const apiKey = input.apiKey ?? readEnv("MESH0_API_KEY");
  if (!apiKey || !isAcceptedCredential(apiKey)) {
    throw new ConfigurationError(
      "mesh0: apiKey is required and must start with 'm0_' or 'm0u_', or be a JWT instance token (set MESH0_API_KEY or pass { apiKey }).",
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

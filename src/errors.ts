/** Base error for everything thrown by the SDK. */
export class Mesh0Error extends Error {
  /** Discriminant for exhaustive switching without `instanceof` chains. */
  readonly kind: string = "mesh0";
  constructor(message: string) {
    super(message);
    this.name = "Mesh0Error";
  }
}

/** Thrown for misconfiguration (missing key, bad baseUrl, missing fetch/WebSocket). */
export class ConfigurationError extends Mesh0Error {
  override readonly kind = "configuration";
  constructor(message: string) {
    super(message);
    this.name = "ConfigurationError";
  }
}

/** Thrown when caller-supplied arguments are invalid before any request is made. */
export class ValidationError extends Mesh0Error {
  override readonly kind = "validation";
  readonly field?: string;
  constructor(message: string, field?: string) {
    super(message);
    this.name = "ValidationError";
    if (field !== undefined) this.field = field;
  }
}

/** Transport-layer failure: fetch rejected, WS errored, malformed body, timeout. */
export class NetworkError extends Mesh0Error {
  override readonly kind = "network";
  override readonly cause?: unknown;
  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "NetworkError";
    if (cause !== undefined) this.cause = cause;
  }
}

/** Base class for any error originating from a server HTTP response. */
export class ApiError extends Mesh0Error {
  override readonly kind: string = "api";
  readonly status: number;
  readonly code: string;
  readonly body: unknown;
  readonly errorId?: string;
  readonly retryAfter?: number;
  constructor(
    status: number,
    code: string,
    message: string,
    body: unknown,
    opts: { errorId?: string; retryAfter?: number } = {},
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.body = body;
    if (opts.errorId !== undefined) this.errorId = opts.errorId;
    if (opts.retryAfter !== undefined) this.retryAfter = opts.retryAfter;
  }
}

/** 401/403 — bad or missing credentials. */
export class AuthenticationError extends ApiError {
  override readonly kind = "api.auth";
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "AuthenticationError";
  }
}

/** 400 (or other 4xx) — request was rejected as malformed by the server. */
export class BadRequestError extends ApiError {
  override readonly kind = "api.bad_request";
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "BadRequestError";
  }
}

/** 404 — resource not found. */
export class NotFoundError extends ApiError {
  override readonly kind = "api.not_found";
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "NotFoundError";
  }
}

/** 429 — too many requests. `retryAfter` is in seconds when the server supplied it. */
export class RateLimitError extends ApiError {
  override readonly kind = "api.rate_limit";
  constructor(
    status: number,
    code: string,
    message: string,
    body: unknown,
    retryAfter?: number,
  ) {
    super(status, code, message, body, { retryAfter });
    this.name = "RateLimitError";
  }
}

/** 5xx — server-side failure. `errorId` echoes any server-supplied trace id. */
export class ServerError extends ApiError {
  override readonly kind = "api.server";
  constructor(
    status: number,
    code: string,
    message: string,
    body: unknown,
    errorId?: string,
  ) {
    super(status, code, message, body, { errorId });
    this.name = "ServerError";
  }
}

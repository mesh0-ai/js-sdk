export class Mesh0Error extends Error {
  constructor(message: string) {
    super(message);
    this.name = "Mesh0Error";
  }
}

export class ConfigurationError extends Mesh0Error {
  constructor(message: string) {
    super(message);
    this.name = "ConfigurationError";
  }
}

export class NetworkError extends Mesh0Error {
  override readonly cause?: unknown;
  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "NetworkError";
    if (cause !== undefined) this.cause = cause;
  }
}

export class ApiError extends Mesh0Error {
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

export class AuthenticationError extends ApiError {
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "AuthenticationError";
  }
}

export class BadRequestError extends ApiError {
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "BadRequestError";
  }
}

export class NotFoundError extends ApiError {
  constructor(status: number, code: string, message: string, body: unknown) {
    super(status, code, message, body);
    this.name = "NotFoundError";
  }
}

export class RateLimitError extends ApiError {
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

export class ServerError extends ApiError {
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

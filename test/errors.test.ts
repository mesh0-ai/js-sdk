import { describe, it, expect } from "vitest";
import {
  Mesh0Error,
  ApiError,
  AuthenticationError,
  BadRequestError,
  ConfigurationError,
  NetworkError,
  NotFoundError,
  RateLimitError,
  ServerError,
  ValidationError,
} from "../src/index.js";

describe("error classes", () => {
  it("carry the right name/kind and inherit from Mesh0Error", () => {
    const cases: { err: Mesh0Error; name: string; kind: string }[] = [
      { err: new ConfigurationError("c"), name: "ConfigurationError", kind: "configuration" },
      { err: new ValidationError("v", "events"), name: "ValidationError", kind: "validation" },
      { err: new NetworkError("n"), name: "NetworkError", kind: "network" },
      {
        err: new AuthenticationError(401, "x", "m", null),
        name: "AuthenticationError",
        kind: "api.auth",
      },
      { err: new BadRequestError(400, "x", "m", null), name: "BadRequestError", kind: "api.bad_request" },
      { err: new NotFoundError(404, "x", "m", null), name: "NotFoundError", kind: "api.not_found" },
      {
        err: new RateLimitError(429, "x", "m", null, 5),
        name: "RateLimitError",
        kind: "api.rate_limit",
      },
      { err: new ServerError(500, "x", "m", null, "trace-1"), name: "ServerError", kind: "api.server" },
    ];
    for (const { err, name, kind } of cases) {
      expect(err).toBeInstanceOf(Mesh0Error);
      expect(err).toBeInstanceOf(Error);
      expect(err.name).toBe(name);
      expect(err.kind).toBe(kind);
    }
  });

  it("ApiError captures status/code/body/errorId/retryAfter", () => {
    const e = new ApiError(503, "unavailable", "msg", { ok: false }, {
      errorId: "trace-1",
      retryAfter: 3,
    });
    expect(e.status).toBe(503);
    expect(e.code).toBe("unavailable");
    expect(e.body).toEqual({ ok: false });
    expect(e.errorId).toBe("trace-1");
    expect(e.retryAfter).toBe(3);
  });

  it("ValidationError records the offending field", () => {
    const e = new ValidationError("nope", "traceId");
    expect(e.field).toBe("traceId");
  });

  it("NetworkError preserves underlying cause", () => {
    const cause = new Error("ECONNRESET");
    const e = new NetworkError("transport", cause);
    expect(e.cause).toBe(cause);
  });
});

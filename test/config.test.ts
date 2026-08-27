import { describe, it, expect } from "vitest";
import { Mesh0, ConfigurationError } from "../src/index.js";
import { isInstanceToken } from "../src/config.js";
import { fakeFetch } from "./helpers.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

/** A shape-valid open-mode instance token: JOSE header, claims, signature. */
const INSTANCE_TOKEN = [
  Buffer.from('{"alg":"EdDSA","typ":"JWT"}').toString("base64url"),
  Buffer.from('{"iss":"i","aud":"a","sub":"inst","m0/workspace":"workspace-42"}').toString(
    "base64url",
  ),
  "c2lnbmF0dXJl",
].join(".");

describe("config", () => {
  it("requires a recognizable credential", () => {
    expect(() => new Mesh0({ apiKey: "" })).toThrow(ConfigurationError);
    expect(() => new Mesh0({ apiKey: "wrong" })).toThrow(ConfigurationError);
  });

  it("accepts m0_ project keys and m0u_ user keys", () => {
    const fetchFn = (async () => new Response("{}")) as typeof fetch;
    expect(new Mesh0({ apiKey: KEY, fetch: fetchFn })).toBeInstanceOf(Mesh0);
    expect(new Mesh0({ apiKey: "m0u_secret", fetch: fetchFn })).toBeInstanceOf(Mesh0);
  });

  it("accepts an open-mode instance token", () => {
    // In open mode this JWT is the ONLY credential a workspace has — it
    // authenticates ingest, the firehose, the management API and /mcp alike.
    // Rejecting it made every one of those unreachable from this SDK.
    const m = new Mesh0({
      apiKey: INSTANCE_TOKEN,
      fetch: (async () => new Response("{}")) as typeof fetch,
    });
    expect(m).toBeInstanceOf(Mesh0);
  });

  it("forwards an instance token verbatim as the bearer", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: { spans: [] } }]);
    const m = new Mesh0({ apiKey: INSTANCE_TOKEN, fetch: fetchFn as typeof fetch });
    await m.events.trace("t1");
    expect(calls[0]!.headers).toMatchObject({
      authorization: `Bearer ${INSTANCE_TOKEN}`,
    });
  });

  describe("isInstanceToken", () => {
    it("accepts a well-formed unpadded base64url JWT", () => {
      expect(isInstanceToken(INSTANCE_TOKEN)).toBe(true);
    });

    it("rejects three dot-separated runs that are not a token", () => {
      // The case segment-counting alone would wave through, and exactly the
      // kind of mistake the check exists to catch.
      expect(isInstanceToken("api.mesh0.ai")).toBe(false);
    });

    it("rejects a header that is not a JOSE object", () => {
      const notAnObject = Buffer.from('"alg"').toString("base64url");
      expect(isInstanceToken(`${notAnObject}.claims.sig`)).toBe(false);
      const noAlg = Buffer.from('{"typ":"JWT"}').toString("base64url");
      expect(isInstanceToken(`${noAlg}.claims.sig`)).toBe(false);
      const arr = Buffer.from('["alg"]').toString("base64url");
      expect(isInstanceToken(`${arr}.claims.sig`)).toBe(false);
    });

    it("rejects padded or standard-alphabet base64", () => {
      // mesh0 requires unpadded base64url on the wire. Pick a payload whose
      // byte length is not a multiple of 3 so base64 actually pads it.
      const header = Buffer.from('{"alg":"EdDSA","typ":"JW"}').toString("base64");
      expect(header).toContain("=");
      expect(isInstanceToken(`${header}.claims.sig`)).toBe(false);
      expect(isInstanceToken("aGVsbG8+.claims.sig")).toBe(false);
    });

    it("rejects wrong segment counts", () => {
      expect(isInstanceToken("a.b")).toBe(false);
      expect(isInstanceToken("a.b.c.d")).toBe(false);
      expect(isInstanceToken("")).toBe(false);
    });

    it("rejects an m0_ key", () => {
      expect(isInstanceToken(KEY)).toBe(false);
    });
  });

  it("strips trailing slashes from baseUrl and uses it for requests", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: { spans: [] } }]);
    const m = new Mesh0({
      apiKey: KEY,
      baseUrl: "https://api.example.com////",
      fetch: fetchFn as typeof fetch,
    });
    await m.events.trace("t1");
    expect(calls[0]!.url).toBe("https://api.example.com/v1/traces/t1");
  });

  it("falls back to env MESH0_API_KEY", () => {
    process.env.MESH0_API_KEY = KEY;
    try {
      const m = new Mesh0({ fetch: (async () => new Response("{}")) as typeof fetch });
      expect(m).toBeInstanceOf(Mesh0);
    } finally {
      delete process.env.MESH0_API_KEY;
    }
  });

  it("falls back to env MESH0_BASE_URL", async () => {
    process.env.MESH0_BASE_URL = "https://api.staging.mesh0.ai";
    try {
      const { fetchFn, calls } = fakeFetch([{ status: 200, body: { spans: [] } }]);
      const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
      await m.events.trace("t1");
      expect(calls[0]!.url).toBe("https://api.staging.mesh0.ai/v1/traces/t1");
    } finally {
      delete process.env.MESH0_BASE_URL;
    }
  });
});

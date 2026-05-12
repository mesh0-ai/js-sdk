import { describe, it, expect } from "vitest";
import { Mesh0, ConfigurationError } from "../src/index.js";
import { fakeFetch } from "./helpers.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

describe("config", () => {
  it("requires a m0_ api key", () => {
    expect(() => new Mesh0({ apiKey: "" })).toThrow(ConfigurationError);
    expect(() => new Mesh0({ apiKey: "wrong" })).toThrow(ConfigurationError);
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

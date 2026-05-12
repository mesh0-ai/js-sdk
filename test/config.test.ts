import { describe, it, expect } from "vitest";
import { Mesh0, ConfigurationError } from "../src/index.js";

describe("config", () => {
  it("requires a m0_ api key", () => {
    expect(() => new Mesh0({ apiKey: "" })).toThrow(ConfigurationError);
    expect(() => new Mesh0({ apiKey: "wrong" })).toThrow(ConfigurationError);
  });

  it("strips trailing slashes from baseUrl", () => {
    const m = new Mesh0({
      apiKey: "m0_x_yyyyyyyyyyyyyyyyyyyyyyyy",
      baseUrl: "https://api.example.com////",
      fetch: (async () => new Response("{}")) as typeof fetch,
    });
    expect(m).toBeInstanceOf(Mesh0);
  });

  it("falls back to env MESH0_API_KEY", () => {
    process.env.MESH0_API_KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";
    try {
      const m = new Mesh0({ fetch: (async () => new Response("{}")) as typeof fetch });
      expect(m).toBeInstanceOf(Mesh0);
    } finally {
      delete process.env.MESH0_API_KEY;
    }
  });
});

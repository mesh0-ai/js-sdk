import { describe, it, expect } from "vitest";
import { Mesh0 } from "../src/index.js";
import { fakeFetch } from "./helpers.js";

const KEY = "m0_test_xxxxxxxxxxxxxxxxxxxxxxxx";

describe("query.run", () => {
  it("POSTs the wire-form request body verbatim (projectId is server-side)", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 200, body: { columns: ["c"], rows: [[1]] } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    const res = await m.query.run({
      range: { from: "now-1h", to: "now" },
      metrics: [{ fn: "count" }],
    });
    expect(calls[0]!.url).toBe("https://api.mesh0.ai/v1/query");
    expect(calls[0]!.body).toEqual({
      range: { from: "now-1h", to: "now" },
      metrics: [{ fn: "count" }],
    });
    expect(res.rows).toEqual([[1]]);
  });
});

describe("identity", () => {
  it("hits the right paths", async () => {
    const { fetchFn, calls } = fakeFetch([
      { body: { user: null } },
      { body: { id: "o", slug: "s", name: "n", status: "active", createdAt: "t" } },
      { body: { id: "p", name: "proj" } },
    ]);
    const m = new Mesh0({ apiKey: KEY, fetch: fetchFn as typeof fetch });
    await m.identity.me();
    await m.identity.org();
    await m.identity.project();
    expect(calls.map((c) => c.url)).toEqual([
      "https://api.mesh0.ai/v1/me",
      "https://api.mesh0.ai/v1/org",
      "https://api.mesh0.ai/v1/project",
    ]);
  });
});

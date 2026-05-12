import type { HttpClient } from "../http.js";
import type { QueryRequest, QueryResponse } from "../types.js";

export class QueryResource {
  constructor(private readonly http: HttpClient) {}

  /**
   * Run a TQL query (POST /v1/query). `projectId` is filled in server-side
   * from the API key, so the caller never supplies it.
   */
  async run(req: QueryRequest, opts: { signal?: AbortSignal } = {}): Promise<QueryResponse> {
    return this.http.request<QueryResponse>({
      method: "POST",
      path: "/v1/query",
      body: req,
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }
}

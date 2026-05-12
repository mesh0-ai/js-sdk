import type { HttpClient } from "../http.js";
import type { MeResponse, OrgResponse, ProjectResponse } from "../types.js";

/** Identity introspection — who the current API key authorizes as. */
export class IdentityResource {
  constructor(private readonly http: HttpClient) {}

  /** GET /v1/me — current user, or `{ user: null }` for project-scoped keys. */
  me(opts: { signal?: AbortSignal } = {}): Promise<MeResponse> {
    return this.http.request<MeResponse>({
      method: "GET",
      path: "/v1/me",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }

  /** GET /v1/org — the org owning this API key. */
  org(opts: { signal?: AbortSignal } = {}): Promise<OrgResponse> {
    return this.http.request<OrgResponse>({
      method: "GET",
      path: "/v1/org",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }

  /** GET /v1/project — the project scoped by this API key. */
  project(opts: { signal?: AbortSignal } = {}): Promise<ProjectResponse> {
    return this.http.request<ProjectResponse>({
      method: "GET",
      path: "/v1/project",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }
}

import type { HttpClient } from "../http.js";
import type { MeResponse, OrgResponse, ProjectResponse } from "../types.js";

export class IdentityResource {
  constructor(private readonly http: HttpClient) {}

  me(opts: { signal?: AbortSignal } = {}): Promise<MeResponse> {
    return this.http.request<MeResponse>({
      method: "GET",
      path: "/v1/me",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }

  org(opts: { signal?: AbortSignal } = {}): Promise<OrgResponse> {
    return this.http.request<OrgResponse>({
      method: "GET",
      path: "/v1/org",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }

  project(opts: { signal?: AbortSignal } = {}): Promise<ProjectResponse> {
    return this.http.request<ProjectResponse>({
      method: "GET",
      path: "/v1/project",
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }
}

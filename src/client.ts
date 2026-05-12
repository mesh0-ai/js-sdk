import { resolveConfig, type Mesh0ConfigInput } from "./config.js";
import { HttpClient } from "./http.js";
import { EventsResource } from "./resources/events.js";
import { QueryResource } from "./resources/query.js";
import { IdentityResource } from "./resources/identity.js";
import {
  openFirehose,
  type FirehoseCallbacks,
  type FirehoseHandle,
  type FirehoseOpts,
} from "./stream/firehose.js";

export class Mesh0 {
  readonly events: EventsResource;
  readonly query: QueryResource;
  readonly identity: IdentityResource;
  private readonly http: HttpClient;

  constructor(input: Mesh0ConfigInput = {}) {
    const cfg = resolveConfig(input);
    this.http = new HttpClient(cfg);
    this.events = new EventsResource(this.http);
    this.query = new QueryResource(this.http);
    this.identity = new IdentityResource(this.http);
  }

  /** Convenience static — same as `new Mesh0({ apiKey })`. */
  static create(apiKey: string): Mesh0 {
    return new Mesh0({ apiKey });
  }

  /** Open the org-wide firehose at GET /v1/firehose. `opts.transport`
   *  picks between `"ws"` (default) and `"sse"`; both share the same
   *  auth, query params, payload shape, and handle contract. */
  firehose(opts?: FirehoseOpts, callbacks?: FirehoseCallbacks): FirehoseHandle {
    return openFirehose(this.http, opts, callbacks);
  }
}

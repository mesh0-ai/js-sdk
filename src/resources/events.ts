import type { HttpClient } from "../http.js";
import type {
  EventInput,
  EventsListResponse,
  EventRow,
} from "../types.js";
import { ValidationError } from "../errors.js";

export const MAX_EVENTS_PER_REQUEST = 5000;

export interface EventsListOpts {
  limit?: number;
  cursor?: string;
  /** Inclusive lower bound. ISO 8601 string or epoch-ms number. */
  from?: string | number;
  /** Exclusive upper bound. ISO 8601 string or epoch-ms number. */
  to?: string | number;
  signal?: AbortSignal;
}

export class EventsResource {
  constructor(private readonly http: HttpClient) {}

  /** Send a single event via POST /v1/events. */
  async send(event: EventInput, opts: { signal?: AbortSignal } = {}): Promise<void> {
    await this.sendMany([event], opts);
  }

  /**
   * Send up to {@link MAX_EVENTS_PER_REQUEST} events per HTTP call. Larger
   * batches are auto-split into sequential POSTs.
   */
  async sendMany(events: EventInput[], opts: { signal?: AbortSignal } = {}): Promise<void> {
    if (!Array.isArray(events) || events.length === 0) {
      throw new ValidationError("mesh0: events must be a non-empty array", "events");
    }
    for (let i = 0; i < events.length; i += MAX_EVENTS_PER_REQUEST) {
      const chunk = events.slice(i, i + MAX_EVENTS_PER_REQUEST);
      await this.http.request<void>({
        method: "POST",
        path: "/v1/events",
        body: { events: chunk },
        ...(opts.signal ? { signal: opts.signal } : {}),
      });
    }
  }

  /** List events with cursor pagination (GET /v1/events). */
  async list(opts: EventsListOpts = {}): Promise<EventsListResponse> {
    const query: Record<string, string | number | undefined> = {};
    if (opts.limit !== undefined) query.limit = opts.limit;
    if (opts.cursor !== undefined) query.cursor = opts.cursor;
    if (opts.from !== undefined) query.from = String(opts.from);
    if (opts.to !== undefined) query.to = String(opts.to);
    return this.http.request<EventsListResponse>({
      method: "GET",
      path: "/v1/events",
      query,
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }

  /** Stream every event by transparently following cursors. */
  async *iterate(opts: Omit<EventsListOpts, "cursor"> = {}): AsyncGenerator<EventRow> {
    let cursor: string | undefined;
    for (;;) {
      const page: EventsListResponse = await this.list({ ...opts, cursor });
      for (const row of page.events) yield row;
      if (!page.hasMore || !page.nextCursor) return;
      cursor = page.nextCursor;
    }
  }

  /** Fetch one trace by id (GET /v1/traces/:traceId). */
  async trace(traceId: string, opts: { signal?: AbortSignal } = {}): Promise<{ spans: Record<string, unknown>[] }> {
    if (!traceId) throw new ValidationError("mesh0: traceId is required", "traceId");
    return this.http.request<{ spans: Record<string, unknown>[] }>({
      method: "GET",
      path: `/v1/traces/${encodeURIComponent(traceId)}`,
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
  }
}

export type { EventRow };

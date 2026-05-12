// Wire-format types for the mesh0 public HTTP and stream APIs. Field
// names are snake_case where the server returns snake_case (event rows,
// trace spans) and camelCase where the server uses camelCase (config,
// pagination, org/project). The shapes mirror the JSON contract — they
// are not translated, so callers can pass server responses through
// unchanged.

/** One event as accepted by POST /v1/events. */
export interface EventInput {
  /** Required: ISO 8601 string or epoch-ms number. */
  timestamp: string | number;
  /** Optional client-supplied event id (UUID). Server generates if omitted. */
  event_id?: string;
  /** Optional trace id (≤256 chars). Server synthesizes 32-hex if missing. */
  trace_id?: string;
  /** Optional span id (≤256 chars). Server synthesizes 16-hex if missing. */
  span_id?: string;
  /** Empty for root events. */
  parent_span_id?: string;
  /** Queryable/promotable open bin. */
  attributes?: Record<string, unknown>;
  /** Opaque slow bin — not TQL-queryable. */
  data?: Record<string, unknown>;
}

/** An event row as returned by GET /v1/events and /v1/events/stream. */
export interface EventRow {
  event_id: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  timestamp: string;
  project_id: string;
  attributes: Record<string, unknown> | null;
  /** Excluded from /v1/events/stream payloads; present on /v1/events list rows. */
  data?: Record<string, unknown> | null;
}

export interface EventsListResponse {
  events: EventRow[];
  nextCursor: string | null;
  hasMore: boolean;
}

export interface TraceSpan {
  event_id: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  timestamp: string;
  attributes: string | null;
  attributes_truncated: boolean;
  data: string;
  data_truncated: boolean;
}

export interface RangeSpec {
  /** ISO 8601 timestamp, 'now', or 'now-<n><s|m|h|d|w>'. */
  from: string;
  to: string;
}

export interface MetricRequest {
  fn: string;
  field?: string;
  alias?: string;
}

export interface OrderByRequest {
  key: string;
  dir?: "asc" | "desc";
}

/** POST /v1/query body. `projectId` is injected server-side from the API key. */
export interface QueryRequest {
  filter?: string;
  range: RangeSpec;
  groupBy?: string[];
  metrics: MetricRequest[];
  bucket?: string;
  orderBy?: OrderByRequest[];
  limit?: number;
}

export interface QueryResponse {
  columns: string[];
  rows: unknown[][];
}

export interface MeResponse {
  user: {
    id: string;
    email: string;
    fullName: string;
    emailVerifiedAt: string | null;
    createdAt: string;
  } | null;
}

export interface OrgResponse {
  id: string;
  slug: string;
  name: string;
  status: string;
  createdAt: string;
}

export interface ProjectResponse {
  id: string;
  name: string;
}

/** Server→client message on the WS /v1/firehose stream. Discriminated on `type`. */
export type FirehoseMessage =
  | { type: "hello"; topic: string; since: string }
  | { type: "event"; partition: number; offset: string; row: EventRow }
  | { type: "ping"; ts: number };

/** Server→client message on the SSE /v1/events/stream channel. Discriminated on
 *  `event` to match the SSE wire format's `event:` field. */
export type StreamMessage =
  | { event: "hello"; data: Record<string, unknown> }
  | { event: "ping"; data: number }
  | { event: "event"; data: EventRow }
  | { event: "resync"; data: Record<string, unknown> }
  | { event: "error"; data: { reason: string; errorId?: string } };

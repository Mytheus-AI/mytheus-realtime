/**
 * `core-api` is the boundary for everything this gateway does not itself own:
 *
 * - The participant roster for a meeting (and, transitively, which `org_id` a `meeting_id`
 *   belongs to) — used for the deny-by-default authz check on room join.
 * - `chat_messages` — owned entirely by `core-api` (see `03-container-architecture.md` §5,
 *   `04-service-catalog.md` §3.5). This gateway relays chat over the socket for latency, but the
 *   durable write is always this HTTP call; the gateway must never write that table directly, or
 *   the one-writer-per-store invariant breaks.
 *
 * `POST /chat` lands in Task 5 of this band. If it does not exist yet at any given moment, that
 * is fine: this client is testable against a fake (see test/helpers/fakeCoreApi.ts) so the call
 * path is fully exercised without a live `core-api`.
 *
 * **Identity resolution.** This gateway has no database, so its entire identity model going in
 * is the Clerk JWT: `org_id` is Clerk's opaque org claim (e.g. `"org_2abc..."`), `userId` is the
 * JWT `sub`. Neither is a UUID. Every other part of the system — the `presence-events` Avro
 * schema, every other producer, every Postgres row — uses Mytheus's own internal UUIDs for
 * `org_id`/`user_id` (`organizations.id`/`users.id`, resolved server-side from
 * `organizations.clerk_org_id`/`users.clerk_user_id`). This gateway once put the raw Clerk claims
 * straight into the `presence-events` envelope; the Avro schema types both fields as UUID, so
 * every message was poison and `core-api`'s consumer routed 100% of them to
 * `presence-events.dlq` (see `mytheus-backend`'s task-5 report and
 * `tests/test_presence_events_consumer.py::test_a_non_uuid_org_id_header_is_a_poison_message`).
 *
 * `core-api`'s `GET /meetings/:meetingId/participants/:clerkUserId` (this is `isParticipant`
 * below) already resolves the caller's Clerk id to `users.id` for its own RLS-scoped query, and
 * — as of `mytheus-backend` commit `ca9a8db` (`ParticipantCheck.org_id`, added specifically for
 * this fix) — also returns the resolved `organizations.id` alongside it. `isParticipant` reads
 * both out of the response body; `internalOrgId` is non-null on every real `allowed: true` result
 * today. Verified end to end against a real running `core-api` process (real Postgres, real
 * `check_participant` route, real RS256 JWT verification via `PyJWKClient` — no dependency
 * overrides, no fake on the backend side), not only against this repo's own fake.
 *
 * `internalOrgId` stays typed `string | null` rather than `string`, and
 * `presenceEventsProducer.produce()` still validates both ids are UUID-shaped before ever
 * producing (see that module) — kept as defensive design against a future regression on the
 * `core-api` side (a redeploy that drops the field, a different route reusing this client without
 * it), not because the gap is still open. A record built from an unresolved Clerk id was never
 * structurally valid input for the `presence-events` schema in the first place, so skip-and-warn
 * — never producing it at all — is the correct contract regardless of whether the gap is
 * currently open or closed; it just now fires only on an actual regression instead of on every
 * real join.
 *
 * **No `X-Org-Id` header.** An earlier version of both calls below sent one, on the theory that
 * it identified the tenant to `core-api`. Verified (by grepping every header read across
 * `core-api` and its shared libs) that nothing on that side ever reads it — tenancy there is
 * resolved solely from the caller's own re-verified bearer token, exactly like `org_id` in
 * `isParticipant`'s docstring. Sending an unread header was not a security problem (an untrusted
 * header nobody trusts is harmless), but it was a trap: it looks like it carries tenancy while
 * being silently discarded, inviting a future "optimization" that starts trusting it, or a
 * different route being built by copying this client's pattern into a place that isn't as
 * careful. Removed rather than kept-and-relabeled, since nothing here reads it back either.
 */

export interface IsParticipantParams {
  orgId: string;
  meetingId: string;
  userId: string;
  bearerToken: string;
}

export interface PostChatParams {
  meetingId: string;
  userId: string;
  content: string;
  clientMsgId: string;
  bearerToken: string;
}

export interface CoreReadParams {
  meetingId: string;
  bearerToken: string;
}

/** core-api's `CandidateState` (`GET /api/v1/meetings/:id/candidates/:candidate_id`). */
export interface CandidateState {
  id: string;
  kind: "decision" | "action";
  meeting_id: string;
  status: string;
  content: string;
  rationale?: string | null;
  assignee_id?: string | null;
  due_date?: string | null;
  confirmed_by?: string | null;
}

/** The incoming resume point of a meeting (`GET /api/v1/meetings/:id/resume-point`), as far as the
 * gateway needs it to verify a `resume-point` announcement. */
export interface IncomingResumePoint {
  id: string;
  status: string;
}

/** The fields of core-api's `MeetingDetail` this gateway reads to verify a host action. */
export interface MeetingState {
  id: string;
  status: string;
  host_user_id: string | null;
  created_by: string | null;
  agenda: Array<Record<string, unknown> & { id: string; status: string; status_source?: string }>;
  /** The host opened the 60-second wrap-up (polish program, close-out ritual); null when not. */
  wrap_up_started_at: string | null;
  wrap_up_by: string | null;
}

export interface ParticipantCheckAllowed {
  allowed: true;
  /** `users.id` — core-api's internal UUID for the caller, resolved server-side from the Clerk
   * `clerk_user_id` in the request path. Always present on an `allowed: true` result today. */
  internalUserId: string;
  /** `organizations.id` — core-api's internal UUID for the tenant, returned in the response body
   * as of `mytheus-backend` commit `ca9a8db` (see this file's header comment) and non-null on
   * every real `allowed: true` result today. Stays typed `string | null`, and any caller that
   * receives `null`, as defensive design against a future regression — never substitute the
   * JWT's Clerk `orgId` for it if it ever is; that substitution is exactly the bug this type
   * exists to keep from recurring. */
  internalOrgId: string | null;
}

export interface ParticipantCheckDenied {
  allowed: false;
}

export type ParticipantCheckResult = ParticipantCheckAllowed | ParticipantCheckDenied;

export interface CoreApiClient {
  /**
   * Deny-by-default participant check. Resolves `{ allowed: false }` on any non-200, a malformed
   * body, or a network failure (never throws) so a flaky `core-api` fails a join closed rather
   * than open. On `{ allowed: true, ... }`, also carries core-api's resolved internal identity —
   * see `ParticipantCheckAllowed`'s doc comments and this file's header "Identity resolution" note.
   */
  isParticipant(params: IsParticipantParams): Promise<ParticipantCheckResult>;
  /** Durable chat write. Throws on a non-2xx response; the caller logs and moves on (the socket
   * emit already happened — this call only affects durability, not the live relay). */
  postChat(params: PostChatParams): Promise<{ id: string; createdAt: string } | null>;
  /**
   * One candidate of a meeting, read WITH THE ACTOR'S OWN TOKEN, so the gateway only ever tells
   * the room what core-api actually stored (the shared candidate ledger). Null on any non-200,
   * a malformed body or a network failure: an unverifiable claim is never broadcast.
   */
  getCandidate(params: CoreReadParams & { candidateId: string }): Promise<CandidateState | null>;
  /** The meeting (status, host, creator, agenda, wrap-up), read with the actor's token. Null on
   * failure. */
  getMeeting(params: CoreReadParams): Promise<MeetingState | null>;
  /** The thread this meeting picks up ("pick up where we stopped"), read with the actor's
   * token: `{ incoming }` (null when there is none), or null on any failure. */
  getResumePoint(params: CoreReadParams): Promise<{ incoming: IncomingResumePoint | null } | null>;
}

export interface CoreApiClientOptions {
  baseUrl: string;
  fetchImpl?: typeof fetch;
  logger?: { warn: (obj: unknown, msg?: string) => void };
}

export function createCoreApiClient(options: CoreApiClientOptions): CoreApiClient {
  const fetchFn = options.fetchImpl ?? fetch;

  return {
    async isParticipant({ orgId, meetingId, userId, bearerToken }) {
      try {
        const res = await fetchFn(
          `${options.baseUrl}/api/v1/meetings/${encodeURIComponent(meetingId)}/participants/${encodeURIComponent(userId)}`,
          {
            headers: {
              Authorization: `Bearer ${bearerToken}`,
            },
          },
        );
        if (res.status !== 200) {
          return { allowed: false };
        }
        const body = (await res.json()) as { user_id?: unknown; org_id?: unknown };
        if (typeof body.user_id !== "string" || body.user_id.length === 0) {
          options.logger?.warn(
            { orgId, meetingId, userId },
            "core-api participant check returned 200 with no user_id in the body; denying by default",
          );
          return { allowed: false };
        }
        return {
          allowed: true,
          internalUserId: body.user_id,
          internalOrgId:
            typeof body.org_id === "string" && body.org_id.length > 0 ? body.org_id : null,
        };
      } catch (err) {
        options.logger?.warn(
          { err, orgId, meetingId, userId },
          "core-api participant check failed; denying by default",
        );
        return { allowed: false };
      }
    },

    async getCandidate({ meetingId, candidateId, bearerToken }) {
      try {
        const res = await fetchFn(
          `${options.baseUrl}/api/v1/meetings/${encodeURIComponent(meetingId)}/candidates/${encodeURIComponent(candidateId)}`,
          { headers: { Authorization: `Bearer ${bearerToken}` } },
        );
        if (res.status !== 200) return null;
        const body = (await res.json()) as Partial<CandidateState>;
        if (
          typeof body.id !== "string" ||
          (body.kind !== "decision" && body.kind !== "action") ||
          typeof body.status !== "string" ||
          typeof body.content !== "string" ||
          body.meeting_id !== meetingId
        ) {
          return null;
        }
        return body as CandidateState;
      } catch (err) {
        options.logger?.warn({ err, meetingId, candidateId }, "core-api candidate read failed");
        return null;
      }
    },

    async getResumePoint({ meetingId, bearerToken }) {
      try {
        const res = await fetchFn(
          `${options.baseUrl}/api/v1/meetings/${encodeURIComponent(meetingId)}/resume-point`,
          { headers: { Authorization: `Bearer ${bearerToken}` } },
        );
        if (res.status !== 200) return null;
        const body = (await res.json()) as { incoming?: Partial<IncomingResumePoint> | null };
        const inc = body?.incoming;
        if (inc === null || inc === undefined) return { incoming: null };
        if (typeof inc.id !== "string" || typeof inc.status !== "string") return null;
        return { incoming: { id: inc.id, status: inc.status } };
      } catch (err) {
        options.logger?.warn({ err, meetingId }, "core-api resume point read failed");
        return null;
      }
    },

    async getMeeting({ meetingId, bearerToken }) {
      try {
        const res = await fetchFn(
          `${options.baseUrl}/api/v1/meetings/${encodeURIComponent(meetingId)}`,
          { headers: { Authorization: `Bearer ${bearerToken}` } },
        );
        if (res.status !== 200) return null;
        const body = (await res.json()) as Partial<MeetingState>;
        if (body.id !== meetingId || typeof body.status !== "string") return null;
        return {
          id: body.id,
          status: body.status,
          host_user_id: typeof body.host_user_id === "string" ? body.host_user_id : null,
          created_by: typeof body.created_by === "string" ? body.created_by : null,
          agenda: Array.isArray(body.agenda) ? body.agenda : [],
          wrap_up_started_at:
            typeof body.wrap_up_started_at === "string" ? body.wrap_up_started_at : null,
          wrap_up_by: typeof body.wrap_up_by === "string" ? body.wrap_up_by : null,
        };
      } catch (err) {
        options.logger?.warn({ err, meetingId }, "core-api meeting read failed");
        return null;
      }
    },

    async postChat({ meetingId, userId, content, clientMsgId, bearerToken }) {
      const res = await fetchFn(`${options.baseUrl}/api/v1/chat`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${bearerToken}`,
          "Idempotency-Key": clientMsgId,
        },
        body: JSON.stringify({ meeting_id: meetingId, content, user_id: userId }),
      });
      if (!res.ok) {
        throw new Error(`core-api POST /api/v1/chat failed: ${res.status}`);
      }
      // The canonical row (`ChatMessage`): its id and time let every client dedupe the live relay
      // against the history it re-reads on reconnect. A body without them is still a success.
      try {
        const row = (await res.json()) as { id?: unknown; created_at?: unknown };
        return typeof row.id === "string" && typeof row.created_at === "string"
          ? { id: row.id, createdAt: row.created_at }
          : null;
      } catch {
        return null;
      }
    },
  };
}

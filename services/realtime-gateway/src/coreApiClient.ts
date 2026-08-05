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
 * **CONTRACT GAP, being closed incrementally (see `isParticipant`'s return type).** This gateway
 * has no database, so its entire identity model is the Clerk JWT: `org_id` is Clerk's opaque org
 * claim (e.g. `"org_2abc..."`), `userId` is the JWT `sub`. Neither is a UUID. Every other part of
 * the system — the `presence-events` Avro schema, every other producer, every Postgres row —
 * uses Mytheus's own internal UUIDs for `org_id`/`user_id` (`organizations.id`/`users.id`,
 * resolved server-side from `organizations.clerk_org_id`/`users.clerk_user_id`). This gateway
 * used to put the raw Clerk claims straight into the `presence-events` envelope; the Avro schema
 * types both fields as UUID, so every message was poison and `core-api`'s consumer routed 100% of
 * them to `presence-events.dlq` (see `mytheus-backend`'s task-5 report and
 * `tests/test_presence_events_consumer.py::test_a_non_uuid_org_id_header_is_a_poison_message`).
 *
 * `core-api`'s `GET /meetings/:meetingId/participants/:clerkUserId` (this is `isParticipant`
 * below) already resolves the caller's Clerk id to `users.id` for its own RLS-scoped query and
 * returns it in the response body — that half of the fix needs no upstream change, just actually
 * reading the body instead of only the status code. The `org_id` half is currently unresolvable:
 * that same request also resolves `org_id` internally (via `tenant_context`) but does not return
 * it. `isParticipant`'s `internalOrgId` is therefore `null` until `core-api` adds an `org_id`
 * field to that response (a one-field addition — the value is already computed for that request,
 * just not serialized back out). Until then, `presenceEventsProducer.produce()` validates its
 * inputs are UUID-shaped and skips (logs a warning, does not throw, does not produce) rather than
 * ever repeating the bug by falling back to a Clerk id — see that module for the enforcement
 * point. This keeps the live join/room/chat path fully working either way; only the
 * `presence-events` analytics record is affected, and it self-heals with no further code change
 * on this side the moment `core-api` ships the field.
 */

export interface IsParticipantParams {
  orgId: string;
  meetingId: string;
  userId: string;
  bearerToken: string;
}

export interface PostChatParams {
  orgId: string;
  meetingId: string;
  userId: string;
  content: string;
  clientMsgId: string;
  bearerToken: string;
}

export interface ParticipantCheckAllowed {
  allowed: true;
  /** `users.id` — core-api's internal UUID for the caller, resolved server-side from the Clerk
   * `clerk_user_id` in the request path. Always present on an `allowed: true` result today. */
  internalUserId: string;
  /** `organizations.id` — core-api's internal UUID for the tenant. `null` until core-api's
   * response includes an `org_id` field (see this file's header "CONTRACT GAP" note); treat
   * `null` as "not resolvable yet", never substitute the JWT's Clerk `orgId` for it — that
   * substitution is exactly the bug this type exists to keep from recurring. */
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
   * see `ParticipantCheckAllowed`'s doc comments and this file's header "CONTRACT GAP" note.
   */
  isParticipant(params: IsParticipantParams): Promise<ParticipantCheckResult>;
  /** Durable chat write. Throws on a non-2xx response; the caller logs and moves on (the socket
   * emit already happened — this call only affects durability, not the live relay). */
  postChat(params: PostChatParams): Promise<void>;
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
              "X-Org-Id": orgId,
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
          internalOrgId: typeof body.org_id === "string" && body.org_id.length > 0 ? body.org_id : null,
        };
      } catch (err) {
        options.logger?.warn(
          { err, orgId, meetingId, userId },
          "core-api participant check failed; denying by default",
        );
        return { allowed: false };
      }
    },

    async postChat({ orgId, meetingId, userId, content, clientMsgId, bearerToken }) {
      const res = await fetchFn(`${options.baseUrl}/api/v1/chat`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${bearerToken}`,
          "X-Org-Id": orgId,
          "Idempotency-Key": clientMsgId,
        },
        body: JSON.stringify({ meeting_id: meetingId, content, user_id: userId }),
      });
      if (!res.ok) {
        throw new Error(`core-api POST /api/v1/chat failed: ${res.status}`);
      }
    },
  };
}

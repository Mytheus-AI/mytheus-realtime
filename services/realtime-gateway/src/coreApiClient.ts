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

export interface CoreApiClient {
  /**
   * Deny-by-default participant check. Resolves `true` only on an explicit 200 from `core-api`;
   * any 4xx/5xx or network failure resolves `false` (never throws) so a flaky `core-api` fails a
   * join closed rather than open.
   */
  isParticipant(params: IsParticipantParams): Promise<boolean>;
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
        return res.status === 200;
      } catch (err) {
        options.logger?.warn(
          { err, orgId, meetingId, userId },
          "core-api participant check failed; denying by default",
        );
        return false;
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

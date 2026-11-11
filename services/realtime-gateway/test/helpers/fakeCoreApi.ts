/**
 * A fake `core-api` HTTP server standing in for the real one (its `POST /chat` and meeting
 * participant endpoints land in Task 5 of this band). This is a real HTTP server on a real
 * loopback port — `coreApiClient.ts`'s `fetch` calls hit it for real, so the request shape
 * (headers, body, Idempotency-Key) is genuinely exercised, not mocked at the client boundary.
 *
 * The participant-check response mirrors core-api's real contract (`ParticipantCheck`, per
 * `mytheus-backend`'s `services/core_api/schemas.py`, commit `ca9a8db`): a JSON body with
 * `user_id` (core-api's resolved internal UUID for the caller) and `org_id` (the resolved
 * internal UUID for the tenant) — both non-null on a real `allowed` response today.
 * `allowParticipant`'s `resolved` parameter lets a test model either state: omit it to simulate
 * "both fields present" (the default, so existing call sites get realistic UUID-shaped resolved
 * ids for free — this is what a real call gets today), or pass `{ orgId: null }` as a regression
 * guard for the gateway's graceful-degradation path in case a future response is ever missing it
 * again. When `resolved.orgId` is omitted, the resolved org UUID is DETERMINISTIC per Clerk
 * `orgId` claim (cached across every `allowParticipant` call on this fake instance), matching
 * real core-api's actual behavior: `org_id` is resolved from the caller's own bearer token, so
 * two different users of the same real org always get the same resolved UUID back. Two
 * `allowParticipant` calls with the same `orgId` but no explicit `resolved.orgId` therefore
 * land in the same tenant, as they would for real — see `resolvedOrgFor` below.
 *
 * The chat endpoint simulates core-api's expected dedupe-on-Idempotency-Key behavior (recording
 * only the first call per key) so tests can assert the gateway wires the key through correctly,
 * without asserting anything about core-api's own persistence, which this repo does not own.
 *
 * Tenancy source: real core-api resolves `org_id` solely from the caller's own re-verified
 * bearer token — it does not read (and `coreApiClient.ts` no longer sends) an `X-Org-Id` header.
 * This fake matches that: it decodes the `org_id` claim out of the JWT in the `Authorization`
 * header (no signature verification needed here — this is a test double, not a security
 * boundary) rather than trusting any header.
 */
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

/** Decodes (without verifying) the `org_id` claim from a `Bearer <jwt>` Authorization header —
 * mirrors what real core-api's `tenant_context()` does with the verified token, minus the
 * verification this fake doesn't need. */
function clerkOrgIdFromAuthHeader(authHeader: string | undefined): string | null {
  const token = authHeader?.startsWith("Bearer ") ? authHeader.slice("Bearer ".length) : undefined;
  const payloadSegment = token?.split(".")[1];
  if (!payloadSegment) return null;
  try {
    const json = Buffer.from(payloadSegment, "base64url").toString("utf8");
    const claims = JSON.parse(json) as { org_id?: unknown };
    return typeof claims.org_id === "string" ? claims.org_id : null;
  } catch {
    return null;
  }
}

export interface RecordedChatCall {
  orgId: string | null;
  meetingId: string;
  userId: string | null;
  content: string;
  idempotencyKey: string | null;
}

export interface ResolvedIdentity {
  /** core-api's internal `users.id`. Defaults to a fresh UUID if omitted. */
  userId?: string;
  /** core-api's internal `organizations.id`. Defaults to a fresh UUID if omitted; pass `null`
   * explicitly to simulate core-api's response not including this field yet. */
  orgId?: string | null;
}

/** A candidate row the fake serves, keyed by `{meetingId}:{candidateId}`. */
export interface FakeCandidate {
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

/** The meeting fields the gateway reads. */
export interface FakeMeeting {
  id: string;
  status: string;
  host_user_id: string | null;
  created_by: string | null;
  agenda: Array<Record<string, unknown> & { id: string; status: string; status_source?: string }>;
  wrap_up_started_at?: string | null;
  wrap_up_by?: string | null;
}

export interface FakeCoreApi {
  baseUrl: string;
  /**
   * Registers `(orgId, meetingId, userId)` — the Clerk-native identity the gateway sends on the
   * wire (org_id claim inside the JWT, Clerk user id in the path) — as an allowed participant.
   * `resolved` controls what the 200 response body contains; see this file's header comment.
   */
  allowParticipant(
    orgId: string,
    meetingId: string,
    userId: string,
    resolved?: ResolvedIdentity,
  ): void;
  chatCalls: RecordedChatCall[];
  /** Serve this candidate row from `GET /api/v1/meetings/:m/candidates/:id`. */
  setCandidate(row: FakeCandidate): void;
  /** Serve this meeting from `GET /api/v1/meetings/:id`. */
  setMeeting(row: FakeMeeting): void;
  /** Serve `{ incoming }` from `GET /api/v1/meetings/:id/resume-point` (unset = 404). */
  setResumePoint(meetingId: string, incoming: { id: string; status: string } | null): void;
  /** Make `POST /api/v1/chat` fail with this status (null = succeed). */
  failChatWith: number | null;
  close(): Promise<void>;
}

export async function startFakeCoreApi(): Promise<FakeCoreApi> {
  const allowed = new Map<string, Required<ResolvedIdentity>>();
  const chatCalls: RecordedChatCall[] = [];
  const seenIdempotencyKeys = new Set<string>();
  // Fix round 2 fidelity gap: real core-api resolves `org_id` from `ctx.org_id` — the CALLER's
  // own re-verified bearer token — so two different users of the SAME real Clerk org always get
  // the SAME resolved internal org UUID back, deterministically, on every call. This fake used
  // to mint a FRESH random UUID per `allowParticipant` call whenever `resolved.orgId` was
  // omitted, so two participants of what a test intended to be "the same org" silently resolved
  // to two DIFFERENT fake org UUIDs — invisible as long as nothing depended on room-level org
  // consistency, but exactly the kind of test-double/reality drift that let the presence-events
  // Clerk-id bug (Task 6) go unnoticed for as long as it did. Caching by the raw Clerk `orgId`
  // claim (never reset between `allowParticipant` calls in one fake instance) matches real
  // core-api's actual determinism.
  const resolvedOrgIdByClerkOrg = new Map<string, string>();
  function resolvedOrgFor(clerkOrgId: string): string {
    let resolved = resolvedOrgIdByClerkOrg.get(clerkOrgId);
    if (!resolved) {
      resolved = randomUUID();
      resolvedOrgIdByClerkOrg.set(clerkOrgId, resolved);
    }
    return resolved;
  }

  const candidates = new Map<string, FakeCandidate>();
  const meetings = new Map<string, FakeMeeting>();
  const resumePoints = new Map<string, { id: string; status: string } | null>();
  const state = { failChatWith: null as number | null };

  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? "", "http://localhost");

    if (req.method === "GET") {
      const cand = /^\/api\/v1\/meetings\/([^/]+)\/candidates\/([^/]+)$/.exec(url.pathname);
      if (cand) {
        const row = candidates.get(`${cand[1]}:${cand[2]}`);
        res.writeHead(row ? 200 : 404, { "content-type": "application/json" });
        res.end(row ? JSON.stringify(row) : "{}");
        return;
      }
      const resume = /^\/api\/v1\/meetings\/([^/]+)\/resume-point$/.exec(url.pathname);
      if (resume) {
        const known = resumePoints.has(resume[1]);
        res.writeHead(known ? 200 : 404, { "content-type": "application/json" });
        res.end(
          known ? JSON.stringify({ incoming: resumePoints.get(resume[1]), saved: null }) : "{}",
        );
        return;
      }
      const meeting = /^\/api\/v1\/meetings\/([^/]+)$/.exec(url.pathname);
      if (meeting) {
        const row = meetings.get(meeting[1]);
        res.writeHead(row ? 200 : 404, { "content-type": "application/json" });
        res.end(row ? JSON.stringify(row) : "{}");
        return;
      }
      const match = /^\/api\/v1\/meetings\/([^/]+)\/participants\/([^/]+)$/.exec(url.pathname);
      if (match) {
        const [, meetingId, userId] = match;
        const orgId = clerkOrgIdFromAuthHeader(req.headers.authorization);
        const resolved = allowed.get(`${String(orgId)}:${meetingId}:${userId}`);
        if (!resolved) {
          res.writeHead(403);
          res.end();
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            user_id: resolved.userId,
            ...(resolved.orgId !== null ? { org_id: resolved.orgId } : {}),
          }),
        );
        return;
      }
    }

    if (req.method === "POST" && url.pathname === "/api/v1/chat") {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString("utf8");
      });
      req.on("end", () => {
        if (state.failChatWith !== null) {
          res.writeHead(state.failChatWith);
          res.end();
          return;
        }
        const idempotencyKey = (req.headers["idempotency-key"] as string | undefined) ?? null;
        if (!idempotencyKey || !seenIdempotencyKeys.has(idempotencyKey)) {
          if (idempotencyKey) seenIdempotencyKeys.add(idempotencyKey);
          const parsed = JSON.parse(body || "{}") as {
            meeting_id: string;
            content: string;
            user_id?: string;
          };
          chatCalls.push({
            orgId: clerkOrgIdFromAuthHeader(req.headers.authorization),
            meetingId: parsed.meeting_id,
            userId: parsed.user_id ?? null,
            content: parsed.content,
            idempotencyKey,
          });
        }
        res.writeHead(201, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: randomUUID(), created_at: new Date().toISOString() }));
      });
      return;
    }

    res.writeHead(404);
    res.end();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    allowParticipant(orgId, meetingId, userId, resolved) {
      allowed.set(`${orgId}:${meetingId}:${userId}`, {
        userId: resolved?.userId ?? randomUUID(),
        orgId: resolved?.orgId === undefined ? resolvedOrgFor(orgId) : resolved.orgId,
      });
    },
    chatCalls,
    setCandidate(row) {
      candidates.set(`${row.meeting_id}:${row.id}`, row);
    },
    setMeeting(row) {
      meetings.set(row.id, row);
    },
    setResumePoint(meetingId, incoming) {
      resumePoints.set(meetingId, incoming);
    },
    get failChatWith() {
      return state.failChatWith;
    },
    set failChatWith(v: number | null) {
      state.failChatWith = v;
    },
    async close() {
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve())),
      );
    },
  };
}

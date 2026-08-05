/**
 * A fake `core-api` HTTP server standing in for the real one (its `POST /chat` and meeting
 * participant endpoints land in Task 5 of this band). This is a real HTTP server on a real
 * loopback port — `coreApiClient.ts`'s `fetch` calls hit it for real, so the request shape
 * (headers, body, Idempotency-Key) is genuinely exercised, not mocked at the client boundary.
 *
 * The chat endpoint simulates core-api's expected dedupe-on-Idempotency-Key behavior (recording
 * only the first call per key) so tests can assert the gateway wires the key through correctly,
 * without asserting anything about core-api's own persistence, which this repo does not own.
 */
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

export interface RecordedChatCall {
  orgId: string | null;
  meetingId: string;
  userId: string | null;
  content: string;
  idempotencyKey: string | null;
}

export interface FakeCoreApi {
  baseUrl: string;
  allowParticipant(orgId: string, meetingId: string, userId: string): void;
  chatCalls: RecordedChatCall[];
  close(): Promise<void>;
}

export async function startFakeCoreApi(): Promise<FakeCoreApi> {
  const allowed = new Set<string>();
  const chatCalls: RecordedChatCall[] = [];
  const seenIdempotencyKeys = new Set<string>();

  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? "", "http://localhost");

    if (req.method === "GET") {
      const match = /^\/api\/v1\/meetings\/([^/]+)\/participants\/([^/]+)$/.exec(url.pathname);
      if (match) {
        const [, meetingId, userId] = match;
        const orgId = req.headers["x-org-id"];
        res.writeHead(allowed.has(`${String(orgId)}:${meetingId}:${userId}`) ? 200 : 403);
        res.end();
        return;
      }
    }

    if (req.method === "POST" && url.pathname === "/api/v1/chat") {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString("utf8");
      });
      req.on("end", () => {
        const idempotencyKey = (req.headers["idempotency-key"] as string | undefined) ?? null;
        if (!idempotencyKey || !seenIdempotencyKeys.has(idempotencyKey)) {
          if (idempotencyKey) seenIdempotencyKeys.add(idempotencyKey);
          const parsed = JSON.parse(body || "{}") as { meeting_id: string; content: string; user_id?: string };
          chatCalls.push({
            orgId: (req.headers["x-org-id"] as string | undefined) ?? null,
            meetingId: parsed.meeting_id,
            userId: parsed.user_id ?? null,
            content: parsed.content,
            idempotencyKey,
          });
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "ok" }));
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
    allowParticipant(orgId, meetingId, userId) {
      allowed.add(`${orgId}:${meetingId}:${userId}`);
    },
    chatCalls,
    async close() {
      await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    },
  };
}

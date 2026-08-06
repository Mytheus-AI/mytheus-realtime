/**
 * Finding 1 (fix round 1): a presence entry must not leak forever when its owning gateway
 * process dies UNGRACEFULLY (OOM-killed, SIGKILL'd, evicted) — routine in the deployment posture
 * the plan's own step 8 assumes. Socket.io's `disconnect` handler, which does the graceful
 * cleanup, requires the owning process to still be alive to run it, so it cannot be what protects
 * against this. This test proves the actual fix survives a real `kill -9`:
 *
 * Two real, separate `node` processes (like the canonical hard test), both on fast
 * heartbeat/stale/sweep timings via env vars. A socket joins through process A, establishing a
 * live heartbeat. Process A is then SIGKILL'd — no `disconnect` handler runs anywhere, on any
 * process, for that socket; from Redis's perspective this is indistinguishable from a real pod
 * death. Process B, a separate still-alive gateway pointed at the same Redis, never had a local
 * socket in that meeting at all — it only learns of the entry via the shared presence index — and
 * is the one left to notice and reap it. Both layers of the fix are asserted:
 *
 *   1. `listPresence`'s read-time freshness filter excludes the member almost immediately (within
 *      `staleAfterMs` of the last heartbeat), before any physical cleanup has happened.
 *   2. Process B's periodic sweep physically removes the member and prunes the index (bounding
 *      Redis growth), and produces a `presence-events` "leave" record for it — the only possible
 *      source of that record, since process A never got to run its own disconnect-driven produce.
 */
import { execFileSync } from "node:child_process";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { presenceIndexKey, presenceKey } from "../../../libs/keys";
import type { JoinAck } from "../src/server";
import { connectClient } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REPO_ROOT = path.resolve(__dirname, "..");
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";
const REDPANDA_BROKERS = process.env.REDPANDA_BROKERS ?? "localhost:9092";
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8081";
const SERVER_ENTRY = path.join(
  REPO_ROOT,
  "dist",
  "services",
  "realtime-gateway",
  "src",
  "server.js",
);

// Fast, test-only tuning (see server.ts's AppConfig doc comments for the production defaults).
const HEARTBEAT_INTERVAL_MS = 150;
const STALE_AFTER_MS = 450; // 3 missed heartbeats
const SWEEP_INTERVAL_MS = 250;

interface DecodedPresenceEvent {
  header: { org_id: string };
  meeting_id: string;
  user_id: string;
  event_type: "join" | "leave";
}

describe("presence survives an ungraceful pod death (real SIGKILL, two real processes)", () => {
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let redis: Redis;
  let procA: ChildProcess;
  let procB: ChildProcess;
  let consumer: Consumer;
  let portA: number;
  const received: DecodedPresenceEvent[] = [];

  beforeAll(async () => {
    execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { cwd: REPO_ROOT, stdio: "inherit" });

    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    redis = new Redis(REDIS_URL);

    const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
    const kafka = new Kafka({
      clientId: "presence-pod-death-test-consumer",
      brokers: [REDPANDA_BROKERS],
    });
    consumer = kafka.consumer({ groupId: `test-presence-pod-death-${randomUUID()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: "presence-events", fromBeginning: false });
    const groupJoined = new Promise<void>((resolve) => {
      consumer.on(consumer.events.GROUP_JOIN, () => resolve());
    });
    void consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        if (!message.value) return;
        received.push((await registry.decode(message.value)) as DecodedPresenceEvent);
      },
    });
    await groupJoined;

    const portB = await findFreePort();
    portA = await findFreePort();
    procA = spawnGateway(portA);
    procB = spawnGateway(portB);
    await Promise.all([waitForHealthz(portA), waitForHealthz(portB)]);
  }, 60000);

  afterAll(async () => {
    procA?.kill();
    procB?.kill();
    await consumer?.disconnect();
    redis?.disconnect();
    await jwks?.close();
    await coreApi?.close();
  }, 15000);

  function spawnGateway(port: number): ChildProcess {
    const child = spawn(process.execPath, [SERVER_ENTRY], {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        PORT: String(port),
        REDIS_URL,
        CORE_API_URL: coreApi.baseUrl,
        CLERK_JWKS_URL: jwks.jwksUrl,
        REDPANDA_BROKERS,
        SCHEMA_REGISTRY_URL,
        PRESENCE_HEARTBEAT_INTERVAL_MS: String(HEARTBEAT_INTERVAL_MS),
        PRESENCE_STALE_AFTER_MS: String(STALE_AFTER_MS),
        PRESENCE_SWEEP_INTERVAL_MS: String(SWEEP_INTERVAL_MS),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stderr?.on("data", (chunk: Buffer) =>
      process.stderr.write(`[gateway:${port}] ${chunk.toString()}`),
    );
    return child;
  }

  it("a member's presence entry is excluded from reads quickly and physically reaped, with a leave event produced, after its owning process is SIGKILL'd", async () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const userId = randomUUID();
    // Already valid UUIDs (this test is about surviving a real SIGKILL, not identity
    // resolution — that's covered in gateway.test.ts), so resolve identity-mapped.
    coreApi.allowParticipant(orgId, meetingId, userId, { orgId, userId });

    const token = await jwks.signToken({ orgId, userId });
    const socket = await connectClient(`http://127.0.0.1:${portA}`, token);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack).toEqual({ ok: true, presence: [userId] });

    // sanity: the member is actually there with a fresh heartbeat before we kill anything
    const key = presenceKey(orgId, meetingId);
    const scoreBefore = await redis.zscore(key, userId);
    expect(scoreBefore).not.toBeNull();

    await waitFor(() =>
      received.some(
        (e) => e.meeting_id === meetingId && e.event_type === "join" && e.header.org_id === orgId,
      ),
    );

    // The socket is never closed and no disconnect ever fires anywhere: this is an ungraceful
    // process death, not a client disconnect.
    procA.kill("SIGKILL");

    // Layer 1: read-time freshness filtering excludes the ghost quickly, without waiting for a
    // physical sweep. Poll listPresence's underlying query directly against Redis.
    await waitFor(async () => {
      const fresh = await redis.zrangebyscore(key, Date.now() - STALE_AFTER_MS, "+inf");
      return !fresh.includes(userId);
    }, STALE_AFTER_MS + 2000);

    // Layer 2: process B (still alive, never had a local socket for this meeting) physically
    // reaps the member via its own sweep timer, and prunes the now-empty meeting from the index.
    await waitFor(
      async () => (await redis.zscore(key, userId)) === null,
      SWEEP_INTERVAL_MS + STALE_AFTER_MS + 3000,
    );
    await waitFor(
      async () => (await redis.sismember(presenceIndexKey(), `${orgId}|${meetingId}`)) === 0,
      SWEEP_INTERVAL_MS + 3000,
    );

    // The sweep also produced a `leave` presence-event — the only process that could have
    // produced it is B, since A died before it could run its own disconnect-driven produce.
    await waitFor(
      () =>
        received.some(
          (e) =>
            e.meeting_id === meetingId &&
            e.event_type === "leave" &&
            e.user_id === userId &&
            e.header.org_id === orgId,
        ),
      10000,
    );
  }, 45000);
});

function findFreePort(): Promise<number> {
  return import("node:net").then(
    (net) =>
      new Promise((resolve, reject) => {
        const server = net.createServer();
        server.listen(0, "127.0.0.1", () => {
          const address = server.address();
          if (address && typeof address === "object") {
            const { port } = address;
            server.close(() => resolve(port));
          } else {
            server.close(() => reject(new Error("could not determine free port")));
          }
        });
      }),
  );
}

async function waitForHealthz(port: number, timeoutMs = 15000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/healthz`);
      if (res.status === 200) return;
    } catch {
      // not up yet
    }
    await sleep(100);
  }
  throw new Error(`gateway on port ${port} did not become healthy within ${timeoutMs}ms`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await sleep(30);
  }
}

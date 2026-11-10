/**
 * ProactiveAgent alerts across two real gateway PROCESSES on one Redis, from the real broker
 * (P11 contract section 4): pod 1 consumes a real Avro `agent-results` alert record, queues it in
 * `nexus:{org_id}:queue:agent:{user_id}` and nudges the meeting channel; the recipient's socket
 * is on pod 2, which consumes nothing, so the alert can only arrive through the backbone (pod 2
 * drains the queue and delivers). The bob socket on pod 2 proves it stays owner-only.
 *
 * Pod 1 consumes under a group of its own, so it never joins (or, once killed, stalls) the shared
 * `realtime-gateway-agent-output` group agent-output-kafka.test.ts uses. The in-process hop is
 * measured precisely in agent-alert.test.ts; this file reports the end-to-end time from the
 * Kafka send to the socket.
 */
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, Partitioners, type Producer } from "kafkajs";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import type { JoinAck } from "../src/server";
import { connectClient } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REPO_ROOT = path.resolve(__dirname, "..");
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const BROKERS = process.env.REDPANDA_BROKERS ?? "localhost:9092";
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8085";
const SERVER_ENTRY = path.join(
  REPO_ROOT,
  "dist",
  "services",
  "realtime-gateway",
  "src",
  "server.js",
);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("ProactiveAgent alert: real broker, pod 1 consumes, socket on pod 2", () => {
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let pod1: ChildProcess;
  let pod2: ChildProcess;
  let port2: number;
  let producer: Producer;
  let registry: SchemaRegistry;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { cwd: REPO_ROOT, stdio: "inherit" });
    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    const port1 = await findFreePort();
    port2 = await findFreePort();
    pod1 = spawnGateway(port1, {
      AGENT_OUTPUT_ENABLED: "1",
      AGENT_OUTPUT_GROUP_ID: `rtgw-alert-test-${randomUUID()}`,
    });
    pod2 = spawnGateway(port2, { AGENT_OUTPUT_ENABLED: "0" });
    await Promise.all([waitForHealthz(port1), waitForHealthz(port2)]);
    registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
    producer = new Kafka({ clientId: "agent-alert-test", brokers: BROKERS.split(",") }).producer({
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    await producer.connect();
  }, 60_000);

  afterAll(async () => {
    for (const s of sockets) s.close();
    await producer?.disconnect();
    pod1?.kill();
    pod2?.kill();
    await jwks?.close();
    await coreApi?.close();
  }, 15_000);

  function spawnGateway(port: number, extra: Record<string, string>): ChildProcess {
    const child = spawn(process.execPath, [SERVER_ENTRY], {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        PORT: String(port),
        REDIS_URL,
        CORE_API_URL: coreApi.baseUrl,
        CLERK_JWKS_URL: jwks.jwksUrl,
        REDPANDA_BROKERS: BROKERS,
        SCHEMA_REGISTRY_URL,
        ...extra,
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    child.stderr?.on("data", (chunk: Buffer) =>
      process.stderr.write(`[gateway:${port}] ${chunk.toString()}`),
    );
    return child;
  }

  async function join(orgId: string, meetingId: string, userId: string) {
    const clerkOrg = `org-${randomUUID()}`;
    const clerkUser = `user-${randomUUID()}`;
    coreApi.allowParticipant(clerkOrg, meetingId, clerkUser, { orgId, userId });
    const socket = await connectClient(
      `http://127.0.0.1:${port2}`,
      await jwks.signToken({ orgId: clerkOrg, userId: clerkUser }),
    );
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  it("delivers the alert to the recipient's socket on the other pod only", async () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const alice = randomUUID();
    const bob = randomUUID();
    const aliceSocket = await join(orgId, meetingId, alice);
    const bobSocket = await join(orgId, meetingId, bob);
    const got: { alice: { payload: { id: string; alertType: string } }[]; bob: unknown[] } = {
      alice: [],
      bob: [],
    };
    const arrivedAt = new Map<string, number>();
    aliceSocket.on("agent-alert", (e: { payload: { id: string; alertType: string } }) => {
      arrivedAt.set(e.payload.id, Date.now());
      got.alice.push(e);
    });
    bobSocket.on("agent-alert", (e) => got.bob.push(e));

    const resultsId = await registry.getLatestSchemaId("agent-results-value");
    const sentAt = new Map<string, number>();
    // the new consumer group joins asynchronously: send a fresh alert each round until one lands
    const deadline = Date.now() + 40_000;
    while (got.alice.length === 0 && Date.now() < deadline) {
      const id = randomUUID();
      const value = await registry.encode(resultsId, {
        header: {
          event_id: randomUUID(),
          idempotency_key: `agentresult:${meetingId}:proactive:dependency:${id}:${alice}:ProactiveAgent`,
          org_id: orgId,
          schema_version: 1,
          producer: "agent-runtime-service",
          occurred_at: Date.now() * 1000,
          correlation_id: null,
          causation_id: null,
        },
        meeting_id: meetingId,
        window_id: `proactive:dependency:${id}`,
        user_id: alice,
        agent: "ProactiveAgent",
        kind: "alert",
        relevance_score: 0.85,
        confidence: 0.85,
        content: "Blocked by an open action",
        payload_json: JSON.stringify({
          alert: {
            id,
            alert_type: "dependency",
            priority: 2,
            title: "Blocked by an open action",
            body: "This depends on the API migration, which is still open.",
            confidence: 0.85,
            surfaced: true,
            rationale: "The action lists the migration as a dependency.",
            subject: { kind: "action", id: randomUUID() },
            related: [],
            link: `/meetings/${meetingId}`,
          },
          prompt: "",
          model_tier: "",
        }),
        source_chunk_ids: [],
      });
      sentAt.set(id, Date.now());
      await producer.send({ topic: "agent-results", messages: [{ key: orgId, value }] });
      await sleep(1500);
    }
    await sleep(300);
    expect(got.alice.length).toBeGreaterThan(0);
    const first = got.alice[0].payload;
    expect(first.alertType).toBe("dependency");
    expect(got.bob).toEqual([]);
    // eslint-disable-next-line no-console
    console.log(
      `[two-process alert] Kafka send to pod-2 socket: ${arrivedAt.get(first.id)! - sentAt.get(first.id)!}ms`,
    );
  }, 90_000);
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

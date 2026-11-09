/**
 * Acceptance check #1 — "the canonical hard test": two sockets for the same `meeting_id`,
 * connected to two SEPARATE gateway *processes*, both receive a chat and a presence event. This
 * spawns two real `node` child processes running the actual compiled server (built fresh in
 * `beforeAll`), both pointed at the same real Redis (:6390). A single in-process test with two
 * Socket.io client connections to one server would never exercise the `@socket.io/redis-adapter`
 * cross-pod path at all — this is why it has to be two processes.
 */
import { execFileSync } from "node:child_process";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { JoinAck } from "../src/server";
import { connectClient } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REPO_ROOT = path.resolve(__dirname, "..");
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const SERVER_ENTRY = path.join(
  REPO_ROOT,
  "dist",
  "services",
  "realtime-gateway",
  "src",
  "server.js",
);

describe("cross-pod fan-out (canonical hard test — two separate processes)", () => {
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let procA: ChildProcess;
  let procB: ChildProcess;
  let portA: number;
  let portB: number;

  beforeAll(async () => {
    // Build fresh so this test is self-contained (doesn't assume `npm run build` already ran).
    execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { cwd: REPO_ROOT, stdio: "inherit" });

    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();

    portA = await findFreePort();
    portB = await findFreePort();

    procA = spawnGateway(portA);
    procB = spawnGateway(portB);

    await Promise.all([waitForHealthz(portA), waitForHealthz(portB)]);
  }, 60000);

  afterAll(async () => {
    procA?.kill();
    procB?.kill();
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
        // not under test here; keeps these spawned (and SIGKILLed) pods out of the shared
        // agent-output consumer group, where a dead member stalls agent-output-kafka.test.ts
        AGENT_OUTPUT_ENABLED: "0",
        REDPANDA_BROKERS: process.env.REDPANDA_BROKERS ?? "localhost:9092",
        SCHEMA_REGISTRY_URL: process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8085",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      // presence-events production against a slow/cold broker is non-fatal to the flows under
      // test here; surface stderr for debugging without failing the test on it.
      process.stderr.write(`[gateway:${port}] ${chunk.toString()}`);
    });
    return child;
  }

  it("delivers presence and chat across two independent gateway processes via the Redis adapter", async () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const userA = randomUUID();
    const userB = randomUUID();
    // Already valid UUIDs (this test is about cross-pod delivery, not identity resolution —
    // that's covered in gateway.test.ts), so resolve identity-mapped.
    coreApi.allowParticipant(orgId, meetingId, userA, { orgId, userId: userA });
    coreApi.allowParticipant(orgId, meetingId, userB, { orgId, userId: userB });

    const tokenA = await jwks.signToken({ orgId, userId: userA });
    const tokenB = await jwks.signToken({ orgId, userId: userB });

    const socketA = await connectClient(`http://127.0.0.1:${portA}`, tokenA);
    const socketB = await connectClient(`http://127.0.0.1:${portB}`, tokenB);

    const ackA = await new Promise<JoinAck>((resolve) =>
      socketA.emit("join", { meetingId }, resolve),
    );
    expect(ackA.ok).toBe(true);

    // socket A is on process A; this presence broadcast must arrive via B's local emit after
    // process B publishes through the adapter -- prove socket A sees userB join from process B.
    const presenceOnA = new Promise<string[]>((resolve) => {
      socketA.on("presence", (members: string[]) => {
        if (members.includes(userB)) resolve(members);
      });
    });
    const ackB = await new Promise<JoinAck>((resolve) =>
      socketB.emit("join", { meetingId }, resolve),
    );
    expect(ackB.ok).toBe(true);

    const presenceMembers = await presenceOnA;
    expect(presenceMembers.sort()).toEqual([userA, userB].sort());

    // now prove chat crosses the same boundary in the other direction: A -> process A -> Redis
    // adapter -> process B -> socket B.
    const chatOnB = new Promise((resolve) => socketB.once("chat", resolve));
    const clientMsgId = randomUUID();
    socketA.emit("chat", { meetingId, content: "hello from process A", clientMsgId });

    await expect(chatOnB).resolves.toEqual({
      meetingId,
      userId: userA,
      content: "hello from process A",
      clientMsgId,
    });

    socketA.close();
    socketB.close();
  }, 20000);
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
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`gateway on port ${port} did not become healthy within ${timeoutMs}ms`);
}

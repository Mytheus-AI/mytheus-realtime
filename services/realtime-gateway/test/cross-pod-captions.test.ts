/**
 * Task 8, acceptance check #1 — the caption analogue of `cross-pod-fanout.test.ts`'s canonical
 * hard test. A caption published once on a meeting's explicit Redis channel must reach every
 * socket in that meeting's room ACROSS TWO SEPARATE GATEWAY PROCESSES, each exactly once. This
 * is not automatic: each process independently subscribes to the same explicit Redis channel
 * (see server.ts's caption-relay comment — this path deliberately does NOT go through the
 * `@socket.io/redis-adapter`'s own cross-pod broadcast, to avoid double-delivery), so this test
 * is what actually proves that "one caption in, one caption out per client" holds when there
 * are two independent subscribers to the same channel, not just one.
 *
 * Same two-real-`node`-process harness as `cross-pod-fanout.test.ts`, duplicated rather than
 * shared: that file is a previous task's already-reviewed, already-passing test, and extracting
 * shared helpers out of it is a refactor with no test-coverage benefit for this task — not worth
 * the risk of touching a file this task doesn't otherwise need to change.
 */
import { execFileSync } from "node:child_process";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel } from "../../../libs/keys";
import type { JoinAck } from "../src/server";
import { connectClient } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REPO_ROOT = path.resolve(__dirname, "..");
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";
const SERVER_ENTRY = path.join(REPO_ROOT, "dist", "services", "realtime-gateway", "src", "server.js");

describe("cross-pod caption fan-out (two separate processes, one Redis pub/sub message each)", () => {
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let publisher: Redis;
  let procA: ChildProcess;
  let procB: ChildProcess;
  let portA: number;
  let portB: number;

  beforeAll(async () => {
    execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { cwd: REPO_ROOT, stdio: "inherit" });

    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    publisher = new Redis(REDIS_URL);

    portA = await findFreePort();
    portB = await findFreePort();

    procA = spawnGateway(portA);
    procB = spawnGateway(portB);

    await Promise.all([waitForHealthz(portA), waitForHealthz(portB)]);
  }, 60000);

  afterAll(async () => {
    procA?.kill();
    procB?.kill();
    publisher?.disconnect();
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
        REDPANDA_BROKERS: process.env.REDPANDA_BROKERS ?? "localhost:9092",
        SCHEMA_REGISTRY_URL: process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8081",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      process.stderr.write(`[gateway:${port}] ${chunk}`);
    });
    return child;
  }

  it(
    "delivers one caption, published once, to sockets on two independent gateway processes, exactly once each",
    async () => {
      const orgId = randomUUID();
      const meetingId = randomUUID();
      const userA = randomUUID();
      const userB = randomUUID();
      // Resolved == Clerk ids here on purpose (this test is about cross-pod delivery, not
      // identity resolution — that's gateway.test.ts's job).
      coreApi.allowParticipant(orgId, meetingId, userA, { orgId, userId: userA });
      coreApi.allowParticipant(orgId, meetingId, userB, { orgId, userId: userB });

      const tokenA = await jwks.signToken({ orgId, userId: userA });
      const tokenB = await jwks.signToken({ orgId, userId: userB });

      const socketA = await connectClient(`http://127.0.0.1:${portA}`, tokenA);
      const socketB = await connectClient(`http://127.0.0.1:${portB}`, tokenB);

      const ackA = await new Promise<JoinAck>((resolve) => socketA.emit("join", { meetingId }, resolve));
      const ackB = await new Promise<JoinAck>((resolve) => socketB.emit("join", { meetingId }, resolve));
      expect(ackA.ok).toBe(true);
      expect(ackB.ok).toBe(true);

      const receivedA: unknown[] = [];
      const receivedB: unknown[] = [];
      socketA.on("caption", (msg: unknown) => receivedA.push(msg));
      socketB.on("caption", (msg: unknown) => receivedB.push(msg));

      const payload = {
        chunkId: randomUUID(),
        speakerId: "0",
        startMs: 0,
        endMs: 1200,
        text: "hello there, how are you",
        isFinal: true,
        confidence: 0.97,
      };
      await publisher.publish(
        pubsubChannel(orgId, meetingId),
        JSON.stringify({ type: "caption", meetingId, payload }),
      );

      await waitFor(() => receivedA.length >= 1 && receivedB.length >= 1);
      // Settle window: prove it's exactly once, not "at least once" — a bug routing this
      // through the Redis adapter's own cross-pod broadcast on top of the raw pub/sub delivery
      // would double it.
      await sleep(300);

      expect(receivedA).toEqual([{ meetingId, payload }]);
      expect(receivedB).toEqual([{ meetingId, payload }]);

      socketA.close();
      socketB.close();
    },
    20000,
  );
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await sleep(20);
  }
}

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

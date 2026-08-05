/**
 * Fix round 2, lower-priority finding — regression test for `App.stop()`'s disconnect-cleanup
 * drain. Fix round 1 made `stop()` await `pendingDisconnectCleanups` before disconnecting Redis,
 * to close the race where `io.close()`'s callback fires before an async `disconnect` handler
 * finishes using `pubClient`/`captionSubClient`. The review's follow-up: a single
 * snapshot-then-await (`Promise.allSettled([...pendingDisconnectCleanups])`, once) is only
 * correct if every relevant cleanup has already registered into the Set by the time the
 * snapshot is taken — true for how `io.close()` currently force-closes still-connected sockets,
 * but an unstated internal-ordering assumption, not something this file enforces.
 *
 * `stop()` now loops (`while (pendingDisconnectCleanups.size > 0) await
 * Promise.allSettled(...)`) so a cleanup that registers during the await is picked up by a
 * subsequent iteration instead of racing the Redis disconnect calls that follow the loop.
 *
 * This test exercises the REACHABLE form of the race: a socket disconnects, its cleanup is
 * artificially slow (a real async gap, not an instant no-op), and `stop()` is invoked
 * concurrently rather than sequenced after the disconnect settles — the same shape the
 * reviewer's own test runs hit organically in fix round 1. It asserts the concrete, externally
 * observable failure mode of the old bug: no "Connection is closed" error surfaces, no unhandled
 * rejection escapes, and the slow cleanup step demonstrably completed (the presence entry was
 * actually removed) before `stop()` resolved — i.e. `stop()` genuinely waited for it rather than
 * disconnecting Redis out from under it.
 */
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";

import { Redis } from "ioredis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type pino from "pino";

import { createClerkJwtVerifier } from "../../../libs/auth";
import { presenceKey } from "../../../libs/keys";
import * as presenceModule from "../src/presence";
import { createApp, type App, type JoinAck } from "../src/server";
import { connectClient, createFakePresenceEventsProducer } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";
const REMOVE_MEMBER_DELAY_MS = 200;

interface RecordedLog {
  level: "error" | "warn" | "info";
  args: unknown[];
}

function createRecordingLogger(): { logger: pino.Logger; records: RecordedLog[] } {
  const records: RecordedLog[] = [];
  const logger = {
    error: (...args: unknown[]) => records.push({ level: "error", args }),
    warn: (...args: unknown[]) => records.push({ level: "warn", args }),
    info: (...args: unknown[]) => records.push({ level: "info", args }),
  };
  return { logger: logger as unknown as pino.Logger, records };
}

describe("App.stop() drains a concurrently-slow disconnect cleanup before disconnecting Redis", () => {
  let app: App;
  let baseUrl: string;
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let inspector: Redis;
  let unhandledRejections: unknown[];
  let onUnhandledRejection: (reason: unknown) => void;

  beforeEach(async () => {
    unhandledRejections = [];
    onUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);

    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    inspector = new Redis(REDIS_URL);
  });

  afterEach(async () => {
    process.off("unhandledRejection", onUnhandledRejection);
    vi.restoreAllMocks();
    inspector.disconnect();
    if (unhandledRejections.length > 0) {
      // Fail loudly with the actual reason(s) rather than a bare "expected 0 to be >0" — this
      // is the exact failure mode ("Connection is closed" propagating as an unhandled rejection)
      // the old single-snapshot code could theoretically produce.
      throw new Error(`unhandled rejection(s) during test: ${unhandledRejections.map(String).join("; ")}`);
    }
  });

  it("waits for a slow, still-in-flight cleanup instead of disconnecting Redis underneath it", async () => {
    const { logger, records } = createRecordingLogger();
    const jwtVerifier = createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl });
    app = createApp(
      {
        port: 0,
        redisUrl: REDIS_URL,
        coreApiUrl: coreApi.baseUrl,
        redpandaBrokers: ["localhost:9092"],
        schemaRegistryUrl: "http://localhost:8081",
      },
      { jwtVerifier, presenceEventsProducer: createFakePresenceEventsProducer(), logger },
    );
    await app.start();
    const { port } = app.httpServer.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${port}`;

    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedOrgId = randomUUID();
    const resolvedUserId = randomUUID();
    coreApi.allowParticipant(orgId, meetingId, userId, { orgId: resolvedOrgId, userId: resolvedUserId });

    const token = await jwks.signToken({ orgId, userId });
    const socket = await connectClient(baseUrl, token);
    const ack = await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId }, resolve));
    expect(ack.ok).toBe(true);

    // Make the presence-removal step of disconnect cleanup a REAL async gap (not an instant
    // no-op), so it is still genuinely in flight — a pending promise doing real work — at the
    // moment stop() is invoked, not just theoretically registered. Delays, then calls the real
    // implementation (captured before spying), so the actual Redis removal still happens — this
    // tests stop() genuinely waiting for real work, not a no-op stand-in.
    const originalRemoveMember = presenceModule.removeMember;
    let removeMemberCalled = false;
    const removeMemberSpy = vi
      .spyOn(presenceModule, "removeMember")
      .mockImplementation(async (...args: Parameters<typeof presenceModule.removeMember>) => {
        removeMemberCalled = true;
        await sleep(REMOVE_MEMBER_DELAY_MS);
        return originalRemoveMember(...args);
      });

    // Race close() and stop() rather than sequencing them: don't await socket.close() settling
    // before calling stop(). This is the concurrent-disconnect-during-shutdown shape under test.
    socket.close();
    const stopPromise = app.stop();

    await waitFor(() => removeMemberCalled);
    await stopPromise;

    // The presence-removal step genuinely completed (not skipped, not raced) before stop()
    // resolved: the real Redis ZSET member is gone.
    const remaining = await inspector.zcard(presenceKey(resolvedOrgId, meetingId));
    expect(remaining).toBe(0);

    expect(removeMemberSpy).toHaveBeenCalled();

    // The concrete symptom of the old bug: no "Connection is closed" (or similarly-shaped)
    // error surfaced anywhere in the logger during this run.
    const connectionClosedLogs = records.filter(
      (r) => r.level === "error" && r.args.some((a) => JSON.stringify(a).includes("Connection is closed")),
    );
    expect(connectionClosedLogs).toEqual([]);
  }, 10000);
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await sleep(10);
  }
}

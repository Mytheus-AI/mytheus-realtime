/**
 * Fix round 1, finding 2 — regression test. Before this fix, `server.ts`'s `disconnect` handler
 * ran presence removal, the presence re-broadcast, the caption-channel unsubscribe, and the
 * `presence-events` "leave" production all under ONE shared try/catch. If `removeMember` threw,
 * every step after it — including `unsubscribeCaptionChannel` — was silently skipped. The dead
 * socket stayed in `localChannelSockets`'s Set forever (a per-socket leak), and once it was the
 * last local socket for that channel, `captionSubClient` never unsubscribed from a channel
 * nobody local is listening to anymore.
 *
 * This proves the fix directly against the real, externally-visible effect — whether Redis
 * itself still considers this process subscribed to the channel (`PUBSUB CHANNELS`) — rather
 * than against this repo's own internal bookkeeping, so it can't pass by coincidentally matching
 * an implementation detail.
 */
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";

import { Redis } from "ioredis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createClerkJwtVerifier } from "../../../libs/auth";
import { pubsubChannel } from "../../../libs/keys";
import * as presenceModule from "../src/presence";
import { createApp, type App, type JoinAck } from "../src/server";
import { connectClient, createFakePresenceEventsProducer } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";

describe("caption channel unsubscribe survives a presence-removal failure on disconnect", () => {
  let app: App;
  let baseUrl: string;
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let inspector: Redis;

  beforeEach(async () => {
    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    inspector = new Redis(REDIS_URL);

    const jwtVerifier = createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl });
    app = createApp(
      {
        port: 0,
        redisUrl: REDIS_URL,
        coreApiUrl: coreApi.baseUrl,
        redpandaBrokers: ["localhost:9092"],
        schemaRegistryUrl: "http://localhost:8081",
      },
      { jwtVerifier, presenceEventsProducer: createFakePresenceEventsProducer() },
    );
    await app.start();
    const { port } = app.httpServer.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    inspector.disconnect();
    await app.stop();
    await jwks.close();
    await coreApi.close();
  });

  it("still unsubscribes the caption channel from Redis even though removeMember rejects", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedOrgId = randomUUID();
    coreApi.allowParticipant(orgId, meetingId, userId, {
      orgId: resolvedOrgId,
      userId: randomUUID(),
    });

    const token = await jwks.signToken({ orgId, userId });
    const socket = await connectClient(baseUrl, token);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);

    const channel = pubsubChannel(resolvedOrgId, meetingId);

    // Confirm the subscription actually exists on the real Redis server before breaking
    // anything — this is the same real, external signal we'll check again after disconnect.
    await waitFor(
      async () => (await inspector.call("PUBSUB", "CHANNELS", channel)) as string[],
      (chans) => chans.includes(channel),
    );

    // Force the FIRST step of disconnect cleanup to fail, exactly as the reviewer's teardown
    // race did organically — but deterministically this time, via the real exported function
    // server.ts actually calls (not a re-implementation of it).
    const removeMemberSpy = vi
      .spyOn(presenceModule, "removeMember")
      .mockRejectedValueOnce(new Error("synthetic presence removal failure"));

    socket.close();

    // The presence-removal failure must have actually been hit (otherwise this test would prove
    // nothing about the fix under test).
    await waitFor(
      () => removeMemberSpy.mock.calls.length,
      (n) => n >= 1,
    );

    // The real assertion: despite that failure, the channel is unsubscribed from Redis — proof
    // that `unsubscribeCaptionChannel` ran independently of the presence step, not skipped by
    // its failure.
    await waitFor(
      async () => (await inspector.call("PUBSUB", "CHANNELS", channel)) as string[],
      (chans) => !chans.includes(channel),
    );
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  getValue: () => T | Promise<T>,
  predicate: (value: T) => boolean,
  timeoutMs = 3000,
): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await getValue();
    if (predicate(value)) return value;
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `waitFor: condition not met within ${timeoutMs}ms (last value: ${JSON.stringify(value)})`,
      );
    }
    await sleep(20);
  }
}

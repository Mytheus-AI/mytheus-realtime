/**
 * Task 8 acceptance check — "measure the gateway's own contribution and report the number you
 * actually observed" (the brief is explicit that the end-to-end p95 < 2s budget, which is
 * Deepgram-bound, cannot be claimed met here — there is no Deepgram account in this build; see
 * transcription-service's task-7 report). This measures ONLY the gateway's own leg: the
 * wall-clock time from `redis.publish()` on the explicit caption channel to the Socket.io
 * client's `"caption"` handler firing, on loopback, against a real Redis and a real in-process
 * App (not a fake pub/sub). That's the entire span this service is responsible for; the
 * Deepgram-to-Redis leg lives in transcription-service and is out of this repo's reach to
 * measure honestly.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel } from "../../../libs/keys";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";
const SAMPLES = 200;

describe("caption bridge latency — the gateway's own leg only", () => {
  let t: TestApp;
  let publisher: Redis;

  beforeAll(async () => {
    t = await startTestApp();
    publisher = new Redis(REDIS_URL);
  });

  afterAll(async () => {
    publisher.disconnect();
    await t.stop();
  });

  it(
    `reports p50/p95/p99 over ${SAMPLES} publish-to-client-receive samples on loopback`,
    async () => {
      const orgId = `org-${randomUUID()}`;
      const meetingId = `meeting-${randomUUID()}`;
      const userId = `user-${randomUUID()}`;
      const resolvedOrgId = randomUUID();
      t.coreApi.allowParticipant(orgId, meetingId, userId, { orgId: resolvedOrgId, userId: randomUUID() });

      const token = await t.jwks.signToken({ orgId, userId });
      const socket = await connectClient(t.baseUrl, token);
      const ack = await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId }, resolve));
      expect(ack.ok).toBe(true);

      const channel = pubsubChannel(resolvedOrgId, meetingId);
      const samples: number[] = [];

      for (let i = 0; i < SAMPLES; i++) {
        const chunkId = randomUUID();
        const sentAt = process.hrtime.bigint();
        const received = new Promise<void>((resolve) => {
          const handler = (msg: { payload: { chunkId: string } }) => {
            if (msg.payload.chunkId === chunkId) {
              socket.off("caption", handler);
              resolve();
            }
          };
          socket.on("caption", handler);
        });

        await publisher.publish(
          channel,
          JSON.stringify({
            type: "caption",
            meetingId,
            payload: {
              chunkId,
              speakerId: "0",
              startMs: i * 400,
              endMs: i * 400 + 400,
              text: `sample ${i}`,
              isFinal: true,
              confidence: 0.9,
            },
          }),
        );

        await received;
        const elapsedMs = Number(process.hrtime.bigint() - sentAt) / 1_000_000;
        samples.push(elapsedMs);
      }

      samples.sort((a, b) => a - b);
      const p50 = percentile(samples, 50);
      const p95 = percentile(samples, 95);
      const p99 = percentile(samples, 99);
      const max = samples[samples.length - 1];

      // eslint-disable-next-line no-console
      console.log(
        `[caption-latency] gateway-only leg (publish -> client receive, loopback, n=${SAMPLES}): ` +
          `p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms max=${max.toFixed(2)}ms`,
      );

      // A generous sanity ceiling for THIS leg only (loopback Redis pub/sub + Socket.io
      // delivery), not a claim about the end-to-end Deepgram-bound budget in the brief (2s),
      // which this test cannot measure and does not attempt to. This just catches a gross
      // regression (e.g. an accidental synchronous block in the message handler).
      expect(p95).toBeLessThan(250);

      socket.close();
    },
    30000,
  );
});

function percentile(sortedAsc: number[], p: number): number {
  const idx = Math.min(sortedAsc.length - 1, Math.ceil((p / 100) * sortedAsc.length) - 1);
  return sortedAsc[Math.max(0, idx)];
}

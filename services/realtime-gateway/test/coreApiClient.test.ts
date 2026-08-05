/**
 * Contract-pinning test for `isParticipant`'s response parsing — the identity-resolution bug fix
 * lives half here (reading core-api's already-resolved `user_id` out of the response body instead
 * of discarding it) and half in `presenceEventsProducer.ts`'s UUID guard (see that file's test).
 * Against a real HTTP server (test/helpers/fakeCoreApi.ts), matching core-api's real
 * `Participant` response shape (`services/core_api/schemas.py` in `mytheus-backend`), not a
 * hand-rolled fetch mock.
 */
import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createCoreApiClient, type CoreApiClient } from "../src/coreApiClient";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";

describe("coreApiClient.isParticipant — response parsing contract", () => {
  let coreApi: FakeCoreApi;
  let client: CoreApiClient;

  beforeAll(async () => {
    coreApi = await startFakeCoreApi();
    client = createCoreApiClient({ baseUrl: coreApi.baseUrl });
  });

  afterAll(async () => {
    await coreApi.close();
  });

  it("resolves { allowed: true, internalUserId, internalOrgId } from a 200 response that includes both", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedOrgId = randomUUID();
    const resolvedUserId = randomUUID();
    coreApi.allowParticipant(orgId, meetingId, userId, { orgId: resolvedOrgId, userId: resolvedUserId });

    const result = await client.isParticipant({ orgId, meetingId, userId, bearerToken: "t" });

    expect(result).toEqual({ allowed: true, internalUserId: resolvedUserId, internalOrgId: resolvedOrgId });
  });

  it("resolves { allowed: true, internalOrgId: null } when core-api's response has no org_id field yet (today's real contract gap)", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedUserId = randomUUID();
    coreApi.allowParticipant(orgId, meetingId, userId, { orgId: null, userId: resolvedUserId });

    const result = await client.isParticipant({ orgId, meetingId, userId, bearerToken: "t" });

    expect(result).toEqual({ allowed: true, internalUserId: resolvedUserId, internalOrgId: null });
  });

  it("resolves { allowed: false } on a 403 (not a participant)", async () => {
    const result = await client.isParticipant({
      orgId: "org-x",
      meetingId: "meeting-x",
      userId: "user-x", // never registered via allowParticipant
      bearerToken: "t",
    });

    expect(result).toEqual({ allowed: false });
  });

  it("resolves { allowed: false }, never throws, when the server is unreachable", async () => {
    const unreachable = createCoreApiClient({ baseUrl: "http://127.0.0.1:1" });
    const result = await unreachable.isParticipant({
      orgId: "org-x",
      meetingId: "meeting-x",
      userId: "user-x",
      bearerToken: "t",
    });

    expect(result).toEqual({ allowed: false });
  });
});

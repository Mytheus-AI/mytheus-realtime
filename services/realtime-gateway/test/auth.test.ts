/**
 * Acceptance check #2: four distinct handshake-rejection cases, four assertions — no token, a
 * malformed token, an expired token, and a token signed by the wrong key. Exercised against a
 * real JWKS HTTP endpoint (see test/helpers/fakeJwks.ts), not an in-memory stub, so the actual
 * `createRemoteJWKSet` fetch + signature verification path in libs/auth.ts runs for real.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createClerkJwtVerifier, type ClerkJwtVerifier } from "../../../libs/auth";
import {
  signTokenWithUnpublishedKey,
  startFakeJwksServer,
  type FakeJwksServer,
} from "./helpers/fakeJwks";

describe("createClerkJwtVerifier", () => {
  let jwks: FakeJwksServer;
  let verifier: ClerkJwtVerifier;

  beforeAll(async () => {
    jwks = await startFakeJwksServer();
    verifier = createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl });
  });

  afterAll(async () => {
    await jwks.close();
  });

  it("accepts a validly signed token and resolves org_id/user_id from claims only", async () => {
    const token = await jwks.signToken({ orgId: "org-1", userId: "user-1" });
    const identity = await verifier.verify(token);
    expect(identity).toEqual({ orgId: "org-1", userId: "user-1" });
  });

  it("rejects an absent token", async () => {
    await expect(verifier.verify(undefined)).rejects.toThrow();
    await expect(verifier.verify(null)).rejects.toThrow();
    await expect(verifier.verify("")).rejects.toThrow();
  });

  it("rejects a malformed token", async () => {
    await expect(verifier.verify("not-a-jwt-at-all")).rejects.toThrow();
  });

  it("rejects an expired token", async () => {
    const token = await jwks.signToken({ orgId: "org-1", userId: "user-1", expiresInSeconds: -60 });
    await expect(verifier.verify(token)).rejects.toThrow();
  });

  it("rejects a token signed by a key not in the JWKS", async () => {
    const token = await signTokenWithUnpublishedKey(jwks.issuer, {
      orgId: "org-1",
      userId: "user-1",
    });
    await expect(verifier.verify(token)).rejects.toThrow();
  });

  it("rejects a validly signed token missing the org_id claim", async () => {
    const token = await jwks.signCustomClaims({ sub: "user-1" });
    await expect(verifier.verify(token)).rejects.toThrow();
  });

  it("rejects a validly signed token missing the sub claim", async () => {
    const token = await jwks.signCustomClaims({ org_id: "org-1" });
    await expect(verifier.verify(token)).rejects.toThrow();
  });
});

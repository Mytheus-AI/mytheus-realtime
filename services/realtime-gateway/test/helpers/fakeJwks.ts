/**
 * A real JWKS endpoint backed by a locally-generated RSA keypair, standing in for Clerk (there is
 * no live Clerk instance available to this repo). This is a genuine HTTP server serving a genuine
 * JWKS document — `createClerkJwtVerifier`'s default `createRemoteJWKSet(jwksUrl)` path fetches
 * from it exactly as it would fetch from Clerk, so signature/issuer verification is exercised for
 * real, not mocked.
 */
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { exportJWK, generateKeyPair, SignJWT, type CryptoKey, type JWK } from "jose";

export interface SignTokenParams {
  orgId: string;
  userId: string;
  /** Seconds from now until expiry. Negative produces an already-expired token. */
  expiresInSeconds?: number;
}

export interface FakeJwksServer {
  jwksUrl: string;
  issuer: string;
  signToken(params: SignTokenParams): Promise<string>;
  /** Escape hatch for tests that need a token with a non-standard claim set (e.g. missing
   * `org_id`), still signed by the published key so only the claim shape is under test. */
  signCustomClaims(claims: Record<string, unknown>): Promise<string>;
  close(): Promise<void>;
}

export async function startFakeJwksServer(): Promise<FakeJwksServer> {
  const { publicKey, privateKey } = await generateKeyPair("RS256");
  const jwk: JWK = { ...(await exportJWK(publicKey)), kid: "test-key-1", alg: "RS256", use: "sig" };

  const server: Server = createServer((req, res) => {
    if (req.url === "/.well-known/jwks.json") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  const issuer = `http://127.0.0.1:${port}`;

  return {
    jwksUrl: `${issuer}/.well-known/jwks.json`,
    issuer,
    async signToken({ orgId, userId, expiresInSeconds = 3600 }) {
      return signWith(privateKey, "test-key-1", issuer, { orgId, userId, expiresInSeconds });
    },
    async signCustomClaims(claims) {
      const now = Math.floor(Date.now() / 1000);
      return new SignJWT(claims)
        .setProtectedHeader({ alg: "RS256", kid: "test-key-1" })
        .setIssuer(issuer)
        .setIssuedAt(now)
        .setExpirationTime(now + 3600)
        .sign(privateKey);
    },
    async close() {
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve())),
      );
    },
  };
}

/** Signs a token with a freshly-generated keypair that was never published to any JWKS — for the
 * "signed by the wrong key" rejection case. */
export async function signTokenWithUnpublishedKey(
  issuer: string,
  params: SignTokenParams,
): Promise<string> {
  const { privateKey } = await generateKeyPair("RS256");
  return signWith(privateKey, "unpublished-key", issuer, params);
}

async function signWith(
  privateKey: CryptoKey,
  kid: string,
  issuer: string,
  { orgId, userId, expiresInSeconds = 3600 }: SignTokenParams,
): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  return new SignJWT({ org_id: orgId })
    .setProtectedHeader({ alg: "RS256", kid })
    .setSubject(userId)
    .setIssuer(issuer)
    .setIssuedAt(now)
    .setExpirationTime(now + expiresInSeconds)
    .sign(privateKey);
}

/**
 * Clerk JWT verification for the WS handshake. `org_id` is trusted ONLY when it comes out of a
 * successfully verified token here — never from a client-supplied payload or message body. Fails
 * closed: any problem (missing token, malformed token, expired token, wrong signing key, wrong
 * issuer, missing claims) throws and the caller must reject the connection.
 *
 * There is no live Clerk instance available to this repo yet, so the JWKS source is injectable:
 * production wires `createRemoteJWKSet` against `CLERK_JWKS_URL`, tests serve a real JWKS
 * document over a local HTTP server backed by a locally-generated RSA keypair. That exercises the
 * actual signature/issuer verification path — nothing is weakened or bypassed to make a test
 * pass.
 */
import { createRemoteJWKSet, jwtVerify, type JWTVerifyGetKey } from "jose";

export interface ClerkIdentity {
  orgId: string;
  userId: string;
}

export interface ClerkJwtVerifierOptions {
  /** Clerk JWKS endpoint, e.g. `https://<slug>.clerk.accounts.dev/.well-known/jwks.json`. */
  jwksUrl: string;
  /** Expected `iss` claim. Defaults to the origin of `jwksUrl` (Clerk's Frontend API origin). */
  issuer?: string;
  /** Injectable JWKS source. Defaults to a remote fetch of `jwksUrl`; tests pass their own. */
  jwks?: JWTVerifyGetKey;
}

export interface ClerkJwtVerifier {
  /** Verifies `token` and resolves the tenant identity. Throws on any failure (fail closed). */
  verify(token: string | undefined | null): Promise<ClerkIdentity>;
}

export function createClerkJwtVerifier(options: ClerkJwtVerifierOptions): ClerkJwtVerifier {
  const issuer = options.issuer ?? new URL(options.jwksUrl).origin;
  const jwks = options.jwks ?? createRemoteJWKSet(new URL(options.jwksUrl));

  return {
    async verify(token) {
      if (!token) {
        throw new Error("UNAUTHENTICATED: missing token");
      }
      const { payload } = await jwtVerify(token, jwks, { issuer });

      const orgId = payload.org_id;
      if (typeof orgId !== "string" || orgId.length === 0) {
        throw new Error("UNAUTHENTICATED: token missing org_id claim");
      }
      const userId = payload.sub;
      if (typeof userId !== "string" || userId.length === 0) {
        throw new Error("UNAUTHENTICATED: token missing sub claim");
      }
      return { orgId, userId };
    },
  };
}

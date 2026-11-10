"""Capture tokens: how a browser capture companion proves who it is to `/capture/audio`.

The Mytheus Capture Chrome extension records a call held somewhere else (Google Meet,
Microsoft Teams, the Zoom web client) and streams its audio here. It holds no Clerk session of
its own, so it cannot present the JWT `/internal/audio`'s senders never need either. Instead
`mytheus-ui`'s server (route `POST /api/capture/session/start` or `/refresh`, Clerk-authenticated
through the user's own session) creates the external meeting through `core-api`, resolves the
caller's internal `users.id` and `organizations.id`, and signs this token for them.

Format: a compact JWS, HS256 only, signed with `CAPTURE_TOKEN_SECRET` (the same value is set on
`mytheus-ui`'s server; at least 32 characters, or the service treats capture as unconfigured
and refuses every connection: fail closed). Claims:

    typ        "capture"                     -- refuses any other token type (e.g. the
                                                extension's link token, which the UI signs
                                                with a key derived from the same secret)
    iss        "mytheus-ui"
    aud        "transcription-service"
    sub        users.id (UUID)               -- the person capturing; the mic channel's speaker
    org        organizations.id (UUID)       -- the tenant, never read from the client payload
    mid        meetings.id (UUID)            -- the external meeting the UI just created
    src        "meet" | "teams" | "zoom"
    consent_at ISO-8601 time the user confirmed they told the participants
    iat, exp   seconds; exp - iat at most MAX_TOKEN_LIFETIME_SECONDS

Why a token signed by the UI rather than a call back to `core-api` per connection: the UI
already holds the user's verified Clerk session at mint time and has just resolved the internal
ids through `core-api` itself, so this service needs no outbound dependency (and no Clerk
JWKS) on the audio path, and a stolen token is only good for one meeting for minutes. The
token only gates connection establishment: a long call keeps streaming past `exp`, and a
reconnect after `exp` asks the UI for a fresh one.

Hand-rolled HS256 instead of a JWT library: the verification is a dozen lines, the algorithm
is pinned (no `alg` negotiation, so no `none`/RS-HS confusion), and it adds no dependency.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

TOKEN_TYPE = "capture"
ISSUER = "mytheus-ui"
AUDIENCE = "transcription-service"
SOURCES = frozenset({"meet", "teams", "zoom"})
MIN_SECRET_LENGTH = 32
# The UI mints 15-minute tokens; anything claiming a longer life was not minted by it.
MAX_TOKEN_LIFETIME_SECONDS = 60 * 60
CLOCK_SKEW_SECONDS = 30
MAX_TOKEN_LENGTH = 4096


class CaptureTokenError(ValueError):
    """The token is not a valid capture token. The message is safe as a close reason: it never
    echoes token content."""


@dataclass(frozen=True)
class CaptureClaims:
    user_id: str
    org_id: str
    meeting_id: str
    source: str
    consent_at: str
    expires_at: int


def secret_usable(secret: str | None) -> bool:
    return bool(secret) and len(secret or "") >= MIN_SECRET_LENGTH


def _b64url_decode(part: str) -> bytes:
    padded = part + "=" * (-len(part) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (ValueError, UnicodeEncodeError) as exc:
        raise CaptureTokenError("capture token is malformed") from exc


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _json_part(part: str) -> dict[str, Any]:
    try:
        obj = json.loads(_b64url_decode(part))
    except ValueError as exc:
        raise CaptureTokenError("capture token is malformed") from exc
    if not isinstance(obj, dict):
        raise CaptureTokenError("capture token is malformed")
    return obj


def _uuid_claim(claims: dict[str, Any], key: str) -> str:
    value = claims.get(key)
    if not isinstance(value, str):
        raise CaptureTokenError(f"capture token: {key} is required")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise CaptureTokenError(f"capture token: {key} must be a UUID") from exc


def _int_claim(claims: dict[str, Any], key: str) -> int:
    value = claims.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CaptureTokenError(f"capture token: {key} is required")
    return value


def sign_capture_token(claims: dict[str, Any], secret: str) -> str:
    """The exact format `mytheus-ui` mints (`src/app/api/capture/_lib/tokens.ts`). Used by the
    tests and kept here so the format has one readable reference on this side."""
    header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url_encode(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode("ascii")
    signature = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    return f"{header}.{payload}.{_b64url_encode(signature)}"


def verify_capture_token(
    token: object, secret: str | None, *, now: float | None = None
) -> CaptureClaims:
    """Verifies signature, type, issuer, audience, lifetime and claim shapes. Raises
    `CaptureTokenError` on any failure, including an unusable secret (fail closed)."""
    if not secret_usable(secret):
        raise CaptureTokenError("capture is not configured on this server")
    assert secret is not None
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_LENGTH:
        raise CaptureTokenError("capture token is required")
    parts = token.split(".")
    if len(parts) != 3:
        raise CaptureTokenError("capture token is malformed")
    header_part, payload_part, signature_part = parts

    header = _json_part(header_part)
    if header.get("alg") != "HS256":
        raise CaptureTokenError("capture token algorithm is not accepted")
    expected = hmac.new(
        secret.encode(), f"{header_part}.{payload_part}".encode("ascii"), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected, _b64url_decode(signature_part)):
        raise CaptureTokenError("capture token signature is invalid")

    claims = _json_part(payload_part)
    if claims.get("typ") != TOKEN_TYPE:
        raise CaptureTokenError("not a capture token")
    if claims.get("iss") != ISSUER or claims.get("aud") != AUDIENCE:
        raise CaptureTokenError("capture token was not issued for this service")

    current = time.time() if now is None else now
    issued_at = _int_claim(claims, "iat")
    expires_at = _int_claim(claims, "exp")
    if expires_at - issued_at > MAX_TOKEN_LIFETIME_SECONDS or expires_at <= issued_at:
        raise CaptureTokenError("capture token lifetime is not accepted")
    if issued_at > current + CLOCK_SKEW_SECONDS:
        raise CaptureTokenError("capture token is not valid yet")
    if expires_at < current - CLOCK_SKEW_SECONDS:
        raise CaptureTokenError("capture token has expired")

    source = claims.get("src")
    if source not in SOURCES:
        raise CaptureTokenError("capture token: src is not a supported meeting source")
    consent_at = claims.get("consent_at")
    if not isinstance(consent_at, str):
        raise CaptureTokenError("capture token: consent_at is required")
    try:
        datetime.fromisoformat(consent_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CaptureTokenError("capture token: consent_at must be an ISO time") from exc

    return CaptureClaims(
        user_id=_uuid_claim(claims, "sub"),
        org_id=_uuid_claim(claims, "org"),
        meeting_id=_uuid_claim(claims, "mid"),
        source=str(source),
        consent_at=consent_at,
        expires_at=expires_at,
    )

"""`/capture/audio`: audio from the Mytheus Capture browser extension (calls held elsewhere).

A Mytheus room's audio reaches this service from `meeting-service`'s LiveKit tap over
`/internal/audio`. A call on Google Meet, Microsoft Teams or the Zoom web client has no tap: the
person who wants it in Mytheus runs the capture extension, which records two things in their
own browser and streams them here as two connections:

- `mic`: their microphone. One speaker, them: attributed to their `users.id`, exactly like a
  participant track on `/internal/audio`, so the agent runtime adds them to the meeting's
  roster and their agents run on the call.
- `tab`: the meeting tab's audio, i.e. everyone else. Opened DIARIZED, and each Deepgram speaker
  becomes `1`, `2`, ... (`sessions.speaker_for`), which the app shows as "Speaker 1". Those
  labels are not user ids: `meeting_speakers` can name them later, and the agent runtime never
  treats them as users.

Both feed the same `SessionTracker`, so everything downstream is unchanged: interims are live
captions only, finals become one `transcript_chunks` row and one `transcript-chunks` record
(`persistence.process_utterance`), the tenant's redaction and residency apply, and the org's
vocabulary reaches Deepgram as key terms (`providers/deepgram.py`, ADR-0039 D1).

THE WIRE CONTRACT (the sender is `mytheus-ui/extension/src/offscreen.ts`; change both together):

1. Handshake. A browser WebSocket to `/capture/audio`. Browsers cannot set headers, so the
   credential travels in the first frame instead. The upgrade is refused before `accept()`
   (HTTP 403) when capture is not configured here (`CAPTURE_TOKEN_SECRET` unset or shorter than
   32 characters: fail closed) or when `CAPTURE_ALLOWED_ORIGINS` is set and the `Origin` header
   (the extension's `chrome-extension://<id>`) is not in it.

2. Init frame. The FIRST frame, within `AUDIO_INIT_TIMEOUT_SECONDS`, is a JSON text frame:

       {"type": "init", "token": "<capture token>", "channel": "mic" | "tab",
        "sample_rate": 16000, "encoding": "pcm_s16le"}

   The token (`capture_auth.py`) names the user, org, meeting, source and when the user
   confirmed they told the participants. `org_id` and `meeting_id` come ONLY from the verified
   token, never from the frame. Once accepted, the server sends one text frame back:

       {"type": "ready", "meeting_id": "<uuid>", "channel": "mic" | "tab"}

3. Audio and control frames: identical to `/internal/audio` (`audio_in.py` points 3 and 4):
   binary 16 kHz mono s16le PCM, `{"type": "stop"}` to end gracefully.

Close codes: those of `/internal/audio`, plus

    4401  capture token missing, invalid or expired  -> mint a fresh token, then reconnect

Consent: the token's `consent_at` is the user's own confirmation in the extension that they
told everyone on the call. It is written to `audit_logs` (`transcription.capture_consent`,
actor = the user) before any audio is forwarded, and a connection whose consent cannot be
recorded is closed 1011.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field

from starlette.websockets import WebSocket

from .audio_in import (
    CLOSE_BAD_INIT,
    CLOSE_COMPLIANCE_GATE,
    CLOSE_GOING_AWAY,
    CLOSE_INTERNAL_ERROR,
    CLOSE_POLICY_VIOLATION,
    CLOSE_TENANT_FORBIDDEN,
    CLOSE_TIMEOUT,
    PROVIDER_FAKE,
    PROVIDER_UNAVAILABLE,
    SUPPORTED_ENCODING,
    SUPPORTED_SAMPLE_RATE,
    AudioHub,
    ConnectionStats,
    _ClientGone,
)
from .audit import record_capture_consent
from .capture_auth import CaptureClaims, CaptureTokenError, secret_usable, verify_capture_token
from .compliance import ComplianceGateClosedError, assert_compliance_gate_green
from .db import tenant_transaction
from .sessions import TenantMismatchError
from .tenancy import meeting_belongs_to_org

logger = logging.getLogger(__name__)

CLOSE_UNAUTHORIZED = 4401
CHANNEL_MIC = "mic"
CHANNEL_TAB = "tab"
CHANNELS = frozenset({CHANNEL_MIC, CHANNEL_TAB})
# Deepgram speaker 0 becomes "1": the app renders a raw label as "Speaker 1".
TAB_SPEAKER_PREFIX = ""


@dataclass(frozen=True)
class CaptureInit:
    token: str
    channel: str


class _Refuse(Exception):
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


def parse_capture_init(raw: str) -> CaptureInit:
    """Shape only; the token itself is verified separately. Errors are safe close reasons."""
    try:
        obj = json.loads(raw)
    except ValueError as exc:
        raise _Refuse(CLOSE_BAD_INIT, "init frame is not valid JSON") from exc
    if not isinstance(obj, dict) or obj.get("type") != "init":
        raise _Refuse(CLOSE_BAD_INIT, "first frame must be an init frame")
    channel = obj.get("channel")
    if channel not in CHANNELS:
        raise _Refuse(CLOSE_BAD_INIT, "init frame: channel must be 'mic' or 'tab'")
    sample_rate = obj.get("sample_rate")
    if isinstance(sample_rate, bool) or sample_rate != SUPPORTED_SAMPLE_RATE:
        raise _Refuse(CLOSE_BAD_INIT, f"init frame: sample_rate must be {SUPPORTED_SAMPLE_RATE}")
    if obj.get("encoding") != SUPPORTED_ENCODING:
        raise _Refuse(CLOSE_BAD_INIT, f"init frame: encoding must be {SUPPORTED_ENCODING!r}")
    token = obj.get("token")
    if not isinstance(token, str) or not token:
        raise _Refuse(CLOSE_UNAUTHORIZED, "capture token is required")
    return CaptureInit(token=token, channel=str(channel))


def participant_identity_for(claims: CaptureClaims, channel: str) -> str:
    """The supersede key of the connection: the mic is the user themself; the tab channel is a
    separate identity so it never supersedes (or is superseded by) the user's own mic."""
    return claims.user_id if channel == CHANNEL_MIC else f"capture-tab:{claims.user_id}"


def parse_allowed_origins(raw: str | None) -> frozenset[str]:
    return frozenset(o.strip().rstrip("/") for o in (raw or "").split(",") if o.strip())


@dataclass
class CaptureHub(AudioHub):
    """`AudioHub` for the browser path: same tracker, limits, backpressure, stop and drain; a
    token in the first frame instead of a shared-secret header."""

    secret: str | None = None
    allowed_origins: frozenset[str] = field(default_factory=frozenset)

    async def handle(self, websocket: WebSocket) -> None:
        if self.shutdown.is_set():
            await websocket.close(code=CLOSE_GOING_AWAY, reason="server shutting down")
            return
        if not secret_usable(self.secret):
            logger.error("/capture/audio refused: CAPTURE_TOKEN_SECRET is unset (fail closed)")
            await websocket.close(code=CLOSE_POLICY_VIOLATION, reason="capture not configured")
            return
        origin = (websocket.headers.get("origin") or "").rstrip("/")
        if self.allowed_origins and origin not in self.allowed_origins:
            logger.warning("/capture/audio refused: origin is not an allowed capture origin")
            await websocket.close(code=CLOSE_POLICY_VIOLATION, reason="origin not allowed")
            return

        done = asyncio.Event()
        key = id(done)
        self._active[key] = done
        stats = ConnectionStats()
        code: int | None
        reason: str | None
        try:
            await websocket.accept()
            code, reason = await self._serve_capture(websocket, stats)
        except _ClientGone:
            code, reason = None, None
        finally:
            self._active.pop(key, None)
            done.set()

        stats.close_code, stats.close_reason = code, reason
        if code is not None:
            with contextlib.suppress(Exception):
                await websocket.close(code=code, reason=reason)
        self.finished.append(stats)
        del self.finished[:-100]
        logger.info(
            "/capture/audio closed meeting=%s code=%s frames=%d bytes=%d dropped=%d",
            stats.meeting_id,
            code,
            stats.frames_received,
            stats.bytes_received,
            stats.frames_dropped,
        )

    async def _serve_capture(self, websocket: WebSocket, stats: ConnectionStats) -> tuple[int, str]:
        try:
            init, claims = await self._read_capture_init(websocket)
        except _Refuse as refuse:
            return refuse.code, refuse.reason
        stats.meeting_id = claims.meeting_id

        if self.provider_mode == PROVIDER_UNAVAILABLE:
            logger.error(
                "/capture/audio meeting=%s refused: no transcription provider configured",
                claims.meeting_id,
            )
            return CLOSE_INTERNAL_ERROR, "no transcription provider configured"
        if self.provider_mode != PROVIDER_FAKE:
            try:
                assert_compliance_gate_green()
            except ComplianceGateClosedError:
                return CLOSE_COMPLIANCE_GATE, "compliance gate closed; audio dropped"

        # Tenant check and the consent record, in one tenant transaction, before any audio.
        try:
            async with tenant_transaction(self.tracker.session_factory, claims.org_id) as session:
                belongs = await meeting_belongs_to_org(session, claims.org_id, claims.meeting_id)
                if belongs:
                    await record_capture_consent(
                        session,
                        org_id=claims.org_id,
                        meeting_id=claims.meeting_id,
                        user_id=claims.user_id,
                        source=claims.source,
                        channel=init.channel,
                        consent_at=claims.consent_at,
                    )
        except Exception:  # noqa: BLE001 - any DB failure is a 1011, never an open
            logger.exception(
                "/capture/audio meeting=%s: tenant check or consent record failed",
                claims.meeting_id,
            )
            return CLOSE_INTERNAL_ERROR, "could not record consent"
        if not belongs:
            logger.warning(
                "/capture/audio refused: meeting %s is not a meeting of org %s",
                claims.meeting_id,
                claims.org_id,
            )
            return CLOSE_TENANT_FORBIDDEN, "meeting does not belong to org"

        tab = init.channel == CHANNEL_TAB
        try:
            lease = await self.tracker.attach_participant(
                org_id=claims.org_id,
                meeting_id=claims.meeting_id,
                participant_identity=participant_identity_for(claims, init.channel),
                diarize=tab,
                speaker_prefix=TAB_SPEAKER_PREFIX if tab else None,
            )
        except TenantMismatchError:
            return CLOSE_TENANT_FORBIDDEN, "meeting session belongs to a different org"
        except ComplianceGateClosedError:
            return CLOSE_COMPLIANCE_GATE, "compliance gate closed; audio dropped"
        except Exception:  # noqa: BLE001 - provider/DB open failure
            logger.exception(
                "/capture/audio meeting=%s: could not open transcription session",
                claims.meeting_id,
            )
            return CLOSE_INTERNAL_ERROR, "transcription provider unavailable"

        logger.info(
            "/capture/audio streaming meeting=%s org=%s source=%s channel=%s connection=%s",
            claims.meeting_id,
            claims.org_id,
            claims.source,
            init.channel,
            lease.connection_id,
        )
        try:
            await websocket.send_text(
                json.dumps(
                    {"type": "ready", "meeting_id": claims.meeting_id, "channel": init.channel}
                )
            )
        except Exception as exc:  # noqa: BLE001 - the browser left before it heard "ready"
            await self.tracker.detach_participant(lease)
            raise _ClientGone from exc
        try:
            return await self._stream(websocket, lease, stats)
        finally:
            await self.tracker.detach_participant(lease)

    async def _read_capture_init(self, websocket: WebSocket) -> tuple[CaptureInit, CaptureClaims]:
        try:
            message = await asyncio.wait_for(
                websocket.receive(), timeout=self.config.init_timeout_seconds
            )
        except TimeoutError as exc:
            raise _Refuse(CLOSE_TIMEOUT, "init frame not received in time") from exc
        if message["type"] == "websocket.disconnect":
            raise _ClientGone
        raw = message.get("text")
        if raw is None:
            raise _Refuse(CLOSE_BAD_INIT, "first frame must be a JSON text init frame")
        init = parse_capture_init(raw)
        try:
            claims = verify_capture_token(init.token, self.secret)
        except CaptureTokenError as exc:
            logger.warning("/capture/audio refused: %s", exc)
            raise _Refuse(CLOSE_UNAUTHORIZED, str(exc)) from exc
        return init, claims

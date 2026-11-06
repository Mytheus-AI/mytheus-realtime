"""`/internal/audio`: the server-side audio hook into `transcription-service`.

THE WIRE CONTRACT (the sender, `meeting-service`'s audio tap in `mytheus-backend`, conforms to
this; change it here and in `README.md` together, never one side alone):

1. Handshake. A WebSocket upgrade to `/internal/audio` carrying the header
   `X-Internal-Auth: <INTERNAL_AUDIO_TOKEN>`. The value is compared in constant time against
   this service's `INTERNAL_AUDIO_TOKEN` env var. If the header is missing or wrong, OR the
   env var is unset on this side (fail closed), the upgrade is refused before `accept()`:
   the sender sees an HTTP 403 handshake failure and no session state is ever touched.

2. Init frame. The FIRST frame after the upgrade MUST be a text frame holding one JSON object:

       {"org_id": "<uuid>", "meeting_id": "<uuid>", "participant_identity": "<non-empty str>",
        "sample_rate": 16000, "encoding": "pcm_s16le"}

   All five keys are required; `org_id`/`meeting_id` must be UUIDs; `participant_identity` is
   a non-empty string of at most 256 chars: the `users.id` of the participant whose audio
   track this connection carries. It is the speaker attribution for everything this
   connection produces -- persisted as `transcript_chunks.speaker_id` (and the Avro
   `speaker_id`, and the caption's `speakerId`), which the existing downstream
   `meeting_speakers` `(org_id, meeting_id, speaker_id)` mapping resolves; diarization is
   not relied on (the provider stream is opened with `diarize=false`). `sample_rate` must be
   exactly `16000` and `encoding` exactly `"pcm_s16le"`. Unknown extra keys are ignored. It
   must arrive within `AUDIO_INIT_TIMEOUT_SECONDS` (default 10 s). On reconnect the sender
   resends the same init frame.

3. Audio frames. Every subsequent BINARY frame is raw PCM: 16 kHz, mono, signed 16-bit
   little-endian, no header. A frame must be an even number of bytes and at most
   `AUDIO_MAX_FRAME_BYTES` (default 65536). 10-100 ms per frame (320-3200 bytes) is the
   intended size; the sender uses about 10 ms. Empty frames are ignored.

4. Control frames. Any later TEXT frame is a JSON control message and is NEVER fed to the
   provider as audio. `{"type": "stop"}` ends the stream gracefully: queued audio is flushed
   to the provider, then the server closes with 1000. Any other text frame is ignored.

5. One connection per PARTICIPANT audio track. A meeting has several concurrent connections,
   one per participant, and each gets its own provider stream. The supersede key is
   `(org_id, meeting_id, participant_identity)`: a newer connection for the SAME participant
   supersedes only that participant's older connection (the older is closed with 4409), so a
   sender that reconnects need not wait for the old socket to time out; connections for
   other participants are unaffected. The meeting's session closes when its last
   participant connection ends.

6. The server never sends data frames back; the only server-to-sender signal is the close.

Close codes (server -> sender), after `accept()`:

    1000  normal: `stop` received, or the stream was ended          -> do not reconnect
    1001  this replica is shutting down                            -> reconnect with backoff
    1007  binary frame with an odd byte length                     -> sender bug
    1009  binary frame larger than AUDIO_MAX_FRAME_BYTES           -> sender bug
    1011  internal error (DB, provider gone after reconnects, or
          no provider configured on this pod)                      -> reconnect with backoff
    4400  invalid/missing init frame                               -> sender bug
    4403  tenant check failed (meeting not in org)                 -> do not reconnect
    4408  timeout: no init frame in time, or idle too long         -> reconnect if audio continues
    4409  superseded by a newer connection for the same participant -> do not reconnect
    4451  compliance gate closed: audio dropped, nothing emitted   -> do not reconnect

Behaviour this module enforces:

- Provider selection fails closed: with no `DEEPGRAM_API_KEY` and no explicit
  `TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1`, the pod's provider is `UnavailableProvider` and every
  connection is closed 1011 right after the init frame, before any database access -- the
  fake can never transcribe a real tenant meeting by accident (`providers/factory.py`).
- Compliance gate (plan 3.5): if the selected provider is anything other than the in-process
  `FakeProvider`, `assert_compliance_gate_green()` runs BEFORE any database read, audit write,
  session open or provider contact. With `COMPLIANCE_GATE_GREEN = False` (today, and
  unbypassable -- see `compliance.py`) every such connection is closed with 4451 right after
  the init frame; no audio is forwarded, no caption, row or event is produced.
- Tenant isolation: `org_id`/`meeting_id` come from the authenticated internal sender, and
  `meeting_id` must belong to `org_id` (checked under `app.current_org` RLS context). Every DB
  write downstream runs in a `tenant_transaction` for that `org_id`
  (`persistence.process_utterance`), and a session opened for one org can never be attached
  by another (`SessionTracker.attach_participant`, 4403).
- Residency: each participant's provider stream opens with the tenant's `data_residency.region`
  (`residency.deepgram_endpoint_for_region`), on the first connection and on every reconnect.
- Backpressure: frames go through a bounded queue (`AUDIO_QUEUE_MAX_FRAMES`, default 100,
  about 2 s at 20 ms). When the provider falls behind, the OLDEST queued frame is dropped and
  counted (logged on the first drop and every 100th) -- silently buffering would only make
  captions later than the p95 < 2 s SLO allows.
- Provider drops are absorbed by `providers.resilient.ReconnectingStream` (backoff, ordering
  preserved); only when it gives up does this connection close with 1011.
- Logging: audio bytes and transcript text are never logged at any level here; INFO lines
  carry ids, close codes and counters only.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from starlette.websockets import WebSocket

from .compliance import ComplianceGateClosedError, assert_compliance_gate_green
from .config import Settings
from .db import tenant_transaction
from .providers.base import TranscriptionProvider, TranscriptionStream
from .providers.factory import UnavailableProvider
from .providers.fake import FakeProvider
from .providers.resilient import StreamClosedError
from .sessions import ParticipantLease, SessionTracker, TenantMismatchError
from .tenancy import meeting_belongs_to_org

logger = logging.getLogger(__name__)

AUTH_HEADER = "X-Internal-Auth"
SUPPORTED_SAMPLE_RATE = 16000
SUPPORTED_ENCODING = "pcm_s16le"
MAX_PARTICIPANT_IDENTITY_LEN = 256
_STOP_FLUSH_TIMEOUT_SECONDS = 5.0
_DROP_LOG_EVERY = 100

CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_POLICY_VIOLATION = 1008  # pre-accept refusal; the sender sees HTTP 403
CLOSE_INVALID_PAYLOAD = 1007
CLOSE_TOO_BIG = 1009
CLOSE_INTERNAL_ERROR = 1011
CLOSE_BAD_INIT = 4400
CLOSE_TENANT_FORBIDDEN = 4403
CLOSE_TIMEOUT = 4408
CLOSE_SUPERSEDED = 4409
CLOSE_COMPLIANCE_GATE = 4451


@dataclass(frozen=True)
class AudioInit:
    org_id: str
    meeting_id: str
    participant_identity: str
    sample_rate: int
    encoding: str


class InitFrameError(ValueError):
    """The init frame is not a valid contract init object; the message is safe to send as a
    close reason (it never echoes frame content)."""


def _require_uuid(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str):
        raise InitFrameError(f"init frame: {key} is required")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise InitFrameError(f"init frame: {key} must be a UUID") from exc


def parse_init_frame(raw: str) -> AudioInit:
    try:
        obj = json.loads(raw)
    except ValueError as exc:
        raise InitFrameError("init frame is not valid JSON") from exc
    if not isinstance(obj, dict):
        raise InitFrameError("init frame must be a JSON object")
    org_id = _require_uuid(obj, "org_id")
    meeting_id = _require_uuid(obj, "meeting_id")
    identity = obj.get("participant_identity")
    if not isinstance(identity, str) or not identity.strip():
        raise InitFrameError("init frame: participant_identity is required")
    if len(identity) > MAX_PARTICIPANT_IDENTITY_LEN:
        raise InitFrameError("init frame: participant_identity is too long")
    sample_rate = obj.get("sample_rate")
    if isinstance(sample_rate, bool) or sample_rate != SUPPORTED_SAMPLE_RATE:
        raise InitFrameError(f"init frame: sample_rate must be {SUPPORTED_SAMPLE_RATE}")
    if obj.get("encoding") != SUPPORTED_ENCODING:
        raise InitFrameError(f"init frame: encoding must be {SUPPORTED_ENCODING!r}")
    return AudioInit(
        org_id=org_id,
        meeting_id=meeting_id,
        participant_identity=identity,
        sample_rate=SUPPORTED_SAMPLE_RATE,
        encoding=SUPPORTED_ENCODING,
    )


def token_matches(presented: str | None, expected: str | None) -> bool:
    """Constant-time compare. Fails closed when no token is configured."""
    if not expected or presented is None:
        return False
    return hmac.compare_digest(presented.encode(), expected.encode())


PROVIDER_FAKE = "fake"
PROVIDER_UNAVAILABLE = "unavailable"
PROVIDER_EXTERNAL = "external"


def provider_mode_for(provider: TranscriptionProvider) -> str:
    """`fake`: the in-process fake (dev opt-in only). `unavailable`: no provider configured,
    fail closed. `external`: everything else sends audio to a third party, so the compliance
    gate applies -- including an unknown provider type (fail closed)."""
    if isinstance(provider, FakeProvider):
        return PROVIDER_FAKE
    if isinstance(provider, UnavailableProvider):
        return PROVIDER_UNAVAILABLE
    return PROVIDER_EXTERNAL


def provider_requires_compliance_gate(provider: TranscriptionProvider) -> bool:
    return provider_mode_for(provider) != PROVIDER_FAKE


@dataclass(frozen=True)
class AudioEndpointConfig:
    token: str | None
    init_timeout_seconds: float
    idle_timeout_seconds: float
    max_frame_bytes: int
    queue_max_frames: int

    def __repr__(self) -> str:
        return "AudioEndpointConfig(<redacted>)"

    @classmethod
    def from_settings(cls, settings: Settings) -> AudioEndpointConfig:
        return cls(
            token=settings.internal_audio_token,
            init_timeout_seconds=settings.audio_init_timeout_seconds,
            idle_timeout_seconds=settings.audio_idle_timeout_seconds,
            max_frame_bytes=settings.audio_max_frame_bytes,
            queue_max_frames=settings.audio_queue_max_frames,
        )


@dataclass
class ConnectionStats:
    meeting_id: str | None = None
    frames_received: int = 0
    bytes_received: int = 0
    frames_dropped: int = 0
    close_code: int | None = None
    close_reason: str | None = None


class _Close(Exception):
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


class _ClientGone(Exception):
    pass


@dataclass
class AudioHub:
    """Per-process owner of every `/internal/audio` connection: config, the tracker, the
    shutdown signal, and the set of live connections (for clean drain on shutdown)."""

    tracker: SessionTracker
    config: AudioEndpointConfig
    provider_mode: str
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)
    finished: list[ConnectionStats] = field(default_factory=list)
    _active: dict[int, asyncio.Event] = field(default_factory=dict)

    @property
    def active_connections(self) -> int:
        return len(self._active)

    async def drain(self, timeout: float) -> None:
        """Signals every live connection to close with 1001 and waits for them."""
        self.shutdown.set()
        waiters = [event.wait() for event in self._active.values()]
        if waiters:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.gather(*waiters), timeout=timeout)

    async def handle(self, websocket: WebSocket) -> None:
        if self.shutdown.is_set():
            await websocket.close(code=CLOSE_GOING_AWAY, reason="server shutting down")
            return
        if not token_matches(websocket.headers.get(AUTH_HEADER), self.config.token):
            if not self.config.token:
                logger.error("/internal/audio refused: INTERNAL_AUDIO_TOKEN is unset (fail closed)")
            else:
                logger.warning("/internal/audio refused: missing or invalid %s", AUTH_HEADER)
            await websocket.close(code=CLOSE_POLICY_VIOLATION, reason="unauthorized")
            return

        done = asyncio.Event()
        key = id(done)
        self._active[key] = done
        stats = ConnectionStats()
        try:
            await websocket.accept()
            code, reason = await self._serve(websocket, stats)
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
        del self.finished[:-100]  # keep only recent stats (tests/diagnostics)
        logger.info(
            "/internal/audio closed meeting=%s code=%s frames=%d bytes=%d dropped=%d",
            stats.meeting_id,
            code,
            stats.frames_received,
            stats.bytes_received,
            stats.frames_dropped,
        )

    async def _serve(self, websocket: WebSocket, stats: ConnectionStats) -> tuple[int, str]:
        try:
            init = await self._read_init(websocket)
        except _Close as close:
            return close.code, close.reason
        stats.meeting_id = init.meeting_id
        logger.debug(
            "/internal/audio init meeting=%s org=%s source=%s",
            init.meeting_id,
            init.org_id,
            init.participant_identity,
        )

        # No provider configured: fail closed before touching anything.
        if self.provider_mode == PROVIDER_UNAVAILABLE:
            logger.error(
                "/internal/audio meeting=%s refused: no transcription provider configured",
                init.meeting_id,
            )
            return CLOSE_INTERNAL_ERROR, "no transcription provider configured"

        # Compliance gate FIRST: before any DB read, audit write or provider contact.
        if self.provider_mode != PROVIDER_FAKE:
            try:
                assert_compliance_gate_green()
            except ComplianceGateClosedError:
                logger.warning(
                    "/internal/audio meeting=%s: compliance gate closed; audio dropped",
                    init.meeting_id,
                )
                return CLOSE_COMPLIANCE_GATE, "compliance gate closed; audio dropped"

        try:
            async with tenant_transaction(self.tracker.session_factory, init.org_id) as session:
                belongs = await meeting_belongs_to_org(session, init.org_id, init.meeting_id)
        except Exception:  # noqa: BLE001 - any DB failure is a 1011, never an open
            logger.exception("/internal/audio meeting=%s: tenant check failed", init.meeting_id)
            return CLOSE_INTERNAL_ERROR, "tenant check unavailable"
        if not belongs:
            logger.warning(
                "/internal/audio refused: meeting %s is not a meeting of org %s",
                init.meeting_id,
                init.org_id,
            )
            return CLOSE_TENANT_FORBIDDEN, "meeting does not belong to org"

        try:
            lease = await self.tracker.attach_participant(
                org_id=init.org_id,
                meeting_id=init.meeting_id,
                participant_identity=init.participant_identity,
            )
        except TenantMismatchError:
            return CLOSE_TENANT_FORBIDDEN, "meeting session belongs to a different org"
        except ComplianceGateClosedError:
            return CLOSE_COMPLIANCE_GATE, "compliance gate closed; audio dropped"
        except Exception:  # noqa: BLE001 - provider/DB open failure
            logger.exception(
                "/internal/audio meeting=%s: could not open transcription session",
                init.meeting_id,
            )
            return CLOSE_INTERNAL_ERROR, "transcription provider unavailable"

        logger.info(
            "/internal/audio streaming meeting=%s org=%s connection=%s",
            init.meeting_id,
            init.org_id,
            lease.connection_id,
        )
        try:
            return await self._stream(websocket, lease, stats)
        finally:
            await self.tracker.detach_participant(lease)

    async def _read_init(self, websocket: WebSocket) -> AudioInit:
        try:
            message = await asyncio.wait_for(
                websocket.receive(), timeout=self.config.init_timeout_seconds
            )
        except TimeoutError as exc:
            raise _Close(CLOSE_TIMEOUT, "init frame not received in time") from exc
        if message["type"] == "websocket.disconnect":
            raise _ClientGone
        raw = message.get("text")
        if raw is None:
            raise _Close(CLOSE_BAD_INIT, "first frame must be a JSON text init frame")
        try:
            return parse_init_frame(raw)
        except InitFrameError as exc:
            raise _Close(CLOSE_BAD_INIT, str(exc)) from exc

    async def _stream(
        self, websocket: WebSocket, lease: ParticipantLease, stats: ConnectionStats
    ) -> tuple[int, str]:
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=self.config.queue_max_frames)
        sender = asyncio.create_task(_send_loop(queue, lease.stream))
        stop_waiter = asyncio.create_task(_wait_any(self.shutdown, lease.superseded))
        try:
            while True:
                receive = asyncio.create_task(websocket.receive())
                done, _ = await asyncio.wait(
                    {receive, sender, stop_waiter},
                    timeout=self.config.idle_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receive not in done:
                    receive.cancel()
                    with contextlib.suppress(BaseException):
                        await receive
                    if not done:
                        return CLOSE_TIMEOUT, "idle timeout"
                    # Checked before the sender's outcome: superseding/shutdown closes this
                    # connection's stream, so the sender may fail first with StreamClosedError.
                    if self.shutdown.is_set():
                        return CLOSE_GOING_AWAY, "server shutting down"
                    if lease.superseded.is_set():
                        return CLOSE_SUPERSEDED, "superseded by a newer connection"
                    return _sender_outcome(sender, lease.meeting_id)

                message = receive.result()
                if message["type"] == "websocket.disconnect":
                    raise _ClientGone
                frame = message.get("bytes")
                if frame is not None:
                    if len(frame) > self.config.max_frame_bytes:
                        return CLOSE_TOO_BIG, "audio frame exceeds max size"
                    if len(frame) % 2:
                        return CLOSE_INVALID_PAYLOAD, "pcm_s16le frame has odd byte length"
                    if frame:
                        stats.frames_received += 1
                        stats.bytes_received += len(frame)
                        self._enqueue(queue, frame, stats)
                    continue
                if _is_stop(message.get("text")):
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(queue.join(), timeout=_STOP_FLUSH_TIMEOUT_SECONDS)
                    return CLOSE_NORMAL, "stop received"
                # Any other control frame is ignored: it is never audio.
        finally:
            for task in (sender, stop_waiter):
                task.cancel()
            await asyncio.gather(sender, stop_waiter, return_exceptions=True)

    def _enqueue(self, queue: asyncio.Queue[bytes], frame: bytes, stats: ConnectionStats) -> None:
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
                queue.task_done()
            stats.frames_dropped += 1
            if stats.frames_dropped == 1 or stats.frames_dropped % _DROP_LOG_EVERY == 0:
                logger.warning(
                    "/internal/audio meeting=%s: provider backpressure, dropped %d oldest "
                    "frame(s) so far",
                    stats.meeting_id,
                    stats.frames_dropped,
                )
        queue.put_nowait(frame)


async def _send_loop(queue: asyncio.Queue[bytes], stream: TranscriptionStream) -> None:
    """The single sender: frames reach the provider strictly in arrival order."""
    while True:
        frame = await queue.get()
        try:
            await stream.send_audio(frame)
        finally:
            queue.task_done()


async def _wait_any(*events: asyncio.Event) -> None:
    waiters = [asyncio.create_task(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()


def _sender_outcome(sender: asyncio.Task[None], meeting_id: str) -> tuple[int, str]:
    exc = None if sender.cancelled() else sender.exception()
    if isinstance(exc, StreamClosedError):
        return CLOSE_NORMAL, "transcription stream ended"
    logger.error("/internal/audio meeting=%s: provider send failed", meeting_id, exc_info=exc)
    return CLOSE_INTERNAL_ERROR, "transcription provider unavailable"


def _is_stop(raw: str | None) -> bool:
    if raw is None:
        return False
    try:
        obj = json.loads(raw)
    except ValueError:
        return False
    return isinstance(obj, dict) and obj.get("type") == "stop"

"""Consumes `admin-events` `erasure_requested` and finishes a data erasure in `transcript_chunks`.

An admin's erasure (Workspace > Data and privacy) runs in `mytheus-backend`: admin-ops-service
has core-api erase its own tables, then records `data_erasure.completed` and emits
`admin-events` `erasure_requested` (`target_type` `person` or `meeting`, `target_id` the
`users.id` or meeting id, `detail_json` `{"erasure_id", "scope"}`) for the stores it does not
own. `transcript_chunks` is this service's, so this consumer erases it:

- MEETING: every `transcript_chunks` row of the meeting is DELETEd. core-api has already
  soft-deleted the meeting (it refuses a live one), so nothing writes new rows for it.
- PERSON: their attribution is removed, the words stay. core-api's rule
  (`services/core_api/routes/erasure.py`): it deletes the person's `meeting_speakers` rows "so
  transcript lines they spoke fall back to an unnamed speaker (the words themselves are the
  meeting's record, owned by transcription-service)". That already anonymizes every diarized
  label (`1`, `2`, ...) mapped to them. What core-api cannot reach is the line whose
  `speaker_id` IS their `users.id` (a single-speaker track or the capture mic, see
  `sessions.speaker_for`): those rows get `speaker_id = 'erased'` (`ERASED_SPEAKER`; the column
  is NOT NULL, and a non-UUID label is what the transcript view and the agent runtime already
  treat as an unnamed speaker). The text is kept, exactly as core-api keeps the meeting record.

`org_id` is the envelope header's, never a payload field, and every statement also carries an
explicit `org_id` predicate on top of RLS (the local superuser connection bypasses RLS).
`target_id` must parse as a UUID; anything else is poison.

Completion. In the SAME transaction as the effect, one `admin-events` `erasure_completed`
outbox row (drained by `mytheus-backend`'s relay like every row this service writes):
`target_type`/`target_id` copied from the request, `detail_json` `{"erasure_id", "store":
"transcript_chunks", "service": "transcription-service", "counts": {...}}`. Its idempotency key
`erasure_completed:{erasure_id}:transcript_chunks` is stable per request and store, so the
outbox's `(topic, idempotency_key)` unique index makes a re-application emit nothing new.

Idempotency, the canon two layers (`mytheus-events` CLAUDE.md "Inbox dedupe on consume"):
1. Redis `nexus:{org_id}:dedupe:admin-events:{CONSUMER_GROUP}:{idempotency_key}`, 24 h, written
   only AFTER the transaction commits. (The meeting-room consumer sets its key first; here a
   failed attempt must never leave a key that makes the retry look like a duplicate, or the
   erasure would silently never finish.)
2. Durable: this service has no inbox table, so the completion outbox row is the inbox record.
   It commits atomically with the effect; when it already exists the request was applied and
   the event is a duplicate. Should the relay ever prune published rows, a very late redelivery
   re-runs statements that are idempotent by construction (a delete, or a rename to `erased`).

Failures. Poison (missing header, `org_id` or `idempotency_key`, a non-UUID target, a value that
is not a record) goes to `admin-events.transcription-service.erasure.dlq` (group-scoped like
`mytheus-intelligence`'s consumers, so a redrive of one group's DLQ never replays into
another's). A transient failure (Postgres down) is retried in place, 5 attempts with bounded
exponential backoff and jitter, offset not advanced; after that it is dead-lettered as
`MAX_RETRIES` and the store shows as not completed until an operator redrives it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .db import tenant_transaction
from .events import build_envelope_header
from .outbox import enqueue

logger = logging.getLogger(__name__)

TOPIC = "admin-events"
SCHEMA_SUBJECT = "admin-events-value"
CONSUMER_GROUP = "transcription-service.erasure"
DLQ_TOPIC = f"{TOPIC}.{CONSUMER_GROUP}.dlq"
PRODUCER_NAME = "transcription-service"
STORE = "transcript_chunks"
ERASED_SPEAKER = "erased"
REQUESTED = "erasure_requested"
COMPLETED = "erasure_completed"

MAX_ATTEMPTS = 5
_BACKOFF_BASE_SECONDS = 0.5
_BACKOFF_CAP_SECONDS = 8.0
_DEDUPE_TTL_SECONDS = 24 * 60 * 60
_POLL_TIMEOUT_SECONDS = 1.0

Outcome = Literal["applied", "duplicate", "ignored"]
Sleep = Callable[[float], Awaitable[None]]


class PoisonMessage(Exception):
    """The message can never be applied: routed to the DLQ and committed past."""


class DlqProducer(Protocol):
    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None: ...
    def flush(self, timeout: float = 10) -> int: ...


@dataclass(frozen=True)
class ErasureRequest:
    org_id: str
    kind: Literal["person", "meeting"]
    target_id: str
    erasure_id: str
    actor_id: str
    idempotency_key: str

    @property
    def completion_key(self) -> str:
        return f"{COMPLETED}:{self.erasure_id}:{STORE}"


def _uuid_str(value: Any, what: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError) as exc:
        raise PoisonMessage(f"admin-events {what} is not a UUID: {value!r}") from exc


def parse_request(event: Any) -> ErasureRequest | None:
    """The erasure this event asks for, or None when the event is not one this store acts on
    (another `event_type`, or a `target_type` other than person/meeting). Raises
    `PoisonMessage` for a value that cannot be applied safely."""
    if not isinstance(event, dict):
        raise PoisonMessage(f"admin-events value did not decode to a record: {event!r}")
    header = event.get("header") or {}
    if not header.get("org_id") or not header.get("idempotency_key"):
        raise PoisonMessage("admin-events message missing header org_id/idempotency_key")
    org_id = _uuid_str(header["org_id"], "header org_id")
    if event.get("event_type") != REQUESTED:
        return None
    kind = event.get("target_type")
    if kind not in ("person", "meeting"):
        return None
    target_id = _uuid_str(event.get("target_id"), "target_id")
    try:
        detail = json.loads(event.get("detail_json") or "{}")
    except ValueError:
        detail = {}
    raw_erasure = detail.get("erasure_id") if isinstance(detail, dict) else None
    try:
        erasure_id = str(uuid.UUID(str(raw_erasure)))
    except (TypeError, ValueError):
        # An older producer without an erasure id: the request's own key still names it.
        erasure_id = str(header["idempotency_key"])
    return ErasureRequest(
        org_id=org_id,
        kind=kind,
        target_id=target_id,
        erasure_id=erasure_id,
        actor_id=str(event.get("actor_id") or ""),
        idempotency_key=str(header["idempotency_key"]),
    )


def dedupe_key(org_id: str, idempotency_key: str) -> str:
    return f"nexus:{org_id}:dedupe:{TOPIC}:{CONSUMER_GROUP}:{idempotency_key}"


async def erase(session: AsyncSession, req: ErasureRequest) -> dict[str, int]:
    """The effect, inside the caller's tenant transaction. Returns the row counts."""
    params = {"org_id": req.org_id, "target": req.target_id}
    if req.kind == "meeting":
        result = await session.execute(
            text("DELETE FROM transcript_chunks WHERE org_id = :org_id AND meeting_id = :target"),
            params,
        )
        return {"deleted": int(result.rowcount or 0)}  # type: ignore[attr-defined]
    result = await session.execute(
        text(
            "UPDATE transcript_chunks SET speaker_id = :erased "
            "WHERE org_id = :org_id AND lower(speaker_id) = :target"
        ),
        {**params, "erased": ERASED_SPEAKER},
    )
    return {"anonymized": int(result.rowcount or 0)}  # type: ignore[attr-defined]


async def _already_completed(session: AsyncSession, req: ErasureRequest) -> bool:
    row = (
        await session.execute(
            text("SELECT 1 FROM outbox WHERE topic = :topic AND idempotency_key = :key"),
            {"topic": TOPIC, "key": req.completion_key},
        )
    ).first()
    return row is not None


def completion_payload(req: ErasureRequest, counts: dict[str, int]) -> dict[str, Any]:
    """The full `AdminEvent` value (header + fields) for this store's `erasure_completed`."""
    header = build_envelope_header(
        org_id=req.org_id,
        producer=PRODUCER_NAME,
        idempotency_key=req.completion_key,
        causation_id=None,
        correlation_id=req.erasure_id,
    )
    detail = {
        "erasure_id": req.erasure_id,
        "store": STORE,
        "service": PRODUCER_NAME,
        "counts": counts,
    }
    return {
        "header": header.model_dump(),
        "event_type": COMPLETED,
        "actor_id": req.actor_id,
        "target_type": req.kind,
        "target_id": req.target_id,
        "detail_json": json.dumps(detail, sort_keys=True),
    }


async def handle_erasure_event(
    session_factory: async_sessionmaker[AsyncSession], redis: Redis, event: Any
) -> Outcome:
    """Apply one decoded `admin-events` value. Raises `PoisonMessage` for an unusable one; any
    other exception is transient and leaves no dedupe mark behind."""
    req = parse_request(event)
    if req is None:
        return "ignored"
    key = dedupe_key(req.org_id, req.idempotency_key)
    if await redis.exists(key):
        logger.info("erasure: duplicate (redis) %s", req.idempotency_key)
        return "duplicate"
    async with tenant_transaction(session_factory, req.org_id) as session:
        if await _already_completed(session, req):
            outcome: Outcome = "duplicate"
        else:
            counts = await erase(session, req)
            await enqueue(
                session,
                org_id=req.org_id,
                topic=TOPIC,
                partition_key=req.org_id,
                idempotency_key=req.completion_key,
                schema_subject=SCHEMA_SUBJECT,
                payload=completion_payload(req, counts),
            )
            logger.info(
                "erasure: %s %s erased from %s for org %s: %s",
                req.kind,
                req.target_id,
                STORE,
                req.org_id,
                counts,
            )
            outcome = "applied"
    await redis.set(key, "1", ex=_DEDUPE_TTL_SECONDS)
    return outcome


def route_to_dlq(
    producer: DlqProducer,
    *,
    key: bytes | None,
    raw_value: bytes | None,
    failure_class: str,
    error: Exception,
) -> None:
    producer.produce(
        topic=DLQ_TOPIC,
        key=key,
        value=json.dumps(
            {
                "original_topic": TOPIC,
                "consumer_group": CONSUMER_GROUP,
                "failure_class": failure_class,
                "error": str(error),
                "raw": raw_value.hex() if raw_value else None,
            }
        ).encode(),
    )
    producer.flush(10)


def _backoff(attempt: int) -> float:
    base = min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * float(2 ** (attempt - 1)))
    return float(base / 2 + random.uniform(0, base / 2))  # noqa: S311 - jitter, not security


async def process_decoded_message(
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    event: Any,
    *,
    raw_value: bytes | None,
    key: bytes | None,
    dlq_producer: DlqProducer,
    sleep: Sleep = asyncio.sleep,
    stop: asyncio.Event | None = None,
) -> Outcome | Literal["dlq", "stopped"]:
    """One message to its effect, or to the DLQ. Returns only when the offset may be committed,
    except `stopped` (shutdown mid-retry): the caller must NOT commit then, so the message is
    redelivered to whichever pod takes the partition."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return await handle_erasure_event(session_factory, redis, event)
        except PoisonMessage as exc:
            logger.error("erasure: poison message routed to DLQ: %s", exc)
            route_to_dlq(
                dlq_producer,
                key=key,
                raw_value=raw_value,
                failure_class="BUSINESS_INVALID",
                error=exc,
            )
            return "dlq"
        except Exception as exc:  # noqa: BLE001 - transient: retry in place, then DLQ
            if attempt == MAX_ATTEMPTS:
                logger.error("erasure: giving up after %d attempts: %s", attempt, exc)
                route_to_dlq(
                    dlq_producer,
                    key=key,
                    raw_value=raw_value,
                    failure_class="MAX_RETRIES",
                    error=exc,
                )
                return "dlq"
            if stop is not None and stop.is_set():
                return "stopped"
            delay = _backoff(attempt)
            logger.warning(
                "erasure: attempt %d failed (%s); retrying in %.1fs", attempt, exc, delay
            )
            await sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


async def run_consumer(
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    *,
    brokers: str,
    schema_registry_url: str,
    stop: asyncio.Event | None = None,
) -> None:  # pragma: no cover - thin harness, exercised via process_decoded_message
    """The confluent-kafka loop for `admin-events`, same shape and the same reasons as
    `meeting_room_consumer.run_consumer`: blocking `poll` off the event loop (this process also
    serves `/internal/audio`), commit only after the effect, stop on the flag rather than by
    cancellation."""
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    loop = asyncio.get_running_loop()
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    dlq_producer = KafkaProducer(
        {"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True}
    )
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": schema_registry_url}))
    try:
        while stop is None or not stop.is_set():
            msg = await loop.run_in_executor(None, consumer.poll, _POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            if msg.error():
                logger.error("admin-events consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            try:
                event = deserializer(
                    raw_value, SerializationContext(msg.topic() or TOPIC, MessageField.VALUE)
                )
            except Exception as exc:  # noqa: BLE001 - undecodable is poison
                route_to_dlq(
                    dlq_producer,
                    key=msg.key(),
                    raw_value=raw_value,
                    failure_class="DESERIALIZE",
                    error=exc,
                )
                consumer.commit(message=msg)
                continue
            outcome = await process_decoded_message(
                session_factory,
                redis,
                event,
                raw_value=raw_value,
                key=msg.key(),
                dlq_producer=dlq_producer,
                stop=stop,
            )
            if outcome == "stopped":
                break
            consumer.commit(message=msg)
    finally:
        consumer.close()

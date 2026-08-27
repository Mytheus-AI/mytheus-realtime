"""Consumes `meeting-room-events` (produced by `meeting-service`) and drives `SessionTracker`.

Idempotency (plan 3.6 step 8: "Ensure idempotency so a redelivered event never opens a
duplicate stream"): two layers, matching the pattern documented in `mytheus-events/CLAUDE.md`
("Inbox dedupe on consume") --

1. `SessionTracker` itself is idempotent in effect: a redelivered `joined` for an
   already-open meeting increments a counter and returns without opening a second stream (see
   `sessions.py`).
2. A Redis dedupe cache in front of it, keyed `nexus:{org_id}:dedupe:meeting-room-events:
   {idempotency_key}` (24h TTL, per this program's documented dedupe-key shape), so a
   redelivery that arrives while THIS pod still holds the meeting open is rejected before it
   even reaches `SessionTracker` -- belt-and-braces, since `SessionTracker`'s in-memory state
   does not survive a pod restart the way the plan's "ensure idempotency" implies redelivery
   handling should.

No durable inbox table: unlike `mytheus-backend`'s consumers, this service holds no durable
store of its own to add an inbox row to (mirrors Task 6's `presence-events` producer decision
on `realtime-gateway` -- no table, no outbox/inbox to bolt onto). The Redis cache plus
`SessionTracker`'s own idempotent state machine is the substitute.

Manual offset commit after the effect (open/close the stream), never before -- an offset
committed before the effect and then a crash would silently skip that meeting's transcription
forever, since Kafka would never redeliver it.

I/O loop: `run_consumer` is the confluent-kafka `Consumer` + `AvroDeserializer` harness
(subscribe/poll/commit), mirroring `mytheus-backend/services/meeting_service/consumer.py`.
`process_decoded_message` is the testable seam between the two -- routes a `PoisonMessage`
(missing header/`org_id`/`idempotency_key`/`meeting_id`/`event_type`, or a value that is not a
record at all) to `{TOPIC}.dlq` instead of crash-looping the partition, same as that module's
pattern.

ONE DELIBERATE DIVERGENCE from `meeting_service/consumer.py`: that consumer owns its whole
process (`asyncio.run(run_consumer())`), so a blocking `Consumer.poll` there stalls an event
loop with nothing else on it. Here the loop MUST share a process with the FastAPI app --
`SessionTracker` holds the open `TranscriptionStream` for each meeting in memory, and
`/internal/audio` (the LiveKit audio hook) has to push audio into that same in-process object,
so a separate consumer process could not reach it. That makes `Consumer.poll`'s blocking
behaviour load-bearing in a way it is not in the backend: called directly it would freeze the
audio hook for up to `_POLL_TIMEOUT_SECONDS` at a time, continuously, against a caption SLO of
p95 < 2 s. It is therefore run in a worker thread via `run_in_executor` (librdkafka releases
the GIL while waiting, so the loop is genuinely free). `Consumer.commit` is left on the event
loop on purpose: it defaults to `asynchronous=True` and only enqueues the offset, so it does
not block -- do not "fix" it into a synchronous commit without moving it to the executor too.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Protocol

from redis.asyncio import Redis

from .sessions import SessionTracker

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "transcription-service"
TOPIC = "meeting-room-events"
_DEDUPE_TTL_SECONDS = 24 * 60 * 60
_POLL_TIMEOUT_SECONDS = 1.0


class PoisonMessage(Exception):
    """The message cannot be safely applied (missing header, missing `meeting_id`/
    `event_type`, or a value that did not decode to a record). Routed to `{TOPIC}.dlq` and
    committed past, never re-raised into the poll loop -- see
    `mytheus-backend/services/meeting_service/consumer.py` for the identical pattern this
    mirrors."""


class DlqProducer(Protocol):
    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None: ...
    def flush(self, timeout: float = 10) -> int: ...


def route_to_dlq(
    producer: DlqProducer,
    *,
    key: bytes | None,
    raw_value: bytes | None,
    error: Exception,
) -> None:
    producer.produce(
        topic=f"{TOPIC}.dlq",
        key=key,
        value=json.dumps(
            {"error": str(error), "raw": raw_value.hex() if raw_value else None}
        ).encode(),
    )
    producer.flush(10)


def _validate(event: dict[str, Any]) -> None:
    header = event.get("header") or {}
    if not header.get("org_id") or not header.get("idempotency_key"):
        raise PoisonMessage(
            f"meeting-room-events message missing header org_id/idempotency_key: {event!r}"
        )
    if not event.get("meeting_id") or not event.get("event_type"):
        raise PoisonMessage(
            f"meeting-room-events message missing meeting_id or event_type: {event!r}"
        )


def _dedupe_key(org_id: str, idempotency_key: str) -> str:
    return f"nexus:{org_id}:dedupe:{TOPIC}:{idempotency_key}"


async def handle_meeting_room_event(
    tracker: SessionTracker, redis: Redis, event: dict[str, Any]
) -> bool:
    """`event` is the decoded Avro value (header + payload fields flattened, matching
    `MeetingRoomEvent`). Returns `True` iff this call had an effect (as opposed to being
    rejected as a duplicate or an event type this service ignores).

    Raises `PoisonMessage` if the header or body cannot be safely applied."""
    _validate(event)
    header = event["header"]
    org_id = header["org_id"]
    idempotency_key = header["idempotency_key"]

    accepted = await redis.set(
        _dedupe_key(org_id, idempotency_key), "1", nx=True, ex=_DEDUPE_TTL_SECONDS
    )
    if not accepted:
        logger.info("meeting-room-events: duplicate %s, skipping", idempotency_key)
        return False

    meeting_id = event["meeting_id"]
    event_type = event["event_type"]

    if event_type == "joined":
        return await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    if event_type == "left":
        return await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    # "recording_ready": no transcription-session action; acknowledged (dedupe key still set)
    # so a redelivery doesn't reprocess it, but nothing to do here today.
    return False


def decode_meeting_room_event(raw_json: bytes) -> dict[str, Any]:
    """Test/dev seam: decodes a JSON-shaped stand-in for the Avro value, for tests that don't
    want to stand up a Schema Registry. The real path is `run_consumer`'s `AvroDeserializer`
    against the registered `meeting-room-events-value` subject."""
    return dict(json.loads(raw_json))


async def process_decoded_message(
    tracker: SessionTracker,
    redis: Redis,
    event: Any,
    *,
    raw_value: bytes | None,
    key: bytes | None,
    dlq_producer: DlqProducer,
) -> None:
    """One already-Avro-decoded `meeting-room-events` message through to `SessionTracker`, or
    to the DLQ. Extracted out of `run_consumer`'s poll loop so the DLQ path is directly
    testable without a live broker, mirroring
    `mytheus-backend/services/meeting_service/consumer.py::process_decoded_message`.

    `event` is typed `Any` because that is honestly what `AvroDeserializer` hands back -- a
    tombstone (null value) decodes to `None`, which would make `_validate`'s `.get` raise an
    `AttributeError` rather than a `PoisonMessage` and so kill the whole loop (and with it this
    pod's transcription) instead of dead-lettering one message. The `isinstance` check below is
    what converts that into the ordinary DLQ path."""
    try:
        if not isinstance(event, dict):
            raise PoisonMessage(f"meeting-room-events value did not decode to a record: {event!r}")
        await handle_meeting_room_event(tracker, redis, event)
    except PoisonMessage as exc:
        logger.error("meeting-room-events: poison message routed to DLQ: %s", exc)
        route_to_dlq(dlq_producer, key=key, raw_value=raw_value, error=exc)


async def run_consumer(
    tracker: SessionTracker,
    redis: Redis,
    *,
    brokers: str,
    schema_registry_url: str,
    stop: asyncio.Event | None = None,
) -> None:  # pragma: no cover - thin harness, exercised via process_decoded_message
    """The confluent-kafka `Consumer` + `AvroDeserializer` I/O loop against the registered
    `meeting-room-events-value` subject: subscribe/poll/commit, same shape as
    `mytheus-backend/services/meeting_service/consumer.py::run_consumer`. Manual offset commit
    happens only AFTER `process_decoded_message` returns (see module docstring) -- never
    before, so a crash mid-effect gets the message redelivered rather than silently skipped.

    `brokers`/`schema_registry_url` are parameters rather than direct `os.environ` reads
    because `config.py` is this service's documented single reader of the environment ("One
    place that reads `os.environ` so tests can construct a `Settings` directly"); `main.py`
    already has the loaded `Settings` and passes them through.

    Stops when `stop` is set, NOT by cancellation, and this is load-bearing rather than
    stylistic. Cancelling would surface at the `run_in_executor` await and run `finally:
    consumer.close()` on the event loop while the worker thread was still inside `poll` --
    concurrent use of one `Consumer` from two threads, which confluent-kafka does not support
    and which is a known segfault hazard. Waiting for the flag instead means the last `poll`
    has always returned before `close()` is called, and an in-flight message finishes and
    commits first (a clean drain). Worst case the loop takes `_POLL_TIMEOUT_SECONDS` plus one
    message's processing to notice; `main.py` allows for that before falling back to cancelling.
    With `stop=None` it runs forever, which is what a standalone/manual run wants.
    """
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
            # Blocking call, deliberately off the event loop -- see the module docstring's
            # "ONE DELIBERATE DIVERGENCE" note. Calling `consumer.poll` directly here would
            # stall `/internal/audio` in this same process for up to a second at a time.
            msg = await loop.run_in_executor(None, consumer.poll, _POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            if msg.error():
                logger.error("meeting-room-events consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            topic_name = msg.topic() or TOPIC
            event = deserializer(raw_value, SerializationContext(topic_name, MessageField.VALUE))
            await process_decoded_message(
                tracker,
                redis,
                event,
                raw_value=raw_value,
                key=msg.key(),
                dlq_producer=dlq_producer,
            )
            # Defaults to `asynchronous=True`: enqueues the offset without blocking, so it can
            # stay on the event loop. At-least-once still holds -- an offset lost to a crash
            # means redelivery, which the Redis dedupe above absorbs.
            consumer.commit(message=msg)
    finally:
        consumer.close()

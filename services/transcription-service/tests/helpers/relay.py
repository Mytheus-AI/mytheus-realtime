"""Test-only one-shot outbox drain: Avro-encodes unpublished `outbox` rows for a given org
against the real Schema Registry and produces them to the real Redpanda broker, then marks
them published. Mirrors `mytheus-backend/libs/shared/relay.py`'s `relay_once` shape (same
`SELECT ... WHERE published_at IS NULL` / `Avro-encode / produce / mark published` sequence)
but is NOT a copy of it (separate repo, no workspace link -- same reasoning as
`events/envelope.py`) and is intentionally narrower: single-batch, no retry/backoff/DLQ
machinery, because its only job is to give the load-bearing test ("assert both zeros ...
against a real database and a real broker") a real broker to assert against, using this
program's actual production relay-drain shape rather than reaching into `mytheus-backend`'s
process. Production draining of rows this service inserts is `mytheus-backend`'s already-running
relay (`services/relay/main.py`), which drains the shared `outbox` table generically
regardless of producing service -- this repo does not run a second relay in production; see
`outbox.py`'s docstring.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def drain_outbox_for_org(
    session: AsyncSession, *, org_id: str, brokers: str, schema_registry_url: str
) -> list[dict[str, Any]]:
    """Drains every unpublished outbox row for `org_id`, returns the rows that were produced
    (as dicts: id, topic, partition_key, payload, PLUS the real broker `partition`/`offset`
    from the delivery report, so a test can seek directly to the message it just produced
    instead of scanning a shared, heavily-partitioned topic from the earliest offset -- this
    topic is shared with every other agent's concurrent test traffic in this environment, so
    an unbounded earliest-offset scan is neither fast nor deterministic)."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, topic, partition_key, schema_subject, payload FROM outbox "
                    "WHERE org_id = :org_id AND published_at IS NULL ORDER BY created_at"
                ),
                {"org_id": org_id},
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        return []

    producer = Producer({"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True})
    sr = SchemaRegistryClient({"url": schema_registry_url})
    serializers: dict[str, AvroSerializer] = {}
    produced: list[dict[str, Any]] = []
    ids: list[int] = []
    delivery_errors: list[Exception] = []

    def _on_delivery(err: Exception | None, msg: Any, produced_row: dict[str, Any]) -> None:
        if err is not None:
            delivery_errors.append(err)
            return
        produced_row["partition"] = msg.partition()
        produced_row["offset"] = msg.offset()

    for r in rows:
        subject = r["schema_subject"]
        if subject not in serializers:
            serializers[subject] = AvroSerializer(
                sr,
                schema_str=None,
                conf={"auto.register.schemas": False, "use.latest.version": True},
            )
        topic = subject[:-6] if subject.endswith("-value") else subject
        ctx = SerializationContext(topic, MessageField.VALUE)
        value = serializers[subject](r["payload"], ctx)
        produced_row = dict(r)
        producer.produce(
            topic=r["topic"],
            key=r["partition_key"].encode(),
            value=value,
            callback=lambda err, msg, _row=produced_row: _on_delivery(err, msg, _row),
        )
        ids.append(r["id"])
        produced.append(produced_row)

    remaining = producer.flush(10)
    assert remaining == 0, f"{remaining} message(s) undelivered to the real broker"
    assert delivery_errors == [], f"delivery errors from the real broker: {delivery_errors}"
    assert all("offset" in p for p in produced), "a delivery callback never fired"

    await session.execute(
        text("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)"), {"ids": ids}
    )
    return produced

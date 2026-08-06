"""Transactional-outbox insert against the SHARED `outbox` table (owned/migrated by
`mytheus-backend`, table `82d44163c933`, RLS'd `a1c4e7f20b18`). This is the one deliberate
cross-service WRITE in this service — see `db.py`'s docstring for why: `outbox` is
cross-cutting infrastructure every producer writes to by design (`mytheus-backend`'s own
`libs/shared/relay.py` drains it regardless of which service inserted a row), not a domain
table this service is reaching into.

Not a copy of `mytheus-backend/libs/shared/outbox.py` — that module lives in a different repo
and this service has no workspace link into it (same non-consumability reasoning as
`events/envelope.py`) — but it reproduces the exact row shape and the exact
`ON CONFLICT (topic, idempotency_key) DO NOTHING` idempotency guard, with one addition this
service's callers need and the original didn't expose: whether the row was newly inserted.
`persistence.py` uses that boolean as the single source of truth for "have I already durably
recorded this final" — see its docstring for why gating the `transcript_chunks` insert off of
it (in the same transaction) is what makes redelivery of a final a true no-op without needing
a cross-partition unique index on `transcript_chunks` itself (`transcript_chunks` is
range-partitioned on `created_at`, so a global uniqueness guarantee on `id` alone isn't
directly expressible there).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class OutboxResult:
    outbox_id: int
    is_new: bool


async def enqueue(
    session: AsyncSession,
    *,
    org_id: str,
    topic: str,
    partition_key: str,
    idempotency_key: str,
    schema_subject: str,
    payload: dict[str, object],
) -> OutboxResult:
    result = await session.execute(
        text(
            "INSERT INTO outbox "
            "(org_id, topic, partition_key, idempotency_key, schema_subject, payload) "
            "VALUES (:org_id, :topic, :partition_key, :idempotency_key, :schema_subject, "
            "CAST(:payload AS jsonb)) "
            "ON CONFLICT (topic, idempotency_key) DO NOTHING "
            "RETURNING id"
        ),
        {
            "org_id": org_id,
            "topic": topic,
            "partition_key": partition_key,
            "idempotency_key": idempotency_key,
            "schema_subject": schema_subject,
            "payload": json.dumps(payload),
        },
    )
    row = result.first()
    if row is not None:
        return OutboxResult(outbox_id=int(row[0]), is_new=True)
    existing = await session.execute(
        text("SELECT id FROM outbox WHERE topic = :topic AND idempotency_key = :ik"),
        {"topic": topic, "ik": idempotency_key},
    )
    return OutboxResult(outbox_id=int(existing.scalar_one()), is_new=False)

"""Shared event envelope for this service, mirrored field-for-field from
`mytheus-events/schemas/_envelope/EventEnvelopeHeader.avsc` (frozen field order, per
`07-event-architecture.md` section 3.1).

Why a hand-kept mirror instead of importing `mytheus_events.envelope` from the generated
Python client (`mytheus-events/clients/python/`): that package has no `pyproject.toml` or
`setup.py` of its own — it is unpublished codegen output living inside a separate git repo,
with no workspace link into this one (same situation Task 6 documented for the TypeScript
client in `libs/envelope.ts`). A relative cross-repo import would only resolve on a machine
that happens to have both repos checked out at exactly this relative path, which does not
hold in CI or a fresh clone, and there is no package index either repo publishes to. So:
mirror the schema here, exactly, rather than invent a payload shape or take on a fragile path
dependency. If the envelope schema changes, this file changes with it — the source of truth
stays `mytheus-events/schemas/_envelope/EventEnvelopeHeader.avsc`.
"""

from __future__ import annotations

import time
import uuid

from pydantic import BaseModel


class EventEnvelopeHeader(BaseModel):
    event_id: str
    idempotency_key: str
    org_id: str
    schema_version: int = 1
    producer: str
    occurred_at: int
    correlation_id: str | None = None
    causation_id: str | None = None


def build_envelope_header(
    *,
    org_id: str,
    producer: str,
    idempotency_key: str,
    schema_version: int = 1,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    event_id: str | None = None,
    occurred_at_micros: int | None = None,
) -> EventEnvelopeHeader:
    return EventEnvelopeHeader(
        event_id=event_id or str(uuid.uuid4()),
        idempotency_key=idempotency_key,
        org_id=org_id,
        schema_version=schema_version,
        producer=producer,
        occurred_at=(
            occurred_at_micros if occurred_at_micros is not None else int(time.time() * 1_000_000)
        ),
        correlation_id=correlation_id,
        causation_id=causation_id,
    )

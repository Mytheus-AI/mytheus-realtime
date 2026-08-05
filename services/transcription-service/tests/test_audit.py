"""Fix round 1, item 2: the compliance gate's audit-log acceptance check ("an audit-log entry
records the redaction-toggle and any cross-border decision"), against the real, shared
`audit_logs` table."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.audit import ACTION_SESSION_OPENED, record_session_decision
from transcription_service.db import set_current_org


@pytest.mark.asyncio
async def test_record_session_decision_writes_redaction_and_region(
    session_factory: async_sessionmaker[AsyncSession], tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            await record_session_decision(
                session,
                org_id=org_id,
                meeting_id=meeting_id,
                region="eu",
                pii_redaction_enabled=True,
            )

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT actor_type, action, target_type, target_id, metadata "
                            "FROM audit_logs WHERE org_id = :org_id AND target_id = :meeting_id"
                        ),
                        {"org_id": org_id, "meeting_id": meeting_id},
                    )
                )
                .mappings()
                .one()
            )

    assert row["actor_type"] == "system"
    assert row["action"] == ACTION_SESSION_OPENED
    assert row["target_type"] == "meeting"
    assert row["target_id"] == meeting_id
    assert row["metadata"]["pii_redaction_enabled"] is True
    assert row["metadata"]["region"] == "eu"


@pytest.mark.asyncio
async def test_session_open_writes_an_audit_entry(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    """End-to-end through `SessionTracker.on_participant_joined`, not just the direct
    `audit.py` unit above -- proves the wiring, not only the function."""
    from transcription_service.providers.fake import FakeProvider
    from transcription_service.sessions import SessionTracker

    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            count = (
                await session.execute(
                    text(
                        "SELECT count(*) FROM audit_logs WHERE org_id = :org_id "
                        "AND target_id = :meeting_id AND action = :action"
                    ),
                    {"org_id": org_id, "meeting_id": meeting_id, "action": ACTION_SESSION_OPENED},
                )
            ).scalar_one()
    assert count == 1

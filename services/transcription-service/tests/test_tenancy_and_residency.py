"""Reads of `tenant_settings.pii_redaction_enabled` / `data_residency.region` against the real
Postgres, and the region -> Deepgram-endpoint selection (compliance gate condition 3's
acceptance check: "an eu tenant's stream opens against the EU Deepgram endpoint")."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.db import set_current_org
from transcription_service.residency import deepgram_endpoint_for_region
from transcription_service.tenancy import get_data_residency_region, get_pii_redaction_enabled


@pytest.mark.asyncio
async def test_pii_redaction_flag_read_from_tenant_settings(
    session_factory: async_sessionmaker[AsyncSession], tenant: dict[str, str]
) -> None:
    org_id = tenant["org_id"]
    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            assert await get_pii_redaction_enabled(session, org_id) is True  # fixture default

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            await session.execute(
                text(
                    "UPDATE tenant_settings SET pii_redaction_enabled = false "
                    "WHERE org_id = :org_id"
                ),
                {"org_id": org_id},
            )

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            assert await get_pii_redaction_enabled(session, org_id) is False


@pytest.mark.asyncio
async def test_data_residency_region_read_and_endpoint_selection_us_and_eu(
    session_factory: async_sessionmaker[AsyncSession], tenant: dict[str, str]
) -> None:
    org_id = tenant["org_id"]
    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            assert await get_data_residency_region(session, org_id) == "us"  # fixture default
    assert deepgram_endpoint_for_region("us") == "wss://api.deepgram.com/v1/listen"

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            await session.execute(
                text("UPDATE data_residency SET region = 'eu' WHERE org_id = :org_id"),
                {"org_id": org_id},
            )

    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            region = await get_data_residency_region(session, org_id)
    assert region == "eu"
    assert deepgram_endpoint_for_region("eu") == "wss://api.eu.deepgram.com/v1/listen"


@pytest.mark.asyncio
async def test_missing_rows_fail_safe(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """No `tenant_settings`/`data_residency` row for an org (shouldn't happen in steady state,
    but must fail toward the SAFER default: redact by default, pin to `us`)."""
    import uuid

    unknown_org = str(uuid.uuid4())
    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, unknown_org)
            assert await get_pii_redaction_enabled(session, unknown_org) is True
            assert await get_data_residency_region(session, unknown_org) == "us"

"""Shared fixtures for tests that hit the real local infra (per the task brief: Postgres
`mytheus-backend-postgres-1` on host port 5433 db `mytheus`, Redis 6380, Redpanda 9092, Schema
Registry 8081). Nothing here is mocked -- these are genuinely the services this program's
`docker compose` brings up, matching this repo's own testing convention
("Mock these in tests: LiveKit ... Deepgram ... and Redis pub/sub" -- notably NOT Postgres or
Redpanda, which this suite deliberately exercises for real per the task brief's explicit
"assert both zeros ... against a real database and a real broker").
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from transcription_service.db import make_engine, make_session_factory

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5433/mytheus"
)
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6380")
TEST_REDPANDA_BROKERS = os.environ.get("TEST_REDPANDA_BROKERS", "localhost:9092")
TEST_SCHEMA_REGISTRY_URL = os.environ.get("TEST_SCHEMA_REGISTRY_URL", "http://localhost:8081")


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    # Function-scoped (not session-scoped): pytest-asyncio gives each async test its own
    # event loop by default, and an asyncpg connection pool bound to one loop breaks on the
    # next test's loop ("Task ... got Future ... attached to a different loop"). A fresh
    # engine per test avoids that entirely; disposed after so no connection outlives its loop.
    eng = make_engine(TEST_DATABASE_URL)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client: Redis = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def tenant(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[dict[str, str]]:
    """A fresh `organizations` + `tenant_settings` + `data_residency` row set, so `outbox`'s
    FK to `organizations` and the RLS-protected reads in `tenancy.py` have something real to
    resolve against. `region`/`pii_redaction_enabled` are set explicitly by the test that
    needs a specific value (via direct UPDATE), defaulting here to `us` / redaction on."""
    org_id = str(uuid.uuid4())
    slug = f"test-{uuid.uuid4().hex[:12]}"
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text("INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)"),
                {"id": org_id, "name": "Test Org", "slug": slug},
            )
            await session.execute(
                text(
                    "INSERT INTO tenant_settings (org_id, pii_redaction_enabled) "
                    "VALUES (:org_id, true)"
                ),
                {"org_id": org_id},
            )
            await session.execute(
                text("INSERT INTO data_residency (org_id, region) VALUES (:org_id, 'us')"),
                {"org_id": org_id},
            )
    try:
        yield {"org_id": org_id, "meeting_id": str(uuid.uuid4())}
    finally:
        async with session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("DELETE FROM transcript_chunks WHERE org_id = :org_id"), {"org_id": org_id}
                )
                await session.execute(
                    text("DELETE FROM outbox WHERE org_id = :org_id"), {"org_id": org_id}
                )
                await session.execute(
                    text("DELETE FROM data_residency WHERE org_id = :org_id"), {"org_id": org_id}
                )
                await session.execute(
                    text("DELETE FROM tenant_settings WHERE org_id = :org_id"), {"org_id": org_id}
                )
                await session.execute(
                    text("DELETE FROM organizations WHERE id = :org_id"), {"org_id": org_id}
                )

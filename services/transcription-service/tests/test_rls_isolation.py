"""Fix round 1, item 4: empirical proof that RLS isolation on `transcript_chunks` actually
holds, connecting as the LEAST-PRIVILEGE `mytheus_app` role rather than the `postgres`
superuser every other test in this suite uses for setup convenience (`db.py`'s docstring
already discloses that choice; `postgres` bypasses RLS unconditionally regardless of
`app.current_org`, so nothing else in this suite could have caught the item-3 defect, which
is exactly the kind of thing this test class exists to catch).

`mytheus_app`'s dev password (`scripts/bootstrap_roles.py` in `mytheus-backend`) is the role
name itself, `mytheus_app`, and is already set on this shared local Postgres instance
(verified: `PGPASSWORD=mytheus_app psql -U mytheus_app -h localhost -p 5443 -d mytheus`
connects). A real deployment's `mytheus_app` password is operator-set and out of this test's
reach; local dev/CI is where this empirical check can actually run.
"""

from __future__ import annotations

import uuid
from datetime import date

import pytest
from conftest import TEST_DATABASE_URL
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from transcription_service.db import make_session_factory, set_current_org

APP_ROLE_DATABASE_URL = TEST_DATABASE_URL.replace("postgres:postgres@", "mytheus_app:mytheus_app@")


@pytest.fixture
def app_role_engine() -> AsyncEngine:
    return create_async_engine(APP_ROLE_DATABASE_URL, pool_pre_ping=True)


@pytest.fixture
def app_role_session_factory(app_role_engine: AsyncEngine) -> async_sessionmaker:  # type: ignore[type-arg]
    return make_session_factory(app_role_engine)


@pytest.fixture
async def two_tenants(session_factory):  # type: ignore[no-untyped-def]
    """Two orgs, each with one `transcript_chunks` row, inserted as the `postgres` superuser
    (legitimate test-harness setup, same pattern `conftest.py`'s `tenant` fixture already
    uses) so the isolation assertions below have real cross-tenant data to fail to leak."""
    org_a, org_b = str(uuid.uuid4()), str(uuid.uuid4())
    meeting_a, meeting_b = str(uuid.uuid4()), str(uuid.uuid4())
    chunk_a, chunk_b = str(uuid.uuid4()), str(uuid.uuid4())
    async with session_factory() as session:
        async with session.begin():
            for org_id in (org_a, org_b):
                await session.execute(
                    text("INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)"),
                    {"id": org_id, "name": "RLS Test Org", "slug": f"rls-test-{org_id[:8]}"},
                )
            for org_id, meeting_id, chunk_id in (
                (org_a, meeting_a, chunk_a),
                (org_b, meeting_b, chunk_b),
            ):
                await session.execute(
                    text(
                        "INSERT INTO transcript_chunks "
                        "(id, org_id, meeting_id, speaker_id, start_ms, end_ms, text, "
                        "is_final, confidence) "
                        "VALUES (:id, :org_id, :meeting_id, '0', 0, 100, 'rls test', true, 0.9)"
                    ),
                    {"id": chunk_id, "org_id": org_id, "meeting_id": meeting_id},
                )
    try:
        yield {
            "org_a": org_a,
            "org_b": org_b,
            "meeting_a": meeting_a,
            "meeting_b": meeting_b,
            "chunk_a": chunk_a,
            "chunk_b": chunk_b,
        }
    finally:
        async with session_factory() as session:
            async with session.begin():
                for org_id in (org_a, org_b):
                    await session.execute(
                        text("DELETE FROM transcript_chunks WHERE org_id = :org_id"),
                        {"org_id": org_id},
                    )
                    await session.execute(
                        text("DELETE FROM organizations WHERE id = :org_id"), {"org_id": org_id}
                    )


@pytest.mark.asyncio
async def test_mytheus_app_sees_only_its_own_org_through_the_parent(
    app_role_session_factory, two_tenants: dict[str, str]
) -> None:
    async with app_role_session_factory() as session:
        async with session.begin():
            await set_current_org(session, two_tenants["org_a"])
            rows = (await session.execute(text("SELECT id, org_id FROM transcript_chunks"))).all()
    ids = {str(r[0]) for r in rows}
    assert two_tenants["chunk_a"] in ids
    assert two_tenants["chunk_b"] not in ids, "org A's connection can see org B's row"


@pytest.mark.asyncio
async def test_mytheus_app_with_no_current_org_set_sees_nothing(
    app_role_session_factory, two_tenants: dict[str, str]
) -> None:
    """Fail-closed, not an error: rows exist (both orgs' fixture rows), but with
    `app.current_org` unset the `tenant_isolation` predicate is false for every row."""
    async with app_role_session_factory() as session:
        async with session.begin():
            rows = (
                await session.execute(
                    text("SELECT id FROM transcript_chunks WHERE id = ANY(:ids)"),
                    {"ids": [two_tenants["chunk_a"], two_tenants["chunk_b"]]},
                )
            ).all()
    assert rows == []


@pytest.mark.asyncio
async def test_mytheus_app_cannot_query_a_partition_directly(
    app_role_session_factory, two_tenants: dict[str, str]
) -> None:
    """Empirical proof fix round 1 item 3 actually blocks the escape, not just structurally
    (no grants in `information_schema`): `mytheus_app` naming the current month's partition
    directly must be refused by Postgres itself, regardless of `app.current_org`."""
    partition = f"transcript_chunks_{date.today().strftime('%Y_%m')}"
    async with app_role_session_factory() as session:
        async with session.begin():
            await set_current_org(session, two_tenants["org_a"])
            # SQLAlchemy's asyncpg dialect wraps the driver error in its own `DBAPIError`/
            # `ProgrammingError` shim; the original `asyncpg.exceptions.InsufficientPrivilegeError`
            # class name and Postgres's own message text survive inside `.orig`'s message.
            with pytest.raises(DBAPIError) as exc_info:
                await session.execute(text(f"SELECT * FROM {partition}"))
    assert "InsufficientPrivilegeError" in str(exc_info.value.orig)
    assert "permission denied" in str(exc_info.value.orig)

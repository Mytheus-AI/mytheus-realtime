"""Journal plan Part 3 fixes 4 and 5: transcription-service runs as a least-privilege role,
and its engine refuses any role that would make row-level security inert."""

from __future__ import annotations

import pytest
from conftest import SERVICE_ROLE, TEST_BOOTSTRAP_DATABASE_URL, TEST_DATABASE_URL
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from transcription_service.config import load_settings_from_env
from transcription_service.db import make_engine, set_current_org
from transcription_service.db_guard import PrivilegedRoleError, verify_role, violations


def test_the_code_default_is_the_least_privilege_role(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    url = load_settings_from_env().database_url
    assert "//mytheus_transcription:" in url
    assert "//postgres:" not in url


def test_violations_names_every_problem() -> None:
    assert violations([("mytheus_transcription", False, False, [])]) == []
    assert violations([("postgres", False, True, ["pg_read_all_data"])]) == [
        "postgres holds BYPASSRLS",
        "postgres is a member of pg_read_all_data",
    ]


@pytest.mark.asyncio
async def test_the_service_role_is_admitted(engine: AsyncEngine) -> None:
    await verify_role(engine)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT current_user"))).scalar_one() == SERVICE_ROLE


@pytest.mark.asyncio
async def test_the_schema_owner_is_refused_unless_explicitly_allowed() -> None:
    guarded = make_engine(TEST_BOOTSTRAP_DATABASE_URL)
    try:
        with pytest.raises(PrivilegedRoleError):
            await verify_role(guarded)
    finally:
        await guarded.dispose()
    # The owner's tooling opts out explicitly; nothing in the service does.
    allowed = make_engine(TEST_BOOTSTRAP_DATABASE_URL, allow_privileged=True)
    try:
        await verify_role(allowed)
    finally:
        await allowed.dispose()


@pytest.mark.asyncio
async def test_the_service_role_is_subject_to_rls_and_holds_no_bypass(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT rolsuper, rolbypassrls, "
                    "pg_has_role(current_user, 'pg_read_all_data', 'MEMBER') "
                    "FROM pg_roles WHERE rolname = current_user"
                )
            )
        ).one()
    assert tuple(row) == (False, False, False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM users",  # no grant at all
        "SELECT name FROM organizations",  # only (id) is granted
        "SELECT * FROM tenant_settings",  # only (org_id, pii_redaction_enabled)
        "SELECT title FROM meetings",  # only (id, org_id)
        "UPDATE outbox SET payload = '{}'::jsonb",  # INSERT/SELECT only
        "SELECT * FROM audit_logs",  # INSERT only, append-only
        "DELETE FROM outbox",
    ],
)
async def test_the_service_role_reaches_nothing_beyond_its_sql(
    engine: AsyncEngine, sql: str
) -> None:
    async with engine.connect() as conn:
        await conn.begin()
        with pytest.raises(DBAPIError, match="permission denied"):
            await conn.execute(text(sql))


@pytest.mark.asyncio
async def test_without_a_tenant_the_service_role_sees_no_transcript(
    engine: AsyncEngine, tenant: dict[str, str]
) -> None:
    from transcription_service.db import make_session_factory

    factory = make_session_factory(engine)
    async with factory() as session, session.begin():
        assert (
            await session.execute(text("SELECT count(*) FROM transcript_chunks"))
        ).scalar_one() == 0
        await set_current_org(session, tenant["org_id"])
        assert (
            await session.execute(text("SELECT count(*) FROM tenant_settings"))
        ).scalar_one() == 1


def test_the_suite_connects_as_the_service_role() -> None:
    assert f"//{SERVICE_ROLE}:" in TEST_DATABASE_URL

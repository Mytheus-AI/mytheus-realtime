"""Async Postgres engine/session plumbing, plus the `app.current_org` RLS helper.

`DATABASE_URL` points at the SAME physical Postgres `mytheus-backend` migrates
(`mytheus-backend-postgres-1`, db `mytheus`) — this service owns and migrates only
`transcript_chunks` (see `migrations/`), but reads `tenant_settings`/`data_residency` and
writes the shared `outbox` table directly, per this repo's `CLAUDE.md` recipe for producing
events and the precedent already set in `mytheus-backend/CLAUDE.md` itself ("this repo reads
[`transcript_chunks`/vectors/the graph] but never writes them" — i.e. cross-service reads
against the shared Postgres are the established pattern here; only cross-service WRITES to a
table this service does not own are forbidden, and `outbox` is the one deliberate exception,
being infrastructure every producer writes to by design). See `tenancy.py` and `outbox.py`.

The service connects as `mytheus_transcription` (migration `d8b4f2a6c0e1`): no superuser, no
BYPASSRLS, no predefined-role membership, and grants on exactly the tables and columns its SQL
touches. That is the default in code (`config.py`), in `.env.example` and in the test suite.
It used to be `postgres`, which on supabase/postgres holds BYPASSRLS and `pg_read_all_data`, so
every `tenant_isolation` policy was off for this service and its `SET LOCAL app.current_org`
calls were decoration. `make_engine` installs the pool guard (`db_guard.py`): a connection as
a role that would make RLS inert is refused before the pool hands it out. Only the schema
owner's tooling (migrations, test fixtures that seed cross-tenant rows) opts out, explicitly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .db_guard import install_role_guard


def make_engine(database_url: str, *, allow_privileged: bool = False) -> AsyncEngine:
    """The service's engine, guarded so it can never run as a role that bypasses RLS.

    `allow_privileged=True` is for the schema owner's own tooling only (test fixtures that seed
    rows across tenants); the service never passes it.
    """
    engine = create_async_engine(database_url, pool_pre_ping=True)
    if not allow_privileged:
        install_role_guard(engine)
    return engine


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def set_current_org(session: AsyncSession, org_id: str) -> None:
    """Transaction-local `app.current_org` — the `true` third argument to `set_config` is
    what makes it transaction-local rather than session-local, required so a pooled connection
    never leaks one tenant's setting into another tenant's reuse of it (per
    `mytheus-backend/CLAUDE.md` Pitfalls)."""
    await session.execute(
        text("SELECT set_config('app.current_org', :org_id, true)"), {"org_id": org_id}
    )


@asynccontextmanager
async def tenant_transaction(
    session_factory: async_sessionmaker[AsyncSession], org_id: str
) -> AsyncIterator[AsyncSession]:
    """Opens one transaction with `app.current_org` set for its duration."""
    async with session_factory() as session:
        async with session.begin():
            await set_current_org(session, org_id)
            yield session

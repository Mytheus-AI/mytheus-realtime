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

Local dev/test connects as the `postgres` superuser, unchanged from the existing scaffold's
`.env.example` (`postgresql+asyncpg://postgres:postgres@localhost:5432/mytheus`) — see
`work-done-till-now` for why a properly scoped least-privilege role for this service
(mirroring `mytheus_app`/`mytheus_relay`) is deferred: `mytheus-backend`'s own roles
(`mytheus_app`, `mytheus_relay`, `mytheus_admin`) are created with NO PASSWORD by design
(`a1c4e7f20b18`'s docstring: "cannot authenticate until an operator sets it"), and granting a
new cross-repo role SELECT on tables this service does not own is an ops/bootstrap concern,
not something a migration in THIS repo should silently arrogate. The `SET LOCAL
app.current_org` calls below are still always issued, so the code is correct for a properly
scoped role in production even though the local superuser connection bypasses RLS.
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


def make_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


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

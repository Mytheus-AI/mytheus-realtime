"""Alembic environment for `transcription-service`'s OWN migration history.

This is a SEPARATE Alembic history from `mytheus-backend/migrations/` (a different repo,
different `script_location`, no shared revision graph) that happens to point at the same
physical Postgres database. `mytheus-backend`'s Alembic history owns every table that exists
today; this service owns exactly ONE table going forward, `transcript_chunks`
(`mytheus-realtime/CLAUDE.md`: "transcription-service ... Writes only transcript_chunks
(Postgres); otherwise stateless"). Landing a second competing migration inside
`mytheus-backend/migrations/versions/` was explicitly ruled out by the task brief: that repo's
history is owned by that repo's team, and adding a table there this repo does not deploy or
run tests against would create a migration nobody running `mytheus-backend` alone actually
needs, plus review noise across a repo boundary that gains nothing (this table has zero
foreign keys back into anything `mytheus-backend` owns, per the plan's own literal
`CREATE TABLE` text -- no FK to `organizations`/`meetings`).

To avoid the two histories' bookkeeping tables colliding (`mytheus-backend` uses the default
`alembic_version`), this history's version-tracking table is renamed via `version_table` below.
Both point at the same database; both track their own `head` independently and never touch a
table the other owns. `outbox`/`tenant_settings`/`data_residency` (read/written by application
code, see `db.py`/`outbox.py`/`tenancy.py`) are never touched by a migration in this repo.
"""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# `transcript_chunks` is created via raw SQL in the migration (partitioned tables and RLS
# policies are not autogenerate-friendly), so there is no SQLAlchemy metadata to diff against.
target_metadata = None

VERSION_TABLE = "alembic_version_transcription_service"


def _database_url() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/mytheus"
    )


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=VERSION_TABLE,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection, target_metadata=target_metadata, version_table=VERSION_TABLE
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = _database_url()
    connectable = async_engine_from_config(
        configuration, prefix="sqlalchemy.", poolclass=pool.NullPool
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

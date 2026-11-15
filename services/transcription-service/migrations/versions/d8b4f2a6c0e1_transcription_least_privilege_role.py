"""fix: a least-privilege database role for transcription-service

Journal plan Part 3 fix 4. transcription-service connected as `postgres` by default (code,
tests, `.env.example`). On supabase/postgres that role holds BYPASSRLS and membership of
`pg_read_all_data`; on a vanilla image it is a superuser. Either way every `tenant_isolation`
policy this service relies on was switched off for it, and its `SET LOCAL app.current_org`
calls were decoration.

`mytheus_transcription` is created here with exactly what the service's SQL needs (the full
list was read from `src/transcription_service/*.py`):

- `transcript_chunks`: SELECT, INSERT, UPDATE, DELETE (its own table: finals, the resume
  offset, erasure);
- `outbox`: SELECT, INSERT, plus USAGE on `outbox_id_seq` (`outbox.py`'s idempotent insert and
  re-read);
- `audit_logs`: INSERT (`audit.py`; append-only, like every role);
- `tenant_settings`: SELECT (org_id, pii_redaction_enabled) (`tenancy.py`);
- `data_residency`: SELECT (org_id, region) (`tenancy.py`);
- `meetings`: SELECT (id, org_id) (`tenancy.py`'s meeting-belongs-to-org check);
- `organizations`: SELECT (id) (`devseed.py`'s existence check, dev only).

Every one of those tables carries FORCE ROW LEVEL SECURITY with a `tenant_isolation` policy
for PUBLIC, so the role sees only the tenant its transaction set. It is NOT a superuser, has
no BYPASSRLS, no CREATEROLE/CREATEDB, and no membership of any predefined role.

Grants on the backend-owned tables are made here, not in mytheus-backend, because they are
this service's access requirements and must exist whenever this service's migrations have
run; they are read-only column grants plus the outbox insert every producer makes, which
`db.py`'s docstring already records as the sanctioned cross-service write. Each is guarded by
`to_regclass` so this history still applies to a database where a table does not exist yet.

The role is created NOLOGIN with no password, like mytheus-backend's three roles: the
operator gives it `LOGIN PASSWORD` (dev/test: the test suite and `.env.example` use the
role name as its password) and points `DATABASE_URL` at it.

Revision ID: d8b4f2a6c0e1
Revises: a5d3e7f9c1b2
Create Date: 2026-11-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "d8b4f2a6c0e1"
down_revision: str | None = "a5d3e7f9c1b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ROLE = "mytheus_transcription"

GRANTS: tuple[tuple[str, str], ...] = (
    ("transcript_chunks", "SELECT, INSERT, UPDATE, DELETE"),
    ("outbox", "SELECT, INSERT"),
    ("audit_logs", "INSERT"),
    ("tenant_settings", "SELECT (org_id, pii_redaction_enabled)"),
    ("data_residency", "SELECT (org_id, region)"),
    ("meetings", "SELECT (id, org_id)"),
    ("organizations", "SELECT (id)"),
)


def upgrade() -> None:
    op.execute(f"""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN
            CREATE ROLE {ROLE} NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE
              NOREPLICATION;
          END IF;
        END $$;
        """)
    for table, privilege in GRANTS:
        op.execute(f"""
            DO $$
            BEGIN
              IF to_regclass('public.{table}') IS NOT NULL THEN
                GRANT {privilege} ON public.{table} TO {ROLE};
              END IF;
              IF '{table}' = 'outbox' AND to_regclass('public.outbox_id_seq') IS NOT NULL THEN
                GRANT USAGE ON SEQUENCE public.outbox_id_seq TO {ROLE};
              END IF;
            END $$;
            """)


def downgrade() -> None:
    op.execute(f"""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN
            EXECUTE 'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {ROLE}';
            EXECUTE 'REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {ROLE}';
            EXECUTE 'DROP ROLE {ROLE}';
          END IF;
        END $$;
        """)

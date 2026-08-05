"""feat: create transcript_chunks (finals-only, monthly range partitioned, RLS)

Plan 3.6 step 6, verbatim SQL shape plus two deliberate additions:

1. A `redacted boolean NOT NULL DEFAULT false` column, not present in the plan's literal
   `CREATE TABLE` text. This repo's own `CLAUDE.md` recipe "Add a transcription event field"
   says: "Keep the transcript_chunks row and the Avro value in sync if the field is
   persisted." `redacted` was added to the `TranscriptChunk` Avro schema earlier today
   (mytheus-events commit `4139d5a`) specifically because it IS persisted (the redaction
   result), so the row needs the column to stay in sync with the event.
2. No foreign keys to `organizations`/`meetings` (both owned by `mytheus-backend`, a different
   repo's migration history). The plan's literal SQL has none either. Declarative partitioning
   also restricts FKs FROM a partitioned table in ways that would add real complexity for a
   guarantee this table does not need at the code level (the `org_id`/`meeting_id` values
   always come from a `meeting-room-events` payload `core-api` already validated when it wrote
   `meetings`).

Partitioning: `PARTITION BY RANGE (created_at)`, one partition per calendar month. pg_partman
is NOT installed on the shared `mytheus-backend-postgres-1` Postgres image (`SELECT extname
FROM pg_extension` at the time of this migration lists `plpgsql, uuid-ossp, pgcrypto, pgjwt,
pg_stat_statements, supabase_vault, pg_graphql` -- no `pg_partman`), so this migration creates
partitions by hand: the current month plus a 2-month premake, mirroring pg_partman's default
premake-3 behavior (current + 2 ahead) without the extension. The brief explicitly scopes
ongoing partition maintenance to a later band's `workflow-service` nightly job ("create the
partitions for the current month now"); this migration does exactly that and no more. A
`DEFAULT` partition catches anything outside the premade range so an insert never hard-fails
for want of a partition, at the cost of that row not benefiting from partition pruning until
the maintenance job catches up -- documented via a table comment so it's visible from `\\d+`.

RLS: `ENABLE` + `FORCE` + `tenant_isolation`, identical predicate shape to every other tenant
table in this database (`mytheus-backend`'s `82d44163c933`/`a1c4e7f20b18`): `org_id =
NULLIF(current_setting('app.current_org', true), '')::uuid`, fail-closed (zero rows, not an
error, when unset). Row Level Security policies defined on a partitioned PARENT table apply
automatically to every partition, current and future, since Postgres 11 -- no per-partition
policy statements needed.

Local index `(meeting_id, start_ms)`: created on the partitioned parent, which Postgres
propagates to every existing and future partition automatically (`CREATE INDEX ON <parent>`
on a partitioned table since PG 11).

Revision ID: be66698dbefb
Revises:
Create Date: 2026-08-05
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from alembic import op

revision: str = "be66698dbefb"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "transcript_chunks"
# How many calendar months (including the current one) to premake, mirroring pg_partman's
# default `premake = 3` (current + 2 ahead) in the absence of the extension.
PREMAKE_MONTHS = 3


def _month_bounds(base: date, offset: int) -> tuple[date, date]:
    """Returns (first_of_month, first_of_next_month) `offset` months after `base`'s month."""
    year = base.year + (base.month - 1 + offset) // 12
    month = (base.month - 1 + offset) % 12 + 1
    start = date(year, month, 1)
    next_year = year + (month // 12)
    next_month = month % 12 + 1
    end = date(next_year, next_month, 1)
    return start, end


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    op.execute(f"""
        CREATE TABLE {TABLE} (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL,
          meeting_id  uuid        NOT NULL,
          speaker_id  text        NOT NULL,
          start_ms    integer     NOT NULL,
          end_ms      integer     NOT NULL,
          text        text        NOT NULL,
          is_final    boolean     NOT NULL DEFAULT true,
          confidence  real        NOT NULL,
          redacted    boolean     NOT NULL DEFAULT false,
          created_at  timestamptz NOT NULL DEFAULT now(),
          deleted_at  timestamptz,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
        """)
    op.execute(f"""
        COMMENT ON TABLE {TABLE} IS
        'Finalized diarized utterances (finals only -- interims are never persisted). Owned '
        'and migrated by transcription-service (mytheus-realtime), NOT mytheus-backend, '
        'despite sharing this Postgres instance. Monthly range partition on created_at; '
        'pg_partman is not installed on this Postgres image so partitions are premade by '
        'hand in this migration and by a later-band workflow-service nightly job -- rows '
        'outside the premade range land in transcript_chunks_default until that job catches '
        'up.';
        """)

    op.execute(f"CREATE INDEX ix_{TABLE}_meeting_start ON {TABLE} (meeting_id, start_ms);")

    op.execute(f"ALTER TABLE {TABLE} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {TABLE} FORCE ROW LEVEL SECURITY;")
    _predicate = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
    op.execute(
        f"CREATE POLICY tenant_isolation ON {TABLE} USING ({_predicate}) WITH CHECK ({_predicate});"
    )

    today = date.today()
    for offset in range(PREMAKE_MONTHS):
        start, end = _month_bounds(today, offset)
        partition_name = f"{TABLE}_{start.strftime('%Y_%m')}"
        op.execute(
            f"CREATE TABLE {partition_name} PARTITION OF {TABLE} "
            f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');"
        )

    # Catch-all so an insert for a month outside the premade range fails loudly into a real
    # (if unpruned) partition rather than erroring outright; the maintenance job replaces the
    # need for this by keeping premade partitions ahead of `now()`.
    op.execute(f"CREATE TABLE {TABLE}_default PARTITION OF {TABLE} DEFAULT;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")

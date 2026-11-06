"""fix: transcript_chunks monthly partitions 2026-11 .. 2027-12

`be66698dbefb` premade 2026-08..2026-10 plus a DEFAULT and deferred maintenance. The calendar
moved to 2026-11, so new rows fall into `transcript_chunks_default` (works, but unpruned) and
nothing creates later months. This revision creates the explicit months 2026-11 through
2027-12 (fixed constants, not `date.today()`, so replay is deterministic) and
`scripts/ensure_partitions.py` keeps them coming (run monthly, `--months 3`;
`tests/test_partition_coverage.py` fails when coverage lapses).

Mirrors `mytheus-backend` migration `9817d41e0603`. Because the DEFAULT exists, CREATE
TABLE ... PARTITION OF scans it and FAILS if it already holds rows in the new range, so for
every month the rows are first moved out (lock default, DELETE ... RETURNING into a temp
table), the partition is created, and the rows are inserted into it. No-op on an empty default.

Per-partition indexes: `ix_transcript_chunks_meeting_start` and the primary key are created on
the PARENT and propagate to each new partition automatically, so siblings carry no others.
ACL: owner-only (ADR-0021). A partition is never granted to an application role; it is reached
through the parent, where the grants and the `tenant_isolation` RLS policy live. The explicit
REVOKE plus non-owner sweep materialises the same owner-only ACL the older siblings hold.

Revision ID: e4a7c91b3d28
Revises: c7d2e9a41f05
Create Date: 2026-11-06
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from alembic import op

revision: str = "e4a7c91b3d28"
down_revision: str | None = "c7d2e9a41f05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "transcript_chunks"
DEFAULT = f"{TABLE}_default"
# Frozen: a migration must replay identically forever.
_FIRST = (2026, 11)
_LAST = (2027, 12)
_LOCKDOWN_ROLES = "mytheus_app, mytheus_admin, mytheus_relay, PUBLIC"

_SWEEP = """
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT DISTINCT a.grantee AS g
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.oid = '{p}'::regclass AND a.grantee <> c.relowner AND a.grantee <> 0
    LOOP
        EXECUTE format('REVOKE ALL ON {p} FROM %I', pg_get_userbyid(r.g));
    END LOOP;
END $$;
"""


def _months() -> list[tuple[int, int]]:
    y, m = _FIRST
    out: list[tuple[int, int]] = []
    while (y, m) <= _LAST:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _bounds(y: int, m: int) -> tuple[str, str]:
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return date(y, m, 1).isoformat(), end.isoformat()


def upgrade() -> None:
    for y, m in _months():
        start, end = _bounds(y, m)
        p = f"{TABLE}_{y:04d}_{m:02d}"
        op.execute(f"LOCK TABLE {DEFAULT} IN ACCESS EXCLUSIVE MODE;")
        op.execute(f"CREATE TEMP TABLE _spill (LIKE {TABLE});")
        op.execute(
            f"WITH d AS (DELETE FROM {DEFAULT} WHERE created_at >= '{start}' "
            f"AND created_at < '{end}' RETURNING *) INSERT INTO _spill SELECT * FROM d;"
        )
        op.execute(
            f"CREATE TABLE {p} PARTITION OF {TABLE} FOR VALUES FROM ('{start}') TO ('{end}');"
        )
        op.execute(f"INSERT INTO {p} SELECT * FROM _spill;")
        op.execute("DROP TABLE _spill;")
        op.execute(f"REVOKE ALL ON {p} FROM {_LOCKDOWN_ROLES};")
        op.execute(_SWEEP.format(p=p))


def downgrade() -> None:
    # Dropping a partition drops its rows: acceptable only on a disposable dev database.
    for y, m in _months():
        op.execute(f"DROP TABLE IF EXISTS {TABLE}_{y:04d}_{m:02d};")

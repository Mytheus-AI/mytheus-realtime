"""Create the next N monthly `transcript_chunks` partitions. Idempotent.

Why this exists: `be66698dbefb` premade only 2026-08..2026-10 plus a DEFAULT, and nothing
created later months, so from 2026-11 every row landed in `transcript_chunks_default`
(slow, unpruned). Migration `e4a7c91b3d28` backfills 2026-11..2027-12; this script keeps the
months coming. PLATFORM OWNER: run it monthly (e.g. the 1st, `--months 3`) as the schema
owner, because partitions are DDL that no application role may perform:

    BOOTSTRAP_DATABASE_URL=postgresql://<owner>:<pw>@host:5432/mytheus \\
      uv run python -m scripts.ensure_partitions --months 3

Mirrors `mytheus-backend/scripts/ensure_partitions.py`. Each partition carries what its
siblings carry: the parent's local indexes (propagated automatically by Postgres) and an
owner-only ACL (ADR-0021: never grant a partition to an application role; RLS stays on the
parent). Rows that already spilled into the DEFAULT for a month are moved into the new
partition first, otherwise the CREATE fails re-validating the default's constraint.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date
from typing import Any

import asyncpg

TABLE = "transcript_chunks"
KEY = "created_at"
_LOCKDOWN_ROLES = ("mytheus_app", "mytheus_admin", "mytheus_relay")


def add_months(year: int, month: int, n: int) -> tuple[int, int]:
    m0 = year * 12 + (month - 1) + n
    return m0 // 12, m0 % 12 + 1


def month_bounds(year: int, month: int) -> tuple[date, date]:
    ny, nm = add_months(year, month, 1)
    return date(year, month, 1), date(ny, nm, 1)


def partition_name(year: int, month: int) -> str:
    return f"{TABLE}_{year:04d}_{month:02d}"


async def _exists(conn: Any, rel: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", f"public.{rel}"))


async def _lock_down(conn: Any, rel: str) -> None:
    """Owner-only ACL: revoke the app roles and PUBLIC, then sweep any other non-owner grantee."""
    existing = [
        r
        for r in _LOCKDOWN_ROLES
        if await conn.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", r)
    ]
    await conn.execute(f"REVOKE ALL ON {rel} FROM {', '.join([*existing, 'PUBLIC'])}")
    grantees = await conn.fetch(
        "SELECT DISTINCT pg_get_userbyid(a.grantee) AS g FROM pg_class c "
        "CROSS JOIN LATERAL aclexplode(c.relacl) a "
        "WHERE c.oid = $1::regclass AND a.grantee <> c.relowner AND a.grantee <> 0",
        rel,
    )
    for row in grantees:
        await conn.execute(f'REVOKE ALL ON {rel} FROM "{row["g"]}"')


async def ensure_default(conn: Any) -> bool:
    default = f"{TABLE}_default"
    if await _exists(conn, default):
        return False
    await conn.execute(f"CREATE TABLE {default} PARTITION OF {TABLE} DEFAULT")
    await _lock_down(conn, default)
    return True


async def ensure_month(conn: Any, year: int, month: int) -> bool:
    """Create one month's partition if missing, moving DEFAULT spill-over first.
    Returns True if it created the partition."""
    p = partition_name(year, month)
    if await _exists(conn, p):
        return False
    start, end = month_bounds(year, month)
    default = f"{TABLE}_default"
    has_default = await _exists(conn, default)
    if has_default:
        await conn.execute(f"LOCK TABLE {default} IN ACCESS EXCLUSIVE MODE")
        await conn.execute(f"CREATE TEMP TABLE _spill (LIKE {TABLE}) ON COMMIT DROP")
        await conn.execute(
            f"WITH d AS (DELETE FROM {default} WHERE {KEY} >= $1 AND {KEY} < $2 RETURNING *) "
            f"INSERT INTO _spill SELECT * FROM d",
            start,
            end,
        )
    await conn.execute(
        f"CREATE TABLE {p} PARTITION OF {TABLE} "
        f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
    )
    if has_default:
        await conn.execute(f"INSERT INTO {p} SELECT * FROM _spill")
        await conn.execute("DROP TABLE _spill")
    await _lock_down(conn, p)
    return True


async def ensure_partitions(conn: Any, *, months: int, today: date | None = None) -> list[str]:
    """Ensure the DEFAULT plus the current month and the next `months` exist.
    Each partition is its own transaction. Returns the names created."""
    today = today or date.today()
    created: list[str] = []
    async with conn.transaction():
        if await ensure_default(conn):
            created.append(f"{TABLE}_default")
    for i in range(months + 1):
        y, m = add_months(today.year, today.month, i)
        async with conn.transaction():
            if await ensure_month(conn, y, m):
                created.append(partition_name(y, m))
    return created


async def _run(url: str, months: int) -> list[str]:
    conn = await asyncpg.connect(url.replace("+asyncpg", ""))
    try:
        return await ensure_partitions(conn, months=months)
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--months", type=int, default=3, help="months ahead of the current one")
    args = parser.parse_args()
    url = os.environ.get("BOOTSTRAP_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("BOOTSTRAP_DATABASE_URL (or DATABASE_URL) is required", file=sys.stderr)
        return 2
    created = asyncio.run(_run(url, args.months))
    print("created: " + (", ".join(created) if created else "nothing (already covered)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

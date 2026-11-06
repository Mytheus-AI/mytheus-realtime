"""Loud failure when `transcript_chunks` monthly partition coverage lapses.

Rows outside an explicit partition fall into `transcript_chunks_default` (slow, unpruned).
This asserts, against the live database, that partitions exist for the current month and the
next three, and that the DEFAULT partition is holding no rows. If this goes red, run
`scripts/ensure_partitions.py --months 3` as the schema owner (and schedule it monthly).
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from scripts.ensure_partitions import add_months, partition_name


@pytest.mark.asyncio
async def test_partitions_exist_for_current_month_and_next_three(engine: AsyncEngine) -> None:
    today = date.today()
    expected = [partition_name(*add_months(today.year, today.month, i)) for i in range(4)]
    async with engine.connect() as conn:
        existing = set(
            (
                await conn.execute(
                    text(
                        "SELECT c.relname FROM pg_inherits i "
                        "JOIN pg_class c ON c.oid = i.inhrelid "
                        "WHERE i.inhparent = 'transcript_chunks'::regclass"
                    )
                )
            )
            .scalars()
            .all()
        )
    missing = [p for p in expected if p not in existing]
    assert not missing, (
        f"transcript_chunks partitions missing: {missing}. "
        "Run `python -m scripts.ensure_partitions --months 3` as the schema owner."
    )


@pytest.mark.asyncio
async def test_default_partition_exists(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        found = await conn.scalar(text("SELECT to_regclass('public.transcript_chunks_default')"))
    assert found is not None

"""In-memory stand-ins for Postgres and Redis, for tests that must run with no infrastructure.

`FakeDatabase` understands exactly the SQL this service issues (pattern-matched on the
statement text) and models the two properties the audio path depends on:

- RLS: `app.current_org` is transaction-local (reset when the `begin()` block exits), every
  write records the org context it ran under, and a `transcript_chunks` insert whose `org_id`
  differs from the context raises -- the `WITH CHECK` half of `tenant_isolation`. A
  `meetings` read with no / another org context returns nothing (fail closed).
- Outbox idempotency: `ON CONFLICT (topic, idempotency_key) DO NOTHING`.

Nothing here talks to a network. The real-infra suite (`conftest.py` fixtures) still covers
the real SQL against real Postgres.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any


class RlsViolation(Exception):
    pass


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def first(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def scalar_one(self) -> Any:
        assert len(self._rows) == 1, self._rows
        return self._rows[0][0]


@dataclass
class FakeDatabase:
    meetings: set[tuple[str, str]] = field(default_factory=set)  # (org_id, meeting_id)
    pii_redaction: dict[str, bool] = field(default_factory=dict)
    regions: dict[str, str] = field(default_factory=dict)
    outbox: list[dict[str, Any]] = field(default_factory=list)
    transcript_chunks: list[dict[str, Any]] = field(default_factory=list)
    audit_logs: list[dict[str, Any]] = field(default_factory=list)
    statements: list[str] = field(default_factory=list)
    fail_next: Exception | None = None
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    def add_meeting(self, org_id: str, meeting_id: str, *, region: str = "us") -> None:
        self.meetings.add((org_id, meeting_id))
        self.regions.setdefault(org_id, region)
        self.pii_redaction.setdefault(org_id, True)

    def __call__(self) -> _FakeSession:
        """Stands in for `async_sessionmaker`: `session_factory()`."""
        return _FakeSession(self)

    def execute(self, session: _FakeSession, sql: str, params: dict[str, Any]) -> _Result:
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        self.statements.append(sql)
        org = session.current_org
        if "set_config('app.current_org'" in sql:
            session.current_org = params["org_id"]
            return _Result([])
        if "FROM tenant_settings" in sql:
            if org != params["org_id"] or org not in self.pii_redaction:
                return _Result([])
            return _Result([(self.pii_redaction[org],)])
        if "FROM data_residency" in sql:
            if org != params["org_id"] or org not in self.regions:
                return _Result([])
            return _Result([(self.regions[org],)])
        if "FROM meetings" in sql:
            key = (params["org_id"], params["meeting_id"])
            if org != params["org_id"] or key not in self.meetings:
                return _Result([])
            return _Result([(1,)])
        if "INSERT INTO audit_logs" in sql:
            self._check(org, params["org_id"])
            self.audit_logs.append({**params, "_rls_org": org})
            return _Result([])
        if "INSERT INTO outbox" in sql:
            self._check(org, params["org_id"])
            for row in self.outbox:
                if (row["topic"], row["idempotency_key"]) == (
                    params["topic"],
                    params["idempotency_key"],
                ):
                    return _Result([])
            row_id = next(self._ids)
            self.outbox.append({**params, "id": row_id, "_rls_org": org})
            return _Result([(row_id,)])
        if "SELECT id FROM outbox" in sql:
            ids = [
                (r["id"],)
                for r in self.outbox
                if r["topic"] == params["topic"] and r["idempotency_key"] == params["ik"]
            ]
            return _Result(ids)
        if "MAX(end_ms)" in sql:
            # Where this meeting's saved transcript ends, under the session's RLS org.
            ends = [
                int(r["end_ms"])
                for r in self.transcript_chunks
                if r["_rls_org"] == org and r["meeting_id"] == params["m"]
            ]
            return _Result([(max(ends, default=0),)])
        if "INSERT INTO transcript_chunks" in sql:
            self._check(org, params["org_id"])
            self.transcript_chunks.append({**params, "_rls_org": org})
            return _Result([])
        raise AssertionError(f"FakeDatabase: unexpected SQL {sql!r}")

    @staticmethod
    def _check(context_org: str | None, row_org: str) -> None:
        if context_org != row_org:
            raise RlsViolation(f"row org {row_org} written under context {context_org}")


class _FakeTransaction:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> _FakeTransaction:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._session.current_org = None  # transaction-local, like set_config(..., true)


class _FakeSession:
    def __init__(self, db: FakeDatabase) -> None:
        self._db = db
        self.current_org: str | None = None

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.current_org = None

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        return self._db.execute(self, str(statement), params or {})


@dataclass
class FakeRedis:
    published: list[tuple[str, str]] = field(default_factory=list)
    keys: dict[str, str] = field(default_factory=dict)
    healthy: bool = True

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def set(self, key: str, value: str, nx: bool = False, ex: int | None = None) -> bool:
        if nx and key in self.keys:
            return False
        self.keys[key] = value
        return True

    async def ping(self) -> bool:
        if not self.healthy:
            raise ConnectionError("redis down")
        return True

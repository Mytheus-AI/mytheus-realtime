"""Company vocabulary for captions (ADR-0039 D1): the org's key terms, sent to Deepgram Nova-3 as
`keyterm` when a meeting's stream opens.

Source: gRPC `mytheus.identity.v1.IdentityService/GetOrgContext` on `identity-tenant-service`
(ADR-0039 A9), with `org_id` in metadata under `org-id`, never in the message body. The address
is `IDENTITY_GRPC_ADDR`, the same variable `mytheus-intelligence` uses for `GetUserContext`.
Unset means "no company vocabulary": streams open exactly as before.

Same no-protoc approach as `mytheus-intelligence/libs/shared/user_context.py`: the messages
are built at import time from a descriptor that mirrors `mytheus-backend/proto/identity.proto`
field for field (`tests/test_org_context.py` checks the mirror against that file when it is
present on disk).

Everything here is best effort. Any failure (no address, the 800 ms deadline, an RPC error, a
malformed reply) yields no key terms and the stream opens without them; captions never wait on
or fail because of the company brain. Results are cached per org in-process for 5 minutes (the
server also caches at `nexus:{org_id}:org_ctx`), so a meeting's participants and a stream's
reconnects cost one RPC, not one each. A failure is remembered for 30 s so an identity outage
does not add the deadline to every join.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

logger = logging.getLogger(__name__)

PACKAGE = "mytheus.identity.v1"
METHOD = f"/{PACKAGE}.IdentityService/GetOrgContext"
_F = descriptor_pb2.FieldDescriptorProto
_S = _F.TYPE_STRING
_M = _F.TYPE_MESSAGE

# (name, number, type, repeated, message type name or None) - mirrors identity.proto (ADR-0039 A9)
FIELDS: dict[str, list[tuple[str, int, int, bool, str | None]]] = {
    "GetOrgContextRequest": [],
    "GlossaryTerm": [
        ("term", 1, _S, False, None),
        ("kind", 2, _S, False, None),
        ("definition", 3, _S, False, None),
        ("aliases", 4, _S, True, None),
    ],
    "TopicOwner": [("topic", 1, _S, False, None), ("owner_user_id", 2, _S, False, None)],
    "Goal": [
        ("id", 1, _S, False, None),
        ("title", 2, _S, False, None),
        ("period", 3, _S, False, None),
    ],
    "TopicWatch": [("user_id", 1, _S, False, None), ("topic", 2, _S, False, None)],
    "OrgContext": [
        ("description", 1, _S, False, None),
        ("industry", 2, _S, False, None),
        ("products", 3, _S, True, None),
        ("customers", 4, _S, True, None),
        ("glossary", 5, _M, True, "GlossaryTerm"),
        ("topic_owners", 6, _M, True, "TopicOwner"),
        ("goals", 7, _M, True, "Goal"),
        ("never_record_topics", 8, _S, True, None),
        ("topic_watches", 9, _M, True, "TopicWatch"),
    ],
}


def _build() -> dict[str, Any]:
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "mytheus_identity_v1_org_context_mirror.proto"
    fdp.package = PACKAGE
    fdp.syntax = "proto3"
    for msg_name, fields in FIELDS.items():
        m = fdp.message_type.add()
        m.name = msg_name
        for name, number, ftype, repeated, type_name in fields:
            f = m.field.add()
            f.name, f.number, f.type = name, number, ftype
            f.label = _F.LABEL_REPEATED if repeated else _F.LABEL_OPTIONAL
            if type_name is not None:
                f.type_name = f".{PACKAGE}.{type_name}"
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return {
        n: message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{PACKAGE}.{n}"))
        for n in FIELDS
    }


_classes = _build()
GetOrgContextRequest: Any = _classes["GetOrgContextRequest"]
OrgContext: Any = _classes["OrgContext"]
GlossaryTerm: Any = _classes["GlossaryTerm"]

# Deepgram's documented keyterm limits: "up to 100" terms, and "the maximum number of tokens
# across all keyterms is 500" (an over-limit request is rejected, which would fail the stream
# open). Deepgram does not publish its tokenizer, so tokens are estimated conservatively at one
# per 3 characters and the budget keeps a margin under 500.
MAX_KEYTERMS = 100
KEYTERM_TOKEN_BUDGET = 450
_CHARS_PER_TOKEN = 3

FETCH_TIMEOUT_SECONDS = 0.8
CACHE_TTL_SECONDS = 300.0
FAILURE_TTL_SECONDS = 30.0


def estimate_tokens(term: str) -> int:
    return max(1, math.ceil(len(term) / _CHARS_PER_TOKEN))


def build_keyterms(
    ctx: Any, *, limit: int = MAX_KEYTERMS, token_budget: int = KEYTERM_TOKEN_BUDGET
) -> list[str]:
    """Glossary terms and their aliases, products, customers: trimmed (inner whitespace
    collapsed), de-duplicated case-insensitively (first spelling wins, glossary first), then
    longest first, cut at `limit` terms and `token_budget` estimated tokens. Longest first
    because multi-word names and phrases are what ASR mangles most and short ones risk
    boosting common words. Deepgram reads a comma or semicolon as part of one literal term,
    so those are kept as given rather than split."""
    candidates: list[str] = []
    for g in ctx.glossary:
        candidates.append(g.term)
        candidates.extend(g.aliases)
    candidates.extend(ctx.products)
    candidates.extend(ctx.customers)

    seen: set[str] = set()
    unique: list[str] = []
    for raw in candidates:
        term = " ".join(raw.split())
        key = term.casefold()
        if not term or key in seen:
            continue
        seen.add(key)
        unique.append(term)

    unique.sort(key=len, reverse=True)  # stable: equal lengths keep glossary-first order
    out: list[str] = []
    spent = 0
    for term in unique:
        if len(out) >= limit:
            break
        cost = estimate_tokens(term)
        if spent + cost > token_budget:
            continue  # a shorter term further down may still fit
        out.append(term)
        spent += cost
    return out


def keyterms_supported(model: str, language: str) -> bool:
    """Deepgram's rule: `keyterm` is a Nova-3 (and Flux) feature, "available for both
    monolingual and multilingual transcription", so `language=multi` is fine on Nova-3. Older
    models (Nova-2, Enhanced, Base) take the legacy `keywords` parameter instead, and an
    unsupported parameter must not be sent. Flux is a different endpoint (`/v2/listen`) that
    this provider does not use. `language` is kept in the signature so a future per-language
    restriction lands here and nowhere else."""
    del language  # every Nova-3 language, including multi, accepts keyterm today
    return model.casefold().startswith("nova-3")


class OrgContextFetcher(Protocol):
    async def fetch(self, org_id: str) -> Any: ...


class GrpcOrgContextFetcher:
    """One `GetOrgContext` call per fetch over a lazily opened insecure mesh channel (the same
    transport `UserContextClient` uses). Raises on any failure; `OrgKeyterms` absorbs it."""

    def __init__(self, address: str, timeout_s: float = FETCH_TIMEOUT_SECONDS) -> None:
        self._address = address
        self._timeout = timeout_s
        self._channel: grpc.aio.Channel | None = None

    async def fetch(self, org_id: str) -> Any:
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self._address)
        call = self._channel.unary_unary(
            METHOD,
            request_serializer=GetOrgContextRequest.SerializeToString,
            response_deserializer=OrgContext.FromString,
        )
        return await call(
            GetOrgContextRequest(), metadata=(("org-id", org_id),), timeout=self._timeout
        )

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None


class OrgKeyterms:
    """Per-org key terms with an in-process TTL cache. `for_org` never raises."""

    def __init__(
        self,
        fetcher: OrgContextFetcher,
        *,
        ttl_s: float = CACHE_TTL_SECONDS,
        failure_ttl_s: float = FAILURE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetcher = fetcher
        self._ttl = ttl_s
        self._failure_ttl = failure_ttl_s
        self._clock = clock
        self._cache: dict[str, tuple[float, list[str]]] = {}

    async def for_org(self, org_id: str) -> list[str]:
        now = self._clock()
        hit = self._cache.get(org_id)
        if hit is not None and hit[0] > now:
            return list(hit[1])
        try:
            ctx = await self._fetcher.fetch(org_id)
            terms = build_keyterms(ctx)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - any failure means "no key terms", never a failed open
            logger.warning(
                "GetOrgContext failed for org=%s; opening the stream without key terms: %s",
                org_id,
                type(exc).__name__,
            )
            self._cache[org_id] = (now + self._failure_ttl, [])
            return []
        self._cache[org_id] = (now + self._ttl, terms)
        return list(terms)


def keyterm_params(terms: Iterable[str]) -> list[tuple[str, str]]:
    """Deepgram takes one `keyterm` query parameter per term (`keyterm=a&keyterm=b`)."""
    return [("keyterm", t) for t in terms]

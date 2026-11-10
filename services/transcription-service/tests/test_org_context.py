"""ADR-0039 D1: the org's vocabulary reaches Deepgram as `keyterm`s, within Deepgram's limits,
and any failure to get it opens the stream exactly as before."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import grpc
import pytest

from transcription_service.config import Settings
from transcription_service.org_context import (
    FIELDS,
    KEYTERM_TOKEN_BUDGET,
    MAX_KEYTERMS,
    GlossaryTerm,
    GrpcOrgContextFetcher,
    OrgContext,
    OrgKeyterms,
    build_keyterms,
    estimate_tokens,
    keyterms_supported,
)
from transcription_service.providers.deepgram import DeepgramProvider
from transcription_service.providers.factory import select_provider

ORG = "11111111-1111-1111-1111-111111111111"


def _ctx(
    glossary: list[tuple[str, list[str]]] | None = None,
    products: list[str] | None = None,
    customers: list[str] | None = None,
) -> Any:
    return OrgContext(
        glossary=[GlossaryTerm(term=t, kind="product", aliases=a) for t, a in glossary or []],
        products=products or [],
        customers=customers or [],
        never_record_topics=["salaries"],
    )


class _Fetcher:
    def __init__(self, ctx: Any = None, exc: BaseException | None = None) -> None:
        self.ctx = ctx if ctx is not None else _ctx(products=["Mytheus"])
        self.exc = exc
        self.calls = 0

    async def fetch(self, org_id: str) -> Any:
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.ctx


# --- building the list ---------------------------------------------------------------------


def test_terms_aliases_products_and_customers_are_all_used() -> None:
    terms = build_keyterms(
        _ctx(glossary=[("Nexus", ["NX"])], products=["Mytheus Brain"], customers=["Acme Corp"])
    )
    assert set(terms) == {"Nexus", "NX", "Mytheus Brain", "Acme Corp"}


def test_trimmed_deduplicated_case_insensitively_and_longest_first() -> None:
    terms = build_keyterms(
        _ctx(
            glossary=[("  Nexus  ", ["nexus", "", "   "]), ("Project   Orion", [])],
            products=["NEXUS", "Orion"],
            customers=["project orion"],
        )
    )
    assert terms == ["Project Orion", "Nexus", "Orion"]


def test_an_empty_brain_gives_no_terms() -> None:
    assert build_keyterms(OrgContext()) == []


def test_cut_at_100_keeps_the_longest() -> None:
    products = [f"P{i:03d}" for i in range(150)] + ["A much longer product name"]
    terms = build_keyterms(_ctx(products=products))
    assert len(terms) == MAX_KEYTERMS
    assert terms[0] == "A much longer product name"


def test_token_budget_is_respected_and_shorter_terms_still_fill_in() -> None:
    long_terms = [f"{'x' * 68} {i:02d}" for i in range(30)]  # 24 tokens each
    terms = build_keyterms(_ctx(products=long_terms, customers=["Acme"]))
    assert sum(estimate_tokens(t) for t in terms) <= KEYTERM_TOKEN_BUDGET < 500
    assert len(terms) < 30
    assert "Acme" in terms, "a short term after the budget-busting ones still fits"


# --- Deepgram's language/model rule ----------------------------------------------------------


def test_keyterm_rule_is_nova_3_any_language_including_multi() -> None:
    assert keyterms_supported("nova-3", "multi")
    assert keyterms_supported("nova-3", "en")
    assert keyterms_supported("nova-3-medical", "en")
    assert not keyterms_supported("nova-2", "multi")
    assert not keyterms_supported("enhanced", "en")


async def test_unsupported_model_sends_no_keyterm_and_logs_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fetcher = _Fetcher()
    provider = DeepgramProvider(api_key="dg", model="nova-2", keyterms=OrgKeyterms(fetcher))
    caplog.set_level(logging.WARNING)
    with patch(
        "transcription_service.providers.deepgram.websockets.connect", new=AsyncMock()
    ) as connect:
        await provider.open_stream(org_id=ORG, meeting_id="m", region="us")
        await provider.open_stream(org_id=ORG, meeting_id="m2", region="us")
    for call in connect.call_args_list:
        assert "keyterm" not in parse_qs(urlparse(call.args[0]).query)
    assert fetcher.calls == 0
    assert len([r for r in caplog.records if "keyterm is not supported" in r.getMessage()]) == 1


# --- the Deepgram request ------------------------------------------------------------------


async def _open_query(provider: DeepgramProvider) -> dict[str, list[str]]:
    with patch(
        "transcription_service.providers.deepgram.websockets.connect", new=AsyncMock()
    ) as connect:
        await provider.open_stream(org_id=ORG, meeting_id="m", region="us")
    return parse_qs(urlparse(connect.call_args.args[0]).query)


async def test_key_terms_are_sent_as_repeated_keyterm_params_with_multi() -> None:
    fetcher = _Fetcher(_ctx(glossary=[("Nexus", ["NX"])], products=["Mytheus Brain"]))
    query = await _open_query(DeepgramProvider(api_key="dg", keyterms=OrgKeyterms(fetcher)))
    assert query["language"] == ["multi"] and query["model"] == ["nova-3"]
    assert query["keyterm"] == ["Mytheus Brain", "Nexus", "NX"]


async def test_no_identity_address_means_no_keyterm() -> None:
    query = await _open_query(DeepgramProvider(api_key="dg"))
    assert "keyterm" not in query


async def test_fetch_failure_opens_the_stream_without_key_terms() -> None:
    for exc in (
        RuntimeError("boom"),
        TimeoutError(),
        grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(), grpc.aio.Metadata()),
    ):
        provider = DeepgramProvider(api_key="dg", keyterms=OrgKeyterms(_Fetcher(exc=exc)))
        query = await _open_query(provider)
        assert "keyterm" not in query
        assert query["model"] == ["nova-3"]


def test_factory_wires_key_terms_only_when_the_address_is_set() -> None:
    base: dict[str, Any] = dict(
        database_url="x",
        redis_url="x",
        redpanda_brokers="x",
        schema_registry_url="x",
        transcription_ws_bind="x",
        deepgram_api_key="dg",
        livekit_url=None,
        livekit_api_key=None,
        livekit_api_secret=None,
        otel_exporter_otlp_endpoint=None,
    )
    without = select_provider(Settings(**base))
    with_addr = select_provider(Settings(**base, identity_grpc_addr="identity:50051"))
    assert isinstance(without, DeepgramProvider) and without._keyterms is None
    assert isinstance(with_addr, DeepgramProvider) and with_addr._keyterms is not None


# --- cache --------------------------------------------------------------------------------


async def test_cached_per_org_for_five_minutes() -> None:
    now = [1000.0]
    fetcher = _Fetcher()
    keyterms = OrgKeyterms(fetcher, clock=lambda: now[0])
    assert await keyterms.for_org(ORG) == ["Mytheus"]
    now[0] += 299
    assert await keyterms.for_org(ORG) == ["Mytheus"]
    assert fetcher.calls == 1
    await keyterms.for_org("22222222-2222-2222-2222-222222222222")
    assert fetcher.calls == 2, "the cache is per org"
    now[0] += 2
    await keyterms.for_org(ORG)
    assert fetcher.calls == 3, "expired after 5 minutes"


async def test_a_failure_is_retried_after_a_short_pause_not_on_every_join() -> None:
    now = [1000.0]
    fetcher = _Fetcher(exc=RuntimeError("down"))
    keyterms = OrgKeyterms(fetcher, clock=lambda: now[0])
    assert await keyterms.for_org(ORG) == []
    assert await keyterms.for_org(ORG) == []
    assert fetcher.calls == 1
    fetcher.exc = None
    now[0] += 31
    assert await keyterms.for_org(ORG) == ["Mytheus"]


# --- gRPC: deadline, metadata, wire shape --------------------------------------------------


async def test_grpc_fetch_sends_org_id_in_metadata_and_times_out_at_800ms() -> None:
    """A real gRPC server that never answers: the 800 ms deadline fires and the stream would
    open without key terms. A second server answers and proves the metadata and wire shape."""
    seen: list[tuple[str, ...]] = []
    hang = asyncio.Event()

    async def handler(request: bytes, context: grpc.aio.ServicerContext) -> bytes:
        seen.append(tuple(v for k, v in context.invocation_metadata() or () if k == "org-id"))
        if not hang.is_set():
            await asyncio.sleep(5)
        return bytes(
            OrgContext(
                products=["Mytheus"], glossary=[GlossaryTerm(term="Nexus", aliases=["NX"])]
            ).SerializeToString()
        )

    server = grpc.aio.server()
    method = grpc.unary_unary_rpc_method_handler(handler)
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "mytheus.identity.v1.IdentityService", {"GetOrgContext": method}
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        fetcher = GrpcOrgContextFetcher(f"127.0.0.1:{port}")
        keyterms = OrgKeyterms(fetcher)
        loop = asyncio.get_running_loop()
        started = loop.time()
        assert await keyterms.for_org(ORG) == []
        assert loop.time() - started < 2.0
        hang.set()
        assert await OrgKeyterms(fetcher).for_org(ORG) == ["Mytheus", "Nexus", "NX"]
        assert seen and all(s == (ORG,) for s in seen)
        await fetcher.close()
    finally:
        await server.stop(None)


_BACKEND_PROTO_CANDIDATES = [
    Path(__file__).resolve().parents[4] / "mytheus-backend" / "proto" / "identity.proto",
    Path(__file__).resolve().parents[5] / "mytheus-backend" / "proto" / "identity.proto",
]


def _proto_messages(text: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for name, body in re.findall(r"message\s+(\w+)\s*\{([^}]*)\}", text):
        out[name] = {
            f: int(n) for f, n in re.findall(r"(?:repeated\s+)?\w+\s+(\w+)\s*=\s*(\d+)\s*;", body)
        }
    return out


def test_descriptor_mirrors_the_backend_proto_when_it_has_get_org_context() -> None:
    proto = next((p for p in _BACKEND_PROTO_CANDIDATES if p.exists()), None)
    if proto is None or "GetOrgContext" not in proto.read_text():
        pytest.skip("mytheus-backend identity.proto with GetOrgContext is not on disk here")
    messages = _proto_messages(proto.read_text())
    for name, fields in FIELDS.items():
        assert messages.get(name, {}) == {f[0]: f[1] for f in fields}, name

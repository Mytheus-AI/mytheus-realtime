"""The compliance gate itself: since condition 1 (signed Deepgram DPA/SCC) is unmet,
`transcription-service` must refuse to open a stream against real audio, and it must be
IMPOSSIBLE to configure it into doing so -- not merely discouraged by a comment (task brief,
resolution #1)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from transcription_service.compliance import (
    COMPLIANCE_GATE_GREEN,
    ComplianceGateClosedError,
    assert_compliance_gate_green,
)
from transcription_service.config import Settings
from transcription_service.providers.deepgram import DeepgramProvider
from transcription_service.providers.factory import select_provider
from transcription_service.providers.fake import FakeProvider


def test_gate_is_closed_today() -> None:
    """Condition 1 does not exist; the gate must be False in source, not True."""
    assert COMPLIANCE_GATE_GREEN is False


def test_assert_compliance_gate_green_raises() -> None:
    with pytest.raises(ComplianceGateClosedError):
        assert_compliance_gate_green()


@pytest.mark.asyncio
async def test_deepgram_provider_refuses_to_open_stream_and_never_touches_the_network() -> None:
    """The refusal happens BEFORE any network I/O -- proven by patching `websockets.connect`
    to a mock that would fail the test if it were ever called, then asserting the gate error
    is raised instead."""
    provider = DeepgramProvider(api_key="dg_fake_key_should_never_be_used")
    with patch(
        "transcription_service.providers.deepgram.websockets.connect", new=AsyncMock()
    ) as mock_connect:
        with pytest.raises(ComplianceGateClosedError):
            await provider.open_stream(org_id="org-1", meeting_id="meeting-1", region="us")
        mock_connect.assert_not_called()


@pytest.mark.asyncio
async def test_no_env_var_can_reopen_the_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Even a deliberately hostile environment (every plausible override name someone might
    try) cannot flip the gate -- because there is no config surface that feeds it at all."""
    for var in (
        "DEEPGRAM_COMPLIANCE_GATE",
        "COMPLIANCE_GATE_GREEN",
        "DEEPGRAM_GATE_OVERRIDE",
        "FORCE_DEEPGRAM",
    ):
        monkeypatch.setenv(var, "true")
    with pytest.raises(ComplianceGateClosedError):
        assert_compliance_gate_green()


def test_select_provider_defaults_to_fake_when_no_key() -> None:
    settings = Settings(
        database_url="",
        redis_url="",
        redpanda_brokers="",
        schema_registry_url="",
        transcription_ws_bind="",
        deepgram_api_key=None,
        livekit_url=None,
        livekit_api_key=None,
        livekit_api_secret=None,
        otel_exporter_otlp_endpoint=None,
    )
    assert isinstance(select_provider(settings), FakeProvider)


@pytest.mark.asyncio
async def test_select_provider_with_a_key_configured_still_refuses_on_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Selection alone (a key present) is NOT sufficient to open a real stream -- the
    compliance check inside `open_stream` is the actual enforcement point, independent of
    selection, per `compliance.py`'s docstring."""
    settings = Settings(
        database_url="",
        redis_url="",
        redpanda_brokers="",
        schema_registry_url="",
        transcription_ws_bind="",
        deepgram_api_key="dg_configured_but_gate_closed",
        livekit_url=None,
        livekit_api_key=None,
        livekit_api_secret=None,
        otel_exporter_otlp_endpoint=None,
    )
    provider = select_provider(settings)
    assert isinstance(provider, DeepgramProvider)
    with pytest.raises(ComplianceGateClosedError):
        await provider.open_stream(org_id="org-1", meeting_id="meeting-1", region="us")

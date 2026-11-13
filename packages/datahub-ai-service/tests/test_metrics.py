from typing import Any

import httpx
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve
from prometheus_client.parser import text_string_to_metric_families

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig


def _app(gms: FakeGms, **config: Any) -> Any:
    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080", **config),
        client_factory=FakeClientFactory(gms),
        server_info=lambda: gms.server_info(),
        telemetry_ping=lambda name, properties: None,
    )


def _samples(text: str) -> dict[tuple[str, frozenset[tuple[str, str]]], float]:
    return {
        (sample.name, frozenset(sample.labels.items())): sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
    }


async def test_metrics_count_calls_outcomes_and_refusals(gms: FakeGms) -> None:
    async with serve(_app(gms)) as url:
        transport = StreamableHttpTransport(
            f"{url}/mcp", headers={"Authorization": "Bearer alice-token"}
        )
        async with Client(transport) as client:
            await client.call_tool("get_me")
            await client.call_tool("get_me")
            # Client-chosen names never become labels.
            await client.call_tool("no_such_tool_x1", raise_on_error=False)
        async with httpx.AsyncClient() as http:
            await http.post(f"{url}/mcp", json={})
            await http.post(
                f"{url}/mcp", json={}, headers={"Authorization": "Bearer stolen"}
            )
            # No credential needed, on a GMS that requires one.
            response = await http.get(f"{url}/metrics")

    assert response.status_code == 200
    samples = _samples(response.text)

    def value(name: str, **labels: str) -> float:
        return samples.get((name, frozenset(labels.items())), 0.0)

    assert value("datahub_ai_mcp_tool_calls_total", tool="get_me", outcome="ok") == 2
    assert value("datahub_ai_mcp_tool_calls_total", tool="other", outcome="error") == 1
    assert value("datahub_ai_mcp_tool_call_duration_seconds_count", tool="get_me") == 2
    assert value("datahub_ai_mcp_auth_failures_total", reason="missing_token") == 1
    assert value("datahub_ai_mcp_auth_failures_total", reason="invalid_token") == 1
    assert value("datahub_ai_usage_events_dropped_total") == 0
    assert "no_such_tool_x1" not in response.text
    assert "alice" not in response.text


async def test_metrics_can_be_disabled(gms: FakeGms) -> None:
    async with serve(_app(gms, metrics_enabled=False)) as url:
        async with httpx.AsyncClient() as http:
            response = await http.get(f"{url}/metrics")

    # Not served: the path falls through to /mcp's authentication.
    assert response.status_code == 401

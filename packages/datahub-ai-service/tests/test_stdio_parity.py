from typing import Any

import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig
from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.stdio import build_stdio_server


def _no_ping(name: str, properties: dict[str, Any]) -> None:
    pass


# 1.3.0 predates the write tools; 1.10.0 adds glossary term versioning.
@pytest.mark.parametrize("version", ["v1.3.0", "v1.10.0"])
@pytest.mark.parametrize("mutations", ["true", "false"])
async def test_stdio_serves_the_same_tools_as_the_service(
    gms: FakeGms, monkeypatch: pytest.MonkeyPatch, version: str, mutations: str
) -> None:
    monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", mutations)
    monkeypatch.setenv("ENTITY_VERSIONING_ENABLED", "true")

    def server_info() -> GmsServerInfo:
        return GmsServerInfo(version=version, server_env="core", auth_enabled=True)

    stdio = build_stdio_server(
        FakeClientFactory(gms).build("alice-token"),
        server_info=server_info,
        telemetry_ping=_no_ping,
    )
    async with Client(stdio) as client:
        stdio_tools = [tool.name for tool in await client.list_tools()]

    app = create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080"),
        client_factory=FakeClientFactory(gms),
        server_info=server_info,
        telemetry_ping=_no_ping,
        usage_event_post=lambda event, token: None,
    )
    async with serve(app) as url:
        transport = StreamableHttpTransport(
            f"{url}/mcp", headers={"Authorization": "Bearer alice-token"}
        )
        async with Client(transport) as client:
            service_tools = [tool.name for tool in await client.list_tools()]

    assert stdio_tools == service_tools
    assert ("add_tags" in stdio_tools) == (mutations == "true" and version != "v1.3.0")
    assert ("get_glossary_term_versions" in stdio_tools) == (version == "v1.10.0")

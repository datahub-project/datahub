from typing import Any, Optional

import httpx
import pytest
from helpers import FakeClientFactory, FakeGms, serve

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig
from datahub_core_mcp_tools.gms_server_info import GmsServerInfo


def _app(gms: FakeGms, version: Optional[str] = "v1.7.0", **config: Any) -> Any:
    def server_info() -> GmsServerInfo:
        if gms.down:
            raise ConnectionError("connection refused")
        return GmsServerInfo(
            version=version,
            server_env="core",
            auth_enabled=True,
            mcp_server_enabled=gms.mcp_server_enabled,
        )

    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080", **config),
        client_factory=FakeClientFactory(gms),
        server_info=server_info,
        telemetry_ping=lambda name, properties: None,
        usage_event_post=lambda event, token: None,
    )


async def _catalog(app: Any) -> httpx.Response:
    async with serve(app) as url, httpx.AsyncClient() as http:
        return await http.get(f"{url}/catalog/tools")


async def test_catalog_lists_selectable_tools_without_a_token(gms: FakeGms) -> None:
    # The settings select only read tools: the catalog still offers the rest.
    gms.mcp_settings = {
        "enabled": True,
        "defaultMcpServerOverrides": {
            "enabled": True,
            "instructionSize": None,
            "customInstructions": None,
            "tools": [{"refType": "GROUP", "toolId": "read_metadata", "enabled": True}],
        },
    }

    response = await _catalog(_app(gms))

    assert response.status_code == 200
    tools = {tool["toolId"]: tool for tool in response.json()["tools"]}
    assert tools["search"]["category"] == "read_only"
    assert tools["search"]["groups"] == ["read_metadata"]
    assert tools["search"]["description"]
    assert tools["add_tags"]["category"] == "mutation"
    assert tools["add_tags"]["groups"] == ["write_metadata"]
    # A system tool is always served, so it isn't offered for selection.
    assert "get_me" not in tools
    assert "agents" not in response.json()
    assert gms.settings_calls == 0


@pytest.mark.parametrize(
    ("version", "versioning_tools"), [("v1.7.0", False), ("v1.10.0", True)]
)
async def test_catalog_leaves_out_tools_gms_cannot_run(
    gms: FakeGms,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    versioning_tools: bool,
) -> None:
    monkeypatch.setenv("ENTITY_VERSIONING_ENABLED", "true")

    response = await _catalog(_app(gms, version))

    tools = {tool["toolId"]: tool for tool in response.json()["tools"]}
    assert ("get_glossary_term_versions" in tools) == versioning_tools
    if versioning_tools:
        assert tools["get_glossary_term_versions"]["minVersion"]["core"] == "1.10.0"
    assert "minVersion" not in tools["search"]


async def test_catalog_is_unfiltered_while_gms_is_down(gms: FakeGms) -> None:
    gms.down = True

    response = await _catalog(_app(gms))

    assert response.status_code == 200
    assert "search" in {tool["toolId"] for tool in response.json()["tools"]}


async def test_catalog_follows_the_mutation_switch(
    gms: FakeGms, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", "false")

    response = await _catalog(_app(gms))

    categories = {tool["category"] for tool in response.json()["tools"]}
    assert categories == {"read_only"}


async def test_no_catalog_while_the_mcp_server_flag_is_off(gms: FakeGms) -> None:
    gms.mcp_server_enabled = False

    response = await _catalog(_app(gms))

    assert response.status_code == 404


async def test_no_catalog_without_mcp(gms: FakeGms) -> None:
    response = await _catalog(_app(gms, mcp_enabled=False))

    assert response.status_code == 404

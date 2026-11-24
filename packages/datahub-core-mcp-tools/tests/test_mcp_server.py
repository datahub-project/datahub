import asyncio
import pathlib
import threading
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastmcp import Client, FastMCP

import datahub_core_mcp_tools
from datahub_core_mcp_tools.graphql_helpers import (
    get_datahub_client,
    with_datahub_client,
)
from datahub_core_mcp_tools.mcp_server import (
    async_background,
    disable_list_changed_capabilities,
    register_tools,
)


async def _tools(mcp: FastMCP) -> dict[str, Any]:
    async with Client(mcp) as client:
        return {tool.name: tool for tool in await client.list_tools()}


async def test_registration_is_per_instance() -> None:
    registered, untouched = FastMCP("a"), FastMCP("b")

    register_tools(registered, is_oss=True)

    assert set(await _tools(registered)) == {"get_me"}
    assert await _tools(untouched) == {}


async def test_registering_twice_on_separate_instances_is_independent() -> None:
    first, second = FastMCP("a"), FastMCP("b")
    register_tools(first, is_oss=True)
    register_tools(second, is_oss=False)

    assert set(await _tools(first)) == set(await _tools(second)) == {"get_me"}


async def test_tools_carry_tags_and_explicit_annotations() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    get_me = (await _tools(mcp))["get_me"]

    assert get_me.annotations.read_only_hint is True
    assert get_me.annotations.destructive_hint is False
    assert get_me.annotations.open_world_hint is False
    assert get_me.meta["fastmcp"]["tags"] == ["user"]


async def test_no_proposal_tools_or_modules() -> None:
    # Proposals have no OSS backend; they live in DataHub Cloud's own package.
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    assert not [name for name in await _tools(mcp) if name.startswith("propose_")]
    package_dir = pathlib.Path(datahub_core_mcp_tools.__file__).parent
    assert not [p for p in package_dir.rglob("*.py") if "propos" in p.name]


@pytest.mark.parametrize("mode", ["legacy", "auto"])
async def test_list_changed_is_not_advertised(mode: str) -> None:
    mcp = FastMCP("t")
    disable_list_changed_capabilities(mcp)

    async with Client(mcp, mode=mode) as client:
        capabilities = client.server_capabilities

    assert capabilities is not None
    assert capabilities.tools is not None
    assert not capabilities.tools.list_changed


async def test_background_tools_see_their_own_callers_client() -> None:
    # Two callers run at once; each tool body must see the client its caller set,
    # even though both run in worker threads from the same pool.
    both_started = threading.Barrier(2, timeout=5)

    def whoami() -> Any:
        both_started.wait()
        return get_datahub_client()

    tool = async_background(whoami)
    alice, bob = MagicMock(), MagicMock()

    async def call_as(client: Any) -> Any:
        with with_datahub_client(client):
            return await tool()

    seen = await asyncio.gather(call_as(alice), call_as(bob))

    assert seen[0] is alice
    assert seen[1] is bob

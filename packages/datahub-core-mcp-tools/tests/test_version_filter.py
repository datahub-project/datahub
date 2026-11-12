from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.mcp_server import ToolType, register_tool
from datahub_core_mcp_tools.version_filter import (
    VersionFilterMiddleware,
    is_tool_available,
)
from datahub_core_mcp_tools.version_requirements import VersionRequirement, min_version

OSS_1_7 = GmsServerInfo(version="v1.7.0", server_env="core")
OSS_DEV = GmsServerInfo(version="v0.0.0-dev", server_env="core")
CLOUD = GmsServerInfo(version="v0.3.16", server_env="cloud")


@pytest.mark.parametrize(
    ("requirement", "info", "available"),
    [
        (None, OSS_1_7, True),
        (VersionRequirement(oss_min=(1, 7, 0, 0)), OSS_1_7, True),
        (VersionRequirement(oss_min=(1, 8, 0, 0)), OSS_1_7, False),
        # Unreleased builds don't know their version: only the deployment counts.
        (VersionRequirement(oss_min=(9, 0, 0, 0)), OSS_DEV, True),
        (VersionRequirement(cloud_min=(0, 3, 0, 0)), OSS_DEV, False),
        (VersionRequirement(oss_min=(1, 0, 0, 0)), CLOUD, False),
        (VersionRequirement(cloud_min=(0, 3, 16, 0)), CLOUD, True),
    ],
)
def test_is_tool_available(
    requirement: VersionRequirement | None, info: GmsServerInfo, available: bool
) -> None:
    assert is_tool_available(requirement, info) == available


@min_version(oss="1.7.0", cloud="0.3.16")
def everywhere() -> str:
    """Available on 1.7.0."""
    return "ok"


@min_version(oss="1.9.0")
def newer() -> str:
    """Needs 1.9.0."""
    return "ok"


def unmarked() -> str:
    """No requirement."""
    return "ok"


def _server(info: Any) -> FastMCP:
    mcp = FastMCP("t")
    for fn in (everywhere, newer, unmarked):
        register_tool(mcp, fn.__name__, fn, tool_type=ToolType.READ_ONLY)
    mcp.add_middleware(VersionFilterMiddleware(info))
    return mcp


async def test_lists_and_calls_only_supported_tools() -> None:
    async with Client(_server(lambda: OSS_1_7)) as client:
        names = {tool.name for tool in await client.list_tools()}
        assert (await client.call_tool("everywhere")).data == "ok"
        with pytest.raises(ToolError, match="not available"):
            await client.call_tool("newer")

    assert names == {"everywhere", "unmarked"}


async def test_unreadable_version_does_not_filter() -> None:
    def broken() -> GmsServerInfo:
        raise ConnectionError("GMS is down")

    async with Client(_server(broken)) as client:
        names = {tool.name for tool in await client.list_tools()}
        assert (await client.call_tool("newer")).data == "ok"

    assert names == {"everywhere", "newer", "unmarked"}


async def test_servers_filter_independently() -> None:
    old, new = _server(lambda: OSS_1_7), _server(lambda: OSS_DEV)

    async with Client(old) as a, Client(new) as b:
        assert len(await a.list_tools()) == 2
        assert len(await b.list_tools()) == 3

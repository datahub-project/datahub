"""The tool catalog: which tools an admin can select on the settings page.

``GET /catalog/tools`` lists every tool this service registered (after its
tool switches, such as TOOLS_IS_MUTATION_ENABLED) that the connected GMS can
run, whatever the settings select, so a deselected tool can be selected again.
System tools (get_me) are always served, so they aren't offered.

It needs no credential, like /health: it holds only the tools' published
names, descriptions and groups, never settings, metadata or anything about a
caller. Browsers reach it through the frontend's ``/ai/catalog/tools``, which
requires a DataHub session.
"""

import asyncio
from typing import Awaitable, Callable, Optional, Sequence

from fastmcp import FastMCP
from fastmcp.tools import Tool
from loguru import logger
from starlette.requests import Request
from starlette.responses import JSONResponse

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.mcp_server import ToolType
from datahub_core_mcp_tools.tool_groups import SYSTEM_TOOL_IDS, groups_for_tags
from datahub_core_mcp_tools.version_filter import (
    filter_tools_by_version,
    tool_version_requirement,
)

CATALOG_PATH = "/catalog/tools"

Version = tuple[int, int, int, int]


def _version(version: Optional[Version]) -> Optional[str]:
    if version is None:
        return None
    return ".".join(str(part) for part in (version if version[3] else version[:3]))


def catalog_entry(tool: Tool) -> dict[str, object]:
    tags = tool.tags or set()
    entry: dict[str, object] = {
        "toolId": tool.name,
        "description": tool.description or "",
        "category": "mutation" if ToolType.MUTATION.value in tags else "read_only",
        "groups": groups_for_tags(tags),
    }
    requirement = tool_version_requirement(tool)
    if requirement is not None:
        # Tools GMS can't run are left out, so this only says what each needs.
        entry["minVersion"] = {
            side: version
            for side, version in (
                ("core", _version(requirement.oss_min)),
                ("cloud", _version(requirement.cloud_min)),
            )
            if version is not None
        }
    return entry


def build_catalog(
    tools: Sequence[Tool], info: Optional[GmsServerInfo]
) -> dict[str, object]:
    """The catalog for ``tools``; with no GMS info, nothing is filtered by version."""
    if info is not None:
        tools = filter_tools_by_version(tools, info)
    return {
        "tools": [
            catalog_entry(tool) for tool in tools if tool.name not in SYSTEM_TOOL_IDS
        ]
    }


def catalog_endpoint(
    mcp: FastMCP, server_info: Callable[[], GmsServerInfo]
) -> Callable[[Request], Awaitable[JSONResponse]]:
    async def catalog(request: Request) -> JSONResponse:
        tools = await mcp.list_tools(run_middleware=False)
        try:
            info: Optional[GmsServerInfo] = await asyncio.to_thread(server_info)
        except Exception as e:
            # /mcp still hides what GMS can't run, so offering a tool or two
            # too many meanwhile is harmless.
            logger.warning(f"Could not read the GMS version for the catalog: {e}")
            info = None
        if info is not None and info.mcp_server_enabled is False:
            # Off with MCP_SERVER_ENABLED, like /mcp. Unlike /mcp, an unreadable
            # /config doesn't hide the catalog: it holds tool metadata only.
            return JSONResponse(
                {
                    "error": "mcp_disabled",
                    "error_description": "DataHub's MCP server is off.",
                },
                status_code=404,
            )
        return JSONResponse(build_catalog(tools, info))

    return catalog

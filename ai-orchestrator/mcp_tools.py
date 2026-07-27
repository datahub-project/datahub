"""MCP tool layer: connects to DataHub's MCP server (mcp-server-datahub) instead of
hand-written GraphQL. Same GMS underneath. Transport is HTTP if DATAHUB_MCP_URL is
set, else stdio via `uvx mcp-server-datahub`."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

# HTTP transport (Cloud / remote). If unset, fall back to stdio + uvx.
DATAHUB_MCP_URL = os.environ.get("DATAHUB_MCP_URL", "")
DATAHUB_MCP_TOKEN = os.environ.get("DATAHUB_MCP_TOKEN", "")

# stdio transport: the MCP server connects to this GMS.
DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")
# Opt-in to write tools (tags/descriptions/owners/...).
ENABLE_MUTATIONS = os.environ.get("DATAHUB_MCP_ENABLE_MUTATIONS", "").lower() in (
    "1",
    "true",
    "yes",
)

# Tool schemas are stable per process; discover once.
_TOOL_DEFS_CACHE: Optional[list[dict[str, Any]]] = None


class MCPSession:
    """Exposes an MCP session as (definitions, execute_tool) for the agent loop."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self.definitions: list[dict[str, Any]] = []

    async def load_definitions(self) -> None:
        global _TOOL_DEFS_CACHE
        if _TOOL_DEFS_CACHE is None:
            tools = (await self._session.list_tools()).tools
            _TOOL_DEFS_CACHE = [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "input_schema": t.inputSchema,
                }
                for t in tools
            ]
        self.definitions = _TOOL_DEFS_CACHE

    async def execute_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await self._session.call_tool(name, args or {})
        except Exception as exc:  # noqa: BLE001
            return {"error": f"Tool {name} failed: {exc}"}

        # Collapse MCP content blocks into text for the LLM.
        text = "\n".join(
            block.text
            for block in result.content
            if getattr(block, "type", None) == "text"
        )
        if result.isError:
            return {"error": text or f"Tool {name} failed"}
        return {"result": text}


def _stdio_params() -> StdioServerParameters:
    # Inherit os.environ so PATH/uvx resolve; add the vars the server reads at startup.
    env = {**os.environ, "DATAHUB_GMS_URL": DATAHUB_GMS_URL}
    if DATAHUB_GMS_TOKEN:
        env["DATAHUB_GMS_TOKEN"] = DATAHUB_GMS_TOKEN
    if ENABLE_MUTATIONS:
        env["TOOLS_IS_MUTATION_ENABLED"] = "true"
    # If `uvx` isn't on PATH you'll get `spawn uvx ENOENT` — use its full path here.
    return StdioServerParameters(
        command="uvx", args=["mcp-server-datahub@latest"], env=env
    )


@asynccontextmanager
async def mcp_session() -> AsyncIterator[MCPSession]:
    # Fresh session per request (a ClientSession isn't safe to share across tasks);
    # tool discovery is cached. For higher traffic, pool a persistent server instead.
    if DATAHUB_MCP_URL:
        headers = (
            {"Authorization": f"Bearer {DATAHUB_MCP_TOKEN}"} if DATAHUB_MCP_TOKEN else None
        )
        async with streamablehttp_client(DATAHUB_MCP_URL, headers=headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                wrapper = MCPSession(session)
                await wrapper.load_definitions()
                yield wrapper
    else:
        async with stdio_client(_stdio_params()) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                wrapper = MCPSession(session)
                await wrapper.load_definitions()
                yield wrapper

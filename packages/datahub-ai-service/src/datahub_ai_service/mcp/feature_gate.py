"""Keeps MCP off until DataHub turns it on with MCP_SERVER_ENABLED.

GMS reports the flag in /config (``mcpServer.enabled``), read through the same
60-second cache as the version and auth mode. While it is off, MCP paths answer
404 as if they didn't exist. A GMS from before the flag doesn't report it; that
GMS was paired with this service on purpose, so MCP stays on. When /config
can't be read, requests get a 503, like every other check that needs GMS.
"""

import asyncio
from typing import Callable, Sequence

from loguru import logger
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo


class McpFeatureGate:
    """ASGI middleware: refuses requests to ``paths`` while the flag is off."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        server_info: Callable[[], GmsServerInfo],
        paths: Sequence[str],
    ) -> None:
        self._app = app
        self._server_info = server_info
        self._paths = tuple(paths)

    def _gated(self, path: str) -> bool:
        return any(path == p or path.startswith(f"{p}/") for p in self._paths)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self._gated(scope["path"]):
            await self._app(scope, receive, send)
            return
        try:
            info = await asyncio.to_thread(self._server_info)
        except Exception as e:
            logger.warning(f"Could not read GMS /config: {e}")
            response = JSONResponse(
                {
                    "error": "datahub_unavailable",
                    "error_description": "Can't tell whether DataHub's MCP server is "
                    "on: DataHub may be down. Try again shortly.",
                },
                status_code=503,
            )
            await response(scope, receive, send)
            return
        if info.mcp_server_enabled is False:
            response = JSONResponse(
                {
                    "error": "mcp_disabled",
                    "error_description": "DataHub's MCP server is off. An administrator "
                    "can turn it on by setting MCP_SERVER_ENABLED=true on DataHub GMS.",
                },
                status_code=404,
            )
            await response(scope, receive, send)
            return
        await self._app(scope, receive, send)

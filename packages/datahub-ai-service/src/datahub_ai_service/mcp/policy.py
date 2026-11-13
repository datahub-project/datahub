"""Apply the settings-page policy to /mcp.

``McpSettingsGate`` sits behind McpAuthMiddleware: it reads the policy with the
admitted caller's token (from the shared cache when fresh) and answers 404 while
MCP is off, or 503 while the settings can't be read. Requests already running
when MCP is turned off finish.

``McpPolicyMiddleware`` then applies the request's policy inside the MCP
server: it lists only the selected tools, refuses calls to the rest, and serves
the instructions the settings ask for.
"""

import asyncio
import json
from typing import Any, Callable, Optional, Sequence

from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger
from mcp import types as mt
from starlette.types import ASGIApp, Receive, Scope, Send

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.mcp_instructions import build_instructions
from datahub_core_mcp_tools.tool_groups import (
    SYSTEM_TOOL_IDS,
    ToolLike,
    resolve_tool_refs,
)
from datahub_core_mcp_tools.version_filter import filter_tools_by_version

from ..metrics import ServiceMetrics
from .auth import CALLER_STATE_KEY, Caller
from .client_factory import CallerClientFactory
from .settings import (
    McpPolicy,
    McpPolicyCache,
    SettingsDeniedError,
    SettingsUnavailableError,
    read_mcp_policy,
)

POLICY_STATE_KEY = "datahub_mcp_policy"

# Bounds how long one settings read can hold up the requests queued behind it.
SETTINGS_READ_LIMIT_SECONDS = 10.0


def is_tool_allowed(tool: ToolLike, policy: McpPolicy) -> bool:
    if policy.tool_refs is None or tool.name in SYSTEM_TOOL_IDS:
        return True
    # A tool's groups come from its own tags, so resolving against it alone
    # gives the same answer as resolving against every tool.
    allowed = resolve_tool_refs(policy.tool_refs, [tool])
    return allowed is None or tool.name in allowed


async def _respond(send: Send, status: int, error: str, description: str) -> None:
    body = json.dumps({"error": error, "error_description": description}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class McpSettingsGate:
    """ASGI middleware: admits a request only while the settings say MCP is on."""

    def __init__(
        self,
        app: ASGIApp,
        cache: McpPolicyCache,
        client_factory: CallerClientFactory,
        server_info: Callable[[], GmsServerInfo],
        *,
        gms_url: str,
        on_token_rejected: Optional[Callable[[str], None]] = None,
        metrics: Optional[ServiceMetrics] = None,
    ) -> None:
        self._app = app
        self._cache = cache
        self._client_factory = client_factory
        self._server_info = server_info
        self._gms_url = gms_url
        self._on_token_rejected = on_token_rejected
        self._metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        caller = scope.get("state", {}).get(CALLER_STATE_KEY)
        if not isinstance(caller, Caller):
            raise RuntimeError("MCP request reached the settings gate without a caller")

        try:
            policy = await self._policy(caller)
        except SettingsDeniedError as e:
            if e.status == 401 and caller.token and self._on_token_rejected:
                self._on_token_rejected(caller.token)
            await self._refuse(
                send,
                e.status,
                "invalid_token" if e.status == 401 else "forbidden",
                "DataHub refused to show this caller the MCP settings.",
            )
            return
        except SettingsUnavailableError:
            await self._refuse(
                send,
                503,
                "mcp_settings_unavailable",
                "DataHub's MCP settings could not be read. Try again shortly.",
            )
            return
        if not policy.enabled:
            await self._refuse(
                send,
                404,
                "mcp_disabled",
                "MCP is turned off in DataHub's settings.",
            )
            return

        scope.setdefault("state", {})[POLICY_STATE_KEY] = policy
        await self._app(scope, receive, send)

    async def _policy(self, caller: Caller) -> McpPolicy:
        try:
            info = await asyncio.to_thread(self._server_info)
        except Exception as e:
            raise SettingsUnavailableError(f"GMS /config: {e}") from e
        key = (self._gms_url, info.version, info.server_env)

        def read() -> McpPolicy:
            client = self._client_factory.build(
                caller.token, time_limit_seconds=SETTINGS_READ_LIMIT_SECONDS
            )
            try:
                return read_mcp_policy(client)
            finally:
                client._graph.close()

        return await self._cache.get(key, read)

    async def _refuse(
        self, send: Send, status: int, error: str, description: str
    ) -> None:
        if self._metrics is not None:
            self._metrics.auth_failures.labels(error).inc()
        await _respond(send, status, error, description)


def current_policy() -> McpPolicy:
    request = get_http_request()
    policy = request.scope.get("state", {}).get(POLICY_STATE_KEY)
    if not isinstance(policy, McpPolicy):
        # McpSettingsGate sits in front of every /mcp request: a wiring bug.
        raise RuntimeError("MCP request reached the server without a policy")
    return policy


class McpPolicyMiddleware(Middleware):
    """Serves the request's tool selection and instructions."""

    def __init__(self, server_info: Callable[[], GmsServerInfo]) -> None:
        self._server_info = server_info

    async def on_list_tools(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        policy = current_policy()
        tools = await call_next(context)
        return [tool for tool in tools if is_tool_allowed(tool, policy)]

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        policy = current_policy()
        name = getattr(context.message, "name", None)
        if name and policy.tool_refs is not None and context.fastmcp_context:
            tool = await context.fastmcp_context.fastmcp.get_tool(name)
            if tool is not None and not is_tool_allowed(tool, policy):
                raise ToolError(
                    f"{name} is turned off for this DataHub MCP server in DataHub's "
                    "settings."
                )
        return await call_next(context)

    async def on_initialize(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        result = await call_next(context)
        if isinstance(result, mt.InitializeResult):
            instructions = await self._instructions(context)
            return result.model_copy(update={"instructions": instructions})
        return result

    async def on_discover(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        result = await call_next(context)
        if isinstance(result, mt.DiscoverResult):
            instructions = await self._instructions(context)
            return result.model_copy(update={"instructions": instructions})
        if isinstance(result, dict):
            return {**result, "instructions": await self._instructions(context)}
        return result

    async def _instructions(self, context: MiddlewareContext[Any]) -> Optional[str]:
        policy = current_policy()
        tools: Sequence[Any] = []
        if context.fastmcp_context is not None:
            tools = await context.fastmcp_context.fastmcp.list_tools(
                run_middleware=False
            )
        try:
            info = await asyncio.to_thread(self._server_info)
            tools = filter_tools_by_version(tools, info)
        except Exception as e:
            logger.warning(f"Could not read the GMS version for instructions: {e}")
        served = [tool.name for tool in tools if is_tool_allowed(tool, policy)]
        return build_instructions(
            policy.instruction_size, served, policy.custom_instructions
        )

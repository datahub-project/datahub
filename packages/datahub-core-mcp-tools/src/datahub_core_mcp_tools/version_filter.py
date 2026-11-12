"""Hide and refuse tools the connected GMS is too old (or the wrong kind) for.

A tool's ``@min_version`` marker travels with its registered function, so the
filter reads it from the tool itself: nothing is kept in module-level state,
and two servers in one process filter independently.
"""

import asyncio
from typing import Any, Callable, Optional, Sequence, TypeVar

from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from .gms_server_info import GmsServerInfo
from .version_requirements import VersionRequirement, get_version_requirement

T = TypeVar("T")


def tool_version_requirement(tool: object) -> Optional[VersionRequirement]:
    fn = getattr(tool, "fn", None)
    return get_version_requirement(fn) if fn is not None else None


def is_tool_available(
    requirement: Optional[VersionRequirement], info: GmsServerInfo
) -> bool:
    """Whether a tool with ``requirement`` works on the GMS ``info`` describes.

    The deployment type comes from serverEnv and is always honored. The version
    is compared only when GMS reports a real release: unreleased builds keep
    every tool their deployment type allows.
    """
    if requirement is None:
        return True
    minimum = requirement.cloud_min if info.is_cloud else requirement.oss_min
    if minimum is None:
        return False
    version = info.parsed_version
    return version is None or version >= minimum


def filter_tools_by_version(tools: Sequence[T], info: GmsServerInfo) -> list[T]:
    return [t for t in tools if is_tool_available(tool_version_requirement(t), info)]


class VersionFilterMiddleware(Middleware):
    """Lists only the tools GMS supports, and refuses direct calls to the rest.

    ``server_info`` returns the connected GMS's info; it may block, so it runs
    in a worker thread. If it fails, tools are left unfiltered: GMS is then
    failing anyway, and the tools' own calls report that.
    """

    def __init__(self, server_info: Callable[[], GmsServerInfo]) -> None:
        self._server_info = server_info

    async def _info(self) -> Optional[GmsServerInfo]:
        try:
            return await asyncio.to_thread(self._server_info)
        except Exception as e:
            logger.warning(f"Could not read the GMS version; not filtering tools: {e}")
            return None

    async def on_list_tools(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        tools = await call_next(context)
        info = await self._info()
        return tools if info is None else filter_tools_by_version(tools, info)

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        name = getattr(context.message, "name", None)
        if name and context.fastmcp_context is not None:
            tool = await context.fastmcp_context.fastmcp.get_tool(name)
            requirement = tool_version_requirement(tool)
            if requirement is not None:
                info = await self._info()
                if info is not None and not is_tool_available(requirement, info):
                    raise ToolError(
                        f"{name} is not available on this DataHub "
                        f"({'Cloud' if info.is_cloud else 'Core'} {info.version})."
                    )
        return await call_next(context)

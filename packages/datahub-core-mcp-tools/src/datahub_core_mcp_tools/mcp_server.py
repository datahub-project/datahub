"""Register DataHub's MCP tools on a host's FastMCP instance.

There is no module-level server: each host (the datahub-ai-service, the local
stdio wrapper, DataHub Cloud) creates its own FastMCP instance, owns transport
and authentication, and calls ``register_tools`` on it. Registering on one
instance never affects another.
"""

import functools
import inspect
from enum import Enum
from typing import Awaitable, Callable, Collection, Optional, ParamSpec, TypeVar

from fastmcp import FastMCP
from loguru import logger
from mcp.server.lowlevel.server import NotificationOptions

from .background_execution import run_in_background
from .graphql_helpers import get_mcp_context
from .tool_context import BackgroundExecution
from .tools.get_me import get_me
from .version_requirements import tool_annotations

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ToolType(Enum):
    """Tags that sort tools into the read and write groups."""

    SEARCH = "search"
    MUTATION = "mutation"
    USER = "user"
    # Read-only tools outside the search surface. Like SEARCH, they belong to the
    # read group, so they are selected by default rather than left ungrouped.
    READ_ONLY = "read_only"


def _tool_fn_label(fn: Callable) -> str:
    """Best-effort name for logging (handles functools.partial wrappers)."""
    name = getattr(fn, "__name__", None) or getattr(
        getattr(fn, "func", None), "__name__", None
    )
    return name if isinstance(name, str) and name else repr(fn)


def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    """Wrap a synchronous tool so it runs in a worker thread.

    FastMCP runs sync tools on the event loop, where one slow GMS call would block
    every other request (https://github.com/jlowin/fastmcp/issues/864).
    """
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        execution = None
        try:
            execution = get_mcp_context().tool_context.get(BackgroundExecution)
        except LookupError:
            pass
        try:
            return await run_in_background(fn, *args, **kwargs)
        except Exception:
            # A host that supplies BackgroundExecution owns exception logging.
            # Arguments stay out of the log: they can carry user data.
            if execution is None:
                logger.exception(f"Tool function {_tool_fn_label(fn)} failed")
            raise

    return wrapper


def disable_list_changed_capabilities(mcp_instance: FastMCP) -> None:
    """Stop advertising ``listChanged`` for tools, prompts and resources.

    FastMCP advertises ``listChanged: true`` unconditionally, which tells clients
    their cached list stays valid until the server says otherwise. A stateless HTTP
    host has no session to send ``notifications/*/list_changed`` over, so clients
    that trust the advertisement keep a stale tool list after an admin changes it.
    ``false`` is the honest signal: re-list rather than cache indefinitely.
    """
    mcp_instance._mcp_server.notification_options = NotificationOptions(
        prompts_changed=False,
        resources_changed=False,
        tools_changed=False,
    )


def register_tool(
    mcp_instance: FastMCP,
    name: str,
    fn: Callable,
    *,
    tool_type: ToolType,
    description: Optional[str] = None,
) -> None:
    """Register one synchronous tool function on ``mcp_instance``.

    The function runs in a worker thread (see ``async_background``), is tagged
    with its tool type, and carries MCP annotations from its @read_only,
    @destructive and @open_world markers.
    """
    mcp_instance.tool(
        name=name,
        description=description or fn.__doc__,
        tags={tool_type.value},
        annotations=tool_annotations(fn),
    )(async_background(fn))


def register_tools(
    mcp_instance: FastMCP,
    *,
    is_oss: bool,
    groups: Optional[Collection[str]] = None,
) -> list[str]:
    """Register every DataHub tool this deployment supports on ``mcp_instance``.

    Args:
        mcp_instance: The host's FastMCP instance.
        is_oss: Whether the tools talk to DataHub Core (True) or DataHub Cloud
            (False). Some tools describe themselves differently on each.
        groups: Register only these tool groups (see tool_groups), plus the
            system tools every server serves. None registers every group.

    Returns:
        The names of the tools registered.
    """
    # tool_groups builds on ToolType from this module.
    from .tool_groups import SYSTEM_TOOL_IDS, groups_for_tags

    logger.info(f"Registering DataHub MCP tools (is_oss={is_oss}, groups={groups})")
    registered: list[str] = []

    def add(name: str, fn: Callable, tool_type: ToolType) -> None:
        if groups is not None and name not in SYSTEM_TOOL_IDS:
            if not set(groups_for_tags({tool_type.value})) & set(groups):
                return
        register_tool(mcp_instance, name, fn, tool_type=tool_type)
        registered.append(name)

    add("get_me", get_me, ToolType.USER)
    return registered

"""
Version-based tool filtering for DataHub MCP server.

This module provides:
1. A `min_version` decorator to declare minimum DataHub version requirements on tool functions
2. A middleware that intercepts MCP `list_tools` requests and filters out tools
   incompatible with the connected GMS version
3. A helper function `filter_tools_by_version` for non-MCP contexts (e.g., agent tool composition)

How it works:
1. Tool functions are decorated with `@min_version(cloud="0.3.16", oss="1.4.0")`
2. During tool registration, `_register_tool` reads the decorator attribute and populates
   `TOOL_VERSION_REQUIREMENTS` (a dict mapping tool name -> VersionRequirement)
3. When tools are listed, the middleware checks the GMS version and filters out incompatible tools

Version schemes:
- Cloud versions use 4-part format: 0.x.y.z (e.g., "0.3.16" or "0.3.16.1")
- OSS/Core versions use 3-part format: x.y.z (e.g., "1.4.0")

Semantics:
- Tools WITHOUT `@min_version` are available on all versions (no restriction).
- `@min_version(cloud="0.3.16", oss="1.4.0")`: Available on Cloud >= 0.3.16 AND OSS >= 1.4.0.
- `@min_version(cloud="0.3.16")`: Available on Cloud >= 0.3.16, NOT available on OSS.
- `@min_version(oss="1.4.0")`: Available on OSS >= 1.4.0, NOT available on Cloud.

Usage (decorator on tool functions):
    from ..version_requirements import min_version

    @min_version(cloud="0.3.16", oss="1.4.0")
    def my_tool(...):
        ...

Usage (MCP middleware):
    from .version_requirements import VersionFilterMiddleware
    mcp_instance.add_middleware(VersionFilterMiddleware())

Usage (tool filtering):
    from .version_requirements import filter_tools_by_version
    filtered_tools = filter_tools_by_version(tools)
"""

import dataclasses
import re
from typing import Any, Callable, Sequence, TypeVar

import cachetools
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

# How long to cache the server version check (in seconds)
VERSION_CHECK_CACHE_TTL_SECONDS = 60  # 1 minute

# TypeVar for generic tool filtering
T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class VersionRequirement:
    """Minimum DataHub version required for a tool.

    Each field represents a minimum version tuple (major, minor, patch, build).
    Set to None to indicate the tool is not available on that deployment type.
    """

    cloud_min: tuple[int, int, int, int] | None = None
    oss_min: tuple[int, int, int, int] | None = None


def _parse_version(version_str: str) -> tuple[int, int, int, int]:
    """Parse a version string into a (major, minor, patch, build) tuple.

    Supports both 3-part (1.4.0) and 4-part (0.3.16.1) versions,
    with optional 'v' prefix and rc/suffix.

    Raises:
        ValueError: If the version string cannot be parsed.
    """
    s = version_str
    if s.startswith("v"):
        s = s[1:]
    match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?(?:rc\d+|-.*)?$", s)
    if not match:
        raise ValueError(f"Invalid version format: {version_str!r}")
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        int(match.group(4)) if match.group(4) else 0,
    )


def min_version(
    cloud: str | None = None,
    oss: str | None = None,
) -> Callable:
    """Decorator to declare the minimum DataHub version a tool requires.

    Tools without this decorator are available on all versions.

    Args:
        cloud: Minimum Cloud version string, e.g. "0.3.16". None = not available on Cloud.
        oss: Minimum OSS/Core version string, e.g. "1.4.0". None = not available on OSS.
    """
    req = VersionRequirement(
        cloud_min=_parse_version(cloud) if cloud else None,
        oss_min=_parse_version(oss) if oss else None,
    )

    def decorator(fn: Callable) -> Callable:
        fn._version_requirement = req  # type: ignore[attr-defined]
        return fn

    return decorator


# Auto-populated during tool registration by _register_tool().
# Maps tool name -> VersionRequirement. Tools not in this dict have no version restriction.
TOOL_VERSION_REQUIREMENTS: dict[str, VersionRequirement] = {}


# Cache keyed by GMS server URL so that switching between servers (e.g., in evals)
# doesn't return stale results from a different server.
_version_info_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=8, ttl=VERSION_CHECK_CACHE_TTL_SECONDS
)


@cachetools.cached(cache=_version_info_cache)
def _get_server_version_info(
    server_url: str,
) -> tuple[bool, tuple[int, int, int, int]]:
    """Get the server deployment type and version (cached per server URL).

    Args:
        server_url: The GMS server URL, used as the cache key.

    Returns:
        Tuple of (is_cloud, version_tuple).
        is_cloud is True for Cloud deployments, False for OSS/Core.
        version_tuple is (major, minor, patch, build).

    Raises:
        LookupError: If no DataHub client is set in the context.
        Exception: If the server config cannot be fetched.
    """
    # Import here to avoid circular imports at module load time
    from .mcp_server import get_datahub_client

    client = get_datahub_client()
    config = client._graph.server_config

    is_cloud = config.is_datahub_cloud
    version = config.parsed_version or (0, 0, 0, 0)

    logger.debug(
        f"Server version info for {server_url}: is_cloud={is_cloud}, version={version} "
        f"(cached for {VERSION_CHECK_CACHE_TTL_SECONDS}s)"
    )
    return is_cloud, version


def _is_tool_compatible(
    req: VersionRequirement,
    is_cloud: bool,
    server_version: tuple[int, int, int, int],
) -> bool:
    """Check if a tool with the given requirement is compatible with the server."""
    if is_cloud:
        if req.cloud_min is None:
            return False
        return server_version >= req.cloud_min
    else:
        if req.oss_min is None:
            return False
        return server_version >= req.oss_min


def filter_tools_by_version(tools: Sequence[T]) -> list[T]:
    """Filter out tools that are incompatible with the connected GMS version.

    Tools without a version requirement (not in TOOL_VERSION_REQUIREMENTS) are
    always included. Tools with a requirement are checked against the server's
    version and deployment type.

    On error (e.g., DataHub unavailable), fails open by returning all tools
    unchanged -- this is safer than incorrectly hiding tools.

    Each tool must have a 'name' attribute.

    Args:
        tools: Sequence of tool objects with a 'name' attribute.

    Returns:
        Filtered list of tools, with incompatible tools removed.
    """
    if not TOOL_VERSION_REQUIREMENTS:
        return list(tools)

    try:
        # Import here to avoid circular imports at module load time
        from .mcp_server import get_datahub_client

        client = get_datahub_client()
        server_url = client._graph._gms_server
        is_cloud, server_version = _get_server_version_info(server_url)
    except Exception as e:
        logger.warning(
            f"Failed to get server version info, returning all tools. Error: {e}"
        )
        return list(tools)

    filtered = []
    for tool in tools:
        tool_name = getattr(tool, "name", None)
        req = TOOL_VERSION_REQUIREMENTS.get(tool_name) if tool_name else None

        if req is None:
            # No version requirement -- always available
            filtered.append(tool)
            continue

        if _is_tool_compatible(req, is_cloud, server_version):
            filtered.append(tool)
        else:
            deployment = "cloud" if is_cloud else "oss"
            min_ver = req.cloud_min if is_cloud else req.oss_min
            logger.info(
                f"Filtering out tool '{tool_name}': server {deployment} "
                f"version {server_version} does not meet minimum {min_ver}"
            )

    return filtered


class VersionFilterMiddleware(Middleware):
    """FastMCP middleware that hides tools incompatible with the GMS version.

    This middleware hooks into the `on_list_tools` lifecycle event to filter
    out tools whose version requirements are not met by the connected GMS.

    The server version check is cached for VERSION_CHECK_CACHE_TTL_SECONDS
    to avoid making a /config call on every tool listing request.

    Example:
        >>> middleware = VersionFilterMiddleware()
        >>> mcp.add_middleware(middleware)
    """

    async def on_list_tools(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ) -> Any:
        tools = await call_next(context)
        return filter_tools_by_version(tools)

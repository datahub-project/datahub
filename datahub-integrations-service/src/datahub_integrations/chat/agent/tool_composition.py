"""
Utilities for composing and managing agent tools.

This module provides helper functions for combining tools from different sources
(FastMCP, ToolWrapper, etc.) into a unified tool list for agents.
"""

from typing import TYPE_CHECKING, List, Union

from fastmcp import FastMCP

from datahub_integrations.mcp_integration.tool import (
    Tool,
    tools_from_fastmcp,
)

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


def flatten_tools(
    tools: List[Union[Tool, FastMCP]], client: "DataHubClient"
) -> List[Tool]:
    """
    Flatten a mixed list of Tool and FastMCP objects into a uniform list.

    This is useful when accepting tools from different sources and needing
    a consistent Tool list. FastMCP instances use their own name
    as the tool prefix.

    Args:
        tools: Mixed list of Tool and FastMCP objects
        client: DataHub client for querying catalog contents during tool filtering

    Returns:
        Flattened list of Tool objects
    """
    result = []
    for entry in tools:
        if isinstance(entry, FastMCP):
            result.extend(tools_from_fastmcp(entry, client))
        else:
            result.append(entry)
    return result


def filter_tools_by_names(tools: List[Tool], allowed_names: List[str]) -> List[Tool]:
    """
    Filter tools by a list of allowed names.

    Args:
        tools: List of tools to filter
        allowed_names: List of tool names to keep

    Returns:
        Filtered list containing only tools with names in allowed_names
    """
    allowed_set = set(allowed_names)
    return [tool for tool in tools if tool.name in allowed_set]


def exclude_tools_by_names(tools: List[Tool], excluded_names: List[str]) -> List[Tool]:
    """
    Exclude tools by a list of names to remove.

    Args:
        tools: List of tools to filter
        excluded_names: List of tool names to exclude

    Returns:
        Filtered list without tools whose names are in excluded_names
    """
    excluded_set = set(excluded_names)
    return [tool for tool in tools if tool.name not in excluded_set]

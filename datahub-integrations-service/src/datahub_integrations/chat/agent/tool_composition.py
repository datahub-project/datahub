"""
Utilities for composing and managing agent tools.

This module provides helper functions for combining tools from different sources
(FastMCP, ToolWrapper, etc.) into a unified tool list for agents.
"""

from typing import List, Union

from fastmcp import FastMCP

from datahub_integrations.mcp_integration.tool import ToolWrapper


def tools_from_fastmcp(mcp_server: FastMCP) -> List[ToolWrapper]:
    """
    Extract ToolWrapper objects from a FastMCP server instance.

    Uses the MCP server's own name as the prefix (e.g., if mcp.name="datahub",
    tools will be named "datahub__search", "datahub__get_entities", etc.)

    Args:
        mcp_server: FastMCP server instance with registered tools

    Returns:
        List of ToolWrapper objects, one for each tool in the MCP server
    """
    return [
        ToolWrapper(_tool=tool, name_prefix=mcp_server.name)
        for tool in mcp_server._tool_manager._tools.values()
    ]


def flatten_tools(tools: List[Union[ToolWrapper, FastMCP]]) -> List[ToolWrapper]:
    """
    Flatten a mixed list of ToolWrapper and FastMCP objects into a uniform list.

    This is useful when accepting tools from different sources and needing
    a consistent ToolWrapper list. FastMCP instances use their own name
    as the tool prefix.

    Args:
        tools: Mixed list of ToolWrapper and FastMCP objects

    Returns:
        Flattened list of ToolWrapper objects
    """
    result = []
    for entry in tools:
        if isinstance(entry, FastMCP):
            result.extend(tools_from_fastmcp(entry))
        else:
            result.append(entry)
    return result


def filter_tools_by_names(
    tools: List[ToolWrapper], allowed_names: List[str]
) -> List[ToolWrapper]:
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


def exclude_tools_by_names(
    tools: List[ToolWrapper], excluded_names: List[str]
) -> List[ToolWrapper]:
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

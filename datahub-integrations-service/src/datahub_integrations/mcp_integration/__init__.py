"""MCP integration utilities.

This package provides utilities for integrating with MCP (Model Context Protocol) servers,
including both local FastMCP servers and external remote MCP servers.
"""

from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalMCPManager,
    ExternalToolWrapper,
    PluginConnectionError,
)
from datahub_integrations.mcp_integration.tool import (
    ToolRunError,
    ToolWrapper,
    async_background,
    tools_from_fastmcp,
)

__all__ = [
    # External MCP management
    "ExternalMCPManager",
    "ExternalToolWrapper",
    "PluginConnectionError",
    # Tool utilities
    "ToolWrapper",
    "ToolRunError",
    "async_background",
    "tools_from_fastmcp",
]

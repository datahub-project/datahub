"""Configuration models for external MCP servers.

This module provides Pydantic models for loading and validating external MCP server
configurations from JSON files, with support for environment variable substitution.
"""

from __future__ import annotations

import os
import re
from typing import Dict, Literal, Optional

from pydantic import BaseModel, field_validator


def substitute_env_vars(value: str) -> str:
    """Substitute environment variables in a string.

    Supports ${VAR_NAME} syntax. If the environment variable is not set,
    raises a ValueError.

    Args:
        value: String potentially containing ${VAR_NAME} patterns

    Returns:
        String with environment variables substituted

    Raises:
        ValueError: If an environment variable is referenced but not set
    """
    pattern = r"\$\{([^}]+)\}"

    def replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ValueError(
                f"Environment variable '{var_name}' is not set. "
                f"Please set it before using this MCP server configuration."
            )
        return env_value

    return re.sub(pattern, replace, value)


class MCPServerConfig(BaseModel):
    """Configuration for a single external MCP server."""

    url: str
    """The URL of the MCP server endpoint."""

    transport: Literal["http", "sse"] = "http"
    """Transport type: 'http' for Streamable HTTP, 'sse' for Server-Sent Events."""

    headers: Dict[str, str] = {}
    """HTTP headers to send with requests (e.g., Authorization)."""

    timeout: float = 30.0
    """Request timeout in seconds."""

    enabled: bool = True
    """Whether this server is enabled. Disabled servers are skipped."""

    instructions: Optional[str] = None
    """Instructions for the LLM when using tools from this server.
    
    All tools from this server will carry these instructions. When the agent
    builds its prompt, it should collect unique instructions from its tools
    (multiple tools from the same server will have identical instructions,
    so only one copy should be included).
    """

    def get_resolved_url(self) -> str:
        """Get the URL with environment variables substituted."""
        return substitute_env_vars(self.url)

    def get_resolved_headers(self) -> Dict[str, str]:
        """Get headers with environment variables substituted."""
        return {key: substitute_env_vars(value) for key, value in self.headers.items()}


class ExternalMCPConfig(BaseModel):
    """Root configuration for external MCP servers."""

    mcpServers: Dict[str, MCPServerConfig] = {}
    """Map of server name to server configuration."""

    @field_validator("mcpServers", mode="before")
    @classmethod
    def validate_server_names(cls, v: Dict[str, Dict]) -> Dict[str, Dict]:
        """Validate that server names are valid identifiers."""
        for name in v.keys():
            if not name.isidentifier():
                raise ValueError(
                    f"Server name '{name}' is not a valid identifier. "
                    f"Use alphanumeric characters and underscores only."
                )
        return v

    def get_enabled_servers(self) -> Dict[str, MCPServerConfig]:
        """Get only the enabled server configurations."""
        return {
            name: config for name, config in self.mcpServers.items() if config.enabled
        }

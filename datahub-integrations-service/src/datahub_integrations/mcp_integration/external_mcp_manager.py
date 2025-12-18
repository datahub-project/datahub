"""Manager for external MCP server connections.

This module provides the ExternalMCPManager class for connecting to and managing
external MCP servers, wrapping their tools for use with the DataCatalog agent.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Callable, List, Optional

import asyncer
import mlflow
import mlflow.entities
from fastmcp import Client
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from loguru import logger
from mcp.types import TextContent

from datahub_integrations.mcp_integration.external_mcp_config import (
    ExternalMCPConfig,
    MCPServerConfig,
)


@dataclasses.dataclass
class ExternalToolWrapper:
    """Wrapper for tools from external MCP servers.

    Unlike ToolWrapper which wraps fastmcp.tools.Tool, this wraps tool metadata
    from a remote MCP server and executes via the MCP client.

    Note on client_factory pattern:
        We use a factory function instead of a persistent client connection because
        fastmcp.Client is async-first and its session is bound to a specific event loop.
        Our agent loop is sync, so each tool.run() call uses asyncer.syncify() which
        creates a new event loop. A client connected in one event loop cannot be used
        from another. The workaround is to create a fresh connection per tool call.
        This adds overhead but avoids event loop isolation issues.

        Future optimization: Make the agent loop fully async to enable connection reuse.
    """

    name: str
    description: str
    parameters: dict
    _client_factory: "Callable[[], Client]"
    """Factory to create a fresh MCP client for each call (see note above)."""
    _original_name: str
    instructions: Optional[str] = None
    """Server-provided instructions for the LLM.
    
    Multiple tools from the same server will have identical instructions.
    When building prompts, collect unique instructions to avoid duplication.
    """

    def to_bedrock_spec(self) -> dict:
        return {
            "toolSpec": {
                "name": self.name,
                "description": self.description,
                "inputSchema": {"json": self.parameters},
            }
        }

    def run(self, arguments: dict) -> str | dict:
        """Run the tool via the MCP client.

        Creates a fresh client connection for each call to avoid event loop issues.
        Prioritizes structured_content when available, falls back to text content.
        """
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name, "external": True},
        ) as span:
            span.set_inputs(arguments)

            async def _call_tool() -> str | dict:
                client = self._client_factory()
                async with client:
                    result = await client.call_tool(self._original_name, arguments)

                    # Prioritize structured_content (machine-readable) over content (human-readable)
                    if result.structured_content is not None:
                        return result.structured_content

                    # Fall back to extracting text content from MCP response
                    if result.content:
                        texts = []
                        for content in result.content:
                            if isinstance(content, TextContent):
                                texts.append(content.text)
                            else:
                                texts.append(str(content))
                        return "\n".join(texts) if texts else ""
                    return ""

            try:
                return asyncer.syncify(_call_tool, raise_sync_error=False)()
            except Exception:
                logger.exception(f"Error calling external tool {self.name}")
                raise


class ExternalMCPManager:
    """Manager for external MCP server connections.

    Handles loading configuration, connecting to external MCP servers,
    and providing their tools for agent use.

    Usage:
        manager = ExternalMCPManager.from_config_file("mcp_servers.json")
        tools = manager.get_tools()  # Returns list of ToolWrapper-compatible objects
    """

    def __init__(self, config: ExternalMCPConfig):
        self._config = config
        self._tools: Optional[List[ExternalToolWrapper]] = None

    @classmethod
    def from_config_file(cls, path: str | pathlib.Path) -> ExternalMCPManager:
        """Load configuration from a JSON file.

        Args:
            path: Path to the JSON configuration file

        Returns:
            ExternalMCPManager instance

        Raises:
            FileNotFoundError: If the config file doesn't exist
            ValueError: If the config file is invalid
        """
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(f"MCP config file not found: {path}")

        with open(path) as f:
            data = json.load(f)

        config = ExternalMCPConfig.model_validate(data)
        return cls(config)

    @classmethod
    def from_config_file_if_exists(
        cls, path: str | pathlib.Path
    ) -> Optional[ExternalMCPManager]:
        """Load configuration from a JSON file if it exists.

        Args:
            path: Path to the JSON configuration file

        Returns:
            ExternalMCPManager instance if file exists, None otherwise
        """
        path = pathlib.Path(path)
        if not path.exists():
            logger.debug(f"MCP config file not found, skipping: {path}")
            return None

        try:
            return cls.from_config_file(path)
        except Exception as e:
            logger.warning(f"Failed to load MCP config from {path}: {e}")
            return None

    def _create_client(self, name: str, config: MCPServerConfig) -> Client:
        """Create an MCP client for the given server configuration."""
        try:
            url = config.get_resolved_url()
            headers = config.get_resolved_headers()
        except ValueError as e:
            raise ValueError(
                f"Failed to resolve config for server '{name}': {e}"
            ) from e

        transport: SSETransport | StreamableHttpTransport
        if config.transport == "sse":
            transport = SSETransport(url=url, headers=headers)
        else:  # http
            transport = StreamableHttpTransport(url=url, headers=headers)

        return Client(transport, timeout=config.timeout)

    async def _discover_tools(
        self, name: str, config: MCPServerConfig
    ) -> List[ExternalToolWrapper]:
        """Connect to an MCP server to discover its tools.

        Creates a temporary connection to list available tools, then creates
        tool wrappers with a factory function for on-demand connections.
        """

        # Create a client factory that tool wrappers will use for each call
        def client_factory() -> Client:
            return self._create_client(name, config)

        # Temporarily connect to discover available tools
        client = client_factory()
        async with client:
            logger.info(f"Discovering tools from MCP server: {name} ({config.url})")
            tools = await client.list_tools()
            logger.info(f"Server '{name}' provides {len(tools)} tools")

            return [
                ExternalToolWrapper(
                    name=f"{name}__{tool.name}",
                    description=tool.description or "",
                    parameters=tool.inputSchema if tool.inputSchema else {},
                    _client_factory=client_factory,
                    _original_name=tool.name,
                    instructions=config.instructions,
                )
                for tool in tools
            ]

    async def _discover_all_tools(self) -> List[ExternalToolWrapper]:
        """Discover tools from all enabled MCP servers."""
        all_tools: List[ExternalToolWrapper] = []
        enabled_servers = self._config.get_enabled_servers()

        for name, config in enabled_servers.items():
            try:
                tools = await self._discover_tools(name, config)
                all_tools.extend(tools)
            except Exception as e:
                logger.error(f"Failed to discover tools from MCP server '{name}': {e}")
                # Continue with other servers

        return all_tools

    def get_tools(self) -> List[ExternalToolWrapper]:
        """Get all tools from external MCP servers.

        Lazily discovers tools on first call. Results are cached.
        Each tool call creates its own connection (no persistent connections).

        Returns:
            List of ExternalToolWrapper objects compatible with agent tool interfaces
        """
        if self._tools is not None:
            return self._tools

        # Use syncify to run async tool discovery
        self._tools = asyncer.syncify(
            self._discover_all_tools, raise_sync_error=False
        )()
        return self._tools

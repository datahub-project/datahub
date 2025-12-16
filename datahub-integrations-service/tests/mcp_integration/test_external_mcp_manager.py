"""Unit tests for external MCP manager.

Note: mypy dict-item errors are ignored because Pydantic automatically coerces
dicts to model instances, which mypy doesn't understand.
"""

# mypy: disable-error-code="dict-item"

import json
import pathlib
import tempfile
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from datahub_integrations.mcp_integration.external_mcp_config import (
    ExternalMCPConfig,
    MCPServerConfig,
)
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalMCPManager,
    ExternalToolWrapper,
)


class TestExternalToolWrapper:
    """Tests for the ExternalToolWrapper class."""

    def test_to_bedrock_spec(self) -> None:
        """Test that to_bedrock_spec returns correct format."""
        wrapper = ExternalToolWrapper(
            name="github__search_code",
            description="Search code in GitHub repositories",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "repo": {"type": "string", "description": "Repository name"},
                },
                "required": ["query"],
            },
            _client_factory=Mock(),
            _original_name="search_code",
        )

        spec = wrapper.to_bedrock_spec()

        assert spec == {
            "toolSpec": {
                "name": "github__search_code",
                "description": "Search code in GitHub repositories",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string", "description": "Search query"},
                            "repo": {
                                "type": "string",
                                "description": "Repository name",
                            },
                        },
                        "required": ["query"],
                    }
                },
            }
        }

    def test_to_bedrock_spec_with_empty_parameters(self) -> None:
        """Test to_bedrock_spec with empty parameters."""
        wrapper = ExternalToolWrapper(
            name="simple_tool",
            description="A simple tool",
            parameters={},
            _client_factory=Mock(),
            _original_name="simple",
        )

        spec = wrapper.to_bedrock_spec()

        assert spec["toolSpec"]["name"] == "simple_tool"
        assert spec["toolSpec"]["inputSchema"]["json"] == {}

    def test_instructions_attribute(self) -> None:
        """Test that instructions attribute is stored correctly."""
        wrapper = ExternalToolWrapper(
            name="tool",
            description="desc",
            parameters={},
            _client_factory=Mock(),
            _original_name="tool",
            instructions="Always include repo:owner/name in queries.",
        )

        assert wrapper.instructions == "Always include repo:owner/name in queries."

    def test_instructions_default_none(self) -> None:
        """Test that instructions defaults to None."""
        wrapper = ExternalToolWrapper(
            name="tool",
            description="desc",
            parameters={},
            _client_factory=Mock(),
            _original_name="tool",
        )

        assert wrapper.instructions is None

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    @patch("datahub_integrations.mcp_integration.external_mcp_manager.mlflow")
    def test_run_returns_structured_content(
        self, mock_mlflow: Mock, mock_asyncer: Mock
    ) -> None:
        """Test that run returns structured_content when available."""
        # Setup mock client
        mock_client = AsyncMock()
        mock_result = MagicMock()
        mock_result.structured_content = {"data": "structured"}
        mock_result.content = None
        mock_client.call_tool = AsyncMock(return_value=mock_result)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Setup syncify to call the async function
        def syncify_side_effect(async_fn: Any, **kwargs: Any) -> Any:
            def sync_wrapper() -> Any:
                import asyncio

                return asyncio.run(async_fn())

            return sync_wrapper

        mock_asyncer.syncify.side_effect = syncify_side_effect

        # Setup mlflow mock
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=None)
        mock_mlflow.start_span.return_value = mock_span

        wrapper = ExternalToolWrapper(
            name="test_tool",
            description="Test",
            parameters={},
            _client_factory=lambda: mock_client,
            _original_name="original_test",
        )

        result = wrapper.run({"arg": "value"})

        assert result == {"data": "structured"}
        mock_client.call_tool.assert_called_once_with("original_test", {"arg": "value"})


class TestExternalMCPManagerFromConfigFile:
    """Tests for ExternalMCPManager.from_config_file."""

    def test_loads_valid_config_file(self) -> None:
        """Test loading a valid configuration file."""
        config_data = {
            "mcpServers": {
                "github": {
                    "url": "http://localhost:3001",
                    "transport": "sse",
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            f.flush()

            manager = ExternalMCPManager.from_config_file(f.name)

            assert "github" in manager._config.mcpServers
            assert manager._config.mcpServers["github"].url == "http://localhost:3001"

    def test_raises_file_not_found(self) -> None:
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError) as exc_info:
            ExternalMCPManager.from_config_file("/nonexistent/path/config.json")

        assert "not found" in str(exc_info.value)

    def test_raises_on_invalid_json(self) -> None:
        """Test that invalid JSON raises an error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {")
            f.flush()

            with pytest.raises(json.JSONDecodeError):
                ExternalMCPManager.from_config_file(f.name)

    def test_raises_on_invalid_config_structure(self) -> None:
        """Test that invalid config structure raises validation error."""
        config_data = {
            "mcpServers": {
                "invalid-name": {  # Invalid: has hyphen
                    "url": "http://localhost:3001",
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            f.flush()

            with pytest.raises(ValueError) as exc_info:
                ExternalMCPManager.from_config_file(f.name)

            assert "invalid-name" in str(exc_info.value)

    def test_accepts_pathlib_path(self) -> None:
        """Test that pathlib.Path is accepted."""
        config_data: dict[str, dict[str, Any]] = {"mcpServers": {}}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            f.flush()

            path = pathlib.Path(f.name)
            manager = ExternalMCPManager.from_config_file(path)

            assert manager._config.mcpServers == {}


class TestExternalMCPManagerFromConfigFileIfExists:
    """Tests for ExternalMCPManager.from_config_file_if_exists."""

    def test_returns_manager_when_file_exists(self) -> None:
        """Test that manager is returned when file exists."""
        config_data = {"mcpServers": {"server1": {"url": "http://localhost:3001"}}}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            f.flush()

            manager = ExternalMCPManager.from_config_file_if_exists(f.name)

            assert manager is not None
            assert "server1" in manager._config.mcpServers

    def test_returns_none_when_file_missing(self) -> None:
        """Test that None is returned when file doesn't exist."""
        manager = ExternalMCPManager.from_config_file_if_exists(
            "/nonexistent/config.json"
        )

        assert manager is None

    def test_returns_none_on_load_error(self) -> None:
        """Test that None is returned on load errors (not exceptions)."""
        # Create invalid config (invalid server name)
        config_data = {"mcpServers": {"invalid-name": {"url": "http://localhost"}}}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            f.flush()

            # Should return None instead of raising
            manager = ExternalMCPManager.from_config_file_if_exists(f.name)

            assert manager is None


class TestExternalMCPManagerCreateClient:
    """Tests for ExternalMCPManager._create_client."""

    def test_creates_sse_transport_client(self) -> None:
        """Test that SSE transport client is created correctly."""
        config = ExternalMCPConfig(
            mcpServers={
                "test": {
                    "url": "http://localhost:3001/sse",
                    "transport": "sse",
                    "timeout": 45.0,
                }
            }
        )
        manager = ExternalMCPManager(config)

        with (
            patch(
                "datahub_integrations.mcp_integration.external_mcp_manager.SSETransport"
            ) as mock_sse,
            patch(
                "datahub_integrations.mcp_integration.external_mcp_manager.Client"
            ) as mock_client,
        ):
            manager._create_client("test", config.mcpServers["test"])

            mock_sse.assert_called_once_with(
                url="http://localhost:3001/sse", headers={}
            )
            mock_client.assert_called_once()
            # Check timeout was passed
            call_kwargs = mock_client.call_args
            assert call_kwargs[1]["timeout"] == 45.0

    def test_creates_http_transport_client(self) -> None:
        """Test that HTTP (Streamable) transport client is created correctly."""
        config = ExternalMCPConfig(
            mcpServers={
                "test": {
                    "url": "http://localhost:3001",
                    "transport": "http",
                    "timeout": 30.0,
                }
            }
        )
        manager = ExternalMCPManager(config)

        with (
            patch(
                "datahub_integrations.mcp_integration.external_mcp_manager.StreamableHttpTransport"
            ) as mock_http,
            patch(
                "datahub_integrations.mcp_integration.external_mcp_manager.Client"
            ) as mock_client,
        ):
            manager._create_client("test", config.mcpServers["test"])

            mock_http.assert_called_once_with(url="http://localhost:3001", headers={})
            mock_client.assert_called_once()

    def test_creates_client_with_headers(self) -> None:
        """Test that headers are passed to transport."""
        config = ExternalMCPConfig(
            mcpServers={
                "test": {
                    "url": "http://localhost:3001",
                    "headers": {"Authorization": "Bearer token"},
                }
            }
        )
        manager = ExternalMCPManager(config)

        with (
            patch(
                "datahub_integrations.mcp_integration.external_mcp_manager.StreamableHttpTransport"
            ) as mock_http,
            patch("datahub_integrations.mcp_integration.external_mcp_manager.Client"),
        ):
            manager._create_client("test", config.mcpServers["test"])

            mock_http.assert_called_once_with(
                url="http://localhost:3001",
                headers={"Authorization": "Bearer token"},
            )

    def test_raises_on_env_var_resolution_failure(self) -> None:
        """Test that env var resolution failures are propagated."""
        config = ExternalMCPConfig(
            mcpServers={
                "test": {
                    "url": "http://${MISSING_HOST}/api",
                }
            }
        )
        manager = ExternalMCPManager(config)

        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                manager._create_client("test", config.mcpServers["test"])

            assert "MISSING_HOST" in str(exc_info.value)
            assert "test" in str(exc_info.value)


class TestExternalMCPManagerGetTools:
    """Tests for ExternalMCPManager.get_tools."""

    def test_caches_tools(self) -> None:
        """Test that tools are cached after first call."""
        config = ExternalMCPConfig(mcpServers={})
        manager = ExternalMCPManager(config)

        # Pre-populate cache
        cached_tools = [
            ExternalToolWrapper(
                name="cached_tool",
                description="Cached",
                parameters={},
                _client_factory=Mock(),
                _original_name="cached",
            )
        ]
        manager._tools = cached_tools

        # Should return cached tools without any network calls
        result = manager.get_tools()

        assert result is cached_tools
        assert len(result) == 1
        assert result[0].name == "cached_tool"

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    def test_discovers_tools_on_first_call(self, mock_asyncer: Mock) -> None:
        """Test that tools are discovered on first call."""
        config = ExternalMCPConfig(
            mcpServers={"server1": {"url": "http://localhost:3001"}}
        )
        manager = ExternalMCPManager(config)

        # Mock the async discovery
        mock_tools = [
            ExternalToolWrapper(
                name="server1__tool1",
                description="Tool 1",
                parameters={},
                _client_factory=Mock(),
                _original_name="tool1",
            )
        ]

        def syncify_side_effect(async_fn: Any, **kwargs: Any) -> Any:
            def sync_wrapper() -> Any:
                return mock_tools

            return sync_wrapper

        mock_asyncer.syncify.side_effect = syncify_side_effect

        result = manager.get_tools()

        assert result == mock_tools
        assert manager._tools is not None

    def test_returns_empty_list_for_no_servers(self) -> None:
        """Test that empty list is returned when no servers configured."""
        config = ExternalMCPConfig(mcpServers={})
        manager = ExternalMCPManager(config)

        with patch(
            "datahub_integrations.mcp_integration.external_mcp_manager.asyncer"
        ) as mock_asyncer:
            mock_asyncer.syncify.return_value = lambda: []

            result = manager.get_tools()

            assert result == []


class TestExternalMCPManagerDiscoverTools:
    """Tests for ExternalMCPManager._discover_tools async method."""

    @pytest.mark.asyncio
    async def test_discovers_tools_from_server(self) -> None:
        """Test that tools are discovered from MCP server."""
        config = ExternalMCPConfig(
            mcpServers={
                "github": {
                    "url": "http://localhost:3001",
                    "instructions": "Use repo:owner/name format.",
                }
            }
        )
        manager = ExternalMCPManager(config)

        # Mock tool response
        mock_tool = MagicMock()
        mock_tool.name = "search_code"
        mock_tool.description = "Search code in repos"
        mock_tool.inputSchema = {"type": "object", "properties": {}}

        mock_client = AsyncMock()
        mock_client.list_tools = AsyncMock(return_value=[mock_tool])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(manager, "_create_client", return_value=mock_client):
            tools = await manager._discover_tools("github", config.mcpServers["github"])

        assert len(tools) == 1
        assert tools[0].name == "github__search_code"
        assert tools[0].description == "Search code in repos"
        assert tools[0]._original_name == "search_code"
        assert tools[0].instructions == "Use repo:owner/name format."

    @pytest.mark.asyncio
    async def test_handles_empty_description(self) -> None:
        """Test that None description is converted to empty string."""
        config = ExternalMCPConfig(
            mcpServers={"server": {"url": "http://localhost:3001"}}
        )
        manager = ExternalMCPManager(config)

        mock_tool = MagicMock()
        mock_tool.name = "tool"
        mock_tool.description = None
        mock_tool.inputSchema = {}

        mock_client = AsyncMock()
        mock_client.list_tools = AsyncMock(return_value=[mock_tool])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(manager, "_create_client", return_value=mock_client):
            tools = await manager._discover_tools("server", config.mcpServers["server"])

        assert tools[0].description == ""

    @pytest.mark.asyncio
    async def test_handles_none_input_schema(self) -> None:
        """Test that None inputSchema is converted to empty dict."""
        config = ExternalMCPConfig(
            mcpServers={"server": {"url": "http://localhost:3001"}}
        )
        manager = ExternalMCPManager(config)

        mock_tool = MagicMock()
        mock_tool.name = "tool"
        mock_tool.description = "desc"
        mock_tool.inputSchema = None

        mock_client = AsyncMock()
        mock_client.list_tools = AsyncMock(return_value=[mock_tool])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(manager, "_create_client", return_value=mock_client):
            tools = await manager._discover_tools("server", config.mcpServers["server"])

        assert tools[0].parameters == {}


class TestExternalMCPManagerDiscoverAllTools:
    """Tests for ExternalMCPManager._discover_all_tools async method."""

    @pytest.mark.asyncio
    async def test_discovers_from_multiple_servers(self) -> None:
        """Test that tools are discovered from multiple servers."""
        config = ExternalMCPConfig(
            mcpServers={
                "server1": {"url": "http://localhost:3001"},
                "server2": {"url": "http://localhost:3002"},
            }
        )
        manager = ExternalMCPManager(config)

        async def mock_discover(
            name: str, cfg: MCPServerConfig
        ) -> list[ExternalToolWrapper]:
            return [
                ExternalToolWrapper(
                    name=f"{name}__tool",
                    description=f"Tool from {name}",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        assert len(tools) == 2
        tool_names = {t.name for t in tools}
        assert "server1__tool" in tool_names
        assert "server2__tool" in tool_names

    @pytest.mark.asyncio
    async def test_skips_disabled_servers(self) -> None:
        """Test that disabled servers are skipped."""
        config = ExternalMCPConfig(
            mcpServers={
                "enabled_server": {"url": "http://localhost:3001", "enabled": True},
                "disabled_server": {"url": "http://localhost:3002", "enabled": False},
            }
        )
        manager = ExternalMCPManager(config)

        discovered_servers: list[str] = []

        async def mock_discover(
            name: str, cfg: MCPServerConfig
        ) -> list[ExternalToolWrapper]:
            discovered_servers.append(name)
            return [
                ExternalToolWrapper(
                    name=f"{name}__tool",
                    description="Tool",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        assert len(tools) == 1
        assert "enabled_server" in discovered_servers
        assert "disabled_server" not in discovered_servers

    @pytest.mark.asyncio
    async def test_continues_on_server_error(self) -> None:
        """Test that discovery continues when one server fails."""
        config = ExternalMCPConfig(
            mcpServers={
                "failing_server": {"url": "http://localhost:3001"},
                "working_server": {"url": "http://localhost:3002"},
            }
        )
        manager = ExternalMCPManager(config)

        call_count = 0

        async def mock_discover(
            name: str, cfg: MCPServerConfig
        ) -> list[ExternalToolWrapper]:
            nonlocal call_count
            call_count += 1
            if name == "failing_server":
                raise ConnectionError("Failed to connect")
            return [
                ExternalToolWrapper(
                    name=f"{name}__tool",
                    description="Tool",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        # Should have attempted both servers
        assert call_count == 2
        # Should have tools from working server
        assert len(tools) == 1
        assert tools[0].name == "working_server__tool"

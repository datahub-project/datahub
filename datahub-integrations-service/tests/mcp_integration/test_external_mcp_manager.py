"""Unit tests for external MCP manager."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest

from datahub_integrations.mcp_integration.ai_plugin_loader import (
    AiPluginAuthType,
    AiPluginConfig,
    McpServerConfig,
    McpTransport,
)
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalMCPManager,
    ExternalToolWrapper,
    PluginConnectionError,
    _is_credential_error,
    _is_discovery_auth_error,
    _sanitize_tool_prefix,
)
from datahub_integrations.oauth.credential_store import TokenRefreshError


def create_test_plugin(
    plugin_id: str = "test-plugin",
    display_name: str = "Test Plugin",
    url: str = "http://localhost:3001",
    auth_type: AiPluginAuthType = AiPluginAuthType.NONE,
) -> AiPluginConfig:
    """Create a test plugin configuration."""
    return AiPluginConfig(
        id=plugin_id,
        service_urn=f"urn:li:service:{plugin_id}",
        display_name=display_name,
        description=None,
        instructions="Test instructions",
        auth_type=auth_type,
        mcp_config=McpServerConfig(
            url=url,
            transport=McpTransport.HTTP,
            timeout=30.0,
            custom_headers={},
        ),
    )


class TestIsCredentialError:
    """Tests for _is_credential_error helper function.

    This function determines if an error indicates invalid credentials (401)
    vs. access denied for a specific resource (403). Only credential errors
    should trigger plugin disconnection.
    """

    def test_token_refresh_error_is_credential_error(self) -> None:
        """Test that TokenRefreshError is recognized as credential error."""
        error = TokenRefreshError("Token refresh failed")
        assert _is_credential_error(error) is True

    def test_http_401_is_credential_error(self) -> None:
        """Test that HTTP 401 is recognized as credential error."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        error = httpx.HTTPStatusError(
            "Unauthorized", request=mock_request, response=mock_response
        )
        assert _is_credential_error(error) is True

    def test_http_403_is_not_credential_error(self) -> None:
        """Test that HTTP 403 is NOT a credential error.

        403 means credentials are valid but user lacks permission for a
        specific resource. The agent should handle this gracefully.
        """
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        error = httpx.HTTPStatusError(
            "Forbidden", request=mock_request, response=mock_response
        )
        assert _is_credential_error(error) is False

    def test_http_500_is_not_credential_error(self) -> None:
        """Test that HTTP 500 is NOT a credential error."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        error = httpx.HTTPStatusError(
            "Internal Server Error", request=mock_request, response=mock_response
        )
        assert _is_credential_error(error) is False

    def test_http_400_is_not_credential_error(self) -> None:
        """Test that HTTP 400 is NOT a credential error."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 400
        error = httpx.HTTPStatusError(
            "Bad Request", request=mock_request, response=mock_response
        )
        assert _is_credential_error(error) is False

    def test_error_with_unauthorized_in_message(self) -> None:
        """Test that errors with 'unauthorized' in message are credential errors."""
        error = Exception("Request failed: unauthorized access")
        assert _is_credential_error(error) is True

    def test_error_with_forbidden_in_message_is_not_credential_error(self) -> None:
        """Test that errors with 'forbidden' in message are NOT credential errors.

        'Forbidden' indicates access denied for a resource, not invalid credentials.
        """
        error = Exception("Access forbidden for this resource")
        assert _is_credential_error(error) is False

    def test_generic_error_is_not_credential_error(self) -> None:
        """Test that generic errors are NOT credential errors."""
        error = ValueError("Invalid parameter")
        assert _is_credential_error(error) is False

    def test_nested_credential_error(self) -> None:
        """Test that nested credential errors are detected."""
        inner_error = TokenRefreshError("Token expired")
        outer_error = Exception("Wrapper error")
        outer_error.__cause__ = inner_error
        assert _is_credential_error(outer_error) is True

    def test_nested_non_credential_error(self) -> None:
        """Test that nested non-credential errors are not credential errors."""
        inner_error = ValueError("Bad value")
        outer_error = Exception("Wrapper error")
        outer_error.__cause__ = inner_error
        assert _is_credential_error(outer_error) is False


class TestIsDiscoveryAuthError:
    """Tests for _is_discovery_auth_error helper function.

    During discovery, ALL 4xx errors are treated as auth failures because
    they indicate a problem with the plugin configuration or credentials.
    This is stricter than _is_credential_error which is used for execution.
    """

    def test_token_refresh_error_is_discovery_auth_error(self) -> None:
        """Test that TokenRefreshError is recognized as discovery auth error."""
        error = TokenRefreshError("Token refresh failed")
        assert _is_discovery_auth_error(error) is True

    def test_http_400_is_discovery_auth_error(self) -> None:
        """Test that HTTP 400 IS a discovery auth error.

        400 during discovery usually means malformed/garbage token.
        """
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 400
        error = httpx.HTTPStatusError(
            "Bad Request", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is True

    def test_http_401_is_discovery_auth_error(self) -> None:
        """Test that HTTP 401 is a discovery auth error."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        error = httpx.HTTPStatusError(
            "Unauthorized", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is True

    def test_http_403_is_discovery_auth_error(self) -> None:
        """Test that HTTP 403 IS a discovery auth error.

        During discovery, 403 means the credentials don't have permission
        to access the MCP server at all, which is a configuration problem.
        """
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        error = httpx.HTTPStatusError(
            "Forbidden", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is True

    def test_http_404_is_discovery_auth_error(self) -> None:
        """Test that HTTP 404 IS a discovery auth error.

        404 during discovery likely means wrong URL or auth-gated endpoint.
        """
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        error = httpx.HTTPStatusError(
            "Not Found", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is True

    def test_http_500_is_not_discovery_auth_error(self) -> None:
        """Test that HTTP 500 is NOT a discovery auth error.

        5xx errors are server-side issues, not auth problems.
        """
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        error = httpx.HTTPStatusError(
            "Internal Server Error", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is False

    def test_http_503_is_not_discovery_auth_error(self) -> None:
        """Test that HTTP 503 is NOT a discovery auth error."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 503
        error = httpx.HTTPStatusError(
            "Service Unavailable", request=mock_request, response=mock_response
        )
        assert _is_discovery_auth_error(error) is False

    def test_connection_error_is_not_discovery_auth_error(self) -> None:
        """Test that connection errors are NOT discovery auth errors."""
        error = httpx.ConnectError("Connection refused")
        assert _is_discovery_auth_error(error) is False

    def test_generic_error_is_not_discovery_auth_error(self) -> None:
        """Test that generic errors are NOT discovery auth errors."""
        error = ValueError("Invalid parameter")
        assert _is_discovery_auth_error(error) is False

    def test_nested_4xx_error(self) -> None:
        """Test that nested 4xx errors are detected."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        inner_error = httpx.HTTPStatusError(
            "Forbidden", request=mock_request, response=mock_response
        )
        outer_error = Exception("Wrapper error")
        outer_error.__cause__ = inner_error
        assert _is_discovery_auth_error(outer_error) is True


class TestSanitizeToolPrefix:
    """Tests for _sanitize_tool_prefix helper function."""

    def test_lowercase_conversion(self) -> None:
        """Test that names are converted to lowercase."""
        assert _sanitize_tool_prefix("GitHub") == "github"
        assert _sanitize_tool_prefix("SNOWFLAKE") == "snowflake"

    def test_replaces_spaces_with_underscores(self) -> None:
        """Test that spaces are replaced with underscores."""
        assert _sanitize_tool_prefix("My Plugin") == "my_plugin"

    def test_replaces_special_chars_with_underscores(self) -> None:
        """Test that special characters are replaced with underscores."""
        assert _sanitize_tool_prefix("plugin-name") == "plugin_name"
        assert _sanitize_tool_prefix("plugin.name") == "plugin_name"
        assert _sanitize_tool_prefix("plugin@name!") == "plugin_name"

    def test_collapses_multiple_underscores(self) -> None:
        """Test that multiple underscores are collapsed into one."""
        assert _sanitize_tool_prefix("my---plugin") == "my_plugin"
        assert _sanitize_tool_prefix("my   plugin") == "my_plugin"
        assert _sanitize_tool_prefix("a__b__c") == "a_b_c"

    def test_strips_leading_trailing_underscores(self) -> None:
        """Test that leading/trailing underscores are stripped."""
        assert _sanitize_tool_prefix("_plugin_") == "plugin"
        assert _sanitize_tool_prefix("__test__") == "test"
        assert _sanitize_tool_prefix("-plugin-") == "plugin"

    def test_fallback_for_empty_result(self) -> None:
        """Test fallback to 'plugin' when result would be empty."""
        assert _sanitize_tool_prefix("") == "plugin"
        assert _sanitize_tool_prefix("---") == "plugin"
        assert _sanitize_tool_prefix("@#$%") == "plugin"

    def test_unicode_characters_replaced(self) -> None:
        """Test that unicode/non-ASCII characters are replaced."""
        assert _sanitize_tool_prefix("café") == "caf"
        assert (
            _sanitize_tool_prefix("日本語") == "plugin"
        )  # All non-ASCII -> empty -> fallback

    def test_preserves_alphanumeric(self) -> None:
        """Test that alphanumeric characters are preserved."""
        assert _sanitize_tool_prefix("plugin123") == "plugin123"
        assert _sanitize_tool_prefix("my2ndPlugin") == "my2ndplugin"


class TestComputeUniquePrefixes:
    """Tests for ExternalMCPManager._compute_unique_prefixes method."""

    def test_unique_names_no_suffix(self) -> None:
        """Test that unique names don't get suffixes."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "GitHub"),
            create_test_plugin("plugin2", "Snowflake"),
            create_test_plugin("plugin3", "Slack"),
        ]
        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        prefixes = manager._compute_unique_prefixes()

        assert prefixes["plugin1"] == "github"
        assert prefixes["plugin2"] == "snowflake"
        assert prefixes["plugin3"] == "slack"

    def test_duplicate_names_get_suffixes(self) -> None:
        """Test that duplicate names get incrementing suffixes."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "GitHub"),
            create_test_plugin("plugin2", "GitHub"),
            create_test_plugin("plugin3", "GitHub"),
        ]
        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        prefixes = manager._compute_unique_prefixes()

        assert prefixes["plugin1"] == "github"
        assert prefixes["plugin2"] == "github_2"
        assert prefixes["plugin3"] == "github_3"

    def test_mixed_unique_and_duplicate(self) -> None:
        """Test mix of unique and duplicate names."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "GitHub"),
            create_test_plugin("plugin2", "Snowflake"),
            create_test_plugin("plugin3", "GitHub"),  # duplicate
            create_test_plugin("plugin4", "Slack"),
            create_test_plugin("plugin5", "Snowflake"),  # duplicate
        ]
        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        prefixes = manager._compute_unique_prefixes()

        assert prefixes["plugin1"] == "github"
        assert prefixes["plugin2"] == "snowflake"
        assert prefixes["plugin3"] == "github_2"
        assert prefixes["plugin4"] == "slack"
        assert prefixes["plugin5"] == "snowflake_2"

    def test_sanitization_before_collision_check(self) -> None:
        """Test that names are sanitized before collision detection."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "GitHub"),
            create_test_plugin(
                "plugin2", "git-hub"
            ),  # Sanitizes to "git_hub", different
            create_test_plugin(
                "plugin3", "GITHUB"
            ),  # Sanitizes to "github", collision!
        ]
        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        prefixes = manager._compute_unique_prefixes()

        assert prefixes["plugin1"] == "github"
        assert prefixes["plugin2"] == "git_hub"  # Different after sanitization
        assert prefixes["plugin3"] == "github_2"  # Collision detected

    def test_empty_plugins_returns_empty_dict(self) -> None:
        """Test that empty plugins list returns empty dict."""
        mock_credential_store = MagicMock()
        manager = ExternalMCPManager(
            plugins=[],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        prefixes = manager._compute_unique_prefixes()

        assert prefixes == {}


class TestPluginConnectionError:
    """Tests for PluginConnectionError exception."""

    def test_stores_plugin_info(self) -> None:
        """Test that exception stores plugin information."""
        error = PluginConnectionError(
            plugin_id="urn:li:service:test",
            plugin_name="Test Plugin",
            message="Connection failed",
        )

        assert error.plugin_id == "urn:li:service:test"
        assert error.plugin_name == "Test Plugin"
        assert str(error) == "Connection failed"

    def test_is_exception(self) -> None:
        """Test that PluginConnectionError is an Exception."""
        error = PluginConnectionError("id", "name", "message")
        assert isinstance(error, Exception)


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
            plugin_id="urn:li:service:github",
            plugin_name="GitHub",
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

    def test_instructions_attribute(self) -> None:
        """Test that instructions attribute is stored correctly."""
        wrapper = ExternalToolWrapper(
            name="tool",
            description="desc",
            parameters={},
            _client_factory=Mock(),
            _original_name="tool",
            plugin_id="id",
            plugin_name="name",
            instructions="Always include repo:owner/name in queries.",
        )

        assert wrapper.instructions == "Always include repo:owner/name in queries."

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    @patch("datahub_integrations.mcp_integration.external_mcp_manager.mlflow")
    def test_run_raises_plugin_connection_error_on_auth_failure(
        self, mock_mlflow: Mock, mock_asyncer: Mock
    ) -> None:
        """Test that run raises PluginConnectionError for auth errors (401/403)."""
        import httpx

        # Setup mock client that raises HTTP 401
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        auth_error = httpx.HTTPStatusError(
            "Unauthorized", request=mock_request, response=mock_response
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(side_effect=auth_error)

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
            _original_name="test",
            plugin_id="urn:li:service:test",
            plugin_name="Test Plugin",
        )

        with pytest.raises(PluginConnectionError) as exc_info:
            wrapper.run({"arg": "value"})

        assert exc_info.value.plugin_id == "urn:li:service:test"
        assert exc_info.value.plugin_name == "Test Plugin"

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    @patch("datahub_integrations.mcp_integration.external_mcp_manager.mlflow")
    def test_run_reraises_non_auth_errors(
        self, mock_mlflow: Mock, mock_asyncer: Mock
    ) -> None:
        """Test that run re-raises non-auth errors for normal handling."""
        # Setup mock client that raises a generic error (not auth-related)
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(
            side_effect=ValueError("Invalid parameter: query cannot be empty")
        )

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
            _original_name="test",
            plugin_id="urn:li:service:test",
            plugin_name="Test Plugin",
        )

        # Should re-raise the original exception, NOT PluginConnectionError
        with pytest.raises(ValueError) as exc_info:
            wrapper.run({"arg": "value"})

        assert "Invalid parameter" in str(exc_info.value)

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    @patch("datahub_integrations.mcp_integration.external_mcp_manager.mlflow")
    def test_run_reraises_server_errors(
        self, mock_mlflow: Mock, mock_asyncer: Mock
    ) -> None:
        """Test that run re-raises 500 errors (not auth) for normal handling."""
        # Setup mock client that raises HTTP 500
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        server_error = httpx.HTTPStatusError(
            "Internal Server Error", request=mock_request, response=mock_response
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(side_effect=server_error)

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
            _original_name="test",
            plugin_id="urn:li:service:test",
            plugin_name="Test Plugin",
        )

        # Should re-raise the HTTPStatusError, NOT PluginConnectionError
        with pytest.raises(httpx.HTTPStatusError):
            wrapper.run({"arg": "value"})


class TestExternalMCPManagerFromUserSettings:
    """Tests for ExternalMCPManager.from_user_settings."""

    def test_creates_manager_with_plugins(self) -> None:
        """Test that from_user_settings creates manager with loaded plugins."""
        mock_graph = MagicMock()

        # Mock AiPluginLoader
        with patch(
            "datahub_integrations.mcp_integration.external_mcp_manager.AiPluginLoader"
        ) as mock_loader_class:
            mock_loader = MagicMock()
            mock_loader.load_enabled_plugins.return_value = [
                create_test_plugin("plugin1"),
                create_test_plugin("plugin2"),
            ]
            mock_loader_class.return_value = mock_loader

            mock_credential_store = MagicMock()
            manager = ExternalMCPManager.from_user_settings(
                graph=mock_graph,
                credential_store=mock_credential_store,
                user_urn="urn:li:corpuser:admin",
            )

            assert len(manager._plugins) == 2
            assert manager._user_urn == "urn:li:corpuser:admin"


class TestExternalMCPManagerGetTools:
    """Tests for ExternalMCPManager.get_tools."""

    def test_caches_tools(self) -> None:
        """Test that tools are cached after first call."""
        mock_credential_store = MagicMock()
        manager = ExternalMCPManager(
            plugins=[],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        # Pre-populate cache
        cached_tools = [
            ExternalToolWrapper(
                name="cached_tool",
                description="Cached",
                parameters={},
                _client_factory=Mock(),
                _original_name="cached",
                plugin_id="id",
                plugin_name="name",
            )
        ]
        manager._tools = cached_tools

        # Should return cached tools without any network calls
        result = manager.get_tools()

        assert result is cached_tools

    @patch("datahub_integrations.mcp_integration.external_mcp_manager.asyncer")
    def test_returns_empty_list_for_no_plugins(self, mock_asyncer: Mock) -> None:
        """Test that empty list is returned when no plugins configured."""
        mock_credential_store = MagicMock()
        manager = ExternalMCPManager(
            plugins=[],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        mock_asyncer.syncify.return_value = lambda: []

        result = manager.get_tools()
        assert result == []


class TestExternalMCPManagerDiscoverTools:
    """Tests for ExternalMCPManager._discover_tools async method."""

    @pytest.mark.asyncio
    async def test_raises_plugin_connection_error_on_auth_failure(self) -> None:
        """Test that discovery raises PluginConnectionError for auth errors."""
        mock_credential_store = MagicMock()
        plugin = create_test_plugin("failing-plugin", "Failing Plugin")

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        # Mock client that fails with 401
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        auth_error = httpx.HTTPStatusError(
            "Unauthorized", request=mock_request, response=mock_response
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(side_effect=auth_error)

        with patch.object(manager, "_create_client", return_value=mock_client):
            with pytest.raises(PluginConnectionError) as exc_info:
                await manager._discover_tools(plugin, tool_prefix="failing_plugin")

            assert exc_info.value.plugin_id == "failing-plugin"
            assert exc_info.value.plugin_name == "Failing Plugin"

    @pytest.mark.asyncio
    async def test_reraises_non_auth_errors(self) -> None:
        """Test that discovery re-raises non-auth errors (not PluginConnectionError)."""
        mock_credential_store = MagicMock()
        plugin = create_test_plugin("failing-plugin", "Failing Plugin")

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        # Mock client that fails with a generic error (not auth)
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(
            side_effect=ConnectionError("Connection refused")
        )

        with patch.object(manager, "_create_client", return_value=mock_client):
            # Should re-raise ConnectionError, not PluginConnectionError
            with pytest.raises(ConnectionError):
                await manager._discover_tools(plugin, tool_prefix="failing_plugin")

    @pytest.mark.asyncio
    async def test_discovers_tools_from_server(self) -> None:
        """Test that tools are discovered from MCP server."""
        mock_credential_store = MagicMock()
        plugin = create_test_plugin("github", "GitHub")

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

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
            tools = await manager._discover_tools(plugin, tool_prefix="github")

        assert len(tools) == 1
        assert tools[0].name == "github__search_code"
        assert tools[0].description == "Search code in repos"
        assert tools[0].plugin_id == "github"
        assert tools[0].plugin_name == "GitHub"

    @pytest.mark.asyncio
    async def test_truncates_long_tool_names(self) -> None:
        """Test that tool names exceeding 64 chars are truncated."""
        mock_credential_store = MagicMock()
        plugin = create_test_plugin("plugin", "Plugin")

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        # Mock tool with very long name
        mock_tool = MagicMock()
        mock_tool.name = (
            "this_is_a_very_long_tool_name_that_exceeds_the_sixty_four_character_limit"
        )
        mock_tool.description = "Long name tool"
        mock_tool.inputSchema = {}

        mock_client = AsyncMock()
        mock_client.list_tools = AsyncMock(return_value=[mock_tool])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(manager, "_create_client", return_value=mock_client):
            tools = await manager._discover_tools(plugin, tool_prefix="myprefix")

        assert len(tools) == 1
        # Prefix "myprefix" (8 chars) + "__" (2 chars) = 10 chars prefix
        # Max tool name length = 64, so tool portion = 64 - 10 = 54 chars
        assert len(tools[0].name) <= 64
        assert tools[0].name.startswith("myprefix__")


class TestExternalMCPManagerDiscoverAllTools:
    """Tests for ExternalMCPManager._discover_all_tools async method."""

    @pytest.mark.asyncio
    async def test_raises_on_auth_failure(self) -> None:
        """Test that discovery stops on auth error (PluginConnectionError)."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "Plugin 1"),
            create_test_plugin("plugin2", "Plugin 2"),
        ]

        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        async def mock_discover(plugin: AiPluginConfig, tool_prefix: str) -> list:
            if plugin.id == "plugin1":
                raise PluginConnectionError(
                    plugin.id, plugin.display_name, "Auth failed"
                )
            return []

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            with pytest.raises(PluginConnectionError) as exc_info:
                await manager._discover_all_tools()

            assert exc_info.value.plugin_id == "plugin1"

    @pytest.mark.asyncio
    async def test_skips_non_auth_failures_and_continues(self) -> None:
        """Test that non-auth errors are logged and skipped."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "Plugin 1"),
            create_test_plugin("plugin2", "Plugin 2"),
        ]

        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        async def mock_discover(plugin: AiPluginConfig, tool_prefix: str) -> list:
            if plugin.id == "plugin1":
                # Non-auth error - should be skipped
                raise ConnectionError("Server unavailable")
            return [
                ExternalToolWrapper(
                    name=f"{tool_prefix}__tool",
                    description="Tool",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        # Should have tools from plugin2 only (plugin1 was skipped)
        assert len(tools) == 1
        assert tools[0].plugin_id == "plugin2"

    @pytest.mark.asyncio
    async def test_collects_tools_from_all_plugins(self) -> None:
        """Test that tools from all plugins are collected."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "Plugin 1"),
            create_test_plugin("plugin2", "Plugin 2"),
        ]

        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        async def mock_discover(plugin: AiPluginConfig, tool_prefix: str) -> list:
            return [
                ExternalToolWrapper(
                    name=f"{tool_prefix}__tool",
                    description="Tool",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        assert len(tools) == 2
        tool_names = {t.name for t in tools}
        # Prefixes are computed from display names: "Plugin 1" -> "plugin_1", "Plugin 2" -> "plugin_2"
        assert "plugin_1__tool" in tool_names
        assert "plugin_2__tool" in tool_names

    @pytest.mark.asyncio
    async def test_uses_unique_prefixes_for_duplicate_names(self) -> None:
        """Test that duplicate plugin names get unique prefixes."""
        mock_credential_store = MagicMock()
        plugins = [
            create_test_plugin("plugin1", "GitHub"),
            create_test_plugin("plugin2", "GitHub"),
        ]

        manager = ExternalMCPManager(
            plugins=plugins,
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        captured_prefixes: list = []

        async def mock_discover(plugin: AiPluginConfig, tool_prefix: str) -> list:
            captured_prefixes.append(tool_prefix)
            return [
                ExternalToolWrapper(
                    name=f"{tool_prefix}__tool",
                    description="Tool",
                    parameters={},
                    _client_factory=Mock(),
                    _original_name="tool",
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                )
            ]

        with patch.object(manager, "_discover_tools", side_effect=mock_discover):
            tools = await manager._discover_all_tools()

        assert len(tools) == 2
        # First plugin gets "github", second gets "github_2"
        assert "github" in captured_prefixes
        assert "github_2" in captured_prefixes


class TestExternalMCPManagerGetAuthHeaders:
    """Tests for ExternalMCPManager._get_auth_headers."""

    def test_returns_empty_for_none_auth(self) -> None:
        """Test that no auth type returns empty headers."""
        mock_credential_store = MagicMock()
        plugin = create_test_plugin(auth_type=AiPluginAuthType.NONE)

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        headers = manager._get_auth_headers(plugin)
        assert headers == {}

    def test_returns_bearer_token_for_oauth(self) -> None:
        """Test that OAuth auth returns bearer token header."""
        mock_credential_store = MagicMock()
        mock_credential_store.get_access_token.return_value = "test-access-token"

        from datahub_integrations.mcp_integration.ai_plugin_loader import OAuthConfig

        plugin = create_test_plugin(auth_type=AiPluginAuthType.USER_OAUTH)
        plugin.oauth_config = OAuthConfig(
            server_urn="urn:li:oauthServer:test", required_scopes=[]
        )

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=mock_credential_store,
            user_urn="urn:li:corpuser:admin",
        )

        headers = manager._get_auth_headers(plugin)
        assert headers == {"Authorization": "Bearer test-access-token"}

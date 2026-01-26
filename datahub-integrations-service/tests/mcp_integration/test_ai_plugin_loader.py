"""Unit tests for AiPluginLoader."""

from unittest.mock import MagicMock

from datahub_integrations.mcp_integration.ai_plugin_loader import (
    AiPluginAuthType,
    AiPluginLoader,
    McpTransport,
)


class TestAiPluginLoader:
    """Tests for AiPluginLoader.load_enabled_plugins."""

    def _create_loader_with_mocks(
        self, global_plugins: list, user_settings: list
    ) -> AiPluginLoader:
        """Create an AiPluginLoader with mocked GraphQL responses."""
        mock_graph = MagicMock()

        # Setup the mock to return different responses based on the query
        def execute_graphql_side_effect(query: str, variables: dict | None = None):
            if "globalSettings" in query:
                return {"globalSettings": {"aiPlugins": global_plugins}}
            elif "me" in query:
                return {
                    "me": {
                        "corpUser": {
                            "settings": {"aiPluginSettings": {"plugins": user_settings}}
                        }
                    }
                }
            return {}

        mock_graph.execute_graphql.side_effect = execute_graphql_side_effect
        return AiPluginLoader(mock_graph)

    def test_empty_when_no_plugins(self) -> None:
        """Test that empty list is returned when no plugins configured."""
        loader = self._create_loader_with_mocks(global_plugins=[], user_settings=[])
        result = loader.load_enabled_plugins()
        assert result == []

    def test_filters_out_globally_disabled_plugins(self) -> None:
        """Test that globally disabled plugins are not included."""
        global_plugins = [
            {
                "id": "disabled-plugin",
                "enabled": False,
                "authType": "NONE",
                "service": {
                    "urn": "urn:li:service:test",
                    "properties": {"displayName": "Test"},
                    "mcpServerProperties": {
                        "url": "http://localhost:3000",
                        "transport": "HTTP",
                        "timeout": 30.0,
                    },
                },
            }
        ]
        user_settings = [{"id": "disabled-plugin", "enabled": True}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()
        assert result == []

    def test_filters_out_user_disabled_plugins(self) -> None:
        """Test that user-disabled plugins are not included."""
        global_plugins = [
            {
                "id": "user-disabled-plugin",
                "enabled": True,
                "authType": "NONE",
                "service": {
                    "urn": "urn:li:service:test",
                    "properties": {"displayName": "Test"},
                    "mcpServerProperties": {
                        "url": "http://localhost:3000",
                        "transport": "HTTP",
                        "timeout": 30.0,
                    },
                },
            }
        ]
        user_settings = [{"id": "user-disabled-plugin", "enabled": False}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()
        assert result == []

    def test_default_is_disabled_when_not_in_user_settings(self) -> None:
        """Test that plugins not in user settings are disabled by default."""
        global_plugins = [
            {
                "id": "not-in-user-settings",
                "enabled": True,
                "authType": "NONE",
                "service": {
                    "urn": "urn:li:service:test",
                    "properties": {"displayName": "Test"},
                    "mcpServerProperties": {
                        "url": "http://localhost:3000",
                        "transport": "HTTP",
                        "timeout": 30.0,
                    },
                },
            }
        ]
        user_settings: list[
            dict[str, object]
        ] = []  # User has no settings for this plugin

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()
        assert result == []

    def test_includes_both_enabled_plugins(self) -> None:
        """Test that plugins enabled by both admin and user are included."""
        global_plugins = [
            {
                "id": "both-enabled",
                "enabled": True,
                "authType": "NONE",
                "instructions": "Use this plugin wisely.",
                "service": {
                    "urn": "urn:li:service:test",
                    "properties": {
                        "displayName": "Test Plugin",
                        "description": "A test plugin",
                    },
                    "mcpServerProperties": {
                        "url": "http://localhost:3000",
                        "transport": "HTTP",
                        "timeout": 45.0,
                        "customHeaders": [{"key": "X-Custom", "value": "header"}],
                    },
                },
            }
        ]
        user_settings = [{"id": "both-enabled", "enabled": True, "oauthConfig": None}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()

        assert len(result) == 1
        plugin = result[0]
        assert plugin.id == "both-enabled"
        assert plugin.display_name == "Test Plugin"
        assert plugin.description == "A test plugin"
        assert plugin.instructions == "Use this plugin wisely."
        assert plugin.auth_type == AiPluginAuthType.NONE
        assert plugin.mcp_config.url == "http://localhost:3000"
        assert plugin.mcp_config.transport == McpTransport.HTTP
        assert plugin.mcp_config.timeout == 45.0
        assert plugin.mcp_config.custom_headers == {"X-Custom": "header"}

    def test_parses_user_oauth_auth_type(self) -> None:
        """Test that USER_OAUTH auth type is parsed correctly."""
        global_plugins = [
            {
                "id": "oauth-plugin",
                "enabled": True,
                "authType": "USER_OAUTH",
                "service": {
                    "urn": "urn:li:service:oauth",
                    "properties": {"displayName": "OAuth Plugin"},
                    "mcpServerProperties": {
                        "url": "http://localhost:3000",
                        "transport": "HTTP",
                        "timeout": 30.0,
                    },
                },
                "oauthConfig": {
                    "serverUrn": "urn:li:oauthServer:test",
                    "requiredScopes": ["read", "write"],
                },
            }
        ]
        user_settings = [
            {
                "id": "oauth-plugin",
                "enabled": True,
                "oauthConfig": {"isConnected": True},
            }
        ]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()

        assert len(result) == 1
        plugin = result[0]
        assert plugin.auth_type == AiPluginAuthType.USER_OAUTH
        assert plugin.oauth_config is not None
        assert plugin.oauth_config.server_urn == "urn:li:oauthServer:test"
        assert plugin.oauth_config.required_scopes == ["read", "write"]
        assert plugin.user_is_connected is True

    def test_parses_sse_transport(self) -> None:
        """Test that SSE transport is parsed correctly."""
        global_plugins = [
            {
                "id": "sse-plugin",
                "enabled": True,
                "authType": "NONE",
                "service": {
                    "urn": "urn:li:service:sse",
                    "properties": {"displayName": "SSE Plugin"},
                    "mcpServerProperties": {
                        "url": "http://localhost:3000/sse",
                        "transport": "SSE",
                        "timeout": 30.0,
                    },
                },
            }
        ]
        user_settings = [{"id": "sse-plugin", "enabled": True}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()

        assert len(result) == 1
        assert result[0].mcp_config.transport == McpTransport.SSE

    def test_handles_graphql_errors_gracefully(self) -> None:
        """Test that GraphQL errors return empty list."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        loader = AiPluginLoader(mock_graph)
        result = loader.load_enabled_plugins()

        assert result == []

    def test_skips_plugins_without_service(self) -> None:
        """Test that plugins without service data are skipped."""
        global_plugins = [
            {
                "id": "no-service",
                "enabled": True,
                "authType": "NONE",
                "service": None,  # Missing service
            }
        ]
        user_settings = [{"id": "no-service", "enabled": True}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()
        assert result == []

    def test_skips_plugins_without_mcp_properties(self) -> None:
        """Test that plugins without MCP properties are skipped."""
        global_plugins = [
            {
                "id": "no-mcp",
                "enabled": True,
                "authType": "NONE",
                "service": {
                    "urn": "urn:li:service:test",
                    "properties": {"displayName": "Test"},
                    "mcpServerProperties": None,  # Missing MCP properties
                },
            }
        ]
        user_settings = [{"id": "no-mcp", "enabled": True}]

        loader = self._create_loader_with_mocks(global_plugins, user_settings)
        result = loader.load_enabled_plugins()
        assert result == []

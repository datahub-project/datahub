"""Unit tests for external MCP configuration models.

Note: mypy dict-item errors are ignored because Pydantic automatically coerces
dicts to model instances, which mypy doesn't understand.
"""

# mypy: disable-error-code="dict-item"

import os
from unittest.mock import patch

import pytest

from datahub_integrations.mcp_integration.external_mcp_config import (
    ExternalMCPConfig,
    MCPServerConfig,
    substitute_env_vars,
)


class TestSubstituteEnvVars:
    """Tests for the substitute_env_vars function."""

    def test_substitutes_single_env_var(self) -> None:
        """Test substitution of a single environment variable."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = substitute_env_vars("prefix_${TEST_VAR}_suffix")
            assert result == "prefix_test_value_suffix"

    def test_substitutes_multiple_env_vars(self) -> None:
        """Test substitution of multiple environment variables."""
        with patch.dict(os.environ, {"VAR1": "first", "VAR2": "second"}):
            result = substitute_env_vars("${VAR1}_and_${VAR2}")
            assert result == "first_and_second"

    def test_returns_unchanged_when_no_vars(self) -> None:
        """Test that strings without env vars are returned unchanged."""
        result = substitute_env_vars("plain_string_no_vars")
        assert result == "plain_string_no_vars"

    def test_raises_error_for_missing_env_var(self) -> None:
        """Test that missing env vars raise ValueError."""
        # Ensure the var doesn't exist
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                substitute_env_vars("${NONEXISTENT_VAR}")

            assert "NONEXISTENT_VAR" in str(exc_info.value)
            assert "is not set" in str(exc_info.value)

    def test_substitutes_env_var_at_start(self) -> None:
        """Test substitution at the start of string."""
        with patch.dict(os.environ, {"START_VAR": "beginning"}):
            result = substitute_env_vars("${START_VAR}_rest")
            assert result == "beginning_rest"

    def test_substitutes_env_var_at_end(self) -> None:
        """Test substitution at the end of string."""
        with patch.dict(os.environ, {"END_VAR": "ending"}):
            result = substitute_env_vars("start_${END_VAR}")
            assert result == "start_ending"

    def test_substitutes_entire_string(self) -> None:
        """Test when entire string is an env var."""
        with patch.dict(os.environ, {"WHOLE_VAR": "complete_value"}):
            result = substitute_env_vars("${WHOLE_VAR}")
            assert result == "complete_value"

    def test_handles_empty_env_var_value(self) -> None:
        """Test that empty env var values are substituted correctly."""
        with patch.dict(os.environ, {"EMPTY_VAR": ""}):
            result = substitute_env_vars("prefix_${EMPTY_VAR}_suffix")
            assert result == "prefix__suffix"


class TestMCPServerConfig:
    """Tests for the MCPServerConfig model."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = MCPServerConfig(url="http://localhost:8080")

        assert config.url == "http://localhost:8080"
        assert config.transport == "http"
        assert config.headers == {}
        assert config.timeout == 30.0
        assert config.enabled is True
        assert config.instructions is None

    def test_custom_values(self) -> None:
        """Test that custom values are applied correctly."""
        config = MCPServerConfig(
            url="http://example.com",
            transport="sse",
            headers={"Authorization": "Bearer token"},
            timeout=60.0,
            enabled=False,
            instructions="Use this server for GitHub operations.",
        )

        assert config.url == "http://example.com"
        assert config.transport == "sse"
        assert config.headers == {"Authorization": "Bearer token"}
        assert config.timeout == 60.0
        assert config.enabled is False
        assert config.instructions == "Use this server for GitHub operations."

    def test_get_resolved_url_with_env_var(self) -> None:
        """Test URL resolution with environment variable."""
        config = MCPServerConfig(url="http://${HOST}:${PORT}/api")

        with patch.dict(os.environ, {"HOST": "localhost", "PORT": "3000"}):
            resolved = config.get_resolved_url()
            assert resolved == "http://localhost:3000/api"

    def test_get_resolved_url_without_env_var(self) -> None:
        """Test URL resolution without environment variable."""
        config = MCPServerConfig(url="http://localhost:8080")
        resolved = config.get_resolved_url()
        assert resolved == "http://localhost:8080"

    def test_get_resolved_headers_with_env_vars(self) -> None:
        """Test header resolution with environment variables."""
        config = MCPServerConfig(
            url="http://localhost",
            headers={
                "Authorization": "Bearer ${API_TOKEN}",
                "X-Custom-Header": "${CUSTOM_VALUE}",
            },
        )

        with patch.dict(
            os.environ, {"API_TOKEN": "secret123", "CUSTOM_VALUE": "custom"}
        ):
            resolved = config.get_resolved_headers()
            assert resolved == {
                "Authorization": "Bearer secret123",
                "X-Custom-Header": "custom",
            }

    def test_get_resolved_headers_without_env_vars(self) -> None:
        """Test header resolution without environment variables."""
        config = MCPServerConfig(
            url="http://localhost",
            headers={"Content-Type": "application/json"},
        )
        resolved = config.get_resolved_headers()
        assert resolved == {"Content-Type": "application/json"}

    def test_get_resolved_url_raises_on_missing_env_var(self) -> None:
        """Test that missing env var in URL raises error."""
        config = MCPServerConfig(url="http://${MISSING_HOST}/api")

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                config.get_resolved_url()
            assert "MISSING_HOST" in str(exc_info.value)

    def test_get_resolved_headers_raises_on_missing_env_var(self) -> None:
        """Test that missing env var in headers raises error."""
        config = MCPServerConfig(
            url="http://localhost",
            headers={"Authorization": "Bearer ${MISSING_TOKEN}"},
        )

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                config.get_resolved_headers()
            assert "MISSING_TOKEN" in str(exc_info.value)


class TestExternalMCPConfig:
    """Tests for the ExternalMCPConfig model."""

    def test_empty_config(self) -> None:
        """Test that empty config is valid."""
        config = ExternalMCPConfig()
        assert config.mcpServers == {}

    def test_valid_server_names(self) -> None:
        """Test that valid server names are accepted."""
        config = ExternalMCPConfig(
            mcpServers={
                "github": {"url": "http://localhost:3001"},
                "notion_server": {"url": "http://localhost:3002"},
                "server123": {"url": "http://localhost:3003"},
            }
        )

        assert "github" in config.mcpServers
        assert "notion_server" in config.mcpServers
        assert "server123" in config.mcpServers

    def test_invalid_server_name_with_hyphen(self) -> None:
        """Test that server names with hyphens are rejected."""
        with pytest.raises(ValueError) as exc_info:
            ExternalMCPConfig(
                mcpServers={
                    "github-server": {"url": "http://localhost:3001"},
                }
            )

        assert "github-server" in str(exc_info.value)
        assert "not a valid identifier" in str(exc_info.value)

    def test_invalid_server_name_starting_with_number(self) -> None:
        """Test that server names starting with numbers are rejected."""
        with pytest.raises(ValueError) as exc_info:
            ExternalMCPConfig(
                mcpServers={
                    "123server": {"url": "http://localhost:3001"},
                }
            )

        assert "123server" in str(exc_info.value)

    def test_invalid_server_name_with_space(self) -> None:
        """Test that server names with spaces are rejected."""
        with pytest.raises(ValueError) as exc_info:
            ExternalMCPConfig(
                mcpServers={
                    "my server": {"url": "http://localhost:3001"},
                }
            )

        assert "my server" in str(exc_info.value)

    def test_get_enabled_servers_all_enabled(self) -> None:
        """Test getting enabled servers when all are enabled."""
        config = ExternalMCPConfig(
            mcpServers={
                "server1": {"url": "http://localhost:3001", "enabled": True},
                "server2": {"url": "http://localhost:3002", "enabled": True},
            }
        )

        enabled = config.get_enabled_servers()
        assert len(enabled) == 2
        assert "server1" in enabled
        assert "server2" in enabled

    def test_get_enabled_servers_some_disabled(self) -> None:
        """Test getting enabled servers when some are disabled."""
        config = ExternalMCPConfig(
            mcpServers={
                "server1": {"url": "http://localhost:3001", "enabled": True},
                "server2": {"url": "http://localhost:3002", "enabled": False},
                "server3": {"url": "http://localhost:3003", "enabled": True},
            }
        )

        enabled = config.get_enabled_servers()
        assert len(enabled) == 2
        assert "server1" in enabled
        assert "server2" not in enabled
        assert "server3" in enabled

    def test_get_enabled_servers_all_disabled(self) -> None:
        """Test getting enabled servers when all are disabled."""
        config = ExternalMCPConfig(
            mcpServers={
                "server1": {"url": "http://localhost:3001", "enabled": False},
                "server2": {"url": "http://localhost:3002", "enabled": False},
            }
        )

        enabled = config.get_enabled_servers()
        assert len(enabled) == 0

    def test_get_enabled_servers_default_enabled(self) -> None:
        """Test that servers are enabled by default."""
        config = ExternalMCPConfig(
            mcpServers={
                "server1": {"url": "http://localhost:3001"},  # No explicit enabled
            }
        )

        enabled = config.get_enabled_servers()
        assert len(enabled) == 1
        assert "server1" in enabled

    def test_full_config_parsing(self) -> None:
        """Test parsing a complete configuration."""
        config = ExternalMCPConfig.model_validate(
            {
                "mcpServers": {
                    "github": {
                        "url": "http://localhost:3001/sse",
                        "transport": "sse",
                        "headers": {"Authorization": "Bearer token"},
                        "timeout": 45.0,
                        "enabled": True,
                        "instructions": "Use repo:owner/repo in search queries.",
                    },
                    "notion": {
                        "url": "http://localhost:3002",
                        "transport": "http",
                        "enabled": False,
                    },
                }
            }
        )

        assert len(config.mcpServers) == 2

        github = config.mcpServers["github"]
        assert github.url == "http://localhost:3001/sse"
        assert github.transport == "sse"
        assert github.headers == {"Authorization": "Bearer token"}
        assert github.timeout == 45.0
        assert github.enabled is True
        assert github.instructions == "Use repo:owner/repo in search queries."

        notion = config.mcpServers["notion"]
        assert notion.url == "http://localhost:3002"
        assert notion.transport == "http"
        assert notion.enabled is False

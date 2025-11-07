"""
Tests for nested SecretStr field registration in ConfigModel.

This module tests that SecretStr fields are properly registered with the
secret masking registry, including:
- Direct SecretStr fields
- Nested SecretStr fields within ConfigModel instances
- SecretStr fields in lists and dicts of ConfigModels
"""

from typing import Optional

import pytest
from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel
from datahub.masking.secret_registry import SecretRegistry


class NestedAuthConfig(ConfigModel):
    """Mock nested authentication config with secrets."""

    client_secret: SecretStr = Field(description="Client secret")
    client_id: str = Field(description="Client ID")


class DeepNestedConfig(ConfigModel):
    """Mock config with deep nesting."""

    api_key: SecretStr = Field(description="API key")
    service_name: str = Field(description="Service name")


class MiddleConfig(ConfigModel):
    """Mock middle-level config."""

    deep_config: DeepNestedConfig = Field(description="Deep nested config")
    middle_secret: SecretStr = Field(description="Middle level secret")


class DatabaseConfig(ConfigModel):
    """Mock database config with optional nested auth."""

    password: SecretStr = Field(description="Database password")
    nested_auth: NestedAuthConfig = Field(description="Nested authentication")
    host: str = Field(description="Database host")


class ConnectionConfig(ConfigModel):
    """Mock connection config without direct secrets but with nested ones."""

    auth_config: NestedAuthConfig = Field(description="Authentication config")
    url: str = Field(description="Connection URL")


class ListConfig(ConfigModel):
    """Mock config with list of nested configs."""

    auth_list: list[NestedAuthConfig] = Field(description="List of auth configs")
    name: str = Field(description="Config name")


class DictConfig(ConfigModel):
    """Mock config with dict of nested configs."""

    auth_dict: dict[str, NestedAuthConfig] = Field(description="Dict of auth configs")
    name: str = Field(description="Config name")


class TestNestedSecretRegistration:
    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_direct_secret_field(self):
        _nested = NestedAuthConfig(
            client_secret=SecretStr("my-secret-123"), client_id="client-123"
        )

        registry = SecretRegistry.get_instance()
        assert registry.has_secret("client_secret")
        assert registry.get_secret_value("client_secret") == "my-secret-123"

    def test_nested_secret_field(self):
        # Use model_validate (dict-based construction) which is the production pattern
        _config = ConnectionConfig.model_validate(
            {
                "auth_config": {
                    "client_secret": "nested-secret-456",
                    "client_id": "client-456",
                },
                "url": "https://example.com",
            }
        )

        registry = SecretRegistry.get_instance()

        # The nested secret should be registered with a dotted path
        assert registry.has_secret("auth_config.client_secret")
        assert (
            registry.get_secret_value("auth_config.client_secret")
            == "nested-secret-456"
        )
        # The secret VALUE is in the registry and will be masked
        assert "nested-secret-456" in registry._secrets

    def test_multiple_levels_of_nesting(self):
        _config = MiddleConfig.model_validate(
            {
                "deep_config": {
                    "api_key": "deep-api-key",
                    "service_name": "my-service",
                },
                "middle_secret": "middle-secret",
            }
        )

        registry = SecretRegistry.get_instance()

        # Both middle and deep secrets should be registered
        assert registry.has_secret("middle_secret")
        assert registry.get_secret_value("middle_secret") == "middle-secret"

        assert registry.has_secret("deep_config.api_key")
        assert registry.get_secret_value("deep_config.api_key") == "deep-api-key"

    def test_mixed_direct_and_nested_secrets(self):
        _config = DatabaseConfig.model_validate(
            {
                "password": "db-password-789",
                "nested_auth": {
                    "client_secret": "auth-secret-789",
                    "client_id": "client-789",
                },
                "host": "db.example.com",
            }
        )

        registry = SecretRegistry.get_instance()

        # Direct secret
        assert registry.has_secret("password")
        assert registry.get_secret_value("password") == "db-password-789"

        # Nested secret
        assert registry.has_secret("nested_auth.client_secret")
        assert (
            registry.get_secret_value("nested_auth.client_secret") == "auth-secret-789"
        )

    def test_list_of_nested_configs(self):
        _config = ListConfig.model_validate(
            {
                "auth_list": [
                    {"client_secret": "secret-1", "client_id": "client-1"},
                    {"client_secret": "secret-2", "client_id": "client-2"},
                ],
                "name": "list-config",
            }
        )

        registry = SecretRegistry.get_instance()

        # Secrets from list items should be registered with array indices
        assert registry.has_secret("auth_list[0].client_secret")
        assert registry.get_secret_value("auth_list[0].client_secret") == "secret-1"

        assert registry.has_secret("auth_list[1].client_secret")
        assert registry.get_secret_value("auth_list[1].client_secret") == "secret-2"

    def test_dict_of_nested_configs(self):
        _config = DictConfig.model_validate(
            {
                "auth_dict": {
                    "prod": {
                        "client_secret": "prod-secret",
                        "client_id": "prod-client",
                    },
                    "dev": {"client_secret": "dev-secret", "client_id": "dev-client"},
                },
                "name": "dict-config",
            }
        )

        registry = SecretRegistry.get_instance()

        # Secrets from dict values should be registered with keys
        assert registry.has_secret("auth_dict[prod].client_secret")
        assert (
            registry.get_secret_value("auth_dict[prod].client_secret") == "prod-secret"
        )

        assert registry.has_secret("auth_dict[dev].client_secret")
        assert registry.get_secret_value("auth_dict[dev].client_secret") == "dev-secret"

    def test_empty_secret_not_registered(self):
        _nested = NestedAuthConfig(
            client_secret=SecretStr(""), client_id="client-empty"
        )

        registry = SecretRegistry.get_instance()
        # Empty secrets should not be registered
        assert registry.get_count() == 0

    def test_none_nested_config_handled(self):
        class OptionalNestedConfig(ConfigModel):
            auth: Optional[NestedAuthConfig] = Field(
                default=None, description="Optional auth"
            )
            name: str = Field(description="Config name")

        _config = OptionalNestedConfig(auth=None, name="test")

        registry = SecretRegistry.get_instance()
        # Should not error, just skip the None field
        assert registry.get_count() == 0

    def test_real_world_unity_catalog_scenario(self):
        class AzureAuthConfig(ConfigModel):
            client_secret: SecretStr = Field(description="Azure client secret")
            client_id: str = Field(description="Azure client ID")
            tenant_id: str = Field(description="Azure tenant ID")

        class UnityCatalogConnectionConfig(ConfigModel):
            azure_auth: Optional[AzureAuthConfig] = Field(
                default=None, description="Azure configuration"
            )
            token: Optional[str] = Field(default=None, description="Access token")
            workspace_url: str = Field(description="Workspace URL")

        _config = UnityCatalogConnectionConfig.model_validate(
            {
                "azure_auth": {
                    "client_secret": "azure-secret-xyz",
                    "client_id": "12345",
                    "tenant_id": "67890",
                },
                "token": "some-token",
                "workspace_url": "https://workspace.databricks.com",
            }
        )

        registry = SecretRegistry.get_instance()

        # The nested Azure client_secret should be registered
        assert registry.has_secret("azure_auth.client_secret")
        assert (
            registry.get_secret_value("azure_auth.client_secret") == "azure-secret-xyz"
        )


class TestConfigLoggingMasking:
    """Test that SecretStr fields are masked when configs are logged."""

    def setup_method(self):
        from datahub.masking.bootstrap import (
            initialize_secret_masking,
            shutdown_secret_masking,
        )

        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        initialize_secret_masking(force=True)

    def teardown_method(self):
        from datahub.masking.bootstrap import shutdown_secret_masking

        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_config_secrets_masked_in_logs(self):
        """Test that SecretStr fields are masked when config is logged."""
        import logging

        from datahub.masking.masking_filter import SecretMaskingFilter

        class TestSourceConfig(ConfigModel):
            password: SecretStr = Field(description="Database password")
            api_key: SecretStr = Field(description="API key")
            host: str = Field(description="Database host")

        # Create config with secrets
        config = TestSourceConfig(
            password="my-secret-password", api_key="my-api-key", host="my-host"
        )

        # Verify secrets are registered
        registry = SecretRegistry.get_instance()
        assert registry.has_secret("password")
        assert registry.has_secret("api_key")

        # Get the masking filter
        masking_filter = SecretMaskingFilter(registry)

        # Create log records with secrets and verify they're masked
        test_cases = [
            (f"Password is: {config.password.get_secret_value()}", "password"),
            (f"API key is: {config.api_key.get_secret_value()}", "api_key"),
            (f"Host is: {config.host}", None),  # Should not be masked
        ]

        for message, secret_name in test_cases:
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=message,
                args=(),
                exc_info=None,
            )

            # Apply filter
            masking_filter.filter(record)

            # Check result
            masked_message = record.getMessage()

            if secret_name:
                # Secret should be masked
                assert "my-secret-password" not in masked_message, (
                    f"Password not masked: {masked_message}"
                )
                assert "my-api-key" not in masked_message, (
                    f"API key not masked: {masked_message}"
                )
                assert f"***REDACTED:{secret_name}***" in masked_message, (
                    f"Redaction marker not found: {masked_message}"
                )
            else:
                # Host should not be masked
                assert "my-host" in masked_message

    def test_config_secrets_masked_at_all_log_levels(self):
        """Test that SecretStr fields are masked at all log levels (DEBUG, INFO, WARNING, ERROR)."""
        import logging

        from datahub.masking.masking_filter import SecretMaskingFilter

        class TestSourceConfig(ConfigModel):
            password: SecretStr = Field(description="Database password")

        config = TestSourceConfig(password="debug-secret-password")

        # Verify secret is registered
        registry = SecretRegistry.get_instance()
        assert registry.has_secret("password")

        # Get the masking filter
        masking_filter = SecretMaskingFilter(registry)

        # Test all log levels
        log_levels = [
            (logging.DEBUG, "DEBUG"),
            (logging.INFO, "INFO"),
            (logging.WARNING, "WARNING"),
            (logging.ERROR, "ERROR"),
            (logging.CRITICAL, "CRITICAL"),
        ]

        for level, level_name in log_levels:
            record = logging.LogRecord(
                name="test",
                level=level,
                pathname="",
                lineno=0,
                msg=f"[{level_name}] Password: {config.password.get_secret_value()}",
                args=(),
                exc_info=None,
            )

            # Apply filter
            masking_filter.filter(record)

            # Check result
            masked_message = record.getMessage()

            # Secret should be masked at ALL log levels
            assert "debug-secret-password" not in masked_message, (
                f"Password not masked at {level_name} level: {masked_message}"
            )
            assert "***REDACTED:password***" in masked_message, (
                f"Redaction marker not found at {level_name} level: {masked_message}"
            )
            assert level_name in masked_message  # Level name should still be there


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

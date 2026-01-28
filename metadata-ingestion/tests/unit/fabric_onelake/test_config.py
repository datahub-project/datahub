"""Unit tests for Fabric OneLake configuration.

Tests configuration validation and default values.
"""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.azure.azure_auth import (
    AzureAuthenticationMethod,
    AzureCredentialConfig,
)
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig


class TestFabricOneLakeSourceConfig:
    """Tests for FabricOneLakeSourceConfig validation."""

    def test_config_with_default_credential(self) -> None:
        """Config should work with default credential (DefaultAzureCredential)."""
        config = FabricOneLakeSourceConfig(credential=AzureCredentialConfig())
        assert config.credential is not None
        assert (
            config.credential.authentication_method == AzureAuthenticationMethod.DEFAULT
        )

    def test_config_with_service_principal(self) -> None:
        """Config should work with service principal credentials."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(
                authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
                client_id="test-client-id",
                client_secret="test-secret",
                tenant_id="test-tenant-id",
            )
        )
        assert (
            config.credential.authentication_method
            == AzureAuthenticationMethod.SERVICE_PRINCIPAL
        )

    def test_api_timeout_minimum_bound(self) -> None:
        """api_timeout should reject values below 1."""
        with pytest.raises(ValidationError):
            FabricOneLakeSourceConfig(
                credential=AzureCredentialConfig(),
                api_timeout=0,  # Below minimum
            )

    def test_api_timeout_maximum_bound(self) -> None:
        """api_timeout should reject values above 300."""
        with pytest.raises(ValidationError):
            FabricOneLakeSourceConfig(
                credential=AzureCredentialConfig(),
                api_timeout=301,  # Above maximum
            )

    def test_api_timeout_accepts_valid_range(self) -> None:
        """api_timeout should accept values between 1 and 300."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            api_timeout=30,
        )
        assert config.api_timeout == 30

    def test_workspace_pattern_filtering(self) -> None:
        """Workspace pattern should filter correctly."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            workspace_pattern={"allow": ["^prod-.*"], "deny": [".*-test$"]},
        )
        assert config.workspace_pattern.allowed("prod-workspace")
        assert not config.workspace_pattern.allowed("dev-workspace")
        assert not config.workspace_pattern.allowed("prod-test")

    def test_lakehouse_pattern_filtering(self) -> None:
        """Lakehouse pattern should filter correctly."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            lakehouse_pattern={"allow": ["^lakehouse-.*"]},
        )
        assert config.lakehouse_pattern.allowed("lakehouse-prod")
        assert not config.lakehouse_pattern.allowed("warehouse-prod")

    def test_warehouse_pattern_filtering(self) -> None:
        """Warehouse pattern should filter correctly."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            warehouse_pattern={"allow": ["^warehouse-.*"]},
        )
        assert config.warehouse_pattern.allowed("warehouse-prod")
        assert not config.warehouse_pattern.allowed("lakehouse-prod")

    def test_table_pattern_filtering(self) -> None:
        """Table pattern should filter correctly."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            table_pattern={"allow": ["^dbo\\..*"], "deny": [".*_temp$"]},
        )
        assert config.table_pattern.allowed("dbo.customers")
        assert not config.table_pattern.allowed("sales.customers")
        assert not config.table_pattern.allowed("dbo.table_temp")

    def test_extract_flags_defaults(self) -> None:
        """Extract flags should default to True."""
        config = FabricOneLakeSourceConfig(credential=AzureCredentialConfig())
        assert config.extract_lakehouses is True
        assert config.extract_warehouses is True
        assert config.extract_schemas is True

    def test_extract_flags_can_be_disabled(self) -> None:
        """Extract flags can be set to False."""
        config = FabricOneLakeSourceConfig(
            credential=AzureCredentialConfig(),
            extract_lakehouses=False,
            extract_warehouses=False,
            extract_schemas=False,
        )
        assert config.extract_lakehouses is False
        assert config.extract_warehouses is False
        assert config.extract_schemas is False

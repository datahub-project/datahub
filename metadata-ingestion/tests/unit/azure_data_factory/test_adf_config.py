"""Unit tests for Azure Data Factory configuration.

Following the accelerator guidelines, we test:
- Configuration VALIDATION logic (required fields, bounds checking)
- Configuration INTERACTION logic (combinations of fields)

We do NOT test:
- Default configuration values (anti-pattern)
- Simple getters/setters
- Pydantic framework behavior
"""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.azure.azure_auth import (
    AzureAuthenticationMethod,
    AzureCredentialConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    AzureDataFactoryConfig,
)


class TestAzureCredentialConfigValidation:
    """Tests for AzureCredentialConfig validation logic."""

    def test_service_principal_requires_client_secret(self) -> None:
        """Service principal auth should fail without client_secret."""
        with pytest.raises(ValidationError) as exc_info:
            AzureCredentialConfig(
                authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
                client_id="test-client-id",
                tenant_id="test-tenant-id",
                # Missing client_secret
            )
        assert "client_secret" in str(exc_info.value)

    def test_service_principal_requires_tenant_id(self) -> None:
        """Service principal auth should fail without tenant_id."""
        with pytest.raises(ValidationError) as exc_info:
            AzureCredentialConfig(
                authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
                client_id="test-client-id",
                client_secret="test-secret",
                # Missing tenant_id
            )
        assert "tenant_id" in str(exc_info.value)

    def test_service_principal_requires_client_id(self) -> None:
        """Service principal auth should fail without client_id."""
        with pytest.raises(ValidationError) as exc_info:
            AzureCredentialConfig(
                authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
                client_secret="test-secret",
                tenant_id="test-tenant-id",
                # Missing client_id
            )
        assert "client_id" in str(exc_info.value)

    def test_service_principal_valid_when_all_fields_present(self) -> None:
        """Service principal should pass validation with all required fields."""
        # Should not raise
        config = AzureCredentialConfig(
            authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )
        # Verify config was created (not testing values, testing validation passed)
        assert (
            config.authentication_method == AzureAuthenticationMethod.SERVICE_PRINCIPAL
        )


class TestAzureDataFactoryConfigValidation:
    """Tests for AzureDataFactoryConfig validation logic."""

    def test_execution_history_days_minimum_bound(self) -> None:
        """execution_history_days should reject values below 1."""
        with pytest.raises(ValidationError):
            AzureDataFactoryConfig(
                subscription_id="test",
                execution_history_days=0,  # Below minimum
            )

    def test_execution_history_days_maximum_bound(self) -> None:
        """execution_history_days should reject values above 90."""
        with pytest.raises(ValidationError):
            AzureDataFactoryConfig(
                subscription_id="test",
                execution_history_days=91,  # Above maximum
            )

    def test_execution_history_days_accepts_boundary_values(self) -> None:
        """execution_history_days should accept boundary values (1 and 90)."""
        # Should not raise
        config_min = AzureDataFactoryConfig(
            subscription_id="test",
            execution_history_days=1,
        )
        assert config_min.execution_history_days == 1

        config_max = AzureDataFactoryConfig(
            subscription_id="test",
            execution_history_days=90,
        )
        assert config_max.execution_history_days == 90

    def test_subscription_id_required(self) -> None:
        """Config should fail without subscription_id."""
        with pytest.raises(ValidationError):
            AzureDataFactoryConfig()  # type: ignore[call-arg]

    def test_factory_pattern_deny_filters_correctly(self) -> None:
        """Factory pattern deny should filter matching factories."""
        config = AzureDataFactoryConfig(
            subscription_id="test",
            factory_pattern={"allow": [".*"], "deny": [".*-test$", "dev-.*"]},
        )

        # Test that pattern matching works as expected
        assert config.factory_pattern.allowed("prod-factory")
        assert not config.factory_pattern.allowed("prod-test")
        assert not config.factory_pattern.allowed("dev-factory")

    def test_pipeline_pattern_filtering(self) -> None:
        """Pipeline pattern should filter pipelines correctly."""
        config = AzureDataFactoryConfig(
            subscription_id="test",
            pipeline_pattern={"allow": ["^prod_.*"], "deny": [".*_backup$"]},
        )

        # Test filtering logic
        assert config.pipeline_pattern.allowed("prod_ingestion")
        assert config.pipeline_pattern.allowed("prod_transform")
        assert not config.pipeline_pattern.allowed("dev_ingestion")
        assert not config.pipeline_pattern.allowed("prod_backup")


class TestCredentialConfigInteraction:
    """Tests for how credential config interacts with main config."""

    def test_service_principal_credential_embedded_in_config(self) -> None:
        """Service principal credential should integrate with main config."""
        config = AzureDataFactoryConfig(
            subscription_id="test-subscription",
            credential=AzureCredentialConfig(
                authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
                client_id="test-client",
                client_secret="test-secret",
                tenant_id="test-tenant",
            ),
        )

        # Verify credential is properly set
        assert (
            config.credential.authentication_method
            == AzureAuthenticationMethod.SERVICE_PRINCIPAL
        )
        assert config.credential.client_id == "test-client"
        assert config.credential.tenant_id == "test-tenant"

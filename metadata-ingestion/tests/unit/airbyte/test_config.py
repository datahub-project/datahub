import unittest

import pytest
from pydantic import ValidationError
from pydantic.types import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    AirbyteSourceConfig,
    PlatformInstanceConfig,
)
from datahub.ingestion.source.airbyte.models import AirbyteWorkspace


class TestAirbyteClientConfig:
    def test_open_source_config_valid(self):
        # Test valid open source config with host_port
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        assert config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE
        assert config.host_port == "http://localhost:8000"

    def test_open_source_config_with_auth(self):
        # Test valid open source config with basic auth
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            username="user",
            password=SecretStr("pass"),
        )
        assert config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE
        assert config.host_port == "http://localhost:8000"
        assert config.username == "user"
        assert (
            config.password is not None and config.password.get_secret_value() == "pass"
        )

    def test_open_source_config_without_host_port(self):
        # Test invalid open source config without host_port
        with pytest.raises(ValueError) as excinfo:
            # Manually call the validator to trigger the error
            AirbyteClientConfig.validate_host_port_for_open_source(
                None, {"deployment_type": AirbyteDeploymentType.OPEN_SOURCE}
            )

        # Verify the error message contains host_port
        assert "host_port is required" in str(excinfo.value)

    def test_cloud_config_valid(self):
        # Test valid cloud config with api_key
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            api_key=SecretStr("test-api-key"),
            cloud_workspace_id="test-workspace-id",
        )
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert (
            config.api_key is not None
            and config.api_key.get_secret_value() == "test-api-key"
        )
        assert config.cloud_workspace_id == "test-workspace-id"

    def test_cloud_config_without_api_key_or_workspace(self):
        # Test invalid cloud config without required fields
        with pytest.raises(ValueError) as excinfo:
            # Manually call the validator to trigger the error
            AirbyteClientConfig.validate_workspace_for_cloud(
                None, {"deployment_type": AirbyteDeploymentType.CLOUD}
            )

        # Verify the error message contains the required field
        assert "cloud_workspace_id is required" in str(excinfo.value)

    def test_invalid_deployment_type(self):
        # Test with invalid deployment type
        try:
            # Use a string that is not a valid enum value
            AirbyteClientConfig(
                deployment_type="invalid_type",  # This is not a valid enum value
                host_port="http://localhost:8000",
            )
            raise AssertionError("Expected validation to fail")
        except Exception as e:
            # Just verify some error is raised - different versions of Pydantic
            # may use different error types
            assert (
                "deployment_type" in str(e).lower()
                or "not a valid enum" in str(e).lower()
                or "invalid_type" in str(e).lower()
            )


class TestAirbyteWorkspace:
    def test_valid_workspace(self):
        workspace = AirbyteWorkspace(
            workspaceId="test-workspace-id",
            name="Test Workspace",
            workspace_id="test-workspace-id",
        )
        assert workspace.workspace_id == "test-workspace-id"
        assert workspace.name == "Test Workspace"

    def test_missing_id(self):
        with pytest.raises(ValidationError):
            # Pass None instead of empty string to trigger validation error
            AirbyteWorkspace(
                workspaceId=None,
                name="Test Workspace",
                workspace_id=None,
            )

    def test_missing_name(self):
        with pytest.raises(ValidationError):
            # Pass None instead of empty string to trigger validation error
            AirbyteWorkspace(
                workspaceId="test-workspace-id",
                name=None,
                workspace_id="test-workspace-id",
            )


class TestPlatformInstanceConfig:
    def test_basic_config(self):
        config = PlatformInstanceConfig(
            platform="my_platform", env="PROD", platform_instance="instance1"
        )
        assert config.platform == "my_platform"
        assert config.env == "PROD"
        assert config.platform_instance == "instance1"

    def test_mapping_config(self):
        config = PlatformInstanceConfig(
            platform="my_platform",
            database_mapping={"db1": "mapped_db1"},
            schema_mapping={"schema1": "mapped_schema1"},
        )
        assert config.database_mapping == {"db1": "mapped_db1"}
        assert config.schema_mapping == {"schema1": "mapped_schema1"}


class TestAirbyteSourceConfig:
    def test_basic_config(self):
        # Test basic configuration
        config = AirbyteSourceConfig(
            platform_instance="dev",
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            extract_owners=True,
            extract_tags=True,
        )
        assert config.platform_instance == "dev"
        assert config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE
        assert config.host_port == "http://localhost:8000"
        assert config.extract_owners is True
        assert config.extract_tags is True

    def test_cloud_config(self):
        # Test cloud configuration
        config = AirbyteSourceConfig(
            platform_instance="prod",
            deployment_type=AirbyteDeploymentType.CLOUD,
            api_key=SecretStr("test-api-key"),
            cloud_workspace_id="test-workspace-id",
        )
        assert config.platform_instance == "prod"
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert (
            config.api_key is not None
            and config.api_key.get_secret_value() == "test-api-key"
        )
        assert config.cloud_workspace_id == "test-workspace-id"

    def test_validation_oss(self):
        # Test validation for OSS without host_port
        with pytest.raises(ValueError) as excinfo:
            # Manually call the validator to trigger the error
            AirbyteClientConfig.validate_host_port_for_open_source(
                None, {"deployment_type": AirbyteDeploymentType.OPEN_SOURCE}
            )

        # Verify the error message contains host_port
        assert "host_port is required" in str(excinfo.value)

    def test_validation_cloud(self):
        # Test validation for cloud without workspace ID
        with pytest.raises(ValueError) as excinfo:
            # Manually call the validator to trigger the error
            AirbyteClientConfig.validate_workspace_for_cloud(
                None, {"deployment_type": AirbyteDeploymentType.CLOUD}
            )

        # Verify the error message contains cloud_workspace_id
        assert "cloud_workspace_id is required" in str(excinfo.value)

    def test_with_patterns(self):
        # Test with allow/deny patterns
        config = AirbyteSourceConfig(
            platform_instance="dev",
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            source_pattern=AllowDenyPattern(allow=["postgres.*", "mysql.*"]),
            connection_pattern=AllowDenyPattern(allow=["conn-.*"]),
        )
        assert config.source_pattern.allow == ["postgres.*", "mysql.*"]
        assert config.connection_pattern.allow == ["conn-.*"]


if __name__ == "__main__":
    unittest.main()

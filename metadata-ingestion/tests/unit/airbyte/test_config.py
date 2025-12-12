from pydantic.types import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    AirbyteSourceConfig,
)


class TestAirbyteClientConfig:
    def test_open_source_config_valid(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        assert config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE
        assert config.host_port == "http://localhost:8000"

    def test_open_source_config_with_auth(self):
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

    def test_cloud_config_valid(self):
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

    def test_invalid_deployment_type(self):
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


class TestAirbyteSourceConfig:
    def test_basic_config(self):
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

    def test_with_patterns(self):
        config = AirbyteSourceConfig(
            platform_instance="dev",
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            source_pattern=AllowDenyPattern(allow=["postgres.*", "mysql.*"]),
            connection_pattern=AllowDenyPattern(allow=["conn-.*"]),
        )
        assert config.source_pattern.allow == ["postgres.*", "mysql.*"]
        assert config.connection_pattern.allow == ["conn-.*"]

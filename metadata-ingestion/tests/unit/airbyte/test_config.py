from pydantic import ValidationError
from pydantic.types import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    AirbyteSourceConfig,
    OAuth2GrantType,
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
        """Test cloud config requires OAuth credentials."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="test-workspace-id",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert config.cloud_workspace_id == "test-workspace-id"
        assert config.oauth2_client_id == "client-id"
        assert config.oauth2_client_secret is not None

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

    def test_cloud_config_with_refresh_token(self):
        """Test cloud config with auto-detected refresh_token grant type."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="test-workspace-id",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
            # oauth2_grant_type not specified - auto-detects REFRESH_TOKEN
        )
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert config.oauth2_grant_type == OAuth2GrantType.REFRESH_TOKEN
        assert config.oauth2_client_id == "client-id"
        assert config.oauth2_refresh_token is not None

    def test_cloud_config_with_client_credentials(self):
        """Test cloud config with client_credentials (no refresh token)."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="test-workspace-id",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            # No refresh token - auto-detects CLIENT_CREDENTIALS
        )
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert config.oauth2_grant_type == OAuth2GrantType.CLIENT_CREDENTIALS
        assert config.oauth2_client_id == "client-id"
        assert config.oauth2_refresh_token is None

    def test_cloud_config_missing_client_credentials(self):
        """Test that client_id and client_secret are required for cloud deployment."""
        try:
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                cloud_workspace_id="test-workspace-id",
                # Missing oauth2_client_id and oauth2_client_secret
            )
            raise AssertionError("Expected validation to fail")
        except ValidationError as e:
            assert "oauth2_client_id" in str(e) or "oauth2_client_secret" in str(e)


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
            cloud_workspace_id="test-workspace-id",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        assert config.platform_instance == "prod"
        assert config.deployment_type == AirbyteDeploymentType.CLOUD
        assert config.cloud_workspace_id == "test-workspace-id"
        assert config.oauth2_client_id == "client-id"

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

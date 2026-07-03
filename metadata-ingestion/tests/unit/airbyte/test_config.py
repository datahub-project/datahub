"""Tests for validator/business logic in `airbyte.config`.

We deliberately keep this file focused on logic that lives in our codebase
(custom validators, derived properties, deployment-specific URL handling).
Trivial round-trip tests that only re-read pydantic-assigned values are
omitted — they test pydantic, not us.
"""

import pytest
from pydantic import ValidationError
from pydantic.types import SecretStr

from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    OAuth2GrantType,
)


class TestOAuth2GrantTypeAutoDetection:
    """`oauth2_grant_type` is derived from whether a refresh token is set."""

    def test_refresh_token_present_picks_refresh_token_grant(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="ws",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        assert config.oauth2_grant_type == OAuth2GrantType.REFRESH_TOKEN

    def test_refresh_token_absent_picks_client_credentials_grant(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="ws",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
        )
        assert config.oauth2_grant_type == OAuth2GrantType.CLIENT_CREDENTIALS


class TestExternalUrlBase:
    """`external_url_base` is used to build `externalUrl` aspects.

    Regression guard: previously the source used
    `getattr(config, "host_port", DEFAULT)` which returned `None` for Cloud
    (since host_port is a declared field that defaults to None), producing
    literal "None/workspaces/..." URLs.
    """

    def test_oss_uses_host_port(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        assert config.external_url_base == "http://localhost:8000"

    def test_cloud_uses_cloud_base(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="ws",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        assert config.external_url_base == "https://cloud.airbyte.com"


class TestDeploymentValidators:
    """Custom model_validator branches."""

    def test_oss_rejects_refresh_token(self):
        with pytest.raises(
            ValidationError, match="oauth2_refresh_token is not supported for OSS"
        ):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
                host_port="http://localhost:8000",
                oauth2_client_id="oss-client-id",
                oauth2_client_secret=SecretStr("oss-client-secret"),
                oauth2_refresh_token=SecretStr("refresh-token"),
            )

    def test_oss_requires_host_port(self):
        with pytest.raises(ValidationError, match="host_port is required for oss"):
            AirbyteClientConfig(deployment_type=AirbyteDeploymentType.OPEN_SOURCE)

    def test_cloud_requires_workspace_id(self):
        with pytest.raises(ValidationError, match="cloud_workspace_id is required"):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                oauth2_client_id="client-id",
                oauth2_client_secret=SecretStr("client-secret"),
            )

    def test_cloud_requires_oauth_credentials(self):
        with pytest.raises(
            ValidationError,
            match="oauth2_client_id and oauth2_client_secret are required",
        ):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                cloud_workspace_id="ws",
            )

    def test_partial_oauth_credentials_rejected(self):
        """Half-supplied credentials should fail fast rather than silently."""
        with pytest.raises(
            ValidationError,
            match="Both oauth2_client_id and oauth2_client_secret must be provided",
        ):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
                host_port="http://localhost:8000",
                oauth2_client_id="client-id",
            )

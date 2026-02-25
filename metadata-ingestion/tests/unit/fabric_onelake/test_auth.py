"""Unit tests for Fabric authentication helper.

Tests token acquisition and caching logic.
"""

import time
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.azure.azure_auth import (
    AzureAuthenticationMethod,
    AzureCredentialConfig,
)
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper


class TestFabricAuthHelper:
    """Tests for FabricAuthHelper."""

    def test_get_credential_creates_token_credential(self) -> None:
        """get_credential should return a TokenCredential."""
        config = AzureCredentialConfig(
            authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )
        helper = FabricAuthHelper(config)
        credential = helper.get_credential()
        assert credential is not None

    @patch("time.time")
    def test_token_caching(self, mock_time: MagicMock) -> None:
        """Token should be cached and reused if not expired."""
        mock_time.return_value = 1000.0

        config = AzureCredentialConfig(
            authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )
        helper = FabricAuthHelper(config)

        # Mock token response
        mock_token_response = MagicMock()
        mock_token_response.token = "test-token-123"
        mock_token_response.expires_on = 2000.0  # Expires at time 2000

        mock_credential = MagicMock()
        mock_credential.get_token.return_value = mock_token_response
        with patch.object(helper, "get_credential", return_value=mock_credential):
            # First call should get token
            token1 = helper.get_bearer_token()
            assert token1 == "test-token-123"

            # Second call within validity period should use cached token
            mock_time.return_value = 1500.0  # Still valid (2000 - 300 = 1700)
            token2 = helper.get_bearer_token()
            assert token2 == "test-token-123"
            # Verify get_token was only called once
            assert mock_credential.get_token.call_count == 1

    @patch("time.time")
    def test_token_refresh_on_expiry(self, mock_time: MagicMock) -> None:
        """Token should be refreshed when expired."""
        mock_time.return_value = 1000.0

        config = AzureCredentialConfig(
            authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )
        helper = FabricAuthHelper(config)

        # Mock token responses
        mock_token_response1 = MagicMock()
        mock_token_response1.token = "test-token-1"
        mock_token_response1.expires_on = 1500.0

        mock_token_response2 = MagicMock()
        mock_token_response2.token = "test-token-2"
        mock_token_response2.expires_on = 2500.0

        mock_credential = MagicMock()
        mock_credential.get_token.side_effect = [
            mock_token_response1,
            mock_token_response2,
        ]
        with patch.object(helper, "get_credential", return_value=mock_credential):
            # First call
            token1 = helper.get_bearer_token()
            assert token1 == "test-token-1"

            # Move time past expiry (1500 - 300 = 1200 buffer)
            mock_time.return_value = 1201.0
            token2 = helper.get_bearer_token()
            assert token2 == "test-token-2"
            # Verify get_token was called twice
            assert mock_credential.get_token.call_count == 2

    def test_get_authorization_header_format(self) -> None:
        """Authorization header should have correct format."""
        config = AzureCredentialConfig(
            authentication_method=AzureAuthenticationMethod.SERVICE_PRINCIPAL,
            client_id="test-client",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )
        helper = FabricAuthHelper(config)

        mock_token_response = MagicMock()
        mock_token_response.token = "test-token-123"
        mock_token_response.expires_on = time.time() + 3600

        mock_credential = MagicMock()
        mock_credential.get_token.return_value = mock_token_response
        with patch.object(helper, "get_credential", return_value=mock_credential):
            header = helper.get_authorization_header()
            assert header == "Bearer test-token-123"
            assert header.startswith("Bearer ")

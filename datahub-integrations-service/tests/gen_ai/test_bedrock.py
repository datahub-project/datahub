"""Tests for bedrock.py module, specifically get_bedrock_client function."""

import importlib
import os
import sys
from unittest.mock import MagicMock, Mock, patch


def _reload_bedrock_module():
    """Reload the bedrock module to clear functools.cache."""
    if "datahub_integrations.gen_ai.bedrock" in sys.modules:
        importlib.reload(sys.modules["datahub_integrations.gen_ai.bedrock"])


class TestGetBedrockClientRegionResolution:
    """Tests for get_bedrock_client region resolution logic.

    These tests reload the module between tests to clear the @functools.cache
    decorator, allowing us to test different environment variable combinations.

    Region choices:
    - We use eu-west-1, ap-southeast-1, eu-central-1 (non-US regions) to ensure
      we're not accidentally relying on implicit defaults from local dev machines
      or CI/CD runners which are typically in us-west-2 or us-east-1.
    - The @patch.dict(..., clear=True) ensures complete environment isolation.
    """

    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch.dict(os.environ, {"BEDROCK_AWS_REGION": "eu-west-1"}, clear=True)
    def test_get_bedrock_client_with_bedrock_aws_region(
        self, mock_session_class: Mock
    ) -> None:
        """Test that get_bedrock_client uses BEDROCK_AWS_REGION when set."""
        _reload_bedrock_module()
        from datahub_integrations.gen_ai.bedrock import get_bedrock_client

        # Verify environment is isolated (only our test var is set)
        assert os.environ.get("BEDROCK_AWS_REGION") == "eu-west-1"
        assert os.environ.get("AWS_REGION") is None

        # Setup mocks
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session

        # Call the function
        result = get_bedrock_client()

        # Verify Session was created with the correct region from env var
        mock_session_class.assert_called_once_with(region_name="eu-west-1")

        # Verify client was created
        mock_session.client.assert_called_once()
        assert result == mock_client

    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch.dict(os.environ, {"AWS_REGION": "ap-southeast-1"}, clear=True)
    def test_get_bedrock_client_with_aws_region(self, mock_session_class: Mock) -> None:
        """Test that get_bedrock_client uses AWS_REGION when BEDROCK_AWS_REGION is not set."""
        _reload_bedrock_module()
        from datahub_integrations.gen_ai.bedrock import get_bedrock_client

        # Verify environment is isolated (only our test var is set)
        assert os.environ.get("AWS_REGION") == "ap-southeast-1"
        assert os.environ.get("BEDROCK_AWS_REGION") is None

        # Setup mocks
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session

        # Call the function
        result = get_bedrock_client()

        # Verify Session was created with the correct region from env var
        mock_session_class.assert_called_once_with(region_name="ap-southeast-1")

        # Verify client was created
        mock_session.client.assert_called_once()
        assert result == mock_client

    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch.dict(
        os.environ,
        {"BEDROCK_AWS_REGION": "eu-west-1", "AWS_REGION": "eu-central-1"},
        clear=True,
    )
    def test_get_bedrock_client_prefers_bedrock_aws_region(
        self, mock_session_class: Mock
    ) -> None:
        """Test that BEDROCK_AWS_REGION takes precedence over AWS_REGION."""
        _reload_bedrock_module()
        from datahub_integrations.gen_ai.bedrock import get_bedrock_client

        # Verify environment is isolated (both test vars are set)
        assert os.environ.get("BEDROCK_AWS_REGION") == "eu-west-1"
        assert os.environ.get("AWS_REGION") == "eu-central-1"

        # Setup mocks
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session

        # Call the function
        result = get_bedrock_client()

        # Verify Session was created with BEDROCK_AWS_REGION, not AWS_REGION
        mock_session_class.assert_called_once_with(region_name="eu-west-1")

        # Verify client was created
        mock_session.client.assert_called_once()
        assert result == mock_client

    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_bedrock_client_without_region_env_vars(
        self, mock_session_class: Mock
    ) -> None:
        """Test that get_bedrock_client falls back to boto3's default resolution when no region env vars are set.

        This test verifies backwards compatibility: when no region env vars are set,
        we pass no region to boto3.Session(), allowing boto3 to use its default
        resolution chain (config files, instance metadata, etc.).
        """
        _reload_bedrock_module()
        from datahub_integrations.gen_ai.bedrock import get_bedrock_client

        # Verify environment is completely isolated (no region vars)
        assert os.environ.get("BEDROCK_AWS_REGION") is None
        assert os.environ.get("AWS_REGION") is None
        assert os.environ.get("AWS_DEFAULT_REGION") is None

        # Setup mocks
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session

        # Call the function
        result = get_bedrock_client()

        # Verify Session was created WITHOUT region_name parameter (backwards compatible)
        mock_session_class.assert_called_once_with()

        # Verify client was created
        mock_session.client.assert_called_once()
        assert result == mock_client

    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch.dict(
        os.environ,
        {
            "BEDROCK_AWS_ACCESS_KEY_ID": "test-key",
            "BEDROCK_AWS_SECRET_ACCESS_KEY": "test-secret",
            "BEDROCK_AWS_REGION": "eu-west-1",
        },
        clear=True,
    )
    def test_get_bedrock_client_with_explicit_credentials_and_region(
        self, mock_session_class: Mock
    ) -> None:
        """Test that explicit credentials and region work together.

        This simulates the case where users provide explicit AWS credentials
        (e.g., for cross-account access) along with a region.
        """
        _reload_bedrock_module()
        from datahub_integrations.gen_ai.bedrock import get_bedrock_client

        # Verify environment is isolated (only our test vars are set)
        assert os.environ.get("BEDROCK_AWS_ACCESS_KEY_ID") == "test-key"
        assert os.environ.get("BEDROCK_AWS_SECRET_ACCESS_KEY") == "test-secret"
        assert os.environ.get("BEDROCK_AWS_REGION") == "eu-west-1"

        # Setup mocks
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session

        # Call the function
        result = get_bedrock_client()

        # Verify Session was created with explicit credentials AND region
        mock_session_class.assert_called_once_with(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name="eu-west-1",
        )

        # Verify client was created
        mock_session.client.assert_called_once()
        assert result == mock_client

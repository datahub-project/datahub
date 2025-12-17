"""Tests for bedrock.py module, specifically get_bedrock_client function."""

import importlib
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

import datahub_integrations.gen_ai.bedrock as bedrock_module

# Module path constant for reloading
_BEDROCK_MODULE_PATH = "datahub_integrations.gen_ai.bedrock"


def _reload_bedrock_module():
    """Reload the bedrock module to clear functools.cache and session cache."""
    # Clear the refreshable sessions cache on current module before reload
    bedrock_module._refreshable_sessions.clear()

    if _BEDROCK_MODULE_PATH in sys.modules:
        importlib.reload(sys.modules[_BEDROCK_MODULE_PATH])


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


class TestCreateBedrockClientCrossAccount:
    """Tests for cross-account role assumption using _create_bedrock_client.

    These tests use _create_bedrock_client directly with cross_account_role_arn parameter,
    which avoids the need for module reloading and makes tests simpler.
    """

    @pytest.fixture(autouse=True)
    def cleanup_sessions(self):
        """Clear _refreshable_sessions after each test."""
        yield
        bedrock_module._refreshable_sessions.clear()

    @patch.dict(os.environ, {"BEDROCK_AWS_REGION": "eu-west-1"}, clear=True)
    @patch("datahub_integrations.gen_ai.bedrock.boto3.client")
    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch("datahub_integrations.gen_ai.bedrock.get_session")
    def test_create_bedrock_client_with_cross_account_role(
        self,
        mock_get_session: Mock,
        mock_boto3_session: Mock,
        mock_boto3_client: Mock,
    ) -> None:
        # Setup mocks for STS assume_role
        mock_sts_client = MagicMock()
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "assumed-key-id",
                "SecretAccessKey": "assumed-secret-key",
                "SessionToken": "assumed-session-token",
                "Expiration": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            }
        }
        mock_boto3_client.return_value = mock_sts_client

        # Setup mock for botocore session
        mock_botocore_session = MagicMock()
        mock_get_session.return_value = mock_botocore_session

        # Setup mock for boto3.Session created from botocore session
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_boto3_session.return_value = mock_session

        # Call _create_bedrock_client directly with role ARN parameter
        result = bedrock_module._create_bedrock_client(
            cross_account_role_arn="arn:aws:iam::123456789012:role/TestRole"
        )

        # Verify STS client was created with correct region
        mock_boto3_client.assert_called_with("sts", region_name="eu-west-1")

        # Verify assume_role was called with correct parameters
        mock_sts_client.assume_role.assert_called_once()
        call_kwargs = mock_sts_client.assume_role.call_args[1]
        assert call_kwargs["RoleArn"] == "arn:aws:iam::123456789012:role/TestRole"
        assert call_kwargs["DurationSeconds"] == 3600
        assert "RoleSessionName" in call_kwargs

        # Verify region was set on botocore session
        mock_botocore_session.set_config_variable.assert_called_with(
            "region", "eu-west-1"
        )

        # Verify bedrock-runtime client was created
        mock_session.client.assert_called_once()
        assert mock_session.client.call_args[0][0] == "bedrock-runtime"
        assert result == mock_client

    @patch.dict(os.environ, {"BEDROCK_AWS_REGION": "eu-west-1"}, clear=True)
    @patch("datahub_integrations.gen_ai.bedrock.boto3.client")
    def test_create_bedrock_client_cross_account_role_assumption_fails(
        self, mock_boto3_client: Mock
    ) -> None:
        # Setup mock to raise an exception on assume_role
        mock_sts_client = MagicMock()
        mock_sts_client.assume_role.side_effect = Exception("Access denied")
        mock_boto3_client.return_value = mock_sts_client

        # Call the function and verify it raises RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            bedrock_module._create_bedrock_client(
                cross_account_role_arn="arn:aws:iam::123456789012:role/TestRole"
            )

        assert "Failed to refresh credentials for Bedrock cross-account role" in str(
            exc_info.value
        )
        assert "arn:aws:iam::123456789012:role/TestRole" in str(exc_info.value)

    @patch.dict(
        os.environ,
        {
            "BEDROCK_AWS_ACCESS_KEY_ID": "explicit-key",
            "BEDROCK_AWS_SECRET_ACCESS_KEY": "explicit-secret",
            "BEDROCK_AWS_REGION": "eu-west-1",
        },
        clear=True,
    )
    @patch("datahub_integrations.gen_ai.bedrock.boto3.client")
    @patch("datahub_integrations.gen_ai.bedrock.boto3.Session")
    @patch("datahub_integrations.gen_ai.bedrock.get_session")
    def test_cross_account_role_parameter_takes_precedence_over_explicit_credentials(
        self,
        mock_get_session: Mock,
        mock_boto3_session: Mock,
        mock_boto3_client: Mock,
    ) -> None:
        # Setup mocks for STS assume_role
        mock_sts_client = MagicMock()
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "assumed-key-id",
                "SecretAccessKey": "assumed-secret-key",
                "SessionToken": "assumed-session-token",
                "Expiration": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            }
        }
        mock_boto3_client.return_value = mock_sts_client

        # Setup mock for botocore session
        mock_botocore_session = MagicMock()
        mock_get_session.return_value = mock_botocore_session

        # Setup mock for boto3.Session
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_boto3_session.return_value = mock_session

        # Call with explicit cross_account_role_arn parameter
        # Even though BEDROCK_AWS_ACCESS_KEY_ID is set, role should take precedence
        result = bedrock_module._create_bedrock_client(
            cross_account_role_arn="arn:aws:iam::123456789012:role/TestRole"
        )

        # Verify STS assume_role was called (cross-account takes precedence)
        mock_sts_client.assume_role.assert_called_once()

        # Verify boto3.Session was NOT called with explicit credentials
        for call in mock_boto3_session.call_args_list:
            kwargs = call[1] if call[1] else {}
            assert "aws_access_key_id" not in kwargs
            assert "aws_secret_access_key" not in kwargs

        assert result == mock_client

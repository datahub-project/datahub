from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.rds_iam_utils import (
    RDSIAMTokenGenerator,
    RDSIAMTokenManager,
)


class TestRDSIAMTokenGenerator:
    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_init_success(self, mock_boto3):
        generator = RDSIAMTokenGenerator(
            "us-west-2", "test.rds.amazonaws.com", "testuser", 5432
        )
        assert generator.region == "us-west-2"
        assert generator.endpoint == "test.rds.amazonaws.com"
        assert generator.username == "testuser"
        assert generator.port == 5432

    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_generate_token_success(self, mock_boto3):
        mock_client = MagicMock()
        # Simulate full presigned URL returned by boto3
        full_token = "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser&X-Amz-Algorithm=AWS4-HMAC-SHA256"
        mock_client.generate_db_auth_token.return_value = full_token
        mock_boto3.client.return_value = mock_client

        generator = RDSIAMTokenGenerator(
            "us-west-2", "test.rds.amazonaws.com", "testuser"
        )
        token = generator.generate_token()

        # Should return full token as-is (for use with SQLAlchemy event listeners)
        assert token == full_token
        mock_boto3.client.assert_called_with("rds", region_name="us-west-2")
        mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="test.rds.amazonaws.com", Port=5432, DBUsername="testuser"
        )

    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_generate_token_no_credentials_error(self, mock_boto3):
        from botocore.exceptions import NoCredentialsError

        mock_boto3.client.side_effect = NoCredentialsError()

        with pytest.raises(ValueError, match="AWS credentials not found"):
            generator = RDSIAMTokenGenerator(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )
            generator.generate_token()

    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_generate_token_client_error(self, mock_boto3):
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        error_response = {
            "Error": {"Code": "InvalidParameterValue"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        }
        mock_client.generate_db_auth_token.side_effect = ClientError(
            error_response,  # type: ignore
            "generate_db_auth_token",
        )
        mock_boto3.client.return_value = mock_client

        generator = RDSIAMTokenGenerator(
            "us-west-2", "test.rds.amazonaws.com", "testuser"
        )

        with pytest.raises(ValueError, match="Failed to generate RDS IAM token"):
            generator.generate_token()

    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_generate_token_returns_full_url(self, mock_boto3):
        """Test that full presigned URL is returned from boto3."""
        mock_client = MagicMock()
        # Full presigned URL format from boto3
        full_token = (
            "database-1.cluster-xxx.us-west-2.rds.amazonaws.com:3306/"
            "?Action=connect&DBUser=iam-user&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE"
        )
        mock_client.generate_db_auth_token.return_value = full_token
        mock_boto3.client.return_value = mock_client

        generator = RDSIAMTokenGenerator(
            "us-west-2",
            "database-1.cluster-xxx.us-west-2.rds.amazonaws.com",
            "iam-user",
            3306,
        )
        token = generator.generate_token()

        # Should return full token as-is
        assert token == full_token
        assert "database-1.cluster-xxx.us-west-2.rds.amazonaws.com" in token
        assert "3306" in token
        assert "DBUser=iam-user" in token
        assert "X-Amz-Algorithm" in token

    @patch("datahub.ingestion.source.sql.rds_iam_utils.boto3")
    def test_generate_token_no_query_params(self, mock_boto3):
        """Test handling of token without query parameters (edge case)."""
        mock_client = MagicMock()
        # Hypothetical case where token doesn't contain "?"
        mock_client.generate_db_auth_token.return_value = "simple-token-without-params"
        mock_boto3.client.return_value = mock_client

        generator = RDSIAMTokenGenerator(
            "us-west-2", "test.rds.amazonaws.com", "testuser"
        )
        token = generator.generate_token()

        # Should return token as-is if no "?" found
        assert token == "simple-token-without-params"


class TestRDSIAMTokenManager:
    def test_init(self):
        with patch("datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser", 5432, 5
            )
            assert manager.refresh_threshold == timedelta(minutes=5)

    def test_needs_refresh_no_token(self):
        with patch("datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )
            assert manager._needs_refresh() is True

    def test_needs_refresh_token_expired(self):
        with patch("datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )
            manager._current_token = "old-token"
            manager._token_expires_at = datetime.now() - timedelta(minutes=1)

            assert manager._needs_refresh() is True

    def test_needs_refresh_token_valid(self):
        with patch("datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )
            manager._current_token = "valid-token"
            manager._token_expires_at = datetime.now() + timedelta(minutes=12)

            assert manager._needs_refresh() is False

    def test_get_token_refresh_needed(self):
        mock_generator = MagicMock()
        mock_generator.generate_token.return_value = "new-token-456"

        with patch(
            "datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"
        ) as MockGenerator:
            MockGenerator.return_value = mock_generator

            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )

            token = manager.get_token()

            assert token == "new-token-456"
            assert manager._current_token == "new-token-456"
            assert manager._token_expires_at is not None
            mock_generator.generate_token.assert_called_once()

    def test_force_refresh(self):
        mock_generator = MagicMock()
        mock_generator.generate_token.return_value = "forced-token-789"

        with patch(
            "datahub.ingestion.source.sql.rds_iam_utils.RDSIAMTokenGenerator"
        ) as MockGenerator:
            MockGenerator.return_value = mock_generator

            manager = RDSIAMTokenManager(
                "us-west-2", "test.rds.amazonaws.com", "testuser"
            )
            manager._current_token = "old-token"

            token = manager.force_refresh()

            assert token == "forced-token-789"
            assert manager._current_token == "forced-token-789"
            mock_generator.generate_token.assert_called_once()

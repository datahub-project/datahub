from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenGenerator,
    RDSIAMTokenManager,
)


class TestRDSIAMTokenGenerator:
    @patch("datahub.ingestion.source.aws.aws_common.boto3")
    def test_init_success(self, mock_boto3):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        generator = RDSIAMTokenGenerator(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )
        assert generator.aws_config == aws_config
        assert generator.endpoint == "test.rds.amazonaws.com"
        assert generator.username == "testuser"
        assert generator.port == 5432

    def test_generate_token_success(self):
        mock_client = MagicMock()
        # Simulate full presigned URL returned by boto3
        full_token = "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser&X-Amz-Algorithm=AWS4-HMAC-SHA256"
        mock_client.generate_db_auth_token.return_value = full_token

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            generator = RDSIAMTokenGenerator(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            token = generator.generate_token()

            # Should return full token as-is (for use with SQLAlchemy event listeners)
            assert token == full_token
            mock_client.generate_db_auth_token.assert_called_with(
                DBHostname="test.rds.amazonaws.com", Port=5432, DBUsername="testuser"
            )

    def test_generate_token_no_credentials_error(self):
        from botocore.exceptions import NoCredentialsError

        mock_session = MagicMock()
        mock_session.client.side_effect = NoCredentialsError()

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            generator = RDSIAMTokenGenerator(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            with pytest.raises(ValueError, match="AWS credentials not found"):
                generator.generate_token()

    def test_generate_token_client_error(self):
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

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            generator = RDSIAMTokenGenerator(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            with pytest.raises(ValueError, match="Failed to generate RDS IAM token"):
                generator.generate_token()

    def test_generate_token_returns_full_url(self):
        """Test that full presigned URL is returned from boto3."""
        mock_client = MagicMock()
        # Full presigned URL format from boto3
        full_token = (
            "database-1.cluster-xxx.us-west-2.rds.amazonaws.com:3306/"
            "?Action=connect&DBUser=iam-user&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE"
        )
        mock_client.generate_db_auth_token.return_value = full_token

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            generator = RDSIAMTokenGenerator(
                endpoint="database-1.cluster-xxx.us-west-2.rds.amazonaws.com",
                username="iam-user",
                port=3306,
                aws_config=aws_config,
            )
            token = generator.generate_token()

            # Should return full token as-is
            assert token == full_token
            assert "database-1.cluster-xxx.us-west-2.rds.amazonaws.com" in token
            assert "3306" in token
            assert "DBUser=iam-user" in token
            assert "X-Amz-Algorithm" in token

    def test_generate_token_no_query_params(self):
        """Test handling of token without query parameters (edge case)."""
        mock_client = MagicMock()
        # Hypothetical case where token doesn't contain "?"
        mock_client.generate_db_auth_token.return_value = "simple-token-without-params"

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            generator = RDSIAMTokenGenerator(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            token = generator.generate_token()

            # Should return token as-is if no "?" found
            assert token == "simple-token-without-params"


class TestRDSIAMTokenManager:
    def test_init(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        with patch("datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
                refresh_threshold_minutes=5,
            )
            assert manager.refresh_threshold == timedelta(minutes=5)

    def test_needs_refresh_no_token(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        with patch("datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            assert manager._needs_refresh() is True

    def test_needs_refresh_token_expired(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        with patch("datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            manager._current_token = "old-token"
            manager._token_expires_at = datetime.now() - timedelta(minutes=1)

            assert manager._needs_refresh() is True

    def test_needs_refresh_token_valid(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        with patch("datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"):
            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            manager._current_token = "valid-token"
            manager._token_expires_at = datetime.now() + timedelta(minutes=12)

            assert manager._needs_refresh() is False

    def test_get_token_refresh_needed(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        mock_generator = MagicMock()
        mock_generator.generate_token.return_value = "new-token-456"

        with patch(
            "datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"
        ) as MockGenerator:
            MockGenerator.return_value = mock_generator

            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            token = manager.get_token()

            assert token == "new-token-456"
            assert manager._current_token == "new-token-456"
            assert manager._token_expires_at is not None
            mock_generator.generate_token.assert_called_once()

    def test_force_refresh(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        mock_generator = MagicMock()
        mock_generator.generate_token.return_value = "forced-token-789"

        with patch(
            "datahub.ingestion.source.aws.aws_common.RDSIAMTokenGenerator"
        ) as MockGenerator:
            MockGenerator.return_value = mock_generator

            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            manager._current_token = "old-token"

            token = manager.force_refresh()

            assert token == "forced-token-789"
            assert manager._current_token == "forced-token-789"
            mock_generator.generate_token.assert_called_once()

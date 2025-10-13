from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
    generate_rds_iam_token,
)


class TestGenerateRDSIAMToken:
    def test_generate_token_success(self):
        mock_client = MagicMock()
        full_token = "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser&X-Amz-Algorithm=AWS4-HMAC-SHA256"
        mock_client.generate_db_auth_token.return_value = full_token

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with patch.object(
            AwsConnectionConfig, "get_session", return_value=mock_session
        ):
            token = generate_rds_iam_token(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            assert token == full_token
            mock_client.generate_db_auth_token.assert_called_with(
                DBHostname="test.rds.amazonaws.com", Port=5432, DBUsername="testuser"
            )

    def test_generate_token_no_credentials_error(self):
        from botocore.exceptions import NoCredentialsError

        mock_session = MagicMock()
        mock_session.client.side_effect = NoCredentialsError()

        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        with (
            patch.object(AwsConnectionConfig, "get_session", return_value=mock_session),
            pytest.raises(ValueError, match="AWS credentials not found"),
        ):
            generate_rds_iam_token(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

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

        with (
            patch.object(AwsConnectionConfig, "get_session", return_value=mock_session),
            pytest.raises(ValueError, match="Failed to generate RDS IAM token"),
        ):
            generate_rds_iam_token(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

    def test_generate_token_returns_full_url(self):
        """Test that full presigned URL is returned from boto3."""
        mock_client = MagicMock()
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
            token = generate_rds_iam_token(
                endpoint="database-1.cluster-xxx.us-west-2.rds.amazonaws.com",
                username="iam-user",
                port=3306,
                aws_config=aws_config,
            )

            assert token == full_token
            assert "database-1.cluster-xxx.us-west-2.rds.amazonaws.com" in token
            assert "3306" in token
            assert "DBUser=iam-user" in token
            assert "X-Amz-Algorithm" in token


class TestRDSIAMTokenManager:
    def test_init(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
            refresh_threshold_minutes=5,
        )
        assert manager.refresh_threshold == timedelta(minutes=5)
        assert manager.endpoint == "test.rds.amazonaws.com"
        assert manager.username == "testuser"
        assert manager.port == 5432

    def test_needs_refresh_no_token(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )
        assert manager._needs_refresh() is True

    def test_needs_refresh_token_expired(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )
        manager._current_token = "old-token"
        manager._token_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)

        assert manager._needs_refresh() is True

    def test_needs_refresh_token_valid(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )
        manager._current_token = "valid-token"
        manager._token_expires_at = datetime.now(timezone.utc) + timedelta(minutes=12)

        assert manager._needs_refresh() is False

    def test_get_token_refresh_needed(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        # Token with X-Amz-Date and X-Amz-Expires for parsing
        full_token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T120000Z&X-Amz-Expires=900"
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.generate_rds_iam_token"
        ) as mock_gen:
            mock_gen.return_value = full_token

            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            token = manager.get_token()

            assert token == full_token
            assert manager._current_token == full_token
            assert manager._token_expires_at is not None
            mock_gen.assert_called_once()

    def test_force_refresh(self):
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        full_token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T120000Z&X-Amz-Expires=900"
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.generate_rds_iam_token"
        ) as mock_gen:
            mock_gen.return_value = full_token

            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )
            manager._current_token = "old-token"

            token = manager.force_refresh()

            assert token == full_token
            assert manager._current_token == full_token
            mock_gen.assert_called_once()

    def test_parse_token_expiry(self):
        """Test parsing X-Amz-Date and X-Amz-Expires from token URL."""
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )

        # Token issued at 2025-01-01 12:00:00 UTC, expires in 900 seconds (15 minutes)
        token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T120000Z&X-Amz-Expires=900"
        )

        expiry = manager._parse_token_expiry(token)
        expected_expiry = datetime(2025, 1, 1, 12, 15, 0, tzinfo=timezone.utc)

        assert expiry == expected_expiry

    def test_parse_token_expiry_missing_date(self):
        """Test error handling when X-Amz-Date is missing."""
        aws_config = AwsConnectionConfig(aws_region="us-west-2")
        manager = RDSIAMTokenManager(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=aws_config,
        )

        token = "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser&X-Amz-Expires=900"

        with pytest.raises(ValueError, match="Missing X-Amz-Date"):
            manager._parse_token_expiry(token)

    def test_get_token_automatically_refreshes_expired_token(self):
        """Test that get_token() automatically refreshes when token is expired."""
        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        old_token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T120000Z&X-Amz-Expires=900"
        )
        new_token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T130000Z&X-Amz-Expires=900"
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.generate_rds_iam_token"
        ) as mock_gen:
            mock_gen.return_value = new_token

            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            # Set an expired token manually
            manager._current_token = old_token
            manager._token_expires_at = datetime.now(timezone.utc) - timedelta(
                minutes=1
            )

            # Call get_token() - should automatically refresh
            token = manager.get_token()

            # Verify new token was generated
            assert token == new_token
            assert manager._current_token == new_token
            mock_gen.assert_called_once_with(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

    def test_get_token_reuses_valid_token(self):
        """Test that get_token() reuses token when still valid."""
        aws_config = AwsConnectionConfig(aws_region="us-west-2")

        valid_token = (
            "test.rds.amazonaws.com:5432/?Action=connect&DBUser=testuser"
            "&X-Amz-Date=20250101T120000Z&X-Amz-Expires=900"
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.generate_rds_iam_token"
        ) as mock_gen:
            manager = RDSIAMTokenManager(
                endpoint="test.rds.amazonaws.com",
                username="testuser",
                port=5432,
                aws_config=aws_config,
            )

            # Set a valid token that won't expire soon
            manager._current_token = valid_token
            manager._token_expires_at = datetime.now(timezone.utc) + timedelta(
                minutes=20
            )

            # Call get_token() - should reuse existing token
            token = manager.get_token()

            # Verify existing token was returned without generating new one
            assert token == valid_token
            assert manager._current_token == valid_token
            mock_gen.assert_not_called()

"""Utilities for AWS RDS IAM authentication."""

import logging
from datetime import datetime, timedelta
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class RDSIAMTokenGenerator:
    """Generates AWS RDS IAM authentication tokens."""

    def __init__(self, region: str, endpoint: str, username: str, port: int = 5432):
        """
        Initialize the token generator.

        Args:
            region: AWS region (e.g., 'us-west-2')
            endpoint: RDS endpoint hostname
            username: Database username for IAM authentication
            port: Database port (5432 for PostgreSQL, 3306 for MySQL)
        """
        self.region = region
        self.endpoint = endpoint
        self.username = username
        self.port = port
        self._client: Optional[object] = None

    def _get_rds_client(self):
        """Get or create RDS client."""
        if self._client is None:
            try:
                self._client = boto3.client("rds", region_name=self.region)
            except NoCredentialsError as e:
                raise ValueError(
                    "AWS credentials not found. Configure AWS credentials using "
                    "AWS CLI, environment variables, or IAM roles."
                ) from e
        return self._client

    def generate_token(self) -> str:
        """
        Generate a new RDS IAM authentication token.

        boto3's generate_db_auth_token() returns a presigned URL in the format:
        "hostname:port/?Action=connect&DBUser=username&X-Amz-Algorithm=..."

        This token should be used as-is by pymysql/psycopg2 drivers. When using
        SQLAlchemy, inject the token via event listeners rather than the URL
        to avoid URL parsing issues.

        Returns:
            Full authentication token (presigned URL format)
            Valid for 15 minutes

        Raises:
            ValueError: If AWS credentials are not configured
            ClientError: If AWS API call fails
        """
        try:
            client = self._get_rds_client()
            token = client.generate_db_auth_token(
                DBHostname=self.endpoint, Port=self.port, DBUsername=self.username
            )
            logger.debug(
                f"Generated RDS IAM token for {self.username}@{self.endpoint}:{self.port}"
            )
            return token

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise ValueError(
                f"Failed to generate RDS IAM token: {error_code} - {e}"
            ) from e


class RDSIAMTokenManager:
    """
    Manages RDS IAM token lifecycle with automatic refresh.

    RDS IAM tokens expire after 15 minutes. This manager automatically
    refreshes tokens at 10-minute intervals (5 minutes before expiry)
    to ensure uninterrupted database access.

    For additional protection against token expiration during long-running
    connections, use SQLAlchemy's pool_recycle=600 and pool_pre_ping=True
    options to force connection recycling every 10 minutes.
    """

    def __init__(
        self,
        region: str,
        endpoint: str,
        username: str,
        port: int = 5432,
        refresh_threshold_minutes: int = 10,
    ):
        """
        Initialize the token manager.

        Args:
            region: AWS region
            endpoint: RDS endpoint hostname
            username: Database username for IAM authentication
            port: Database port
            refresh_threshold_minutes: Refresh token when this many minutes remain
        """
        self.generator = RDSIAMTokenGenerator(region, endpoint, username, port)
        self.refresh_threshold = timedelta(minutes=refresh_threshold_minutes)

        self._current_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def get_token(self) -> str:
        """
        Get current token, refreshing if necessary.

        Returns:
            Valid authentication token
        """
        if self._needs_refresh():
            self._refresh_token()

        assert self._current_token is not None
        return self._current_token

    def _needs_refresh(self) -> bool:
        """Check if token needs to be refreshed."""
        if self._current_token is None or self._token_expires_at is None:
            return True

        time_until_expiry = self._token_expires_at - datetime.now()
        return time_until_expiry <= self.refresh_threshold

    def _refresh_token(self) -> None:
        """Generate and store a new token."""
        logger.info("Refreshing RDS IAM authentication token")
        self._current_token = self.generator.generate_token()
        # Tokens are valid for 15 minutes, but we refresh earlier for safety
        self._token_expires_at = datetime.now() + timedelta(minutes=15)

    def force_refresh(self) -> str:
        """Force token refresh and return new token."""
        self._refresh_token()
        assert self._current_token is not None
        return self._current_token

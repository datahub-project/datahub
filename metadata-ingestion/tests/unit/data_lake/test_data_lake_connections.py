"""
Unit tests for data lake connections functionality.

Tests the DataLakeConnectionConfig class for S3, GCS, and Azure Blob Storage connections.
"""

import pytest

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.data_lake_common.connections import (
    DataLakeConnectionConfig,
    get_data_lake_uri_type,
    is_data_lake_uri,
)


class TestDataLakeConnectionConfig:
    """Test DataLakeConnectionConfig functionality."""

    def test_uri_type_detection(self):
        """Test URI type detection functions."""
        # Test S3 URIs
        assert get_data_lake_uri_type("s3://bucket/path/file.json") == "s3"
        assert get_data_lake_uri_type("s3a://bucket/path/file.json") == "s3"
        assert get_data_lake_uri_type("s3n://bucket/path/file.json") == "s3"

        # Test GCS URIs
        assert get_data_lake_uri_type("gs://bucket/path/file.json") == "gcs"

        # Test Azure URIs
        assert (
            get_data_lake_uri_type(
                "https://account.blob.core.windows.net/container/file.json"
            )
            == "azure"
        )
        assert (
            get_data_lake_uri_type(
                "abfss://container@account.dfs.core.windows.net/file.json"
            )
            == "azure"
        )

        # Test non-data lake URIs
        assert get_data_lake_uri_type("https://example.com/file.json") is None
        assert get_data_lake_uri_type("/local/path/file.json") is None
        # Test with empty string instead of None to avoid type errors
        assert get_data_lake_uri_type("") is None

    def test_is_data_lake_uri(self):
        """Test data lake URI detection."""
        # Test positive cases
        assert is_data_lake_uri("s3://bucket/path/file.json")
        assert is_data_lake_uri("gs://bucket/path/file.json")
        assert is_data_lake_uri(
            "https://account.blob.core.windows.net/container/file.json"
        )
        assert is_data_lake_uri(
            "abfss://container@account.dfs.core.windows.net/file.json"
        )

        # Test negative cases
        assert not is_data_lake_uri("https://example.com/file.json")
        assert not is_data_lake_uri("/local/path/file.json")
        assert not is_data_lake_uri("")
        # Test with empty string instead of None to avoid type errors

    def test_validate_connections_for_uris_s3(self):
        """Test validation for S3 URIs."""
        config = DataLakeConnectionConfig()

        # Should not raise error for non-S3 URIs
        config.validate_connections_for_uris(
            ["https://example.com/file.json", "/local/path/file.json"]
        )

        # Should raise error for S3 URIs without AWS connection
        with pytest.raises(
            ValueError, match="Please provide aws_connection configuration"
        ):
            config.validate_connections_for_uris(["s3://bucket/file.json"])

        # Should not raise error for S3 URIs with AWS connection
        config.aws_connection = AwsConnectionConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-east-1",
        )
        config.validate_connections_for_uris(["s3://bucket/file.json"])

    def test_validate_connections_for_uris_gcs(self):
        """Test validation for GCS URIs."""
        config = DataLakeConnectionConfig()

        # Should raise error for GCS URIs without GCS connection
        with pytest.raises(
            ValueError, match="Please provide gcs_connection configuration"
        ):
            config.validate_connections_for_uris(["gs://bucket/file.json"])

        # Should not raise error for GCS URIs with GCS connection
        config.gcs_connection = GCPCredential(
            private_key_id="test",
            private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            client_email="test@test.iam.gserviceaccount.com",
            client_id="test",
        )
        config.validate_connections_for_uris(["gs://bucket/file.json"])

    def test_validate_connections_for_uris_azure(self):
        """Test validation for Azure URIs."""
        config = DataLakeConnectionConfig()

        # Should raise error for Azure URIs without Azure connection
        with pytest.raises(
            ValueError, match="Please provide azure_connection configuration"
        ):
            config.validate_connections_for_uris(
                ["https://account.blob.core.windows.net/container/file.json"]
            )

        with pytest.raises(
            ValueError, match="Please provide azure_connection configuration"
        ):
            config.validate_connections_for_uris(
                ["abfss://container@account.dfs.core.windows.net/file.json"]
            )

        # Should not raise error for Azure URIs with Azure connection
        config.azure_connection = AzureConnectionConfig(
            account_name="test", container_name="test", account_key="test"
        )
        config.validate_connections_for_uris(
            [
                "https://account.blob.core.windows.net/container/file.json",
                "abfss://container@account.dfs.core.windows.net/file.json",
            ]
        )

    def test_validate_connections_for_mixed_uris(self):
        """Test validation for mixed URI types."""
        config = DataLakeConnectionConfig(
            aws_connection=AwsConnectionConfig(
                aws_access_key_id="test",
                aws_secret_access_key="test",
                aws_region="us-east-1",
            ),
            gcs_connection=GCPCredential(
                private_key_id="test",
                private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
                client_email="test@test.iam.gserviceaccount.com",
                client_id="test",
            ),
            azure_connection=AzureConnectionConfig(
                account_name="test", container_name="test", account_key="test"
            ),
        )

        # Should not raise error for mixed URIs with all connections
        config.validate_connections_for_uris(
            [
                "s3://bucket/file1.json",
                "gs://bucket/file2.json",
                "https://account.blob.core.windows.net/container/file3.json",
                "abfss://container@account.dfs.core.windows.net/file4.json",
                "https://example.com/file5.json",  # Non-data lake URI
                "/local/path/file6.json",  # Local file
            ]
        )

    def test_empty_config_creation(self):
        """Test that empty configuration can be created."""
        config = DataLakeConnectionConfig()
        assert config.aws_connection is None
        assert config.gcs_connection is None
        assert config.azure_connection is None

    def test_partial_config_creation(self):
        """Test that partial configurations work."""
        # Only AWS connection
        config = DataLakeConnectionConfig(
            aws_connection=AwsConnectionConfig(
                aws_access_key_id="test",
                aws_secret_access_key="test",
                aws_region="us-east-1",
            )
        )
        assert config.aws_connection is not None
        assert config.gcs_connection is None
        assert config.azure_connection is None

        # Only GCS connection
        config = DataLakeConnectionConfig(
            gcs_connection=GCPCredential(
                private_key_id="test",
                private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
                client_email="test@test.iam.gserviceaccount.com",
                client_id="test",
            )
        )
        assert config.aws_connection is None
        assert config.gcs_connection is not None
        assert config.azure_connection is None

    def test_non_data_lake_uris_work_without_connections(self):
        """Test that non-data lake URIs work without any connections."""
        config = DataLakeConnectionConfig()

        # Should not raise errors for non-data lake URIs
        config.validate_connections_for_uris(
            [
                "https://example.com/file.json",
                "/local/path/file.json",
                "file:///local/path/file.json",
            ]
        )

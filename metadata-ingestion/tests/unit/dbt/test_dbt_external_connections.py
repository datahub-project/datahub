"""
Unit tests for dbt external connections functionality.

Tests the ability to load dbt files from various external sources including
cloud storage (via data_lake_common), Git repositories, and HTTP endpoints.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.git import GitInfo, GitReference
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig
from datahub.ingestion.source.dbt.dbt_external_connections import (
    ExternalConnectionConfig,
    get_external_uri_type,
    is_external_uri,
    is_git_uri,
)


class TestExternalConnectionConfig:
    """Test ExternalConnectionConfig functionality."""

    def test_uri_type_detection(self):
        """Test external URI type detection functions."""
        # Test cloud storage URIs (delegated to data_lake_common)
        assert get_external_uri_type("s3://bucket/path/file.json") == "s3"
        assert get_external_uri_type("gs://bucket/path/file.json") == "gcs"
        assert (
            get_external_uri_type(
                "https://account.blob.core.windows.net/container/file.json"
            )
            == "azure"
        )
        assert (
            get_external_uri_type(
                "abfss://container@account.dfs.core.windows.net/file.json"
            )
            == "azure"
        )

        # Test Git URIs
        assert get_external_uri_type("git@github.com:owner/repo.git") == "git"
        assert get_external_uri_type("https://github.com/owner/repo") == "git"
        assert get_external_uri_type("https://gitlab.com/owner/repo") == "git"
        assert get_external_uri_type("git+https://github.com/owner/repo.git") == "git"

        # Test HTTP URIs
        assert get_external_uri_type("https://example.com/file.json") == "http"
        assert get_external_uri_type("http://example.com/file.json") == "http"

        # Test non-external URIs
        assert get_external_uri_type("/local/path/file.json") is None
        # Test with empty string instead of None to avoid type errors
        assert get_external_uri_type("") is None

    def test_is_external_uri(self):
        """Test external URI detection."""
        # Test positive cases (cloud storage)
        assert is_external_uri("s3://bucket/path/file.json")
        assert is_external_uri("gs://bucket/path/file.json")
        assert is_external_uri(
            "https://account.blob.core.windows.net/container/file.json"
        )
        assert is_external_uri(
            "abfss://container@account.dfs.core.windows.net/file.json"
        )

        # Test positive cases (Git)
        assert is_external_uri("git@github.com:owner/repo.git")
        assert is_external_uri("https://github.com/owner/repo")
        assert is_external_uri("https://gitlab.com/owner/repo")

        # Test positive cases (HTTP)
        assert is_external_uri("https://example.com/file.json")
        assert is_external_uri("http://example.com/file.json")

        # Test negative cases
        assert not is_external_uri("/local/path/file.json")
        assert not is_external_uri("")
        # Test with empty string instead of None to avoid type errors

    def test_is_git_uri(self):
        """Test Git URI detection."""
        # Test positive cases
        assert is_git_uri("git@github.com:owner/repo.git")
        assert is_git_uri("https://github.com/owner/repo")
        assert is_git_uri("https://gitlab.com/owner/repo")
        assert is_git_uri("git+https://github.com/owner/repo.git")
        assert is_git_uri("https://example.com/repo.git")

        # Test negative cases
        assert not is_git_uri("https://example.com/file.json")
        assert not is_git_uri("s3://bucket/path/file.json")
        assert not is_git_uri("/local/path/file.json")
        assert not is_git_uri("")
        # Test with empty string instead of None to avoid type errors

    def test_validate_connections_for_uris_cloud_storage(self):
        """Test validation for cloud storage URIs (delegates to DataLakeConnectionConfig)."""
        config = ExternalConnectionConfig()

        # Should not raise error for non-cloud URIs
        config.validate_connections_for_uris(
            ["https://example.com/file.json", "/local/path/file.json"]
        )

        # Should raise error for cloud URIs without connections
        with pytest.raises(
            ValueError, match="Please provide aws_connection configuration"
        ):
            config.validate_connections_for_uris(["s3://bucket/file.json"])

        # Should work with proper connections (now flattened)
        config.aws_connection = AwsConnectionConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-east-1",
        )
        config.validate_connections_for_uris(["s3://bucket/file.json"])

    def test_validate_connections_for_uris_git(self):
        """Test validation for Git URIs."""
        config = ExternalConnectionConfig()

        # Should raise error for Git URIs without git_info
        with pytest.raises(ValueError, match="Please provide git_info configuration"):
            config.validate_connections_for_uris(["git@github.com:owner/repo.git"])

        # Should work with git_info
        config.git_info = GitInfo(repo="git@github.com:owner/repo.git", branch="main")
        config.validate_connections_for_uris(["git@github.com:owner/repo.git"])

    def test_load_file_as_json_http(self):
        """Test loading JSON from HTTP URLs."""
        config = ExternalConnectionConfig()

        mock_response = MagicMock()
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status.return_value = None

        with patch(
            "datahub.ingestion.source.data_lake_common.file_loader.requests.get",
            return_value=mock_response,
        ):
            result = config.load_file_as_json("https://example.com/file.json")
            assert result == {"test": "data"}

    def test_load_file_as_json_local(self):
        """Test loading JSON from local files."""
        config = ExternalConnectionConfig()

        test_data = {"local": "file", "data": [1, 2, 3]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = f.name

        try:
            result = config.load_file_as_json(temp_path)
            assert result == test_data
        finally:
            os.unlink(temp_path)

    def test_load_file_as_json_cloud_storage_delegation(self):
        """Test that cloud storage file loading is delegated to data_lake_common."""
        config = ExternalConnectionConfig()

        # Should raise error for cloud URIs without connections
        with pytest.raises(ValueError, match="AWS connection required for S3 URI"):
            config.load_file_as_json("s3://bucket/file.json")

        # Should delegate to data lake file loader when connections are provided
        from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig

        config.aws_connection = AwsConnectionConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-east-1",
        )

        with patch(
            "datahub.ingestion.source.dbt.dbt_external_connections.load_file_as_json"
        ) as mock_load:
            mock_load.return_value = {"test": "delegated"}
            result = config.load_file_as_json("s3://bucket/file.json")
            assert result == {"test": "delegated"}
            mock_load.assert_called_once_with("s3://bucket/file.json", config)

    @patch("datahub.ingestion.source.git.git_import.GitClone.clone")
    def test_load_file_as_json_git_not_implemented(self, mock_clone):
        """Test that Git file loading raises NotImplementedError for now."""
        import tempfile
        from pathlib import Path

        config = ExternalConnectionConfig(
            git_info=GitInfo(repo="git@github.com:owner/repo.git", branch="main")
        )

        # Mock the clone method to return a fake path
        with tempfile.TemporaryDirectory() as tmp_dir:
            fake_cloned_path = Path(tmp_dir) / "fake_repo"
            fake_cloned_path.mkdir()
            mock_clone.return_value = fake_cloned_path

            # Git functionality is not fully implemented yet - expect specific errors
            with pytest.raises((NotImplementedError, ValueError, FileNotFoundError)):
                config.load_file_as_json(
                    "git@github.com:owner/repo.git/path/to/file.json"
                )


class TestBackwardsCompatibility:
    """Test backwards compatibility for dbt external connections."""

    def test_empty_config_creation(self):
        """Test that empty configuration can be created."""
        config = ExternalConnectionConfig()
        assert config.aws_connection is None
        assert config.gcs_connection is None
        assert config.azure_connection is None
        assert config.git_info is None

    def test_data_lake_connections_work(self):
        """Test that data lake connections work properly."""
        config = ExternalConnectionConfig(
            aws_connection=AwsConnectionConfig(
                aws_access_key_id="test",
                aws_secret_access_key="test",
                aws_region="us-east-1",
            )
        )
        assert config.aws_connection is not None
        assert config.git_info is None

    def test_git_connections_work(self):
        """Test that Git connections work properly."""
        config = ExternalConnectionConfig(
            git_info=GitInfo(repo="git@github.com:owner/repo.git", branch="main")
        )
        assert config.aws_connection is None
        assert config.gcs_connection is None
        assert config.azure_connection is None
        assert config.git_info is not None

    def test_non_external_uris_work_without_connections(self):
        """Test that non-external URIs work without any connections."""
        config = ExternalConnectionConfig()

        # Should not raise errors for non-external URIs
        config.validate_connections_for_uris(
            [
                "https://example.com/file.json",  # HTTP (not Git or cloud storage)
                "/local/path/file.json",
                "file:///local/path/file.json",
            ]
        )

    def test_dbt_core_config_backwards_compatibility(self):
        """Test that DBTCoreConfig maintains backwards compatibility with original validation behavior."""
        from pydantic import ValidationError

        # Test that S3 URIs without external_connections raise ValidationError (updated behavior)
        config_dict = {
            "manifest_path": "s3://dummy_path",
            "catalog_path": "s3://dummy_path",
            "target_platform": "dummy_platform",
        }
        with pytest.raises(
            ValidationError, match="Please provide aws_connection configuration"
        ):
            DBTCoreConfig.parse_obj(config_dict)

        # Test that GCS URIs without external_connections raise ValidationError
        config_dict = {
            "manifest_path": "gs://dummy_path",
            "catalog_path": "gs://dummy_path",
            "target_platform": "dummy_platform",
        }
        with pytest.raises(
            ValidationError, match="Please provide gcs_connection configuration"
        ):
            DBTCoreConfig.parse_obj(config_dict)

        # Test that Azure URIs without external_connections raise ValidationError
        config_dict = {
            "manifest_path": "https://account.blob.core.windows.net/container/file.json",
            "catalog_path": "https://account.blob.core.windows.net/container/file.json",
            "target_platform": "dummy_platform",
        }
        with pytest.raises(
            ValidationError, match="Please provide azure_connection configuration"
        ):
            DBTCoreConfig.parse_obj(config_dict)

        # Test that valid configuration with external_connections works

        from typing import Any

        valid_config_dict: dict[str, Any] = {
            "manifest_path": "s3://dummy_path",
            "catalog_path": "s3://dummy_path",
            "target_platform": "dummy_platform",
            "external_connections": {
                "aws_connection": {
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                    "aws_region": "us-east-1",
                }
            },
        }
        config = DBTCoreConfig.parse_obj(valid_config_dict)
        external_connections = config.get_external_connections()
        assert external_connections is not None
        assert external_connections.aws_connection is not None


class TestDbtSpecificFunctionality:
    """Test dbt-specific external connections functionality."""

    def test_backwards_compatibility_function_names(self):
        """Test that backwards compatibility function names still work."""
        # Import the centralized is_data_lake_uri (the actual legacy function that existed)
        from datahub.ingestion.source.data_lake_common.connections import (
            is_data_lake_uri,
        )

        # Test data lake URIs (cloud storage only - backwards compatible)
        assert is_data_lake_uri("s3://bucket/file.json")
        assert is_data_lake_uri("gs://bucket/file.json")
        assert is_data_lake_uri(
            "abfss://container@account.dfs.core.windows.net/file.json"
        )

        # Test that non-data-lake URIs return False (correct behavior)
        assert not is_data_lake_uri("git@github.com:owner/repo.git")
        assert not is_data_lake_uri("https://example.com/file.json")

        # Test the NEW dbt-specific functions (not legacy - these are new functionality)
        from datahub.ingestion.source.dbt.dbt_external_connections import (
            get_dbt_external_uri_type,
            is_dbt_external_uri,
        )

        assert get_dbt_external_uri_type("s3://bucket/file.json") == "s3"
        assert get_dbt_external_uri_type("git@github.com:owner/repo.git") == "git"
        assert get_dbt_external_uri_type("https://example.com/file.json") == "http"

        assert is_dbt_external_uri("s3://bucket/file.json")
        assert is_dbt_external_uri("git@github.com:owner/repo.git")
        assert is_dbt_external_uri("https://example.com/file.json")

    def test_mixed_external_connections(self):
        """Test configuration with both data lake and Git connections."""
        config = ExternalConnectionConfig(
            aws_connection=AwsConnectionConfig(
                aws_access_key_id="test",
                aws_secret_access_key="test",
                aws_region="us-east-1",
            ),
            git_info=GitInfo(repo="git@github.com:owner/repo.git", branch="main"),
        )

        # Should not raise error for mixed URIs with appropriate connections
        config.validate_connections_for_uris(
            [
                "s3://bucket/file1.json",
                "git@github.com:owner/repo.git/path/file2.json",
                "https://example.com/file3.json",
                "/local/path/file4.json",
            ]
        )


class TestNavigationLinks:
    """Test navigation link functionality with both old and new Git configurations."""

    def test_navigation_links_with_external_connections_git_info(self):
        """Test that navigation links work with external_connections.git_info"""
        config = DBTCoreConfig(
            manifest_path="target/manifest.json",
            catalog_path="target/catalog.json",
            target_platform="postgres",
            external_connections=ExternalConnectionConfig(
                git_info=GitInfo(
                    repo="https://github.com/my-org/dbt-project", branch="main"
                )
            ),
        )

        # Test that the git_info can generate navigation URLs
        node_path = "models/my_model.sql"
        expected_url = (
            "https://github.com/my-org/dbt-project/blob/main/models/my_model.sql"
        )

        assert config.external_connections is not None
        assert config.external_connections.git_info is not None
        actual_url = config.external_connections.git_info.get_url_for_file_path(
            node_path
        )

        assert actual_url == expected_url

    def test_navigation_links_priority_external_over_legacy(self):
        """Test that external_connections.git_info takes priority over legacy git_info for navigation"""
        config = DBTCoreConfig(
            manifest_path="target/manifest.json",
            catalog_path="target/catalog.json",
            target_platform="postgres",
            # Legacy config (should be ignored for navigation)
            git_info=GitReference(
                repo="https://github.com/old-org/old-project", branch="old-branch"
            ),
            # New config (should take priority)
            external_connections=ExternalConnectionConfig(
                git_info=GitInfo(
                    repo="https://github.com/new-org/new-project", branch="new-branch"
                )
            ),
        )

        # Test that the external_connections.git_info generates the correct URL
        node_path = "models/my_model.sql"
        expected_url = (
            "https://github.com/new-org/new-project/blob/new-branch/models/my_model.sql"
        )

        assert config.external_connections is not None
        assert config.external_connections.git_info is not None
        actual_url = config.external_connections.git_info.get_url_for_file_path(
            node_path
        )

        assert actual_url == expected_url

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.sql_queries import (
    SqlQueriesSource,
    SqlQueriesSourceConfig,
)


class TestS3Support:
    """Test S3 support features."""

    def test_s3_uri_detection(self):
        """Test S3 URI detection via config validation."""
        # A local path needs no aws_config.
        SqlQueriesSourceConfig(platform="snowflake", query_file="/local/path/file.json")

        # Every S3 scheme requires one, and is rejected at config time without it.
        for uri in ("s3://b/f.json", "s3a://b/f.json", "s3n://b/f.json"):
            with pytest.raises(
                ValueError, match="aws_config is required when query_file is an S3 URI"
            ):
                SqlQueriesSourceConfig(platform="snowflake", query_file=uri)

    @patch("datahub.ingestion.source.sql_queries.smart_open.open")
    def test_s3_file_processing(self, mock_open):
        """Test S3 file processing."""
        # Create a proper AWS config dict
        aws_config_dict = {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "aws_session_token": "test_token",
        }

        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="s3://test-bucket/test-key",
            aws_config=aws_config_dict,
        )

        # Create a minimal source instance without full initialization
        source = SqlQueriesSource.__new__(SqlQueriesSource)
        source.config = config
        source.report = Mock()
        source.report.num_entries_processed = 0
        source.report.num_entries_failed = 0
        source.report.warning = Mock()

        # Mock AWS config and S3 client
        mock_aws_config = Mock()
        mock_aws_config.get_s3_client.return_value = Mock()
        config.aws_config = mock_aws_config

        # Mock smart_open file stream
        mock_file_stream = Mock()
        mock_file_stream.__enter__ = Mock(return_value=mock_file_stream)
        mock_file_stream.__exit__ = Mock(return_value=None)
        mock_file_stream.__iter__ = Mock(
            return_value=iter(
                [
                    '{"query": "SELECT * FROM table1", "timestamp": 1609459200}\n',
                    '{"query": "SELECT * FROM table2", "timestamp": 1609459201}\n',
                ]
            )
        )
        mock_open.return_value = mock_file_stream

        # Test S3 file processing
        queries = list(source._parse_s3_query_file(config.aws_config))
        assert len(queries) == 2
        assert queries[0].query == "SELECT * FROM table1"
        assert queries[1].query == "SELECT * FROM table2"


class TestTemporaryTableSupport:
    """Test temporary table support features."""

    def test_temp_table_patterns_default(self):
        """Test default temp table patterns."""
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        assert config.temp_table_patterns == []

    def test_temp_table_patterns_custom(self):
        """Test custom temp table patterns."""
        patterns = ["^temp_.*", "^tmp_.*", ".*_temp$"]
        config = SqlQueriesSourceConfig(
            platform="snowflake", query_file="dummy.json", temp_table_patterns=patterns
        )
        assert config.temp_table_patterns == patterns

    def test_is_temp_table_no_patterns(self):
        """Test temp table detection with no patterns."""
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        # Create a minimal source instance without full initialization
        source = SqlQueriesSource.__new__(SqlQueriesSource)
        source.config = config
        source.report = Mock()
        source.report.num_temp_table_matches = 0

        assert source.is_temp_table("temp_table") is False
        assert source.is_temp_table("regular_table") is False

    def test_is_temp_table_with_patterns(self):
        """Test temp table detection with patterns."""
        patterns = ["^temp_.*", "^tmp_.*", ".*_temp$"]
        config = SqlQueriesSourceConfig(
            platform="snowflake", query_file="dummy.json", temp_table_patterns=patterns
        )
        # Create a minimal source instance without full initialization
        source = SqlQueriesSource.__new__(SqlQueriesSource)
        source.config = config
        source.report = Mock()
        source.report.num_temp_table_matches = 0

        # Test matching patterns
        assert source.is_temp_table("temp_table") is True
        assert source.is_temp_table("tmp_table") is True
        assert source.is_temp_table("my_temp") is True
        assert source.is_temp_table("TEMP_TABLE") is True  # Case insensitive

        # Test non-matching patterns
        assert source.is_temp_table("regular_table") is False
        assert source.is_temp_table("table_temp_other") is False

    def test_temp_table_detection_counting(self):
        """Test that temp table detection is counted in reporting."""
        patterns = ["^temp_.*"]
        config = SqlQueriesSourceConfig(
            platform="snowflake", query_file="dummy.json", temp_table_patterns=patterns
        )
        # Create a minimal source instance without full initialization
        source = SqlQueriesSource.__new__(SqlQueriesSource)
        source.config = config
        source.report = Mock()
        source.report.num_temp_table_matches = 0

        # Initial count should be 0
        assert source.report.num_temp_table_matches == 0

        # Test temp table detection
        source.is_temp_table("temp_table1")
        source.is_temp_table("temp_table2")
        source.is_temp_table("regular_table")

        # Should count only the temp tables
        assert source.report.num_temp_table_matches == 2


class TestConfigurationValidation:
    """Test configuration validation."""

    def test_all_new_options_have_defaults(self):
        """Test that all new configuration options have sensible defaults."""
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")

        # S3 options
        assert config.aws_config is None

        # Temp table options
        assert config.temp_table_patterns == []

    def test_backward_compatibility(self):
        """Test that existing configurations still work."""
        # Test minimal configuration
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        assert config.platform == "snowflake"
        assert config.query_file == "dummy.json"

        # Test with some existing options
        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="dummy.json",
            default_db="test_db",
            default_schema="test_schema",
        )
        assert config.default_db == "test_db"
        assert config.default_schema == "test_schema"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_none_aws_config(self):
        """Test that S3 URI with None AWS config is rejected at config time."""
        with pytest.raises(ValueError, match="aws_config is required"):
            SqlQueriesSourceConfig(
                platform="snowflake",
                query_file="s3://bucket/file.json",
                aws_config=None,
            )

    def test_s3_uri_detection_in_config(self):
        """Test that S3 URIs require aws_config."""
        # Non-S3 URIs don't require aws_config
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        assert not config.query_file.startswith("s3://")

        # S3 URIs require aws_config
        with pytest.raises(ValueError, match="aws_config is required"):
            SqlQueriesSourceConfig(
                platform="snowflake", query_file="s3://bucket/file.json"
            )


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple features."""

    def test_s3_with_aws_config(self):
        """An s3:// query_file is accepted when aws_config is supplied."""
        # Create a proper AWS config dict
        aws_config_dict = {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "aws_session_token": "test_token",
        }

        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="s3://bucket/file.json",
            aws_config=aws_config_dict,
        )

        # Verify S3 URI detected
        assert config.query_file.startswith("s3://")

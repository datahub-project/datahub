from contextlib import nullcontext
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.sql_queries.config import SqlQueriesSourceConfig
from datahub.ingestion.source.sql_queries.query_entry import QueryEntryContext
from datahub.ingestion.source.sql_queries.readers.file import FileQueryLogReader
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
from datahub.ingestion.source.sql_queries.source import SqlQueriesSource


class TestS3Support:
    def test_s3_uri_requires_aws_config(self):
        # A local path needs no aws_config.
        SqlQueriesSourceConfig(platform="snowflake", query_file="/local/path/file.json")

        # Every S3 scheme requires one, and is rejected at config time without it.
        for uri in ("s3://b/f.json", "s3a://b/f.json", "s3n://b/f.json"):
            with pytest.raises(
                ValueError, match="aws_config is required when query_file is an S3 URI"
            ):
                SqlQueriesSourceConfig(platform="snowflake", query_file=uri)

    @patch("datahub.ingestion.source.sql_queries.readers.file.smart_open.open")
    def test_s3_file_is_read_with_the_configured_client(self, mock_open):
        aws_config = Mock(spec=AwsConnectionConfig)
        s3_client = Mock()
        aws_config.get_s3_client.return_value = s3_client
        mock_open.return_value = nullcontext(
            iter(
                [
                    '{"query": "SELECT * FROM table1", "timestamp": 1609459200}\n',
                    '{"query": "SELECT * FROM table2", "timestamp": 1609459201}\n',
                ]
            )
        )
        report = SqlQueriesSourceReport()
        reader = FileQueryLogReader(
            query_file="s3://test-bucket/test-key",
            aws_config=aws_config,
            entry_context=QueryEntryContext(
                platform="snowflake", platform_instance=None, env="PROD", report=report
            ),
            report=report,
        )

        queries = list(reader.get_query_entries())

        assert [q.query for q in queries] == [
            "SELECT * FROM table1",
            "SELECT * FROM table2",
        ]
        assert mock_open.call_args.kwargs["transport_params"] == {"client": s3_client}

    def test_s3_open_error_reports_failure(self):
        aws_config = Mock(spec=AwsConnectionConfig)
        aws_config.get_s3_client.side_effect = RuntimeError("no credentials")
        report = SqlQueriesSourceReport()
        reader = FileQueryLogReader(
            query_file="s3://test-bucket/test-key",
            aws_config=aws_config,
            entry_context=QueryEntryContext(
                platform="snowflake", platform_instance=None, env="PROD"
            ),
            report=report,
        )

        with pytest.raises(RuntimeError):
            list(reader.get_query_entries())

        assert any(f.title == "Query file open error" for f in report.failures)


class TestTemporaryTableSupport:
    def _source(self, patterns):
        config = SqlQueriesSourceConfig(
            platform="snowflake", query_file="dummy.json", temp_table_patterns=patterns
        )
        # Create a minimal source instance without full initialization
        source = SqlQueriesSource.__new__(SqlQueriesSource)
        source.config = config
        source.report = SqlQueriesSourceReport()
        return source

    def test_is_temp_table_no_patterns(self):
        source = self._source([])

        assert not source.is_temp_table("temp_table")
        assert not source.is_temp_table("regular_table")

    def test_is_temp_table_with_patterns(self):
        source = self._source(["^temp_.*", "^tmp_.*", ".*_temp$"])

        assert source.is_temp_table("temp_table")
        assert source.is_temp_table("tmp_table")
        assert source.is_temp_table("my_temp")
        assert source.is_temp_table("TEMP_TABLE")  # Case insensitive
        assert not source.is_temp_table("regular_table")
        assert not source.is_temp_table("table_temp_other")

    def test_temp_table_detection_counting(self):
        source = self._source(["^temp_.*"])

        source.is_temp_table("temp_table1")
        source.is_temp_table("temp_table2")
        source.is_temp_table("regular_table")

        assert source.report.num_temp_table_matches == 2

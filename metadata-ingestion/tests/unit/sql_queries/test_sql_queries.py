import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries import (
    QueryEntry,
    SqlQueriesSource,
    SqlQueriesSourceConfig,
)
from datahub.ingestion.workunit_processors.auto_incremental_lineage import (
    AutoIncrementalLineageProcessor,
)
from datahub.ingestion.workunit_processors.auto_workunits_reporter import (
    AutoWorkunitsReporterProcessor,
)
from datahub.metadata.urns import CorpUserUrn, DatasetUrn

# ── Shared fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def mock_graph():
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    mock_graph = Mock(spec=DataHubGraph)

    def mock_make_schema_resolver(
        platform, platform_instance, env, include_graph=True
    ):
        return SchemaResolver(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=mock_graph if include_graph else None,
        )

    mock_graph._make_schema_resolver = mock_make_schema_resolver
    return mock_graph


@pytest.fixture
def pipeline_context(mock_graph):
    return PipelineContext(run_id="test", graph=mock_graph)


@pytest.fixture
def query_file_with(tmp_path):
    """Write JSONL lines to a temp file and return its path."""

    def _make(lines):
        path = tmp_path / "queries.jsonl"
        path.write_text("\n".join(lines) + "\n" if lines else "")
        return str(path)

    return _make


def _make_source(pipeline_context, path, **config_overrides):
    """Create a SqlQueriesSource with sensible defaults."""
    config = SqlQueriesSourceConfig(
        query_file=path, platform="snowflake", **config_overrides
    )
    return SqlQueriesSource(pipeline_context, config)


# ── QueryEntry tests ─────────────────────────────────────────────────────


class TestQueryEntry:
    @pytest.mark.parametrize(
        "entry_dict,entry_config,expected_query_entry,should_raise",
        [
            # Timestamp format variations
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="numeric_unix_timestamp",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200.5,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(
                        2021, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc
                    ),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="float_unix_timestamp",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": "1609459200",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="string_unix_timestamp",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": "2021-01-01T00:00:00Z",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="iso_format_with_z",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": "2025-07-19 15:08:07.000000",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2025, 7, 19, 15, 8, 7, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="datetime_string_with_microseconds",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": "2025-07-19 15:08:07",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2025, 7, 19, 15, 8, 7, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="datetime_string_without_microseconds",
            ),
            # Table filtering - empty strings removed
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1", "", "table2"],
                    "downstream_tables": ["output_table"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,output_table,PROD)"
                        )
                    ],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        ),
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table2,PROD)"
                        ),
                    ],
                    session_id=None,
                ),
                False,
                id="filter_empty_upstream_tables",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": [""],
                    "downstream_tables": [""],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[],
                    session_id=None,
                ),
                False,
                id="all_empty_tables",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["  ", "\t"],
                    "downstream_tables": [" "],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[],
                    session_id=None,
                ),
                False,
                id="whitespace_only_tables",
            ),
            # Missing fields
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=None,
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="no_timestamp",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=None,
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="no_user",
            ),
            # Both upstream and downstream tables
            pytest.param(
                {
                    "query": "CREATE TABLE out AS SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                    "downstream_tables": ["output_table"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                QueryEntry(
                    query="CREATE TABLE out AS SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,output_table,PROD)"
                        )
                    ],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:athena,table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="upstream_and_downstream_tables",
            ),
            # Config variations - platform, platform_instance, env
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(
                    platform="snowflake",
                    query_file="dummy.json",
                    platform_instance="prod_instance",
                    env="DEV",
                ),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_instance.table1,DEV)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="config_with_platform_instance_and_env",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(
                    platform="bigquery", query_file="dummy.json", env="PRE"
                ),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:bigquery,table1,PRE)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="config_with_different_env",
            ),
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": 1609459200,
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(
                    platform="postgres",
                    query_file="dummy.json",
                    platform_instance="dev_cluster",
                ),
                QueryEntry(
                    query="SELECT * FROM table",
                    timestamp=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    user=CorpUserUrn("test_user"),
                    downstream_tables=[],
                    upstream_tables=[
                        DatasetUrn.from_string(
                            "urn:li:dataset:(urn:li:dataPlatform:postgres,dev_cluster.table1,PROD)"
                        )
                    ],
                    session_id=None,
                ),
                False,
                id="config_with_platform_instance_only",
            ),
            # Error cases
            pytest.param(
                {
                    "query": "SELECT * FROM table",
                    "timestamp": "invalid-timestamp",
                    "user": "test_user",
                    "upstream_tables": ["table1"],
                },
                SqlQueriesSourceConfig(platform="athena", query_file="dummy.json"),
                None,
                True,
                id="invalid_timestamp_format",
            ),
        ],
    )
    def test_create(self, entry_dict, entry_config, expected_query_entry, should_raise):
        """Test QueryEntry creation with various input formats and edge cases."""

        if should_raise:
            with pytest.raises(ValueError):
                QueryEntry.create(entry_dict, config=entry_config)
            return

        query_entry = QueryEntry.create(entry_dict, config=entry_config)

        assert query_entry.query == expected_query_entry.query
        assert query_entry.timestamp == expected_query_entry.timestamp
        assert query_entry.user == expected_query_entry.user
        assert query_entry.downstream_tables == expected_query_entry.downstream_tables
        assert query_entry.upstream_tables == expected_query_entry.upstream_tables
        assert query_entry.session_id == expected_query_entry.session_id


# ── Config tests ─────────────────────────────────────────────────────────


class TestSqlQueriesSourceConfig:
    """Test configuration parsing for SqlQueriesSourceConfig."""

    def test_incremental_lineage_default(self):
        config_dict = {"query_file": "test.jsonl", "platform": "snowflake"}
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is False

    def test_incremental_lineage_enabled(self):
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": True,
        }
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is True

    def test_incremental_lineage_disabled_explicitly(self):
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": False,
        }
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is False

    def test_enable_lazy_schema_loading_removed_gracefully(self):
        """Removed config field should produce a deprecation warning, not a hard error."""
        import warnings

        from datahub.configuration.common import ConfigurationWarning

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = SqlQueriesSourceConfig.model_validate(
                {
                    "query_file": "test.jsonl",
                    "platform": "snowflake",
                    "enable_lazy_schema_loading": True,
                }
            )
            assert any(issubclass(x.category, ConfigurationWarning) for x in w)
        assert not hasattr(config, "enable_lazy_schema_loading")

    def test_s3_uri_requires_aws_config(self):
        """S3 URIs without aws_config are rejected at config time."""
        with pytest.raises(ValueError, match="aws_config is required"):
            SqlQueriesSourceConfig(
                platform="snowflake", query_file="s3://bucket/file.json"
            )

        with pytest.raises(ValueError, match="aws_config is required"):
            SqlQueriesSourceConfig(
                platform="snowflake",
                query_file="s3://bucket/file.json",
                aws_config=None,
            )

    def test_invalid_temp_table_regex_rejected(self):
        """Invalid regex patterns are rejected at config time."""
        with pytest.raises(ValueError, match="Invalid regex in temp_table_patterns"):
            SqlQueriesSourceConfig(
                platform="snowflake",
                query_file="dummy.json",
                temp_table_patterns=["[invalid("],
            )

    def test_valid_regex_patterns_accepted(self):
        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="dummy.json",
            temp_table_patterns=["^temp_.*", "^tmp_\\d+$", ".*_staging$"],
        )
        assert len(config.temp_table_patterns) == 3


# ── Source tests ──────────────────────────────────────────────────────────


class TestSqlQueriesSource:
    """Test SqlQueriesSource functionality."""

    @pytest.fixture
    def temp_query_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            queries = [
                {
                    "query": "INSERT INTO target_table SELECT * FROM source_table",
                    "timestamp": 1640995200,
                    "user": "test_user",
                    "downstream_tables": ["target_table"],
                    "upstream_tables": ["source_table"],
                },
                {
                    "query": "CREATE TABLE output AS SELECT * FROM input1 JOIN input2",
                    "timestamp": 1641081600,
                    "user": "another_user",
                    "downstream_tables": ["output"],
                    "upstream_tables": ["input1", "input2"],
                },
            ]
            for query in queries:
                f.write(json.dumps(query) + "\n")
            query_file_path = f.name

        yield query_file_path
        os.unlink(query_file_path)

    def test_workunit_generation_structure(self, pipeline_context, temp_query_file):
        config = SqlQueriesSourceConfig(
            query_file=temp_query_file, platform="snowflake", incremental_lineage=True
        )
        source = SqlQueriesSource(pipeline_context, config)
        work_units = list(source.get_workunits_internal())

        assert len(work_units) >= 0
        for work_unit in work_units:
            assert (
                hasattr(work_unit, "metadata")
                or hasattr(work_unit, "aspectName")
                or hasattr(work_unit, "aspect")
            )

    @pytest.mark.parametrize("incremental_lineage", [None, True, False])
    def test_workunit_processors_with_incremental_lineage(
        self, pipeline_context, temp_query_file, incremental_lineage
    ):
        if incremental_lineage is None:
            config = SqlQueriesSourceConfig(
                query_file=temp_query_file, platform="snowflake"
            )
            expected_value = False
        else:
            config = SqlQueriesSourceConfig(
                query_file=temp_query_file,
                platform="snowflake",
                incremental_lineage=incremental_lineage,
            )
            expected_value = incremental_lineage

        source = SqlQueriesSource(pipeline_context, config)
        assert source.config.incremental_lineage == expected_value

        processors = source.get_workunit_processors()
        assert all(proc is not None for proc in processors)

        active_processor_names = set(
            source.get_report().workunit_processor_reports.keys()
        )
        assert AutoWorkunitsReporterProcessor.__name__ in active_processor_names
        if expected_value:
            assert AutoIncrementalLineageProcessor.__name__ in active_processor_names
            assert len(processors) == 2
        else:
            assert (
                AutoIncrementalLineageProcessor.__name__ not in active_processor_names
            )
            assert len(processors) == 1

    def test_backward_compatibility(self, pipeline_context, temp_query_file):
        config_dict = {
            "query_file": temp_query_file,
            "platform": "snowflake",
            "usage": {"bucket_duration": "DAY"},
        }
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        source = SqlQueriesSource(pipeline_context, config)
        assert source.config.incremental_lineage is False
        processors = source.get_workunit_processors()
        assert len(processors) == 1


# ── Error handling tests ─────────────────────────────────────────────────


class TestErrorHandling:
    """Tests for error handling paths."""

    def _query_line(self, idx=0, **overrides):
        entry = {
            "query": f"SELECT {idx} FROM table_{idx}",
            "timestamp": 1640995200 + idx,
            "user": "test_user",
        }
        entry.update(overrides)
        return json.dumps(entry)

    def test_malformed_lines_skipped_and_counted(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with([
            "this is not json",
            self._query_line(0),
            "{bad json too",
            self._query_line(1),
        ])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_processed == 2
        assert source.report.num_entries_failed == 2

    def test_empty_file_reports_failure(self, pipeline_context, query_file_with):
        path = query_file_with([])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_queries_processed_sequential == 0
        assert len(source.report.failures) > 0

    def test_all_lines_malformed_reports_failure(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with(["bad 1", "bad 2", "bad 3"])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_processed == 0
        assert source.report.num_entries_failed == 3
        assert len(source.report.failures) > 0

    def test_systemic_aggregator_failure_reports_failure(
        self, pipeline_context, query_file_with
    ):
        from datahub.configuration.common import GraphError

        path = query_file_with([self._query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=GraphError("Token expired")
        )

        list(source.get_workunits_internal())

        assert len(source.report.failures) > 0
        assert any(f.title == "Systemic error" for f in source.report.failures)

    def test_consecutive_non_graph_failures_trigger_abort(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with([self._query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=RuntimeError("Internal aggregator error")
        )

        list(source.get_workunits_internal())

        assert len(source.report.failures) > 0
        assert source.report.num_queries_aggregator_failures >= 5

    def test_consecutive_failure_counter_resets_on_success(
        self, pipeline_context, query_file_with
    ):
        """After a successful call, the consecutive counter resets — 4 failures
        separated by a success should NOT trigger the threshold (5)."""
        path = query_file_with([self._query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)

        call_count = 0

        def alternate_failure(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 5 == 0:
                return None  # success every 5th call
            raise RuntimeError("transient error")

        source.aggregator.add_observed_query = Mock(side_effect=alternate_failure)

        list(source.get_workunits_internal())

        assert not any(
            f.title == "Too many consecutive failures" for f in source.report.failures
        )

    def test_file_not_found_reports_failure(self, pipeline_context):
        source = _make_source(pipeline_context, "/nonexistent/path/queries.jsonl")

        with pytest.raises(OSError):
            list(source.get_workunits_internal())

        assert len(source.report.failures) > 0
        assert any(f.title == "Local file read error" for f in source.report.failures)

    def test_wrongly_typed_tables_skipped_not_crash(
        self, pipeline_context, query_file_with
    ):
        """upstream_tables: 42 should be skipped as a parse error, not crash."""
        path = query_file_with([
            json.dumps({
                "query": "SELECT 1",
                "timestamp": 1640995200,
                "upstream_tables": 42,
            }),
            self._query_line(1),
        ])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_failed == 1
        assert source.report.num_entries_processed == 1

    def test_explicit_lineage_routing(self, pipeline_context, query_file_with):
        path = query_file_with([
            json.dumps({
                "query": "INSERT INTO target SELECT * FROM source",
                "timestamp": 1640995200,
                "user": "test_user",
                "upstream_tables": ["source"],
                "downstream_tables": ["target"],
            })
        ])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_known_query_lineage = Mock()
        source.aggregator.add_observed_query = Mock()

        list(source.get_workunits_internal())

        source.aggregator.add_known_query_lineage.assert_called_once()
        call_args = source.aggregator.add_known_query_lineage.call_args[0][0]
        assert call_args.query_text == "INSERT INTO target SELECT * FROM source"
        assert "source" in call_args.upstreams[0]
        assert "target" in call_args.downstream
        source.aggregator.add_observed_query.assert_not_called()

    def test_observed_query_routing(self, pipeline_context, query_file_with):
        path = query_file_with([
            json.dumps({
                "query": "SELECT * FROM some_table",
                "timestamp": 1640995200,
                "user": "test_user",
            })
        ])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock()
        source.aggregator.add_known_query_lineage = Mock()

        list(source.get_workunits_internal())

        source.aggregator.add_observed_query.assert_called_once()
        call_args = source.aggregator.add_observed_query.call_args[0][0]
        assert call_args.query == "SELECT * FROM some_table"
        assert str(call_args.user) == "urn:li:corpuser:test_user"
        source.aggregator.add_known_query_lineage.assert_not_called()

    def test_non_str_table_entries_logged(self, caplog):
        entry_dict = {
            "query": "SELECT * FROM t",
            "timestamp": 1640995200,
            "user": "test_user",
            "upstream_tables": ["valid_table", 42, None, {"name": "bad"}],
        }
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")

        with caplog.at_level(logging.WARNING):
            entry = QueryEntry.create(entry_dict, config=config)

        assert len(entry.upstream_tables) == 1
        assert entry.upstream_tables[0] == DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,valid_table,PROD)"
        )
        assert sum(1 for r in caplog.records if "invalid table entry" in r.message.lower()) == 3

    def test_all_aggregator_failures_under_threshold_reports_failure(
        self, pipeline_context, query_file_with
    ):
        """3 entries, all fail aggregation (< threshold of 5) — pipeline should
        still report failure because num_queries_processed_sequential == 0."""
        path = query_file_with([self._query_line(i) for i in range(3)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=RuntimeError("Error")
        )

        list(source.get_workunits_internal())

        assert source.report.num_queries_processed_sequential == 0
        assert len(source.report.failures) > 0

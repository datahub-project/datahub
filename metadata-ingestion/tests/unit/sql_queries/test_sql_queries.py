import json
import logging
import warnings
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from datahub.configuration.common import ConfigurationWarning, GraphError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries import (
    MIN_SAMPLE_FOR_FAILURE_RATIO,
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
from datahub.sql_parsing.schema_resolver import SchemaResolver

# ── Shared fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def mock_graph():
    mock_graph = Mock(spec=DataHubGraph)

    def mock_make_schema_resolver(platform, platform_instance, env, include_graph=True):
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


def _query_line(idx=0, **overrides):
    entry = {
        "query": f"SELECT {idx} FROM table_{idx}",
        "timestamp": 1640995200 + idx,
        "user": "test_user",
    }
    entry.update(overrides)
    return json.dumps(entry)


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
    def test_incremental_lineage_default(self):
        config = SqlQueriesSourceConfig.model_validate(
            {"query_file": "test.jsonl", "platform": "snowflake"}
        )
        assert config.incremental_lineage is False

    def test_incremental_lineage_enabled(self):
        config = SqlQueriesSourceConfig.model_validate(
            {
                "query_file": "test.jsonl",
                "platform": "snowflake",
                "incremental_lineage": True,
            }
        )
        assert config.incremental_lineage is True

    def test_enable_lazy_schema_loading_removed_gracefully(self):
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
        with pytest.raises(ValueError, match="aws_config is required"):
            SqlQueriesSourceConfig(
                platform="snowflake", query_file="s3://bucket/file.json"
            )

    def test_invalid_temp_table_regex_rejected(self):
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

    def test_configurable_threshold(self):
        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="dummy.json",
            max_consecutive_aggregator_failures=10,
        )
        assert config.max_consecutive_aggregator_failures == 10

    def test_threshold_disabled(self):
        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="dummy.json",
            max_consecutive_aggregator_failures=0,
        )
        assert config.max_consecutive_aggregator_failures == 0

    def test_negative_threshold_rejected(self):
        with pytest.raises(ValueError):
            SqlQueriesSourceConfig(
                platform="snowflake",
                query_file="dummy.json",
                max_consecutive_aggregator_failures=-1,
            )


# ── Source tests ──────────────────────────────────────────────────────────


class TestSqlQueriesSource:
    @pytest.fixture
    def temp_query_file(self, tmp_path):
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
        path = tmp_path / "queries.jsonl"
        path.write_text("\n".join(json.dumps(q) for q in queries) + "\n")
        return str(path)

    def test_workunit_generation_structure(self, pipeline_context, temp_query_file):
        config = SqlQueriesSourceConfig(
            query_file=temp_query_file, platform="snowflake", incremental_lineage=True
        )
        source = SqlQueriesSource(pipeline_context, config)
        work_units = list(source.get_workunits_internal())

        # The fixture holds valid lineage queries, so dropped output must fail.
        assert work_units
        assert all(isinstance(wu, MetadataWorkUnit) for wu in work_units)
        aspects = {
            wu.metadata.aspectName
            for wu in work_units
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.aspectName
        }
        assert "queryProperties" in aspects
        assert "querySubjects" in aspects

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
        active = set(source.get_report().workunit_processor_reports.keys())
        assert AutoWorkunitsReporterProcessor.__name__ in active
        if expected_value:
            assert AutoIncrementalLineageProcessor.__name__ in active
            assert len(processors) == 2
        else:
            assert AutoIncrementalLineageProcessor.__name__ not in active
            assert len(processors) == 1

    def test_backward_compatibility(self, pipeline_context, temp_query_file):
        config = SqlQueriesSourceConfig.model_validate(
            {
                "query_file": temp_query_file,
                "platform": "snowflake",
                "usage": {"bucket_duration": "DAY"},
            }
        )
        source = SqlQueriesSource(pipeline_context, config)
        assert source.config.incremental_lineage is False
        assert len(source.get_workunit_processors()) == 1


# ── Error handling tests ─────────────────────────────────────────────────


class TestErrorHandling:
    def test_malformed_lines_skipped_and_counted(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with(
            [
                "this is not json",
                _query_line(0),
                "{bad json too",
                _query_line(1),
            ]
        )
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_processed == 2
        assert source.report.num_entries_failed == 2

    def test_empty_file_warns_not_fails(self, pipeline_context, query_file_with):
        """An empty file is a warning (normal for scheduled exports), not a failure."""
        path = query_file_with([])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert len(source.report.failures) == 0
        assert any(w.title == "Empty input" for w in source.report.warnings)

    def test_all_lines_malformed_reports_failure(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with(["bad 1", "bad 2", "bad 3"])
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_processed == 0
        assert source.report.num_entries_failed == 3
        assert any(
            f.title == "All entries failed to parse" for f in source.report.failures
        )

    def test_consecutive_failures_trigger_abort(
        self, pipeline_context, query_file_with
    ):
        path = query_file_with([_query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=RuntimeError("Internal aggregator error")
        )

        list(source.get_workunits_internal())

        assert len(source.report.failures) > 0
        assert source.report.num_queries_aggregator_failures >= 5

    def test_consecutive_failure_threshold_disabled(
        self, pipeline_context, query_file_with
    ):
        """With threshold=0, consecutive failures never trigger the abort."""
        path = query_file_with([_query_line(i) for i in range(10)])
        source = _make_source(
            pipeline_context, path, max_consecutive_aggregator_failures=0
        )
        source.aggregator.add_observed_query = Mock(side_effect=RuntimeError("Error"))

        list(source.get_workunits_internal())

        assert not any(
            f.title == "Too many consecutive failures" for f in source.report.failures
        )
        assert source.report.num_queries_aggregator_failures == 10

    def test_consecutive_failure_counter_resets_on_success(
        self, pipeline_context, query_file_with
    ):
        """After a successful call, the consecutive counter resets — 4 failures
        separated by a success should NOT trigger the threshold (5)."""
        path = query_file_with([_query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)

        call_count = 0

        def alternate_failure(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 5 == 0:
                return None
            raise RuntimeError("transient error")

        source.aggregator.add_observed_query = Mock(side_effect=alternate_failure)

        list(source.get_workunits_internal())

        assert not any(
            f.title == "Too many consecutive failures" for f in source.report.failures
        )

    def test_systemic_error_aborts_immediately(self, pipeline_context, query_file_with):
        """If a systemic error propagates through add_observed_query, abort immediately."""
        path = query_file_with([_query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=GraphError("Token expired")
        )

        work_units = list(source.get_workunits_internal())

        assert any(f.title == "Systemic error" for f in source.report.failures)
        assert source.report.num_queries_processed == 0
        # Aborting must emit nothing: with incremental_lineage off, partial
        # output would overwrite good lineage with worse.
        assert work_units == []

    def test_schema_resolver_auth_failure_detected(
        self, pipeline_context, query_file_with
    ):
        """Expired token causes schema resolver graph fetch errors, detected post-loop.

        Simulates the real production path: graph.get_entities raises HTTPError,
        schema_resolver catches it, caches URNs as None, and increments
        num_graph_fetch_errors. Queries parse "successfully" but without schemas.
        """
        path = query_file_with([_query_line(i) for i in range(5)])
        source = _make_source(pipeline_context, path)

        pipeline_context.graph.get_entities = Mock(
            side_effect=HTTPError("401 Unauthorized")
        )

        list(source.get_workunits_internal())

        assert source.report.schema_resolver_report.num_graph_fetch_errors > 0
        assert any(
            f.title == "Schema resolution failed" for f in source.report.failures
        )

    def test_schema_resolver_partial_failure_warns(
        self, pipeline_context, query_file_with
    ):
        """A token that expires mid-run must not produce a clean report.

        The ratio stays under the failure threshold, so this is a warning rather
        than a failure — but lineage for the affected tables is missing and the
        operator has to be told.
        """
        path = query_file_with([_query_line(i) for i in range(20)])
        source = _make_source(pipeline_context, path)

        calls = {"n": 0}

        def fail_near_the_end(*args, **kwargs):
            calls["n"] += 1
            # Keep the error ratio under failure_ratio_threshold so this covers
            # the warning branch rather than the failure branch.
            if calls["n"] > 15:
                raise HTTPError("401 Unauthorized")
            return {}

        pipeline_context.graph.get_entities = Mock(side_effect=fail_near_the_end)

        work_units = list(source.get_workunits_internal())

        resolver_report = source.report.schema_resolver_report
        assert resolver_report.num_graph_fetch_errors > 0
        assert resolver_report.num_graph_fetch_success > 0
        assert any(
            w.title == "Some schema lookups failed" for w in source.report.warnings
        )
        assert not source.report.failures
        # Below the threshold the run is still usable, so it must still emit.
        assert work_units

    def test_schema_lookups_finding_nothing_is_not_a_failure(
        self, pipeline_context, query_file_with
    ):
        """Tables absent from DataHub is the normal first-run case, not an error.

        Guards against a regression where the denominator counts cache misses:
        a fetch that succeeds but finds nothing must never be read as a failure.
        It does warrant a distinct warning, since it usually means the recipe's
        platform/platform_instance/env don't match how the tables were ingested.
        """
        path = query_file_with([_query_line(i) for i in range(20)])
        source = _make_source(pipeline_context, path)

        pipeline_context.graph.get_entities = Mock(return_value={})

        work_units = list(source.get_workunits_internal())

        assert source.report.schema_resolver_report.num_graph_fetch_errors == 0
        assert not source.report.failures
        assert any(w.title == "No schemas resolved" for w in source.report.warnings)
        assert work_units

    def test_single_parse_failure_below_min_sample_is_not_a_failure(
        self, pipeline_context, query_file_with
    ):
        """One unparseable query in a small file is a warning, not a systemic failure."""
        path = query_file_with(
            [
                _query_line(0),
                json.dumps({"query": "}{ not sql", "timestamp": 1640995200}),
            ]
        )
        source = _make_source(pipeline_context, path)

        list(source.get_workunits_internal())

        assert not any(
            f.title == "Most queries failed SQL parsing" for f in source.report.failures
        )

    def test_file_not_found_reports_failure(self, pipeline_context):
        source = _make_source(pipeline_context, "/nonexistent/path/queries.jsonl")

        with pytest.raises(OSError):
            list(source.get_workunits_internal())

        assert any(f.title == "Local file read error" for f in source.report.failures)

    def test_wrongly_typed_tables_skipped_not_crash(
        self, pipeline_context, query_file_with
    ):
        """upstream_tables: 42 should be skipped as a parse error, not crash."""
        path = query_file_with(
            [
                json.dumps(
                    {
                        "query": "SELECT 1",
                        "timestamp": 1640995200,
                        "upstream_tables": 42,
                    }
                ),
                _query_line(1),
            ]
        )
        source = _make_source(pipeline_context, path)
        list(source.get_workunits_internal())

        assert source.report.num_entries_failed == 1
        assert source.report.num_entries_processed == 1

    def test_explicit_lineage_routing(self, pipeline_context, query_file_with):
        path = query_file_with(
            [
                json.dumps(
                    {
                        "query": "INSERT INTO target SELECT * FROM source",
                        "timestamp": 1640995200,
                        "user": "test_user",
                        "upstream_tables": ["source"],
                        "downstream_tables": ["target"],
                    }
                )
            ]
        )
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
        path = query_file_with(
            [
                json.dumps(
                    {
                        "query": "SELECT * FROM some_table",
                        "timestamp": 1640995200,
                        "user": "test_user",
                    }
                )
            ]
        )
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
        assert (
            sum(1 for r in caplog.records if "invalid table entry" in r.message.lower())
            == 3
        )

    def test_unparseable_queries_report_failure(
        self, pipeline_context, query_file_with
    ):
        """Enough genuinely unparseable SQL trips the parse-failure ratio.

        Uses real garbage SQL rather than hand-incrementing the aggregator's
        counter, so this also covers the aggregator's own failure accounting.
        """
        garbage = [
            "!!! this is not sql at all %%%",
            "SELECT FROM FROM WHERE",
            "}{ garbage",
        ]
        path = query_file_with(
            [
                json.dumps(
                    {
                        "query": garbage[i % len(garbage)],
                        "timestamp": 1640995200 + i,
                        "user": "test_user",
                    }
                )
                for i in range(MIN_SAMPLE_FOR_FAILURE_RATIO + 5)
            ]
        )
        source = _make_source(pipeline_context, path)

        list(source.get_workunits_internal())

        assert source.aggregator.report.num_observed_queries_failed > 0
        assert any(
            f.title == "Most queries failed SQL parsing" for f in source.report.failures
        )

    def test_all_queries_throw_under_threshold(self, pipeline_context, query_file_with):
        """3 entries, all throw exceptions from add_observed_query (< threshold of 5).
        Pipeline should still report failure because num_queries_processed == 0."""
        path = query_file_with([_query_line(i) for i in range(3)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=RuntimeError("some error")
        )

        list(source.get_workunits_internal())

        assert any(
            f.title == "All queries failed aggregation" for f in source.report.failures
        )
        assert source.report.num_queries_processed == 0
        assert source.report.num_queries_aggregator_failures == 3

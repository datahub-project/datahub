import json
import warnings
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from datahub.configuration.common import ConfigurationWarning, GraphError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries import (
    QueryEntry,
    SqlQueriesSource,
    SqlQueriesSourceConfig,
    SqlQueriesSourceReport,
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


def _lineage_line(idx=0):
    """A row that reliably produces work units.

    Explicit upstream/downstream tables bypass SQL parsing, so unlike
    _query_line these still emit under the mocked graph. Abort tests need that:
    asserting no output only proves anything if output was otherwise expected.
    """
    return json.dumps(
        {
            "query": f"INSERT INTO out_{idx} SELECT * FROM in_{idx}",
            "timestamp": 1640995200 + idx,
            "user": "test_user",
            "upstream_tables": [f"db.schema.in_{idx}"],
            "downstream_tables": [f"db.schema.out_{idx}"],
        }
    )


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
                QueryEntry.create(
                    entry_dict, config=entry_config, report=SqlQueriesSourceReport()
                )
            return

        query_entry = QueryEntry.create(
            entry_dict, config=entry_config, report=SqlQueriesSourceReport()
        )

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
        aspects = set()
        for wu in work_units:
            if not isinstance(wu, MetadataWorkUnit):
                continue
            mcp = wu.metadata
            if isinstance(mcp, MetadataChangeProposalWrapper) and mcp.aspectName:
                aspects.add(mcp.aspectName)
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

    def test_every_query_failing_reports_and_emits_nothing(
        self, pipeline_context, query_file_with
    ):
        """A dead graph surfaces per-query, then the zero-success gate stops emission.

        There is no special-cased connectivity abort — like every other source,
        errors are reported and the run continues. What must not happen is
        emitting afterwards: with incremental_lineage off, partial output
        replaces complete lineage in DataHub.
        """
        path = query_file_with([_query_line(i) for i in range(10)])
        source = _make_source(pipeline_context, path)
        source.aggregator.add_observed_query = Mock(
            side_effect=GraphError("Token expired")
        )

        work_units = list(source.get_workunits_internal())

        assert source.report.num_queries_aggregator_failures == 10
        assert source.report.num_queries_processed == 0
        assert any(
            f.title == "All queries failed aggregation" for f in source.report.failures
        )
        assert work_units == []

    @pytest.mark.parametrize(
        "bad_field",
        [
            pytest.param({"user": "urn:li:dataset:(a,b,c)"}, id="wrong_entity_urn"),
            pytest.param({"timestamp": 99999999999999999999}, id="timestamp_overflow"),
            pytest.param({"timestamp": 1e30}, id="timestamp_float_overflow"),
            pytest.param({"upstream_tables": 42}, id="tables_not_a_list"),
            pytest.param({"upstream_tables": "my_table"}, id="tables_bare_string"),
        ],
    )
    def test_one_bad_row_never_aborts_the_run(
        self, pipeline_context, query_file_with, bad_field
    ):
        """A single malformed row is counted and skipped, never fatal.

        These are the shapes that motivated the broad catch in _parse_lines:
        InvalidUrnError and OSError/OverflowError are NOT ValueError, so a
        narrowed catch lets them escape and kill the whole run.
        """
        good = [_lineage_line(i) for i in range(4)]
        bad = json.dumps({**json.loads(_lineage_line(99)), **bad_field})
        source = _make_source(pipeline_context, query_file_with(good + [bad]))

        work_units = list(source.get_workunits_internal())

        assert source.report.num_entries_processed == 4
        assert not source.report.failures
        assert work_units

    def test_empty_user_is_treated_as_no_actor(self, pipeline_context, query_file_with):
        """Exported logs use "" for system queries; that must not fail the row."""
        source = _make_source(
            pipeline_context,
            query_file_with(
                [
                    json.dumps({**json.loads(_lineage_line(i)), "user": ""})
                    for i in range(4)
                ]
            ),
        )

        work_units = list(source.get_workunits_internal())

        assert source.report.num_entries_failed == 0
        assert source.report.num_entries_processed == 4
        assert work_units

    def test_bare_string_tables_do_not_become_per_character_urns(
        self, pipeline_context, query_file_with
    ):
        """A str is iterable — without a guard each character becomes a URN."""
        entry = {**json.loads(_lineage_line(0)), "upstream_tables": "my_table"}
        source = _make_source(pipeline_context, query_file_with([json.dumps(entry)]))

        list(source.get_workunits_internal())

        assert source.report.num_entries_failed == 1
        assert source.report.num_entries_processed == 0

    def test_mid_read_error_reports_and_propagates(self, pipeline_context):
        """_guarded_stream converts a mid-transfer read error into a failure."""
        source = _make_source(pipeline_context, "dummy.jsonl")

        def failing_stream():
            yield _lineage_line(0) + "\n"
            raise OSError("connection reset")

        with pytest.raises(OSError):
            list(source._guarded_stream(failing_stream()))

        assert any(f.title == "Query file read error" for f in source.report.failures)

    def test_guarded_stream_ignores_consumer_errors(self, pipeline_context):
        """The whole point of the helper: yield sits outside the try.

        An exception thrown in by the consumer belongs to the consumer, and must
        not be reported as a read failure.
        """
        source = _make_source(pipeline_context, "dummy.jsonl")
        gen = source._guarded_stream(iter(["a\n", "b\n"]))
        next(gen)

        with pytest.raises(ValueError):
            gen.throw(ValueError("consumer blew up"))

        assert not source.report.failures

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

    def test_non_str_table_entries_reported(self):
        """Malformed table references are dropped, counted, and surfaced."""
        entry_dict = {
            "query": "SELECT * FROM t",
            "timestamp": 1640995200,
            "user": "test_user",
            "upstream_tables": ["valid_table", 42, None, {"name": "bad"}],
        }
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        report = SqlQueriesSourceReport()

        entry = QueryEntry.create(entry_dict, config=config, report=report)

        assert len(entry.upstream_tables) == 1
        assert entry.upstream_tables[0] == DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,valid_table,PROD)"
        )
        assert report.num_invalid_table_entries == 3
        assert any(w.title == "Invalid table entry" for w in report.warnings)

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

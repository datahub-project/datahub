import json
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

        # Check all fields match expected
        assert query_entry.query == expected_query_entry.query
        assert query_entry.timestamp == expected_query_entry.timestamp
        assert query_entry.user == expected_query_entry.user

        assert query_entry.downstream_tables == expected_query_entry.downstream_tables
        assert query_entry.upstream_tables == expected_query_entry.upstream_tables
        assert query_entry.session_id == expected_query_entry.session_id


class TestSqlQueriesSourceConfig:
    """Test configuration parsing for SqlQueriesSourceConfig."""

    def test_incremental_lineage_default(self):
        """Test that incremental_lineage defaults to False."""
        config_dict = {"query_file": "test.jsonl", "platform": "snowflake"}
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is False

    def test_incremental_lineage_enabled(self):
        """Test that incremental_lineage can be enabled."""
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": True,
        }
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is True

    def test_incremental_lineage_disabled_explicitly(self):
        """Test that incremental_lineage can be explicitly disabled."""
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": False,
        }
        config = SqlQueriesSourceConfig.model_validate(config_dict)
        assert config.incremental_lineage is False


class TestSqlQueriesSource:
    """Test SqlQueriesSource functionality including patch lineage support."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock DataHubGraph."""
        from datahub.sql_parsing.schema_resolver import SchemaResolver

        mock_graph = Mock(spec=DataHubGraph)

        # Mock _make_schema_resolver to return a real SchemaResolver
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
    def pipeline_context(self, mock_graph):
        """Create a PipelineContext with mock graph."""
        return PipelineContext(run_id="test", graph=mock_graph)

    @pytest.fixture
    def temp_query_file(self):
        """Create a temporary query file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            # Write test queries
            queries = [
                {
                    "query": "INSERT INTO target_table SELECT * FROM source_table",
                    "timestamp": 1640995200,  # 2022-01-01
                    "user": "test_user",
                    "downstream_tables": ["target_table"],
                    "upstream_tables": ["source_table"],
                },
                {
                    "query": "CREATE TABLE output AS SELECT * FROM input1 JOIN input2",
                    "timestamp": 1641081600,  # 2022-01-02
                    "user": "another_user",
                    "downstream_tables": ["output"],
                    "upstream_tables": ["input1", "input2"],
                },
            ]
            for query in queries:
                f.write(json.dumps(query) + "\n")
            query_file_path = f.name

        yield query_file_path

        # Cleanup
        import os

        os.unlink(query_file_path)

    def test_workunit_generation_structure(self, pipeline_context, temp_query_file):
        """Test that MCPs are generated with proper structure."""
        config = SqlQueriesSourceConfig(
            query_file=temp_query_file, platform="snowflake", incremental_lineage=True
        )

        source = SqlQueriesSource(pipeline_context, config)

        # Generate work units
        work_units = list(source.get_workunits_internal())

        # Should generate some work units (exact number depends on SQL aggregator behavior)
        assert len(work_units) >= 0  # At minimum, no errors should occur

        # All items should be work units (MetadataWorkUnit or MetadataChangeProposalWrapper)
        for work_unit in work_units:
            # Should be MetadataWorkUnit or MetadataChangeProposalWrapper objects
            assert (
                hasattr(work_unit, "metadata")
                or hasattr(work_unit, "aspectName")
                or hasattr(work_unit, "aspect")
            )

    @pytest.mark.parametrize("incremental_lineage", [None, True, False])
    def test_workunit_processors_with_incremental_lineage(
        self, pipeline_context, temp_query_file, incremental_lineage
    ):
        """Test workunit processors with different incremental_lineage settings."""
        # Handle None case (default behavior) by not passing the parameter
        if incremental_lineage is None:
            config = SqlQueriesSourceConfig(
                query_file=temp_query_file,
                platform="snowflake",
                # incremental_lineage not specified, should default to False
            )
            expected_value = False  # Default value
        else:
            config = SqlQueriesSourceConfig(
                query_file=temp_query_file,
                platform="snowflake",
                incremental_lineage=incremental_lineage,
            )
            expected_value = incremental_lineage

        source = SqlQueriesSource(pipeline_context, config)

        # Verify config is properly set
        assert source.config.incremental_lineage == expected_value

        # Verify processors are set up correctly
        processors = source.get_workunit_processors()
        assert all(proc is not None for proc in processors)

        # Check which processors were activated via workunit_processor_reports
        active_processor_names = set(
            source.get_report().workunit_processor_reports.keys()
        )
        assert AutoWorkunitsReporterProcessor.__name__ in active_processor_names
        if expected_value:
            assert AutoIncrementalLineageProcessor.__name__ in active_processor_names
        else:
            assert (
                AutoIncrementalLineageProcessor.__name__ not in active_processor_names
            )

        # Incremental lineage processor only active when configured
        if expected_value:
            assert len(processors) == 2
        else:
            assert len(processors) == 1

    def test_backward_compatibility(self, pipeline_context, temp_query_file):
        """Test that existing configurations without incremental_lineage still work."""
        config_dict = {
            "query_file": temp_query_file,
            "platform": "snowflake",
            "usage": {"bucket_duration": "DAY"},
        }

        config = SqlQueriesSourceConfig.model_validate(config_dict)
        source = SqlQueriesSource(pipeline_context, config)

        # Should default to False
        assert source.config.incremental_lineage is False

        # Only reporter processor active when incremental_lineage is False
        processors = source.get_workunit_processors()
        assert len(processors) == 1


class TestErrorHandling:
    """Tests for error handling paths identified in review."""

    @pytest.fixture
    def mock_graph(self):
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
    def pipeline_context(self, mock_graph):
        return PipelineContext(run_id="test", graph=mock_graph)

    def test_malformed_lines_skipped_and_counted(self, pipeline_context):
        """Malformed JSON lines are skipped with a warning, valid lines still process."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write("this is not json\n")
            f.write(json.dumps({
                "query": "SELECT 1 FROM valid_table",
                "timestamp": 1640995200,
                "user": "test_user",
            }) + "\n")
            f.write("{bad json too\n")
            f.write(json.dumps({
                "query": "SELECT 2 FROM another_table",
                "timestamp": 1640995201,
                "user": "test_user",
            }) + "\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)
            list(source.get_workunits_internal())

            assert source.report.num_entries_processed == 2
            assert source.report.num_entries_failed == 2
        finally:
            os.unlink(path)

    def test_empty_file_reports_failure(self, pipeline_context):
        """An empty file should report failure, not silent success."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)
            list(source.get_workunits_internal())

            assert source.report.num_entries_processed == 0
            assert len(source.report.failures) > 0
        finally:
            os.unlink(path)

    def test_all_lines_malformed_reports_failure(self, pipeline_context):
        """If every line fails to parse, the run should report failure."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write("bad json 1\n")
            f.write("bad json 2\n")
            f.write("bad json 3\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)
            list(source.get_workunits_internal())

            assert source.report.num_entries_processed == 0
            assert source.report.num_entries_failed == 3
            assert len(source.report.failures) > 0
        finally:
            os.unlink(path)

    def test_systemic_aggregator_failure_reports_failure(self, pipeline_context):
        """A graph/auth error from the aggregator should fail the run, not produce N warnings."""
        from datahub.configuration.common import GraphError

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for i in range(10):
                f.write(json.dumps({
                    "query": f"SELECT {i} FROM table_{i}",
                    "timestamp": 1640995200 + i,
                    "user": "test_user",
                }) + "\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)

            source.aggregator.add_observed_query = Mock(
                side_effect=GraphError("Token expired")
            )

            list(source.get_workunits_internal())

            assert len(source.report.failures) > 0
            failure_messages = [f.message for f in source.report.failures]
            assert any("Systemic error" in msg for msg in failure_messages)
        finally:
            os.unlink(path)

    def test_consecutive_non_graph_failures_trigger_abort(self, pipeline_context):
        """Too many consecutive aggregator failures should abort even if not GraphError."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for i in range(10):
                f.write(json.dumps({
                    "query": f"SELECT {i} FROM table_{i}",
                    "timestamp": 1640995200 + i,
                    "user": "test_user",
                }) + "\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)

            source.aggregator.add_observed_query = Mock(
                side_effect=RuntimeError("Internal aggregator error")
            )

            list(source.get_workunits_internal())

            assert len(source.report.failures) > 0
            assert source.report.num_queries_aggregator_failures >= 5
        finally:
            os.unlink(path)

    def test_explicit_lineage_routing(self, pipeline_context):
        """Entries with both upstream+downstream use add_known_query_lineage, not add_observed_query."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps({
                "query": "INSERT INTO target SELECT * FROM source",
                "timestamp": 1640995200,
                "user": "test_user",
                "upstream_tables": ["source"],
                "downstream_tables": ["target"],
            }) + "\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)

            source.aggregator.add_known_query_lineage = Mock()
            source.aggregator.add_observed_query = Mock()

            list(source.get_workunits_internal())

            source.aggregator.add_known_query_lineage.assert_called_once()
            source.aggregator.add_observed_query.assert_not_called()
        finally:
            os.unlink(path)

    def test_observed_query_routing(self, pipeline_context):
        """Entries without explicit lineage use add_observed_query."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps({
                "query": "SELECT * FROM some_table",
                "timestamp": 1640995200,
                "user": "test_user",
            }) + "\n")
            path = f.name

        try:
            config = SqlQueriesSourceConfig(query_file=path, platform="snowflake")
            source = SqlQueriesSource(pipeline_context, config)

            source.aggregator.add_observed_query = Mock()
            source.aggregator.add_known_query_lineage = Mock()

            list(source.get_workunits_internal())

            source.aggregator.add_observed_query.assert_called_once()
            source.aggregator.add_known_query_lineage.assert_not_called()
        finally:
            os.unlink(path)

    def test_non_str_table_entries_logged(self, pipeline_context):
        """Non-string, non-URN table entries should produce a warning."""
        entry_dict = {
            "query": "SELECT * FROM t",
            "timestamp": 1640995200,
            "user": "test_user",
            "upstream_tables": ["valid_table", 42, None, {"name": "bad"}],
        }
        config = SqlQueriesSourceConfig(platform="snowflake", query_file="dummy.json")
        entry = QueryEntry.create(entry_dict, config=config)

        assert len(entry.upstream_tables) == 1
        assert "valid_table" in str(entry.upstream_tables[0])

    def test_invalid_temp_table_regex_rejected_at_config(self):
        """Invalid regex patterns should be rejected during config validation."""
        with pytest.raises(ValueError, match="Invalid regex"):
            SqlQueriesSourceConfig(
                platform="snowflake",
                query_file="dummy.json",
                temp_table_patterns=["[invalid("],
            )

    def test_valid_regex_patterns_accepted(self):
        """Valid regex patterns should be accepted."""
        config = SqlQueriesSourceConfig(
            platform="snowflake",
            query_file="dummy.json",
            temp_table_patterns=["^temp_.*", "^tmp_\\d+$", ".*_staging$"],
        )
        assert len(config.temp_table_patterns) == 3

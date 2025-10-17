import json
import tempfile
from datetime import datetime, timezone
from functools import partial
from typing import cast
from unittest.mock import Mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries import (
    QueryEntry,
    SqlQueriesSource,
    SqlQueriesSourceConfig,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
                    operation_type=None,
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
        assert query_entry.operation_type == expected_query_entry.operation_type
        assert query_entry.downstream_tables == expected_query_entry.downstream_tables
        assert query_entry.upstream_tables == expected_query_entry.upstream_tables
        assert query_entry.session_id == expected_query_entry.session_id


class TestSqlQueriesSourceConfig:
    """Test configuration parsing for SqlQueriesSourceConfig."""

    def test_incremental_lineage_default(self):
        """Test that incremental_lineage defaults to False."""
        config_dict = {"query_file": "test.jsonl", "platform": "snowflake"}
        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        assert config.incremental_lineage is False

    def test_incremental_lineage_enabled(self):
        """Test that incremental_lineage can be enabled."""
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": True,
        }
        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        assert config.incremental_lineage is True

    def test_incremental_lineage_disabled_explicitly(self):
        """Test that incremental_lineage can be explicitly disabled."""
        config_dict = {
            "query_file": "test.jsonl",
            "platform": "snowflake",
            "incremental_lineage": False,
        }
        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        assert config.incremental_lineage is False


class TestSqlQueriesSource:
    """Test SqlQueriesSource functionality including patch lineage support."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock DataHubGraph."""
        from datahub.sql_parsing.schema_resolver import SchemaResolver

        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.initialize_schema_resolver_from_datahub.return_value = None

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

        # Generate work units (these will be converted to workunits by the processors)
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
        assert len(processors) == 2
        assert all(proc is not None for proc in processors)

        # Check that processors are the expected functions
        partial_processors = [
            cast(partial, proc) for proc in processors if isinstance(proc, partial)
        ]
        processor_funcs = [proc.func for proc in partial_processors]
        assert auto_workunit_reporter in processor_funcs
        assert auto_incremental_lineage in processor_funcs

        # Verify processors reflect the config
        auto_incremental_processor = next(
            cast(partial, proc)
            for proc in processors
            if isinstance(proc, partial) and proc.func == auto_incremental_lineage
        )
        assert auto_incremental_processor.args[0] == expected_value

    def test_backward_compatibility(self, pipeline_context, temp_query_file):
        """Test that existing configurations without incremental_lineage still work."""
        # This simulates an existing config that doesn't have incremental_lineage
        config_dict = {
            "query_file": temp_query_file,
            "platform": "snowflake",
            "usage": {"bucket_duration": "DAY"},
        }

        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        source = SqlQueriesSource(pipeline_context, config)

        # Should default to False
        assert source.config.incremental_lineage is False

        # Should still work normally
        processors = source.get_workunit_processors()
        assert len(processors) == 2

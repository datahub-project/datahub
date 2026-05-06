from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import (
    MetadataChangeProposalWrapper,
    MetadataWorkUnit,
)
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    TeradataTable,
    get_schema_columns,
    get_schema_foreign_keys,
    get_schema_pk_constraints,
    optimized_get_columns,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery


@pytest.fixture(autouse=True)
def isolate_teradata_caches(monkeypatch):
    """Isolate TeradataSource class-level caches for each test.

    Creates fresh cache instances before each test to prevent cross-contamination.
    """
    monkeypatch.setattr(TeradataSource, "_tables_cache", defaultdict(list))
    monkeypatch.setattr(TeradataSource, "_table_creator_cache", {})


def _base_config() -> Dict[str, Any]:
    """Base configuration for Teradata tests."""
    return {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:1025",
        "include_table_lineage": True,
        "include_usage_statistics": True,
        "include_queries": True,
    }


def _create_mock_table_entry(database, table, creator_name=None, **kwargs):
    """Helper to create mock table entries with consistent structure."""
    mock_entry = MagicMock()
    mock_entry.DataBaseName = MagicMock(strip=MagicMock(return_value=database))
    mock_entry.name = MagicMock(strip=MagicMock(return_value=table))
    mock_entry.description = kwargs.get("description")
    mock_entry.object_type = kwargs.get("object_type", "Table")
    mock_entry.CreateTimeStamp = kwargs.get("create_time", datetime(2024, 1, 1))
    mock_entry.LastAlterName = kwargs.get("alter_name")
    mock_entry.LastAlterTimeStamp = kwargs.get("alter_time")
    mock_entry.RequestText = kwargs.get("request_text")
    mock_entry.CreatorName = (
        MagicMock(strip=MagicMock(return_value=creator_name)) if creator_name else None
    )

    return mock_entry


def _create_source(extract_ownership=False):
    """Helper to create TeradataSource with mocked dependencies."""
    config = TeradataConfig.model_validate(
        {
            **_base_config(),
            "extract_ownership": extract_ownership,
        }
    )

    with patch(
        "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
    ) as mock_class:
        mock_class.return_value = MagicMock()
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))


class TestTeradataConfig:
    """Test configuration validation and initialization."""

    def test_valid_config(self):
        """Test that valid configuration is accepted."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)

        assert config.host_port == "localhost:1025"
        assert config.include_table_lineage is True
        assert config.include_usage_statistics is True
        assert config.include_queries is True

    def test_max_workers_validation_valid(self):
        """Test valid max_workers configuration passes validation."""
        config_dict = {
            **_base_config(),
            "max_workers": 8,
        }
        config = TeradataConfig.model_validate(config_dict)
        assert config.max_workers == 8

    def test_max_workers_default(self):
        """Test max_workers defaults to 10."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)
        assert config.max_workers == 10

    def test_max_workers_custom_value(self):
        """Test custom max_workers value is accepted."""
        config_dict = {
            **_base_config(),
            "max_workers": 5,
        }
        config = TeradataConfig.model_validate(config_dict)
        assert config.max_workers == 5

    def test_include_queries_default(self):
        """Test include_queries defaults to True."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)
        assert config.include_queries is True

    def test_time_window_defaults_applied(self):
        """Test that BaseTimeWindowConfig defaults are automatically applied."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        assert config.start_time is not None
        assert config.end_time is not None
        assert isinstance(config.start_time, datetime)
        assert isinstance(config.end_time, datetime)
        assert config.end_time > config.start_time

    def test_incremental_lineage_config_support(self):
        """Test that incremental_lineage configuration parameter is supported."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        assert hasattr(config, "incremental_lineage")
        assert config.incremental_lineage is True

        config_dict_false = {
            **_base_config(),
            "incremental_lineage": False,
        }
        config_false = TeradataConfig.model_validate(config_dict_false)
        assert config_false.incremental_lineage is False

        config_default = TeradataConfig.model_validate(_base_config())
        assert config_default.incremental_lineage is False

    def test_config_inheritance_chain(self):
        """Test that TeradataConfig properly inherits from all required base classes."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "incremental_lineage": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        # Verify inheritance from BaseTimeWindowConfig
        assert hasattr(config, "start_time")
        assert hasattr(config, "end_time")
        assert hasattr(config, "bucket_duration")

        # Verify inheritance from BaseTeradataConfig
        assert hasattr(config, "scheme")

        # Verify inheritance from TwoTierSQLAlchemyConfig
        assert hasattr(config, "host_port")

    def test_user_original_recipe_compatibility(self):
        """Test that a user's original recipe configuration is parsed correctly."""
        user_recipe_config = {
            "host_port": "vmvantage1720:1025",
            "username": "dbc",
            "password": "dbc",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,
            "stateful_ingestion": {"enabled": True, "fail_safe_threshold": 90},
        }

        config = TeradataConfig.model_validate(user_recipe_config)

        assert config.host_port == "vmvantage1720:1025"
        assert config.username == "dbc"
        assert config.include_table_lineage is True
        assert config.include_usage_statistics is True
        assert config.incremental_lineage is True
        assert config.start_time is not None
        assert config.end_time is not None
        assert config.start_time < config.end_time
        assert config.stateful_ingestion is not None
        assert config.stateful_ingestion.enabled is True
        assert config.stateful_ingestion.fail_safe_threshold == 90

    @pytest.mark.parametrize(
        "override, expected",
        [
            ({}, False),
            ({"extract_ownership": True}, True),
            ({"extract_ownership": False}, False),
        ],
    )
    def test_extract_ownership_config(
        self, override: Dict[str, Any], expected: bool
    ) -> None:
        config_dict = {**_base_config(), **override}
        config = TeradataConfig.model_validate(config_dict)
        assert config.extract_ownership is expected


class TestTeradataSource:
    """Test Teradata source functionality."""

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_source_initialization(self, mock_create_engine):
        """Test source initializes correctly."""
        config = TeradataConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test")

        # Mock the engine creation
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, ctx)

            assert source.config == config
            assert source.platform == "teradata"
            assert hasattr(source, "aggregator")
            assert hasattr(source, "_tables_cache")
            assert hasattr(source, "_tables_cache_lock")

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    @patch("datahub.ingestion.source.sql.teradata.inspect")
    def test_get_inspectors(self, mock_inspect, mock_create_engine):
        """Test inspector creation and database iteration."""
        # Mock database names returned by inspector
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["db1", "db2", "test_db"]
        mock_inspect.return_value = mock_inspector

        mock_connection = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                inspectors = list(source.get_inspectors())

                assert len(inspectors) == 3
                # Check that each inspector has the database name set
                for inspector in inspectors:
                    assert hasattr(inspector, "_datahub_database")

    def test_cache_tables_and_views_thread_safety(self):
        """Test that cache operations are thread-safe."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Use helper function for consistent mocking
            mock_entry = _create_mock_table_entry(
                "test_db", "test_table", description="Test table"
            )

            with patch.object(source, "get_metadata_engine") as mock_get_engine:
                mock_engine = MagicMock()
                mock_engine.execute.return_value = [mock_entry]
                mock_get_engine.return_value = mock_engine

                # Call the method after patching the engine
                source.cache_tables_and_views()

                # Verify table was added to cache
                assert "test_db" in source._tables_cache
                assert len(source._tables_cache["test_db"]) == 1
                assert source._tables_cache["test_db"][0].name == "test_table"

                # Verify engine was disposed
                mock_engine.dispose.assert_called_once()

    def test_convert_entry_to_observed_query(self):
        """Test conversion of database entries to ObservedQuery objects."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock database entry
            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT * FROM table1 (NOT CASESPECIFIC)"
            mock_entry.session_id = "session123"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.user = "test_user"
            mock_entry.default_database = "test_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert isinstance(observed_query, ObservedQuery)
            assert (
                observed_query.query == "SELECT * FROM table1 "
            )  # (NOT CASESPECIFIC) removed
            assert observed_query.session_id == "session123"
            assert observed_query.timestamp == "2024-01-01 10:00:00"
            assert isinstance(observed_query.user, CorpUserUrn)
            assert (
                observed_query.default_db is None
            )  # Fixed for Teradata two-tier architecture
            assert observed_query.default_schema == "test_db"

    def test_convert_entry_to_observed_query_with_none_user(self):
        """Test ObservedQuery conversion handles None user correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT 1"
            mock_entry.session_id = "session123"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.user = None
            mock_entry.default_database = "test_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert observed_query.user is None

    def test_check_historical_table_exists_success(self):
        """Test historical table check when table exists."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock successful query execution
            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                result = source._check_historical_table_exists()

                assert result is True
                mock_engine.dispose.assert_called_once()

    def test_check_historical_table_exists_failure(self):
        """Test historical table check when table doesn't exist."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock failed query execution
            mock_connection = MagicMock()
            mock_connection.execute.side_effect = Exception("Table not found")
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                result = source._check_historical_table_exists()

                assert result is False
                mock_engine.dispose.assert_called_once()

    def test_close_cleanup(self):
        """Test that close() properly cleans up resources."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Pre-populate class-level caches to verify they are cleared on close
            source._tables_cache["db1"] = [
                TeradataTable(
                    database="db1",
                    name="t1",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime(2024, 1, 1),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
            source._table_creator_cache[("db1", "t1")] = "owner"

            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.close"
            ) as mock_super_close:
                source.close()

                mock_aggregator.close.assert_called_once()
                mock_super_close.assert_called_once()

                # Class-level caches must be emptied so memory is released between
                # sequential recipe runs in the same process (OOM fix for #7602).
                assert len(source._tables_cache) == 0
                assert len(source._table_creator_cache) == 0

                # Module-level LRU caches must also be cleared between recipe runs.
                assert get_schema_columns.cache_info().currsize == 0
                assert get_schema_pk_constraints.cache_info().currsize == 0
                assert get_schema_foreign_keys.cache_info().currsize == 0

    def test_make_lineage_queries_with_time_defaults(self):
        """Test that _make_lineage_queries works with automatic time defaults."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=False
            ):
                queries = source._make_lineage_queries()

            assert len(queries) > 0
            query = queries[0]
            assert "TIMESTAMP" in query
            assert "None" not in query

            import re

            timestamp_pattern = r"TIMESTAMP '[^']+'"
            matches = re.findall(timestamp_pattern, query)
            assert len(matches) >= 2


class TestSQLInjectionSafety:
    """Test SQL injection vulnerability fixes."""

    def test_get_schema_columns_parameterized(self):
        """Test that get_schema_columns uses parameterized queries."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Call the function
        get_schema_columns(None, mock_connection, "columnsV", "test_schema")

        # Verify parameterized query was used
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == "test_schema"

    def test_get_schema_pk_constraints_parameterized(self):
        """Test that get_schema_pk_constraints uses parameterized queries."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Call the function
        get_schema_pk_constraints(None, mock_connection, "test_schema")

        # Verify parameterized query was used
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == "test_schema"


class TestMemoryEfficiency:
    """Test memory efficiency improvements."""

    def test_fetch_lineage_entries_chunked_streaming(self):
        """Test that lineage entries are processed in streaming fashion."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Mock the chunked fetching method to return a generator
            def mock_generator():
                for i in range(5):
                    mock_entry = MagicMock()
                    mock_entry.query_text = f"SELECT {i}"
                    mock_entry.session_id = f"session_{i}"
                    mock_entry.timestamp = "2024-01-01 10:00:00"
                    mock_entry.user = "test_user"
                    mock_entry.default_database = "test_db"
                    yield mock_entry

            with patch.object(
                source, "_fetch_lineage_entries_chunked", return_value=mock_generator()
            ):
                mock_aggregator.gen_metadata.return_value = []

                # Process entries
                source._populate_aggregator_from_audit_logs()

                # Verify aggregator.add was called for each entry (streaming)
                assert mock_aggregator.add.call_count == 5


class TestConcurrencySupport:
    """Test thread safety and concurrent operations."""

    def test_tables_cache_thread_safety(self):
        """Test that tables cache operations are thread-safe."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify lock exists
            assert hasattr(source, "_tables_cache_lock")

            # Test safe cache access methods
            result = source._tables_cache.get("nonexistent_schema", [])
            assert result == []

    def test_cached_loop_tables_safe_access(self):
        """Test cached_loop_tables uses safe cache access."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Add test data to cache
            test_table = TeradataTable(
                database="test_db",
                name="test_table",
                description="Test",
                object_type="Table",
                create_timestamp=datetime.now(),
                last_alter_name=None,
                last_alter_timestamp=None,
                request_text=None,
            )
            source._tables_cache["test_schema"] = [test_table]

            # Mock inspector and config
            mock_inspector = MagicMock()
            mock_sql_config = MagicMock()

            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.loop_tables"
            ) as mock_super:
                mock_super.return_value = []

                # This should not raise an exception even with missing schema
                list(
                    source.cached_loop_tables(
                        mock_inspector, "missing_schema", mock_sql_config
                    )
                )


class TestStageTracking:
    """Test stage tracking functionality."""

    def test_stage_tracking_in_cache_operation(self):
        """Test that table caching uses stage tracking."""
        config = TeradataConfig.model_validate(_base_config())

        # Create source without mocking to test the actual stage tracking during init
        with (
            patch("datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"),
            patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ) as mock_cache,
        ):
            TeradataSource(config, PipelineContext(run_id="test"))

            # Verify cache_tables_and_views was called during init (stage tracking happens there)
            mock_cache.assert_called_once()

    def test_stage_tracking_in_aggregator_processing(self):
        """Test that aggregator processing uses stage tracking."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            with patch.object(source.report, "new_stage") as mock_new_stage:
                mock_context_manager = MagicMock()
                mock_new_stage.return_value = mock_context_manager

                with patch.object(
                    source, "_fetch_lineage_entries_chunked", return_value=[]
                ):
                    mock_aggregator.gen_metadata.return_value = []

                    source._populate_aggregator_from_audit_logs()

                    # Should have called new_stage for query processing and metadata generation
                    # The actual implementation uses new_stage for "Fetching queries" and "Generating metadata"
                    assert mock_new_stage.call_count >= 1


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_empty_lineage_entries(self):
        """Test handling of empty lineage entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_fetch_lineage_entries_chunked", return_value=[]
            ):
                mock_aggregator.gen_metadata.return_value = []
                source._populate_aggregator_from_audit_logs()
                # Method doesn't return a value, just populates the aggregator

    def test_malformed_query_entry(self):
        """Test handling of malformed query entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock entry with missing attributes
            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT 1"
            # Simulate missing attributes
            del mock_entry.session_id
            del mock_entry.timestamp
            del mock_entry.user
            del mock_entry.default_database

            # Should handle gracefully using getattr with defaults
            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert observed_query.query == "SELECT 1"
            assert observed_query.session_id is None
            assert observed_query.user is None


class TestLineageQuerySeparation:
    """Test the new separated lineage query functionality (no more UNION)."""

    def test_make_lineage_queries_current_only(self):
        """Test that only current query is returned when historical lineage is disabled."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            queries = source._make_lineage_queries()

            assert len(queries) == 1
            assert '"DBC".QryLogV' in queries[0]
            assert "PDCRDATA.DBQLSqlTbl_Hst" not in queries[0]
            assert "2024-01-01" in queries[0]
            assert "2024-01-02" in queries[0]

    def test_make_lineage_queries_with_historical_available(self):
        """Test that UNION query is returned when historical lineage is enabled and table exists."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1

            # Single UNION query should contain both historical and current data
            union_query = queries[0]
            assert '"DBC".QryLogV' in union_query
            assert '"PDCRINFO".DBQLSqlTbl_Hst' in union_query
            assert "UNION" in union_query
            assert "combined_results" in union_query

            # Should have the time filters
            assert "2024-01-01" in union_query
            assert "2024-01-02" in union_query

    def test_make_lineage_queries_with_historical_unavailable(self):
        """Test that only current query is returned when historical lineage is enabled but table doesn't exist."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=False
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1
            assert '"DBC".QryLogV' in queries[0]
            assert '"PDCRDATA".DBQLSqlTbl_Hst' not in queries[0]

    def test_make_lineage_queries_with_database_filter(self):
        """Test that database filters are correctly applied to UNION query."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "databases": ["test_db1", "test_db2"],
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1

            # UNION query should have case-insensitive database filters for both parts
            union_query = queries[0]
            assert (
                "l.DefaultDatabase (NOT CASESPECIFIC) in ('test_db1' (NOT CASESPECIFIC),'test_db2' (NOT CASESPECIFIC))"
                in union_query
            )
            assert (
                "h.DefaultDatabase (NOT CASESPECIFIC) in ('test_db1' (NOT CASESPECIFIC),'test_db2' (NOT CASESPECIFIC))"
                in union_query
            )

    def test_fetch_lineage_entries_chunked_multiple_queries(self):
        """Test that _fetch_lineage_entries_chunked handles multiple queries correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock the query generation to return 2 queries
            with patch.object(
                source, "_make_lineage_queries", return_value=["query1", "query2"]
            ):
                # Mock database execution for both queries
                mock_result1 = MagicMock()
                mock_result1.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 1")],  # First batch
                    [],  # End of results
                ]

                mock_result2 = MagicMock()
                mock_result2.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 2")],  # First batch
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value.__enter__.return_value = (
                    mock_connection
                )

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source, "_execute_with_cursor_fallback"
                    ) as mock_execute,
                ):
                    mock_execute.side_effect = [mock_result1, mock_result2]

                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have executed both queries
                    assert mock_execute.call_count == 2
                    mock_execute.assert_any_call(mock_connection, "query1")
                    mock_execute.assert_any_call(mock_connection, "query2")

                    # Should return entries from both queries
                    assert len(entries) == 2

    def test_fetch_lineage_entries_chunked_single_query(self):
        """Test that _fetch_lineage_entries_chunked handles single query correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock the query generation to return 1 query
            with patch.object(source, "_make_lineage_queries", return_value=["query1"]):
                mock_result = MagicMock()
                mock_result.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 1")],  # First batch
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value.__enter__.return_value = (
                    mock_connection
                )

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ) as mock_execute,
                ):
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have executed only one query
                    assert mock_execute.call_count == 1
                    mock_execute.assert_called_with(mock_connection, "query1")

                    # Should return entries from the query
                    assert len(entries) == 1

    def test_fetch_lineage_entries_chunked_batch_processing(self):
        """Test that batch processing works correctly with configurable batch size."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(source, "_make_lineage_queries", return_value=["query1"]):
                # Create mock entries
                mock_entries = [MagicMock(query_text=f"SELECT {i}") for i in range(7)]

                mock_result = MagicMock()
                # Simulate batching with batch_size=5 (hardcoded in the method)
                mock_result.fetchmany.side_effect = [
                    mock_entries[:5],  # First batch (5 items)
                    mock_entries[5:],  # Second batch (2 items)
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value.__enter__.return_value = (
                    mock_connection
                )

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ),
                ):
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should return all 7 entries
                    assert len(entries) == 7

                    # Verify fetchmany was called with the right batch size (5000 is hardcoded)
                    calls = mock_result.fetchmany.call_args_list
                    for call in calls:
                        if call[0]:  # If positional args
                            assert call[0][0] == 5000
                        else:  # If keyword args
                            assert call[1].get("size", 5000) == 5000

    def test_end_to_end_separate_queries_integration(self):
        """Test end-to-end integration of separate queries in the aggregator flow."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Mock entries from both current and historical queries
            current_entry = MagicMock()
            current_entry.query_text = "SELECT * FROM current_table"
            current_entry.user = "current_user"
            current_entry.timestamp = "2024-01-01 10:00:00"
            current_entry.default_database = "current_db"

            historical_entry = MagicMock()
            historical_entry.query_text = "SELECT * FROM historical_table"
            historical_entry.user = "historical_user"
            historical_entry.timestamp = "2023-12-01 10:00:00"
            historical_entry.default_database = "historical_db"

            def mock_fetch_generator():
                yield current_entry
                yield historical_entry

            with patch.object(
                source,
                "_fetch_lineage_entries_chunked",
                return_value=mock_fetch_generator(),
            ):
                mock_aggregator.gen_metadata.return_value = []

                # Execute the aggregator flow
                source._populate_aggregator_from_audit_logs()

                # Verify both entries were added to aggregator
                assert mock_aggregator.add.call_count == 2

                # Verify the entries were converted correctly
                added_queries = [
                    call[0][0] for call in mock_aggregator.add.call_args_list
                ]

                assert any(
                    "SELECT * FROM current_table" in query.query
                    for query in added_queries
                )
                assert any(
                    "SELECT * FROM historical_table" in query.query
                    for query in added_queries
                )

    def test_query_logging_and_progress_tracking(self):
        """Test that proper logging occurs when processing multiple queries."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_make_lineage_queries", return_value=["query1", "query2"]
            ):
                mock_result = MagicMock()

                call_counter = {"count": 0}

                def mock_fetchmany_side_effect(batch_size):
                    # Return one batch then empty to simulate end of results
                    call_counter["count"] += 1
                    if call_counter["count"] == 1:
                        return [MagicMock(query_text="SELECT 1")]
                    return []

                mock_result.fetchmany.side_effect = mock_fetchmany_side_effect

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value.__enter__.return_value = (
                    mock_connection
                )

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ),
                    patch(
                        "datahub.ingestion.source.sql.teradata.logger"
                    ) as mock_logger,
                ):
                    list(source._fetch_lineage_entries_chunked())

                    # Verify progress logging for multiple queries
                    info_calls = [
                        call for call in mock_logger.info.call_args_list if call[0]
                    ]

                    # Should log execution of query 1/2 and 2/2
                    assert any("query 1/2" in str(call) for call in info_calls)
                    assert any("query 2/2" in str(call) for call in info_calls)

                    # Should log completion of both queries
                    assert any("Completed query 1" in str(call) for call in info_calls)
                    assert any("Completed query 2" in str(call) for call in info_calls)


class TestQueryConstruction:
    """Test the construction of individual queries."""

    def test_current_query_construction(self):
        """Test that the current query is constructed correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            queries = source._make_lineage_queries()
            current_query = queries[0]

            # Verify current query structure
            assert 'FROM "DBC".QryLogV as l' in current_query
            assert 'JOIN "DBC".QryLogSqlV as s' in current_query
            assert "l.ErrorCode = 0" in current_query
            assert "2024-01-01" in current_query
            assert "2024-01-02" in current_query
            assert 'ORDER BY "timestamp", "query_id", "row_no"' in current_query

    def test_historical_query_construction(self):
        """Test that the UNION query contains historical data correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()
                union_query = queries[0]

                # Verify UNION query contains historical data structure
                assert 'FROM "PDCRINFO".DBQLSqlTbl_Hst as h' in union_query
                assert "h.ErrorCode = 0" in union_query
                assert "h.StartTime AT TIME ZONE 'GMT'" in union_query
                assert "h.DefaultDatabase" in union_query
                assert "2024-01-01" in union_query
                assert "2024-01-02" in union_query
                assert 'ORDER BY "timestamp", "query_id", "row_no"' in union_query
                assert "UNION" in union_query


class TestStreamingQueryReconstruction:
    """Test the streaming query reconstruction functionality."""

    def test_reconstruct_queries_streaming_single_row_queries(self):
        """Test streaming reconstruction with single-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries for single-row queries
            entries = [
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q2", "SELECT * FROM table2", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q3", "SELECT * FROM table3", 1, "2024-01-01 10:02:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 3
            assert reconstructed_queries[0].query == "SELECT * FROM table1"
            assert reconstructed_queries[1].query == "SELECT * FROM table2"
            assert reconstructed_queries[2].query == "SELECT * FROM table3"

            # Verify metadata preservation
            assert reconstructed_queries[0].timestamp == "2024-01-01 10:00:00"
            assert reconstructed_queries[1].timestamp == "2024-01-01 10:01:00"
            assert reconstructed_queries[2].timestamp == "2024-01-01 10:02:00"

    def test_reconstruct_queries_streaming_multi_row_queries(self):
        """Test streaming reconstruction with multi-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries for multi-row queries
            entries = [
                # Query 1: 3 rows
                self._create_mock_entry(
                    "Q1", "SELECT a, b, c ", 1, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q1", "FROM large_table ", 2, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q1", "WHERE id > 1000", 3, "2024-01-01 10:00:00"
                ),
                # Query 2: 2 rows
                self._create_mock_entry(
                    "Q2", "UPDATE table3 SET ", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "status = 'active'", 2, "2024-01-01 10:01:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 2
            assert (
                reconstructed_queries[0].query
                == "SELECT a, b, c FROM large_table WHERE id > 1000"
            )
            assert (
                reconstructed_queries[1].query == "UPDATE table3 SET status = 'active'"
            )

            # Verify metadata preservation (should use metadata from first row of each query)
            assert reconstructed_queries[0].timestamp == "2024-01-01 10:00:00"
            assert reconstructed_queries[1].timestamp == "2024-01-01 10:01:00"

    def test_reconstruct_queries_streaming_mixed_queries(self):
        """Test streaming reconstruction with mixed single and multi-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries mixing single and multi-row queries
            entries = [
                # Single-row query
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00"
                ),
                # Multi-row query (3 rows)
                self._create_mock_entry(
                    "Q2", "SELECT a, b, c ", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "FROM large_table ", 2, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "WHERE id > 1000", 3, "2024-01-01 10:01:00"
                ),
                # Single-row query
                self._create_mock_entry(
                    "Q3", "SELECT COUNT(*) FROM table2", 1, "2024-01-01 10:02:00"
                ),
                # Multi-row query (2 rows)
                self._create_mock_entry(
                    "Q4", "UPDATE table3 SET ", 1, "2024-01-01 10:03:00"
                ),
                self._create_mock_entry(
                    "Q4", "status = 'active'", 2, "2024-01-01 10:03:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 4
            assert reconstructed_queries[0].query == "SELECT * FROM table1"
            assert (
                reconstructed_queries[1].query
                == "SELECT a, b, c FROM large_table WHERE id > 1000"
            )
            assert reconstructed_queries[2].query == "SELECT COUNT(*) FROM table2"
            assert (
                reconstructed_queries[3].query == "UPDATE table3 SET status = 'active'"
            )

    def test_reconstruct_queries_streaming_empty_entries(self):
        """Test streaming reconstruction with empty entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test with empty entries
            entries: List[Any] = []
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))
            assert len(reconstructed_queries) == 0

    def test_reconstruct_queries_streaming_teradata_specific_transformations(self):
        """Test that Teradata-specific transformations are applied."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with Teradata-specific syntax
            entries = [
                self._create_mock_entry(
                    "Q1",
                    "SELECT * FROM table1 (NOT CASESPECIFIC)",
                    1,
                    "2024-01-01 10:00:00",
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            # Should remove (NOT CASESPECIFIC)
            assert reconstructed_queries[0].query == "SELECT * FROM table1 "

    def test_reconstruct_queries_streaming_metadata_preservation(self):
        """Test that all metadata fields are preserved correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with all metadata fields
            entries: List[Any] = [
                self._create_mock_entry(
                    "Q1",
                    "SELECT * FROM table1",
                    1,
                    "2024-01-01 10:00:00",
                    user="test_user",
                    default_database="test_db",
                    session_id="session123",
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            query = reconstructed_queries[0]

            # Verify all metadata fields
            assert query.query == "SELECT * FROM table1"
            assert query.timestamp == "2024-01-01 10:00:00"
            assert isinstance(query.user, CorpUserUrn)
            assert str(query.user) == "urn:li:corpuser:test_user"
            assert query.default_db is None  # Fixed for Teradata two-tier architecture
            assert query.default_schema == "test_db"  # Teradata uses database as schema
            assert query.session_id == "session123"

    def test_reconstruct_queries_streaming_with_none_user(self):
        """Test streaming reconstruction handles None user correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with None user
            entries = [
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00", user=None
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            assert reconstructed_queries[0].user is None

    def test_reconstruct_queries_streaming_empty_query_text(self):
        """Test streaming reconstruction handles empty query text correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries with empty query text
            entries = [
                self._create_mock_entry("Q1", "", 1, "2024-01-01 10:00:00"),
                self._create_mock_entry(
                    "Q2", "SELECT * FROM table1", 1, "2024-01-01 10:01:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            # Should only get one query (the non-empty one)
            assert len(reconstructed_queries) == 1
            assert reconstructed_queries[0].query == "SELECT * FROM table1"

    def test_reconstruct_queries_streaming_space_joining_behavior(self):
        """Test that query parts are joined directly without adding spaces."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test case 1: Parts that include their own spacing
            entries1 = [
                self._create_mock_entry("Q1", "SELECT ", 1, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "col1, ", 2, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "col2 ", 3, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "FROM ", 4, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "table1", 5, "2024-01-01 10:00:00"),
            ]

            # Test case 2: Parts that already have trailing/leading spaces
            entries2 = [
                self._create_mock_entry("Q2", "SELECT * ", 1, "2024-01-01 10:01:00"),
                self._create_mock_entry("Q2", "FROM table2 ", 2, "2024-01-01 10:01:00"),
                self._create_mock_entry("Q2", "WHERE id > 1", 3, "2024-01-01 10:01:00"),
            ]

            # Test streaming reconstruction
            all_entries = entries1 + entries2
            reconstructed_queries = list(
                source._reconstruct_queries_streaming(all_entries)
            )

            assert len(reconstructed_queries) == 2

            # Query 1: Should be joined directly without adding spaces
            assert reconstructed_queries[0].query == "SELECT col1, col2 FROM table1"

            # Query 2: Should handle existing spaces correctly (may have extra spaces)
            assert reconstructed_queries[1].query == "SELECT * FROM table2 WHERE id > 1"

    def _create_mock_entry(
        self,
        query_id,
        query_text,
        row_no,
        timestamp,
        user="test_user",
        default_database="test_db",
        session_id=None,
    ):
        """Create a mock database entry for testing."""
        entry = MagicMock()
        entry.query_id = query_id
        entry.query_text = query_text
        entry.row_no = row_no
        entry.timestamp = timestamp
        entry.user = user
        entry.default_database = default_database
        entry.session_id = session_id or f"session_{query_id}"
        return entry


class TestOwnershipExtraction:
    """Test ownership extraction functionality."""

    def test_table_creator_cache_population(self):
        """Test that _table_creator_cache is populated during cache_tables_and_views."""
        source = _create_source()
        mock_entry = _create_mock_table_entry(
            "test_db",
            "test_table",
            creator_name="creator_user",
            description="test comment",
            alter_name="test_user",
            alter_time=datetime(2024, 1, 2),
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_engine = MagicMock()
            mock_engine.execute.return_value = [mock_entry]
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            assert ("test_db", "test_table") in source._table_creator_cache
            assert (
                source._table_creator_cache[("test_db", "test_table")] == "creator_user"
            )

    def test_table_creator_cache_handles_none_creator(self):
        """Test that _table_creator_cache handles None creator gracefully."""
        source = _create_source()
        mock_entry = _create_mock_table_entry(
            "test_db2", "test_table2", creator_name=None
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_engine = MagicMock()
            mock_engine.execute.return_value = [mock_entry]
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            assert ("test_db2", "test_table2") not in source._table_creator_cache

    def test_get_creator_for_entity_returns_creator(self):
        """Test that _get_creator_for_entity returns creator when available."""
        source = _create_source()
        source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        result = source._get_creator_for_entity("test_db", "test_table")

        assert result == "creator_user"

    def test_get_creator_for_entity_returns_none_when_not_found(self):
        """Test that _get_creator_for_entity returns None when entity not in cache."""
        source = _create_source()

        result = source._get_creator_for_entity("test_db", "nonexistent_table")

        assert result is None

    @pytest.mark.parametrize(
        "extract_ownership,has_creator,expected_work_units",
        [
            (True, True, True),  # Ownership enabled + creator available = emit
            (True, False, False),  # Ownership enabled + no creator = no emit
            (False, True, False),  # Ownership disabled + creator available = no emit
        ],
    )
    def test_emit_ownership_scenarios(
        self, extract_ownership, has_creator, expected_work_units
    ):
        """Test various scenarios for _emit_ownership_if_available."""
        source = _create_source(extract_ownership=extract_ownership)

        if has_creator:
            source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )

        if expected_work_units:
            assert len(work_units) > 0
        else:
            assert len(work_units) == 0

    @pytest.mark.parametrize(
        "entity_type,parent_method",
        [
            (
                "table",
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource._process_table",
            ),
            (
                "view",
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource._process_view",
            ),
        ],
    )
    def test_process_entity_emits_ownership_work_units(
        self, entity_type, parent_method
    ):
        """Test that _process_table and _process_view emit ownership work units when configured."""
        source = _create_source(extract_ownership=True)
        entity_name = f"test_{entity_type}"
        source._table_creator_cache[("test_db", entity_name)] = "creator_user"

        mock_inspector = MagicMock()
        mock_sql_config = MagicMock()

        with patch(parent_method, return_value=iter([])):
            process_method = getattr(source, f"_process_{entity_type}")
            work_units = list(
                process_method(
                    f"test_db.{entity_name}",
                    mock_inspector,
                    "test_db",
                    entity_name,
                    mock_sql_config,
                )
            )

            assert len(work_units) == 1

            wu = work_units[0]
            assert isinstance(wu, MetadataWorkUnit)
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            assert wu.metadata.aspectName == "ownership"

    def test_emit_ownership_early_return(self):
        """Test that _emit_ownership_if_available returns early when extract_ownership is False."""
        source = _create_source(extract_ownership=False)
        # Populate cache to ensure early return happens before creator check
        source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )
        assert len(work_units) == 0

    def test_table_creator_cache_population_with_creator(self):
        """Test that cache is populated when CreatorName is not None."""
        source = _create_source()

        # Create entries with and without creator
        entry_with_creator = _create_mock_table_entry(
            "db1", "table_with_creator", creator_name="user1"
        )
        entry_without_creator = _create_mock_table_entry(
            "db1", "table_without_creator", creator_name=None
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_engine = MagicMock()
            mock_engine.execute.return_value = [
                entry_with_creator,
                entry_without_creator,
            ]
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            # Only entry with creator should be in cache
            assert ("db1", "table_with_creator") in source._table_creator_cache
            assert source._table_creator_cache[("db1", "table_with_creator")] == "user1"
            assert ("db1", "table_without_creator") not in source._table_creator_cache

    def test_get_creator_returns_from_cache(self):
        """Test that _get_creator_for_entity returns value from cache when present."""
        source = _create_source()

        # Manually populate cache
        source._table_creator_cache[("schema1", "entity1")] = "owner1"
        source._table_creator_cache[("schema2", "entity2")] = "owner2"

        # Test retrieval
        result1 = source._get_creator_for_entity("schema1", "entity1")
        result2 = source._get_creator_for_entity("schema2", "entity2")
        result3 = source._get_creator_for_entity("schema3", "entity3")

        assert result1 == "owner1"
        assert result2 == "owner2"
        assert result3 is None

    def test_emit_ownership_creates_user_urn(self):
        """Test that _emit_ownership_if_available creates correct user URN."""
        source = _create_source(extract_ownership=True)
        source._table_creator_cache[("test_db", "test_table")] = "test_creator"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )

        assert len(work_units) == 1
        # The work unit should contain ownership with user URN
        # Verify by checking the work unit was created (indicates make_user_urn was called)
        assert isinstance(work_units[0], MetadataWorkUnit)


class TestTeradataGetIdentifier:
    """Test get_identifier lowercasing behaviour.

    Teradata returns object names in UPPERCASE. The schema resolver is populated
    during Phase 1 (schema extraction) using the identifier returned by
    get_identifier(). sqlglot normalises all identifiers to lowercase during
    Phase 2 (view SQL parsing). If the two phases use different cases the schema
    resolver lookup is always a miss and column-level lineage is never generated.

    get_identifier() must apply convert_urns_to_lowercase at the point the
    identifier is constructed so that both phases use the same case.
    """

    def _make_source(self, convert_urns_to_lowercase: bool) -> TeradataSource:
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "convert_urns_to_lowercase": convert_urns_to_lowercase,
            }
        )
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))

    def test_get_identifier_lowercase_when_flag_enabled(self):
        """Identifiers must be lowercased when convert_urns_to_lowercase is True.

        Teradata returns names like C3_T / MRCH_PRFL. With the flag on,
        get_identifier() must return c3_t.mrch_prfl so the schema resolver is
        populated with the same key that sqlglot produces during view SQL parsing.
        """
        source = self._make_source(convert_urns_to_lowercase=True)
        inspector = MagicMock()

        result = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        assert result == "c3_t.mrch_prfl"

    def test_get_identifier_preserves_case_when_flag_disabled(self):
        """Identifiers must not be modified when convert_urns_to_lowercase is False."""
        source = self._make_source(convert_urns_to_lowercase=False)
        inspector = MagicMock()

        result = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        assert result == "C3_T.MRCH_PRFL"

    def test_get_identifier_schema_resolver_key_matches_sqlglot_output(self):
        """The identifier returned must match what sqlglot produces from the same SQL.

        sqlglot always normalises unquoted identifiers to lowercase. The schema
        resolver key must be the same lowercase string so that Phase 2 lookups
        find the schema registered during Phase 1.
        """
        source = self._make_source(convert_urns_to_lowercase=True)
        inspector = MagicMock()

        identifier = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        # Simulate what sqlglot produces from "SELECT ... FROM C3_T.MRCH_PRFL"
        sqlglot_normalized = "c3_t.mrch_prfl"
        assert identifier == sqlglot_normalized


# ---------------------------------------------------------------------------
# Tests for the 6 performance / scalability improvements
# ---------------------------------------------------------------------------


def _create_source_patched(
    extra_config: Optional[Dict[str, Any]] = None,
) -> TeradataSource:
    """Create a TeradataSource with cache_tables_and_views patched out."""
    config = TeradataConfig.model_validate({**_base_config(), **(extra_config or {})})
    with patch(
        "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
    ) as mock_class:
        mock_class.return_value = MagicMock()
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))


class TestNewConfigDefaults:
    """New config fields have correct defaults and accept custom values."""

    def test_column_extraction_watermark_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.column_extraction_watermark is None

    def test_use_dbc_columns_for_views_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.use_dbc_columns_for_views is False

    def test_request_timeout_ms_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.request_timeout_ms == 120000

    def test_custom_timeout_values_accepted(self) -> None:
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "request_timeout_ms": 300000,
                "connect_timeout_ms": 60000,
            }
        )
        assert config.request_timeout_ms == 300000
        assert config.connect_timeout_ms == 60000

    def test_watermark_timezone_aware_is_normalised_to_naive_utc(self) -> None:
        """A tz-aware watermark must be normalised to naive UTC.

        Teradata returns LastAlterTimeStamp as a timezone-naive datetime.
        Comparing aware vs naive raises TypeError at runtime; the validator
        prevents this by stripping tzinfo after converting to UTC.
        """

        aware_watermark = "2024-06-01T12:00:00+05:30"  # IST = 06:30 UTC
        config = TeradataConfig.model_validate(
            {**_base_config(), "column_extraction_watermark": aware_watermark}
        )
        result = config.column_extraction_watermark
        assert result is not None
        assert result.tzinfo is None  # must be naive
        assert result == datetime(2024, 6, 1, 6, 30, 0)  # converted to UTC

    def test_watermark_naive_datetime_is_unchanged(self) -> None:
        """A timezone-naive watermark passes through unchanged."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "column_extraction_watermark": "2024-06-01T00:00:00",
            }
        )
        assert config.column_extraction_watermark == datetime(2024, 6, 1, 0, 0, 0)
        assert config.column_extraction_watermark.tzinfo is None  # type: ignore[union-attr]

    def test_column_extraction_days_back_accepted(self) -> None:
        config = TeradataConfig.model_validate(
            {**_base_config(), "column_extraction_days_back": 3}
        )
        assert config.column_extraction_days_back == 3

    def test_both_watermark_options_raises_validation_error(self) -> None:
        """Setting both watermark options at the same time must raise a validation error."""
        with pytest.raises(Exception, match="mutually exclusive"):
            TeradataConfig.model_validate(
                {
                    **_base_config(),
                    "column_extraction_watermark": "2024-06-01T00:00:00",
                    "column_extraction_days_back": 3,
                }
            )


class TestIncrementalColumnExtraction:
    """#1 — skip column extraction for tables unchanged since the watermark."""

    def test_no_watermark_leaves_extraction_set_none(self) -> None:
        source = _create_source_patched()
        assert source._tables_needing_column_extraction is None

    def test_watermark_classifies_changed_and_unchanged_tables(self) -> None:
        watermark = datetime(2024, 6, 1)
        source = _create_source_patched(
            {"column_extraction_watermark": watermark.isoformat()}
        )

        entries = [
            _create_mock_table_entry(
                "db1", "new_table", alter_time=datetime(2024, 6, 2)
            ),  # after watermark → include
            _create_mock_table_entry(
                "db1", "old_table", alter_time=datetime(2024, 5, 1)
            ),  # before watermark → exclude
            _create_mock_table_entry(
                "db1", "no_ts_table"
            ),  # None timestamp → include (conservative)
        ]

        mock_engine = MagicMock()
        mock_engine.execute.return_value = entries
        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._tables_needing_column_extraction is not None
        assert ("db1", "new_table") in source._tables_needing_column_extraction
        assert ("db1", "old_table") not in source._tables_needing_column_extraction
        assert ("db1", "no_ts_table") in source._tables_needing_column_extraction

    def test_days_back_resolves_to_watermark_at_runtime(self) -> None:
        """column_extraction_days_back uses the Teradata server clock, not the client clock."""

        days_back = 3
        # Simulate the Teradata server returning a timezone-aware timestamp
        # (Teradata drivers often return timezone-aware datetimes).
        # effective watermark = 2024-06-12 12:00:00 (server_now - 3 days, tzinfo stripped)
        td_server_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        source = _create_source_patched({"column_extraction_days_back": days_back})

        # Table altered 1 day before server_now → within the 3-day window → included
        recent_entry = _create_mock_table_entry(
            "db1", "recent_table", alter_time=datetime(2024, 6, 14, 12, 0, 0)
        )
        # Table altered 10 days before server_now → outside the window → excluded
        stale_entry = _create_mock_table_entry(
            "db1", "stale_table", alter_time=datetime(2024, 6, 5, 12, 0, 0)
        )

        # Mock the engine: engine.connect() returns a context manager whose conn
        # handles the CURRENT_TIMESTAMP query; engine.execute() returns table rows.
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (td_server_now,)

        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_conn)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_ctx
        mock_engine.execute.return_value = [recent_entry, stale_entry]

        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._tables_needing_column_extraction is not None
        assert ("db1", "recent_table") in source._tables_needing_column_extraction
        assert ("db1", "stale_table") not in source._tables_needing_column_extraction

    def test_optimized_get_columns_skips_table_not_in_extraction_set(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="unchanged",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }
        tables_needing_extraction = {("mydb", "other_table")}  # "unchanged" absent

        result = optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "unchanged",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=tables_needing_extraction,
        )

        assert result == []
        mock_dialect._get_column_help.assert_not_called()
        mock_dialect.get_schema_columns.assert_not_called()

    def test_optimized_get_columns_proceeds_for_changed_table(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect.get_schema_columns.return_value = {"my_table": []}
        mock_dialect._get_column_info.return_value = {"name": "col1"}

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="my_table",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }
        tables_needing_extraction = {("mydb", "my_table")}

        # Should not short-circuit; get_schema_columns will be reached
        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_table",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=tables_needing_extraction,
        )

        mock_dialect.get_schema_columns.assert_called_once()

    def test_none_extraction_set_processes_all_tables(self) -> None:
        """tables_needing_extraction=None (no watermark) means extract everything."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect.get_schema_columns.return_value = {"my_table": []}

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="my_table",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_table",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=None,
        )

        mock_dialect.get_schema_columns.assert_called_once()


class TestDbcColumnsForViews:
    """#2 — bulk dbc.ColumnsV for views with HELP fallback for derived columns."""

    def _view_table(self, name: str = "my_view", schema: str = "mydb") -> TeradataTable:
        return TeradataTable(
            database=schema,
            name=name,
            description=None,
            object_type="View",
            create_timestamp=datetime.now(),
            last_alter_name=None,
            last_alter_timestamp=None,
            request_text=None,
        )

    def test_uses_dbc_columns_when_all_types_present(self) -> None:
        """No HELP call when every column has a non-null ColumnType."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = "CV"  # explicit type present
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_info.return_value = {"name": "col1"}

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_not_called()
        mock_dialect.get_schema_columns.assert_called_once()

    def test_falls_back_to_help_when_column_type_is_null(self) -> None:
        """Falls back to HELP when a column has null ColumnType (derived expression)."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = None
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_called_once()

    def test_falls_back_to_help_when_column_type_is_empty_string(self) -> None:
        """Empty ColumnType string is also treated as unknown."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = "   "  # whitespace-only
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_called_once()

    def test_conservative_default_always_uses_help(self) -> None:
        """When use_dbc_columns_for_views=False, HELP is always used for views."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=False,
        )

        mock_dialect._get_column_help.assert_called_once()
        mock_dialect.get_schema_columns.assert_not_called()


class TestLruCacheSize:
    """#3 — caches are unbounded (maxsize=None) so multi-database runs don't lose entries."""

    def test_all_schema_caches_are_bounded_at_32(self) -> None:
        # 32 covers any realistic number of concurrently active schemas without
        # allowing unbounded accumulation of entries for dead connections.
        assert get_schema_columns.cache_info().maxsize == 32
        assert get_schema_pk_constraints.cache_info().maxsize == 32
        assert get_schema_foreign_keys.cache_info().maxsize == 32

    def test_multiple_schemas_are_all_retained(self) -> None:
        """All schemas stay cached; switching databases does not evict earlier results."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        get_schema_columns.cache_clear()
        try:
            get_schema_columns(None, mock_conn, "columnsV", "schema_a")
            get_schema_columns(None, mock_conn, "columnsV", "schema_b")
            get_schema_columns(None, mock_conn, "columnsV", "schema_c")

            info = get_schema_columns.cache_info()
            assert info.currsize == 3
            assert info.misses == 3
            # Repeated call for schema_a should be a cache hit, not a miss
            get_schema_columns(None, mock_conn, "columnsV", "schema_a")
            assert get_schema_columns.cache_info().hits == 1
        finally:
            get_schema_columns.cache_clear()


class TestLineageQueryScoping:
    """#4 — lineage queries are scoped to discovered databases via database_pattern."""

    def test_scopes_to_discovered_databases_filtered_by_pattern(self) -> None:
        source = _create_source_patched(
            {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        # Simulate what cache_tables_and_views populates
        source._tables_cache["sales_db"] = []
        source._tables_cache["hr_db"] = []
        # "All" is in EXCLUDED_DATABASES, so database_pattern.allowed("All") is False
        source._tables_cache["All"] = []

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        query = queries[0]
        assert "sales_db" in query
        assert "hr_db" in query
        assert "'All'" not in query

    def test_config_databases_takes_precedence_over_cache(self) -> None:
        source = _create_source_patched(
            {
                "databases": ["explicit_db"],
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        source._tables_cache["other_db"] = []

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        query = queries[0]
        assert "explicit_db" in query
        assert "other_db" not in query

    def test_no_database_filter_when_cache_is_empty(self) -> None:
        """Lineage-only runs (empty cache) should not produce an empty IN clause."""
        source = _create_source_patched(
            {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        assert len(source._tables_cache) == 0

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        assert len(queries) == 1
        assert "DefaultDatabase in" not in queries[0]


class TestConfigurableTimeouts:
    """#6 — request_timeout_ms / connect_timeout_ms flow through to Teradata connect_args."""

    def _get_connect_args(self, extra_config: Dict[str, Any]) -> Dict[str, str]:
        source = _create_source_patched(extra_config)
        captured: Dict[str, Any] = {}

        def capture(url: Any, **kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            return MagicMock()

        with patch(
            "datahub.ingestion.source.sql.teradata.create_engine", side_effect=capture
        ):
            source._get_or_create_pooled_engine()

        return captured.get("connect_args", {})

    def test_default_timeouts_used(self) -> None:
        connect_args = self._get_connect_args({})
        assert connect_args["request_timeout"] == "120000"
        assert connect_args["connect_timeout"] == "30000"

    def test_custom_timeouts_propagated(self) -> None:
        connect_args = self._get_connect_args(
            {"request_timeout_ms": 300000, "connect_timeout_ms": 60000}
        )
        assert connect_args["request_timeout"] == "300000"
        assert connect_args["connect_timeout"] == "60000"

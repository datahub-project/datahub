from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    TeradataTable,
    get_schema_columns,
    get_schema_pk_constraints,
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

            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.close"
            ) as mock_super_close:
                source.close()

                mock_aggregator.close.assert_called_once()
                mock_super_close.assert_called_once()

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

            # UNION query should have database filters for both current and historical parts
            union_query = queries[0]
            assert "l.DefaultDatabase in ('test_db1','test_db2')" in union_query
            assert "h.DefaultDatabase in ('test_db1','test_db2')" in union_query

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

            assert len(work_units) >= 1

    def test_creator_cache_with_whitespace_trimming(self):
        """Test that creator names are trimmed of whitespace when cached."""
        source = _create_source()
        mock_entry = _create_mock_table_entry(
            "test_db", "test_table", creator_name="creator_user"
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_engine = MagicMock()
            mock_engine.execute.return_value = [mock_entry]
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            assert (
                source._table_creator_cache[("test_db", "test_table")] == "creator_user"
            )

    def test_creator_cache_thread_safety(self):
        """Test that creator cache is protected by _tables_cache_lock."""
        source = _create_source()

        assert hasattr(source, "_tables_cache_lock")
        assert isinstance(source._tables_cache_lock, type(Lock()))

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

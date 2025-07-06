from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    TeradataTable,
    get_schema_columns,
    get_schema_pk_constraints,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery


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


class TestTeradataConfig:
    """Test configuration validation and initialization."""

    def test_valid_config(self):
        """Test that valid configuration is accepted."""
        config_dict = _base_config()
        config = TeradataConfig.parse_obj(config_dict)

        assert config.host_port == "localhost:1025"
        assert config.include_table_lineage is True
        assert config.include_usage_statistics is True
        assert config.include_queries is True

    def test_threading_validation_valid(self):
        """Test valid threading configuration passes validation."""
        config_dict = {
            **_base_config(),
            "view_processing_workers": 4,
            "enable_experimental_threading": True,
        }
        config = TeradataConfig.parse_obj(config_dict)
        assert config.view_processing_workers == 4
        assert config.enable_experimental_threading is True

    def test_threading_validation_invalid(self):
        """Test invalid threading configuration fails validation."""
        config_dict = {
            **_base_config(),
            "view_processing_workers": 4,
            "enable_experimental_threading": False,
        }

        with pytest.raises(ValueError, match="enable_experimental_threading is False"):
            TeradataConfig.parse_obj(config_dict)

    def test_threading_validation_single_worker_allowed(self):
        """Test single worker doesn't require experimental threading."""
        config_dict = {
            **_base_config(),
            "view_processing_workers": 1,
            "enable_experimental_threading": False,
        }
        config = TeradataConfig.parse_obj(config_dict)
        assert config.view_processing_workers == 1
        assert config.enable_experimental_threading is False

    def test_include_queries_default(self):
        """Test include_queries defaults to True."""
        config_dict = _base_config()
        config = TeradataConfig.parse_obj(config_dict)
        assert config.include_queries is True


class TestTeradataSource:
    """Test Teradata source functionality."""

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_source_initialization(self, mock_create_engine):
        """Test source initializes correctly."""
        config = TeradataConfig.parse_obj(_base_config())
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

        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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

            # Mock engine and query results
            mock_entry = MagicMock()
            mock_entry.DataBaseName = "test_db"
            mock_entry.name = "test_table"
            mock_entry.description = "Test table"
            mock_entry.object_type = "Table"
            mock_entry.CreateTimeStamp = None
            mock_entry.LastAlterName = None
            mock_entry.LastAlterTimeStamp = None
            mock_entry.RequestText = None

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
        config = TeradataConfig.parse_obj(_base_config())

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
            assert observed_query.default_db == "test_db"
            assert observed_query.default_schema == "test_db"

    def test_convert_entry_to_observed_query_with_none_user(self):
        """Test ObservedQuery conversion handles None user correctly."""
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
                list(source._get_audit_log_mcps_with_aggregator(set()))

                # Verify aggregator.add was called for each entry (streaming)
                assert mock_aggregator.add.call_count == 5


class TestConcurrencySupport:
    """Test thread safety and concurrent operations."""

    def test_tables_cache_thread_safety(self):
        """Test that tables cache operations are thread-safe."""
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

        # Create source without mocking to test the actual stage tracking during init
        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ), patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ) as mock_cache:
            TeradataSource(config, PipelineContext(run_id="test"))

            # Verify cache_tables_and_views was called during init (stage tracking happens there)
            mock_cache.assert_called_once()

    def test_stage_tracking_in_aggregator_processing(self):
        """Test that aggregator processing uses stage tracking."""
        config = TeradataConfig.parse_obj(_base_config())

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

                    list(source._get_audit_log_mcps_with_aggregator(set()))

                    # Should have called new_stage for query processing and metadata generation
                    # The actual implementation uses new_stage for "Fetching queries" and "Generating metadata"
                    assert mock_new_stage.call_count >= 1


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_empty_lineage_entries(self):
        """Test handling of empty lineage entries."""
        config = TeradataConfig.parse_obj(_base_config())

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
                result = list(source._get_audit_log_mcps_with_aggregator(set()))
                assert result == []

    def test_malformed_query_entry(self):
        """Test handling of malformed query entries."""
        config = TeradataConfig.parse_obj(_base_config())

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


if __name__ == "__main__":
    pytest.main([__file__])

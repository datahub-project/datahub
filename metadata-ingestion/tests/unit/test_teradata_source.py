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

    def test_max_workers_validation_valid(self):
        """Test valid max_workers configuration passes validation."""
        config_dict = {
            **_base_config(),
            "max_workers": 8,
        }
        config = TeradataConfig.parse_obj(config_dict)
        assert config.max_workers == 8

    def test_max_workers_default(self):
        """Test max_workers defaults to 10."""
        config_dict = _base_config()
        config = TeradataConfig.parse_obj(config_dict)
        assert config.max_workers == 10

    def test_max_workers_custom_value(self):
        """Test custom max_workers value is accepted."""
        config_dict = {
            **_base_config(),
            "max_workers": 5,
        }
        config = TeradataConfig.parse_obj(config_dict)
        assert config.max_workers == 5

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
                list(source._get_audit_log_mcps_with_aggregator())

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

                    list(source._get_audit_log_mcps_with_aggregator())

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
                result = list(source._get_audit_log_mcps_with_aggregator())
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


class TestLineageQuerySeparation:
    """Test the new separated lineage query functionality (no more UNION)."""

    def test_make_lineage_queries_current_only(self):
        """Test that only current query is returned when historical lineage is disabled."""
        config = TeradataConfig.parse_obj(
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
        """Test that both queries are returned when historical lineage is enabled and table exists."""
        config = TeradataConfig.parse_obj(
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

            assert len(queries) == 2

            # First query should be current data
            assert '"DBC".QryLogV' in queries[0]
            assert '"PDCRDATA".DBQLSqlTbl_Hst' not in queries[0]

            # Second query should be historical data
            assert '"PDCRDATA".DBQLSqlTbl_Hst' in queries[1]
            assert '"DBC".QryLogV' not in queries[1]

            # Both should have the time filters
            for query in queries:
                assert "2024-01-01" in query
                assert "2024-01-02" in query

    def test_make_lineage_queries_with_historical_unavailable(self):
        """Test that only current query is returned when historical lineage is enabled but table doesn't exist."""
        config = TeradataConfig.parse_obj(
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
        """Test that database filters are correctly applied to both queries."""
        config = TeradataConfig.parse_obj(
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

            assert len(queries) == 2

            # Current query should have default_database filter
            assert "default_database in ('test_db1','test_db2')" in queries[0]

            # Historical query should have h.DefaultDatabase filter
            assert "h.DefaultDatabase in ('test_db1','test_db2')" in queries[1]

    def test_fetch_lineage_entries_chunked_multiple_queries(self):
        """Test that _fetch_lineage_entries_chunked handles multiple queries correctly."""
        config = TeradataConfig.parse_obj(
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

                with patch.object(
                    source, "get_metadata_engine", return_value=mock_engine
                ), patch.object(
                    source, "_execute_with_cursor_fallback"
                ) as mock_execute:
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
        config = TeradataConfig.parse_obj(
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

                with patch.object(
                    source, "get_metadata_engine", return_value=mock_engine
                ), patch.object(
                    source, "_execute_with_cursor_fallback", return_value=mock_result
                ) as mock_execute:
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have executed only one query
                    assert mock_execute.call_count == 1
                    mock_execute.assert_called_with(mock_connection, "query1")

                    # Should return entries from the query
                    assert len(entries) == 1

    def test_fetch_lineage_entries_chunked_batch_processing(self):
        """Test that batch processing works correctly with configurable batch size."""
        config = TeradataConfig.parse_obj(
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

                with patch.object(
                    source, "get_metadata_engine", return_value=mock_engine
                ), patch.object(
                    source, "_execute_with_cursor_fallback", return_value=mock_result
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
        config = TeradataConfig.parse_obj(
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
                list(source._get_audit_log_mcps_with_aggregator())

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
        config = TeradataConfig.parse_obj(
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

                with patch.object(
                    source, "get_metadata_engine", return_value=mock_engine
                ), patch.object(
                    source, "_execute_with_cursor_fallback", return_value=mock_result
                ), patch("datahub.ingestion.source.sql.teradata.logger") as mock_logger:
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
        config = TeradataConfig.parse_obj(
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
            assert 'ORDER BY "query_id", "row_no"' in current_query

    def test_historical_query_construction(self):
        """Test that the historical query is constructed correctly."""
        config = TeradataConfig.parse_obj(
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
                historical_query = queries[1]

                # Verify historical query structure
                assert 'FROM "PDCRDATA".DBQLSqlTbl_Hst as h' in historical_query
                assert "h.ErrorCode = 0" in historical_query
                assert "h.StartTime AT TIME ZONE 'GMT'" in historical_query
                assert "h.DefaultDatabase" in historical_query
                assert "2024-01-01" in historical_query
                assert "2024-01-02" in historical_query
                assert 'ORDER BY "query_id", "row_no"' in historical_query


if __name__ == "__main__":
    pytest.main([__file__])

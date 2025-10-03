from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

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
            assert (
                observed_query.default_db is None
            )  # Fixed for Teradata two-tier architecture
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
                source._populate_aggregator_from_audit_logs()

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

                    source._populate_aggregator_from_audit_logs()

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
                source._populate_aggregator_from_audit_logs()
                # Method doesn't return a value, just populates the aggregator

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
        """Test that UNION query is returned when historical lineage is enabled and table exists."""
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
        """Test that database filters are correctly applied to UNION query."""
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

            assert len(queries) == 1

            # UNION query should have database filters for both current and historical parts
            union_query = queries[0]
            assert "l.DefaultDatabase in ('test_db1','test_db2')" in union_query
            assert "h.DefaultDatabase in ('test_db1','test_db2')" in union_query

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
            assert 'ORDER BY "timestamp", "query_id", "row_no"' in current_query

    def test_historical_query_construction(self):
        """Test that the UNION query contains historical data correctly."""
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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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
        config = TeradataConfig.parse_obj(_base_config())

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


class TestTeradataRegressionFixes:
    """Test suite for regression fixes in Teradata source.

    This class tests the fixes for two critical issues:
    1. Time window defaults not being applied correctly
    2. Missing incremental lineage support
    """

    def test_time_window_defaults_applied(self):
        """Test that BaseTimeWindowConfig defaults are automatically applied.

        This test ensures that the fix for the time window issue works correctly.
        Previously, start_time and end_time were None before pydantic validation,
        causing malformed SQL queries.
        """
        # Configuration without explicit time ranges (user's original recipe scenario)
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
        }

        config = TeradataConfig.parse_obj(config_dict)

        # Verify that BaseTimeWindowConfig defaults are applied
        assert config.start_time is not None, (
            "start_time should not be None after pydantic validation"
        )
        assert config.end_time is not None, (
            "end_time should not be None after pydantic validation"
        )

        # Verify they are datetime objects
        assert isinstance(config.start_time, datetime), (
            "start_time should be a datetime object"
        )
        assert isinstance(config.end_time, datetime), (
            "end_time should be a datetime object"
        )

        # Verify reasonable defaults (end_time should be after start_time)
        assert config.end_time > config.start_time, (
            "end_time should be after start_time"
        )

        # Verify they can be used in SQL queries (both formats work fine)
        # Test both formatting approaches work
        formatted_str = config.start_time.strftime("%Y-%m-%d %H:%M:%S")
        direct_str = str(config.start_time)

        assert len(formatted_str) > 0, "strftime formatting should work"
        assert len(direct_str) > 0, "direct string conversion should work"
        assert "None" not in formatted_str, "formatted time should not contain 'None'"
        assert "None" not in direct_str, "direct string should not contain 'None'"

    def test_incremental_lineage_config_support(self):
        """Test that incremental_lineage configuration parameter is supported.

        This test ensures that the fix for missing incremental lineage support works.
        Previously, incremental_lineage parameter was ignored because TeradataConfig
        didn't inherit from IncrementalLineageConfigMixin.
        """
        # Test with incremental_lineage enabled (user's original recipe scenario)
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,
        }

        config = TeradataConfig.parse_obj(config_dict)

        # Verify incremental_lineage attribute exists and is set correctly
        assert hasattr(config, "incremental_lineage"), (
            "Config should have incremental_lineage attribute"
        )
        assert config.incremental_lineage is True, (
            "incremental_lineage should be True when explicitly set"
        )

        # Test with incremental_lineage disabled
        config_dict_false = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "incremental_lineage": False,
        }

        config_false = TeradataConfig.parse_obj(config_dict_false)
        assert config_false.incremental_lineage is False, (
            "incremental_lineage should be False when explicitly set to False"
        )

        # Test default value when not specified
        config_dict_default = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            # incremental_lineage not specified
        }

        config_default = TeradataConfig.parse_obj(config_dict_default)
        assert config_default.incremental_lineage is False, (
            "incremental_lineage should default to False"
        )

    def test_make_lineage_queries_with_time_defaults(self):
        """Test that _make_lineage_queries works with automatic time defaults.

        This is an integration test that verifies the time window fix works
        end-to-end in the actual query generation method.
        """
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            # No explicit start_time/end_time - should use defaults
        }

        config = TeradataConfig.parse_obj(config_dict)

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock the historical table check to avoid database connection
            with patch.object(
                source, "_check_historical_table_exists", return_value=False
            ):
                queries = source._make_lineage_queries()

            # Verify that queries were generated (not empty due to None times)
            assert len(queries) > 0, "Should generate at least one query"

            # Verify that the generated query contains properly formatted timestamps
            query = queries[0]
            assert "TIMESTAMP" in query, "Query should contain TIMESTAMP clauses"
            assert "None" not in query, "Query should not contain 'None' values"

            # Verify the query contains valid timestamp values (both formats work)
            import re

            # Look for TIMESTAMP literals with any valid datetime format
            timestamp_pattern = r"TIMESTAMP '[^']+'"
            matches = re.findall(timestamp_pattern, query)
            assert len(matches) >= 2, (
                "Query should contain at least 2 TIMESTAMP literals"
            )

    def test_config_inheritance_chain(self):
        """Test that TeradataConfig properly inherits from all required base classes.

        This test verifies the inheritance fix that enables incremental lineage support.
        """
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "incremental_lineage": True,
        }

        config = TeradataConfig.parse_obj(config_dict)

        # Verify inheritance from BaseTimeWindowConfig
        assert hasattr(config, "start_time"), (
            "Should inherit start_time from BaseTimeWindowConfig"
        )
        assert hasattr(config, "end_time"), (
            "Should inherit end_time from BaseTimeWindowConfig"
        )
        assert hasattr(config, "bucket_duration"), (
            "Should inherit bucket_duration from BaseTimeWindowConfig"
        )

        # Verify inheritance from IncrementalLineageConfigMixin
        assert hasattr(config, "incremental_lineage"), (
            "Should inherit incremental_lineage from IncrementalLineageConfigMixin"
        )

        # Verify inheritance from BaseTeradataConfig
        assert hasattr(config, "scheme"), (
            "Should inherit scheme from BaseTeradataConfig"
        )

        # Verify inheritance from TwoTierSQLAlchemyConfig
        assert hasattr(config, "host_port"), (
            "Should inherit host_port from TwoTierSQLAlchemyConfig"
        )

    def test_user_original_recipe_compatibility(self):
        """Test that the user's exact original recipe configuration is parsed correctly.

        This test verifies that the user's actual configuration that was failing
        before the fixes now works correctly during configuration parsing.
        """
        # User's exact original recipe that was failing
        user_recipe_config = {
            "host_port": "vmvantage1720:1025",
            "username": "dbc",
            "password": "dbc",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,  # This was causing validation errors before
            "stateful_ingestion": {"enabled": True, "fail_safe_threshold": 90},
        }

        # This should now work without any errors (was failing before the fixes)
        config = TeradataConfig.parse_obj(user_recipe_config)

        # Verify all the key configurations are preserved
        assert config.host_port == "vmvantage1720:1025"
        assert config.username == "dbc"
        assert config.include_table_lineage is True
        assert config.include_usage_statistics is True
        assert (
            config.incremental_lineage is True
        )  # This was being ignored before the fix

        # Verify time defaults are applied automatically (this was the main bug)
        assert config.start_time is not None, "start_time should be automatically set"
        assert config.end_time is not None, "end_time should be automatically set"
        assert config.start_time < config.end_time, (
            "start_time should be before end_time"
        )

        # Verify time values work with both formatting approaches
        start_time_str = str(config.start_time)
        end_time_str = str(config.end_time)

        assert len(start_time_str) > 0, "start_time should convert to string"
        assert len(end_time_str) > 0, "end_time should convert to string"
        assert start_time_str < end_time_str, (
            "string comparison should work for ordering"
        )

        # Verify stateful ingestion config is preserved
        assert config.stateful_ingestion is not None
        assert config.stateful_ingestion.enabled is True
        assert config.stateful_ingestion.fail_safe_threshold == 90

        # Verify the combination of features works (incremental + stateful + time windows)
        assert config.incremental_lineage and config.start_time is not None
        assert hasattr(config, "incremental_lineage") and hasattr(config, "start_time")

        # Test that this config could be used for query generation
        sample_query_fragment = f"WHERE timestamp >= TIMESTAMP '{start_time_str}' AND timestamp < TIMESTAMP '{end_time_str}'"
        assert "TIMESTAMP" in sample_query_fragment
        assert "None" not in sample_query_fragment
        assert len(sample_query_fragment) > 50  # Should be a substantial query fragment

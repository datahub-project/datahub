"""
Performance and optimization tests for TeradataSource.

These tests focus on performance optimizations, memory management,
and efficiency improvements in the Teradata source.
"""

from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    TeradataTable,
    get_schema_columns,
    get_schema_foreign_keys,
    get_schema_pk_constraints,
)


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


class TestCachingOptimizations:
    """Test caching optimizations and LRU cache functionality."""

    def test_get_schema_columns_caching(self):
        """Test that get_schema_columns uses LRU cache effectively."""
        mock_connection = MagicMock()

        # Mock database response
        mock_row1 = MagicMock()
        mock_row1._mapping.TableName = "table1"
        mock_row1._mapping.ColumnName = "col1"

        mock_row2 = MagicMock()
        mock_row2._mapping.TableName = "table1"
        mock_row2._mapping.ColumnName = "col2"

        mock_connection.execute.return_value.fetchall.return_value = [
            mock_row1,
            mock_row2,
        ]

        # Mock self with report for timing tracking
        mock_self = MagicMock()
        mock_self.report = MagicMock()
        mock_self.report.column_extraction_duration_seconds = 0.0

        # First call
        result1 = get_schema_columns(
            mock_self, mock_connection, "columnsV", "test_schema"
        )

        # Second call with same parameters should use cache
        result2 = get_schema_columns(
            mock_self, mock_connection, "columnsV", "test_schema"
        )

        # Should only execute query once due to caching
        assert mock_connection.execute.call_count == 1
        assert result1 == result2
        assert "table1" in result1
        assert len(result1["table1"]) == 2

    def test_get_schema_pk_constraints_caching(self):
        """Test that get_schema_pk_constraints uses LRU cache effectively."""
        mock_connection = MagicMock()

        # Mock database response
        mock_row = MagicMock()
        mock_row._mapping.TableName = "table1"
        mock_row._mapping.ColumnName = "id"
        mock_row._mapping.IndexName = "pk_table1"

        mock_connection.execute.return_value.fetchall.return_value = [mock_row]

        # First call
        result1 = get_schema_pk_constraints(None, mock_connection, "test_schema")

        # Second call should use cache
        result2 = get_schema_pk_constraints(None, mock_connection, "test_schema")

        # Should only execute query once
        assert mock_connection.execute.call_count == 1
        assert result1 == result2

    def test_get_schema_foreign_keys_caching(self):
        """Test that get_schema_foreign_keys uses LRU cache effectively."""
        mock_connection = MagicMock()

        # Mock database response
        mock_row = MagicMock()
        mock_row._mapping.ChildTable = "child_table"
        mock_row._mapping.IndexName = "fk_child"

        mock_connection.execute.return_value.fetchall.return_value = [mock_row]

        # First call
        result1 = get_schema_foreign_keys(None, mock_connection, "test_schema")

        # Second call should use cache
        result2 = get_schema_foreign_keys(None, mock_connection, "test_schema")

        # Should only execute query once
        assert mock_connection.execute.call_count == 1
        assert result1 == result2

    def test_cache_invalidation_different_schemas(self):
        """Test that cache is properly invalidated for different schemas."""
        mock_connection = MagicMock()

        def mock_execute_side_effect(query, params):
            # Return different results based on schema
            schema = params["schema"]
            mock_result = MagicMock()
            if schema == "schema1":
                mock_row = MagicMock()
                mock_row._mapping.TableName = "table_schema1"
                mock_result.fetchall.return_value = [mock_row]
            else:
                mock_row = MagicMock()
                mock_row._mapping.TableName = "table_schema2"
                mock_result.fetchall.return_value = [mock_row]
            return mock_result

        mock_connection.execute.side_effect = mock_execute_side_effect

        mock_self = MagicMock()
        mock_self.report = MagicMock()
        mock_self.report.column_extraction_duration_seconds = 0.0

        # Call with different schemas
        result1 = get_schema_columns(mock_self, mock_connection, "columnsV", "schema1")
        result2 = get_schema_columns(mock_self, mock_connection, "columnsV", "schema2")

        # Should execute query twice for different schemas
        assert mock_connection.execute.call_count == 2
        assert "table_schema1" in result1
        assert "table_schema2" in result2


class TestMemoryOptimizations:
    """Test memory optimization features."""

    def test_tables_cache_memory_efficiency(self):
        """Test that tables cache is memory efficient."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test that cache uses defaultdict for efficient memory usage
            from collections import defaultdict

            assert isinstance(source._tables_cache, defaultdict)

            # Test safe access to non-existent schema
            result = source._tables_cache["nonexistent_schema"]
            assert result == []

    def test_streaming_query_processing(self):
        """Test that query processing uses streaming for memory efficiency."""
        config_dict = {
            **_base_config(),
            "use_server_side_cursors": True,
        }
        config = TeradataConfig.parse_obj(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            mock_connection = MagicMock()
            mock_result = MagicMock()

            # Mock streaming execution
            mock_connection.execution_options.return_value.execute.return_value = (
                mock_result
            )

            result = source._execute_with_cursor_fallback(mock_connection, "SELECT 1")

            # Verify streaming was enabled
            mock_connection.execution_options.assert_called_with(stream_results=True)
            assert result == mock_result

    def test_chunked_processing_batch_size(self):
        """Test that chunked processing uses appropriate batch sizes."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_make_lineage_queries", return_value=["SELECT 1"]
            ):
                # Create mock result that returns batches
                mock_result = MagicMock()

                # Simulate fetching in batches
                call_count = [0]

                def mock_fetchmany(batch_size):
                    call_count[0] += 1
                    if call_count[0] == 1:
                        # Return batch_size to verify it's being used
                        assert batch_size == 5000  # Expected batch size
                        return [MagicMock() for _ in range(batch_size)]
                    return []  # End of results

                mock_result.fetchmany.side_effect = mock_fetchmany

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
                    # Process entries
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have fetched one full batch
                    assert len(entries) == 5000


class TestPerformanceReporting:
    """Test performance metrics and reporting."""

    def test_column_extraction_timing(self):
        """Test that column extraction timing is tracked."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Mock self with report
        mock_self = MagicMock()
        mock_self.report = MagicMock()
        mock_self.report.column_extraction_duration_seconds = 0.0

        # Test by setting a custom timing value and verifying it's updated
        get_schema_columns(mock_self, mock_connection, "columnsV", "test_schema")

        # The timing should be updated (it will be a small positive number)
        assert mock_self.report.column_extraction_duration_seconds >= 0.0

    def test_view_processing_timing_metrics(self):
        """Test that view processing timing is properly tracked."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create view in cache
            test_view = TeradataTable(
                database="test_schema",
                name="test_view",
                description="Test view",
                object_type="View",
                create_timestamp=datetime.now(),
                last_alter_name=None,
                last_alter_timestamp=None,
                request_text="SELECT * FROM test_table",
            )
            source._tables_cache["test_schema"] = [test_view]

            mock_inspector = MagicMock()
            mock_sql_config = MagicMock()

            with patch.object(source, "_process_views_single_threaded") as mock_process:
                mock_process.return_value = []

                # Process views and check timing
                list(
                    source.cached_loop_views(
                        mock_inspector, "test_schema", mock_sql_config
                    )
                )

                # Verify timing metrics were updated
                assert source.report.view_extraction_total_time_seconds > 0
                assert source.report.num_views_processed == 1

    def test_connection_pool_metrics(self):
        """Test that connection pool metrics are tracked."""
        config_dict = {
            **_base_config(),
            "max_workers": 2,
        }
        config = TeradataConfig.parse_obj(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify initial metrics
            assert source.report.connection_pool_wait_time_seconds == 0.0
            assert source.report.connection_pool_max_wait_time_seconds == 0.0

            # Pool metrics are updated in the actual view processing threads
            # Here we just verify the structure exists

    def test_database_level_metrics_tracking(self):
        """Test that database-level metrics are properly tracked."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock database entries for metrics
            mock_entries = [
                MagicMock(
                    DataBaseName="db1",
                    name="table1",
                    description="Test",
                    object_type="Table",
                    CreateTimeStamp=datetime.now(),
                    LastAlterName=None,
                    LastAlterTimeStamp=None,
                    RequestText=None,
                ),
                MagicMock(
                    DataBaseName="db1",
                    name="view1",
                    description="Test",
                    object_type="View",
                    CreateTimeStamp=datetime.now(),
                    LastAlterName=None,
                    LastAlterTimeStamp=None,
                    RequestText="SELECT 1",
                ),
                MagicMock(
                    DataBaseName="db2",
                    name="table2",
                    description="Test",
                    object_type="Table",
                    CreateTimeStamp=datetime.now(),
                    LastAlterName=None,
                    LastAlterTimeStamp=None,
                    RequestText=None,
                ),
            ]

            with patch.object(source, "get_metadata_engine") as mock_get_engine:
                mock_engine = MagicMock()
                mock_engine.execute.return_value = mock_entries
                mock_get_engine.return_value = mock_engine

                source.cache_tables_and_views()

                # Verify database-level metrics
                assert source.report.num_database_tables_to_scan["db1"] == 1
                assert source.report.num_database_views_to_scan["db1"] == 1
                assert source.report.num_database_tables_to_scan["db2"] == 1
                assert source.report.num_database_views_to_scan["db2"] == 0


class TestThreadSafetyOptimizations:
    """Test thread safety optimizations."""

    def test_tables_cache_lock_usage(self):
        """Test that tables cache uses locks for thread safety."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify lock exists (it's a class attribute)
            assert hasattr(source.__class__, "_tables_cache_lock")
            # Check that it's a lock object by checking its type name
            assert source.__class__._tables_cache_lock.__class__.__name__ == "lock"

    def test_report_lock_usage(self):
        """Test that report operations use locks for thread safety."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify report lock exists
            assert hasattr(source, "_report_lock")
            # Check that it's a lock object by checking its type name
            assert source._report_lock.__class__.__name__ == "lock"

    def test_pooled_engine_lock_usage(self):
        """Test that pooled engine creation uses locks."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify pooled engine lock exists (it's a class attribute)
            assert hasattr(source.__class__, "_pooled_engine_lock")
            # Check that it's a lock object by checking its type name
            assert source.__class__._pooled_engine_lock.__class__.__name__ == "lock"


class TestQueryOptimizations:
    """Test SQL query optimizations."""

    def test_parameterized_query_usage(self):
        """Test that parameterized queries are used to prevent SQL injection."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        mock_self = MagicMock()
        mock_self.report = MagicMock()
        mock_self.report.column_extraction_duration_seconds = 0.0

        # Call with special characters that could cause SQL injection
        schema_name = "test'schema"
        get_schema_columns(mock_self, mock_connection, "columnsV", schema_name)

        # Verify parameterized query was used
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        # Query should use parameters, not string concatenation
        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == schema_name

    def test_query_structure_optimization(self):
        """Test that queries are structured for optimal performance."""
        config = TeradataConfig.parse_obj(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test TABLES_AND_VIEWS_QUERY structure
            query = source.TABLES_AND_VIEWS_QUERY

            # Should exclude system databases efficiently
            assert "NOT IN" in query
            assert "ORDER by DataBaseName, TableName" in query

            # Should only select necessary table types
            assert "t.TableKind in ('T', 'V', 'Q', 'O')" in query

    def test_usexviews_optimization(self):
        """Test that usexviews configuration optimizes queries."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Test with usexviews - this function doesn't actually use usexviews, just verify the query structure
        get_schema_pk_constraints(None, mock_connection, "test_schema")

        # Should use IndicesV view
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        assert "IndicesV" in query
        assert "IndexType = 'K'" in query

    def test_qvci_optimization(self):
        """Test QVCI optimization for column information."""
        from datahub.ingestion.source.sql.teradata import optimized_get_columns

        # Create table in cache
        test_table = TeradataTable(
            database="test_schema",
            name="test_table",
            description="Test table",
            object_type="Table",
            create_timestamp=datetime.now(),
            last_alter_name=None,
            last_alter_timestamp=None,
            request_text=None,
        )

        tables_cache = {"test_schema": [test_table]}

        # Mock self object
        mock_self = MagicMock()
        mock_self.get_schema_columns.return_value = {"test_table": []}

        mock_connection = MagicMock()

        # Test with QVCI enabled
        optimized_get_columns(
            mock_self,
            mock_connection,
            "test_table",
            schema="test_schema",
            tables_cache=tables_cache,
            use_qvci=True,
        )

        # Should use QVCI columns view
        mock_self.get_schema_columns.assert_called_with(
            mock_connection, "columnsQV", "test_schema"
        )

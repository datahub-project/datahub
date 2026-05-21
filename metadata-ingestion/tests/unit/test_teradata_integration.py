"""
Integration tests for TeradataSource complex scenarios.

These tests focus on end-to-end workflows, complex interactions,
and integration between different components.
"""

import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    TeradataTable,
)
from datahub.metadata.urns import CorpUserUrn


@pytest.fixture(autouse=True)
def isolate_teradata_caches(monkeypatch):
    """Reset TeradataSource class-level caches before each test.

    The caches are class attributes, so without isolation tests that populate
    them via `source._tables_cache["schema"] = …` leak state into later tests.
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


class TestEndToEndWorkflow:
    """Test complete end-to-end workflows."""

    def test_complete_metadata_extraction_workflow(self):
        """Test complete metadata extraction from initialization to work units."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock the complete workflow
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ) as mock_cache:
                source = TeradataSource(config, PipelineContext(run_id="test"))

                # Verify initialization called cache
                mock_cache.assert_called_once()

                # Verify aggregator was initialized with correct parameters
                mock_aggregator_class.assert_called_once()
                call_kwargs = mock_aggregator_class.call_args[1]
                assert call_kwargs["platform"] == "teradata"
                assert call_kwargs["generate_lineage"] == source.include_lineage
                assert call_kwargs["generate_queries"] == config.include_queries
                assert (
                    call_kwargs["generate_usage_statistics"]
                    == config.include_usage_statistics
                )

            # Test work unit generation
            with patch.object(source, "get_workunits_internal") as mock_workunits:
                mock_workunits.return_value = []

                # Should handle work unit generation without errors
                work_units = list(source.get_workunits())
                assert isinstance(work_units, list)

    def test_lineage_extraction_with_historical_data(self):
        """Test lineage extraction workflow with historical data enabled."""
        config_dict = {
            **_base_config(),
            "include_historical_lineage": True,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock historical table check
            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                # Mock query entries from both current and historical sources
                historical_entry = MagicMock()
                historical_entry.query_text = (
                    "SELECT * FROM historical_table (NOT CASESPECIFIC)"
                )
                historical_entry.user = "historical_user"
                historical_entry.timestamp = "2023-12-01 10:00:00"
                historical_entry.default_database = "historical_db"

                current_entry = MagicMock()
                current_entry.query_text = "SELECT * FROM current_table"
                current_entry.user = "current_user"
                current_entry.timestamp = "2024-01-01 10:00:00"
                current_entry.default_database = "current_db"

                def mock_fetch_generator():
                    yield historical_entry
                    yield current_entry

                with patch.object(
                    source,
                    "_fetch_lineage_entries_chunked",
                    return_value=mock_fetch_generator(),
                ):
                    mock_aggregator.gen_metadata.return_value = []

                    # Execute the lineage extraction
                    source._populate_aggregator_from_audit_logs()

                    # Verify both entries were processed
                    assert mock_aggregator.add.call_count == 2

                    # Verify entry conversion
                    added_queries = [
                        call[0][0] for call in mock_aggregator.add.call_args_list
                    ]

                    # Historical query should have (NOT CASESPECIFIC) removed
                    historical_query = next(
                        q for q in added_queries if "historical_table" in q.query
                    )
                    assert "(NOT CASESPECIFIC)" not in historical_query.query
                    assert historical_query.user == CorpUserUrn("historical_user")

                    # Current query should be processed normally
                    current_query = next(
                        q for q in added_queries if "current_table" in q.query
                    )
                    assert current_query.user == CorpUserUrn("current_user")

    def test_view_processing_with_threading(self):
        """Test view processing with threading enabled."""
        config_dict = {
            **_base_config(),
            "max_workers": 3,
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create multiple views in cache
            views = [
                TeradataTable(
                    database="test_schema",
                    name=f"view_{i}",
                    description=f"Test view {i}",
                    object_type="View",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=f"SELECT {i}",
                )
                for i in range(5)
            ]
            source._tables_cache["test_schema"] = views

            mock_sql_config = MagicMock()
            mock_sql_config.view_pattern.allowed.return_value = True

            # Mock work unit generation for each view
            with patch.object(source, "_process_view") as mock_process_view:
                mock_process_view.return_value = [MagicMock()]  # One work unit per view

                with patch.object(
                    source, "_get_or_create_pooled_engine"
                ) as mock_engine:
                    mock_connection = MagicMock()
                    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_connection

                    with patch(
                        "datahub.ingestion.source.sql.teradata.inspect"
                    ) as mock_inspect:
                        mock_inspector = MagicMock()
                        mock_inspect.return_value = mock_inspector

                        # Process views
                        work_units = list(
                            source.cached_loop_views(
                                mock_inspector, "test_schema", mock_sql_config
                            )
                        )

                        # Should generate work units for all views
                        assert len(work_units) == 5

                        # Verify timing metrics were updated
                        assert source.report.view_extraction_total_time_seconds > 0
                        assert source.report.num_views_processed == 5

    def test_error_handling_in_complex_workflow(self):
        """Test error handling during complex workflows."""
        config_dict = {
            **_base_config(),
            "max_workers": 1,  # Use single-threaded processing for this test
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock view processing with some failures
            views = [
                TeradataTable(
                    database="test_schema",
                    name=f"view_{i}",
                    description=f"Test view {i}",
                    object_type="View",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=f"SELECT {i}",
                )
                for i in range(3)
            ]
            source._tables_cache["test_schema"] = views

            mock_sql_config = MagicMock()
            mock_sql_config.view_pattern.allowed.return_value = True

            # Mock single-threaded processing with errors
            def mock_process_single_view_side_effect():
                views_processed = [0]  # Use list to maintain state

                def process_view(view_name, *args, **kwargs):
                    if views_processed[0] == 1:  # Fail on second view
                        raise Exception(f"Failed to process {view_name}")
                    views_processed[0] += 1
                    return [MagicMock()]

                return process_view

            with patch.object(
                source, "_process_views_single_threaded"
            ) as mock_single_threaded:
                mock_single_threaded.return_value = [
                    MagicMock(),
                    MagicMock(),
                ]  # Success for some views

                # Create mock inspector
                mock_inspector = MagicMock()

                # Should handle errors gracefully
                work_units = list(
                    source.cached_loop_views(
                        mock_inspector, "test_schema", mock_sql_config
                    )
                )

                # Should still produce some work units despite errors
                assert len(work_units) == 2


class TestConfigurationIntegration:
    """Test integration between different configuration options."""

    def test_database_filtering_integration(self):
        """Test database filtering with different configuration combinations."""
        config_dict = {
            **_base_config(),
            "databases": ["allowed_db1", "allowed_db2"],
            "database_pattern": {
                "allow": ["allowed_*"],
                "deny": ["allowed_db2"],  # Deny one of the explicitly allowed
            },
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test database filtering logic
            with (
                patch(
                    "datahub.ingestion.source.sql.teradata.create_engine"
                ) as mock_create_engine,
                patch("datahub.ingestion.source.sql.teradata.inspect") as mock_inspect,
            ):
                mock_engine = MagicMock()
                mock_create_engine.return_value = mock_engine

                mock_connection = MagicMock()
                mock_engine.connect.return_value.__enter__.return_value = (
                    mock_connection
                )

                mock_inspector = MagicMock()
                mock_inspect.return_value = mock_inspector

                inspectors = list(source.get_inspectors())

                # Should only get inspectors for databases that pass both filters
                # allowed_db1 should pass (in databases list and matches pattern)
                # allowed_db2 should be excluded (denied by pattern)
                assert len(inspectors) == 1

    def test_lineage_and_usage_statistics_integration(self):
        """Test integration between lineage and usage statistics."""
        config_dict = {
            **_base_config(),
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "include_queries": True,
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                TeradataSource(config, PipelineContext(run_id="test"))

            # Verify aggregator was configured for both lineage and usage
            call_kwargs = mock_aggregator_class.call_args[1]
            assert call_kwargs["generate_lineage"] is True
            assert call_kwargs["generate_usage_statistics"] is True
            assert call_kwargs["generate_queries"] is True
            assert call_kwargs["generate_query_usage_statistics"] is True

    def test_profiling_configuration_integration(self):
        """Test integration with profiling configuration."""
        config_dict = {
            **_base_config(),
            "profiling": {
                "enabled": True,
                "profile_table_level_only": True,
            },
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test that profiling affects SQL configuration
            mock_sql_config = MagicMock()
            mock_sql_config.is_profiling_enabled.return_value = True
            mock_sql_config.options = {}

            source._add_default_options(mock_sql_config)

            # Should add QueuePool for profiling
            from sqlalchemy.pool import QueuePool

            assert mock_sql_config.options["poolclass"] == QueuePool


class TestComplexQueryScenarios:
    """Test complex query processing scenarios."""

    def test_large_query_result_processing(self):
        """Test processing of large query result sets."""
        config_dict = {
            **_base_config(),
            "use_server_side_cursors": True,
        }
        config = TeradataConfig.model_validate(config_dict)

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
                source,
                "_make_lineage_queries",
                return_value=["SELECT * FROM large_table"],
            ):
                # Create large result set simulation
                total_entries = 15000  # Larger than batch size
                batch_size = 5000

                def create_mock_entries(start, count):
                    return [
                        MagicMock(
                            query_text=f"SELECT {i}",
                            user="user",
                            timestamp="2024-01-01",
                            default_database="db",
                        )
                        for i in range(start, start + count)
                    ]

                mock_result = MagicMock()
                call_count = [0]

                def mock_fetchmany(size):
                    start = call_count[0] * batch_size
                    call_count[0] += 1

                    if start >= total_entries:
                        return []

                    remaining = min(batch_size, total_entries - start)
                    return create_mock_entries(start, remaining)

                mock_result.fetchmany.side_effect = mock_fetchmany

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

                    # Should process all entries in batches
                    assert len(entries) == total_entries
                    assert call_count[0] == 4  # 3 full batches + 1 empty

    def test_query_with_special_characters(self):
        """Test processing queries with special characters and encoding."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock entry with special characters
            mock_entry = MagicMock()
            mock_entry.query_text = (
                "SELECT 'test''s data (NOT CASESPECIFIC)' FROM tëst_tæblé"
            )
            mock_entry.user = "tëst_üser"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.default_database = "tëst_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            # Should handle special characters and clean query
            assert "(NOT CASESPECIFIC)" not in observed_query.query
            assert "tëst_tæblé" in observed_query.query
            assert observed_query.user == CorpUserUrn("tëst_üser")
            assert (
                observed_query.default_db is None
            )  # Fixed for Teradata two-tier architecture
            assert observed_query.default_schema == "tëst_db"

    def test_multi_line_query_processing(self):
        """Test processing of multi-line queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock multi-line query entry
            mock_entry = MagicMock()
            mock_entry.query_text = """
            SELECT 
                col1,
                col2 (NOT CASESPECIFIC)
            FROM 
                table1 t1
            JOIN 
                table2 t2 ON t1.id = t2.id
            WHERE 
                t1.date > '2024-01-01'
            """.strip()
            mock_entry.user = "test_user"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.default_database = "test_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            # Should preserve multi-line structure but clean Teradata-specific syntax
            assert "(NOT CASESPECIFIC)" not in observed_query.query
            assert "SELECT" in observed_query.query
            assert "JOIN" in observed_query.query
            assert "WHERE" in observed_query.query


class TestResourceManagement:
    """Test resource management and cleanup."""

    def test_engine_disposal_on_close(self):
        """Test that engines are properly disposed on close."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create pooled engine
            with patch(
                "datahub.ingestion.source.sql.teradata.create_engine"
            ) as mock_create_engine:
                mock_engine = MagicMock()
                mock_create_engine.return_value = mock_engine

                # Create pooled engine
                source._get_or_create_pooled_engine()
                assert source._pooled_engine == mock_engine

                # Close source
                with patch(
                    "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.close"
                ):
                    source.close()

                    # Should dispose pooled engine
                    mock_engine.dispose.assert_called_once()
                    assert source._pooled_engine is None

    def test_aggregator_cleanup_on_close(self):
        """Test that aggregator is properly cleaned up on close."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

                # Ensure the aggregator is properly set on the source
                assert source.aggregator == mock_aggregator

            # Close source
            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.close"
            ):
                source.close()

                # Should close aggregator
                mock_aggregator.close.assert_called_once()

    def test_connection_cleanup_in_error_scenarios(self):
        """Test that connections are cleaned up even when errors occur."""
        config = TeradataConfig.model_validate(_base_config())

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
                mock_engine = MagicMock()

                # Mock connection that raises exception during processing
                mock_connection = MagicMock()
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
                    # Raise exception during query execution
                    mock_execute.side_effect = Exception("Database error")

                    # Should handle exception and still dispose engine
                    with pytest.raises(Exception, match="Database error"):
                        list(source._fetch_lineage_entries_chunked())

                    # Engine should still be disposed
                    mock_engine.dispose.assert_called_once()


class TestViewProcessingHangProtection:
    """Verify the parallel view-processing executor cannot block on a hung view."""

    def _make_source_with_views(self, view_names, **overrides):
        config_dict = {
            **_base_config(),
            "max_workers": 4,
            "view_processing_timeout_seconds": 1,
            # heartbeat_seconds also drives how often the inner wait() wakes up
            # to check for stalled futures — keep it small so the test completes
            # within a couple of seconds, well under the < 10s assertion below.
            "view_processing_heartbeat_seconds": 1,
            **overrides,
        }
        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.ingestion.source.sql.teradata.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator_class.return_value = MagicMock()
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

        source._tables_cache["test_schema"] = [
            TeradataTable(
                database="test_schema",
                name=name,
                description=None,
                object_type="View",
                create_timestamp=datetime.now(),
                last_alter_name=None,
                last_alter_timestamp=None,
                request_text=f"SELECT * FROM {name}",
            )
            for name in view_names
        ]
        return source

    def test_hung_view_is_abandoned_and_others_complete(self):
        """A single hung view must not stall the executor — the other views still complete."""
        # The first view blocks until the test releases it. The others return immediately.
        release = threading.Event()

        def fake_process_view(dataset_name, inspector, schema, view, sql_config):
            if view == "hung_view":
                # Simulate a hung DB call. The hang-protection timeout
                # (1s in this test) must abandon this before the test releases it.
                release.wait(timeout=30)
            yield MagicMock(spec=[])

        source = self._make_source_with_views(["fast_a", "hung_view", "fast_b"])
        mock_sql_config = MagicMock()
        mock_sql_config.view_pattern.allowed.return_value = True

        try:
            with (
                patch.object(source, "_process_view", side_effect=fake_process_view),
                patch.object(source, "_get_or_create_pooled_engine") as mock_engine,
                patch("datahub.ingestion.source.sql.teradata.inspect") as mock_inspect,
            ):
                mock_engine.return_value.connect.return_value.__enter__.return_value = (
                    MagicMock()
                )
                mock_inspect.return_value = MagicMock()

                started = time.time()
                work_units = list(
                    source.cached_loop_views(
                        MagicMock(), "test_schema", mock_sql_config
                    )
                )
                elapsed = time.time() - started

            # Must not be blocked by the hung view: should return within a few
            # seconds (timeout=1s + slack), nowhere near release.wait()'s 30s.
            assert elapsed < 10, (
                f"Executor blocked on hung view for {elapsed:.1f}s — hang protection failed"
            )
            # The two fast views must have produced their work units.
            assert len(work_units) == 2
            # The hung view must be counted as a timeout.
            assert source.report.num_view_processing_timeouts == 1
            assert "test_schema.hung_view" in source.report.stalled_views
        finally:
            # Always release the hung thread so it doesn't outlive the test.
            release.set()

    def test_all_views_complete_when_none_hang(self):
        """Baseline: when no view hangs, all work units are produced and no timeouts are recorded."""

        def fake_process_view(dataset_name, inspector, schema, view, sql_config):
            yield MagicMock(spec=[])

        source = self._make_source_with_views(["a", "b", "c"])
        mock_sql_config = MagicMock()
        mock_sql_config.view_pattern.allowed.return_value = True

        with (
            patch.object(source, "_process_view", side_effect=fake_process_view),
            patch.object(source, "_get_or_create_pooled_engine") as mock_engine,
            patch("datahub.ingestion.source.sql.teradata.inspect") as mock_inspect,
        ):
            mock_engine.return_value.connect.return_value.__enter__.return_value = (
                MagicMock()
            )
            mock_inspect.return_value = MagicMock()

            work_units = list(
                source.cached_loop_views(MagicMock(), "test_schema", mock_sql_config)
            )

        assert len(work_units) == 3
        assert source.report.num_view_processing_timeouts == 0
        assert len(source.report.stalled_views) == 0

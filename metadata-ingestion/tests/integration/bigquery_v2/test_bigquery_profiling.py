from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_and_filter_expressions,
    validate_bigquery_identifier,
)


def test_profiling_modules_integration():
    """Test that profiling modules work together correctly."""
    # Create test configuration
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project-123456",
            "profiling": {
                "enabled": True,
                "profile_external_tables": True,
                "partition_profiling_enabled": True,
                "use_sampling": True,
                "sample_size": 10000,
                "profiling_row_limit": 50000,
            },
        }
    )

    report = BigQueryV2Report()

    # Test profiler initialization
    profiler = BigqueryProfiler(config, report)
    assert profiler.config == config
    assert profiler.report == report
    assert isinstance(profiler.partition_discovery, PartitionDiscovery)
    assert isinstance(profiler.query_executor, QueryExecutor)

    # Test partition discovery
    partition_discovery = PartitionDiscovery(config)
    strategic_dates = partition_discovery._get_strategic_candidate_dates()
    assert len(strategic_dates) == 2  # today and yesterday
    assert all(isinstance(d, tuple) and len(d) == 2 for d in strategic_dates)

    # Test query executor
    query_executor = QueryExecutor(config)
    timeout = query_executor.get_effective_timeout()
    assert timeout > 0

    # Test security validation
    safe_identifier = validate_bigquery_identifier("test_table")
    assert safe_identifier == "`test_table`"

    valid_filters = ["`date` = '2023-01-01'", "`id` > 100"]
    validated = validate_and_filter_expressions(valid_filters)
    assert validated == valid_filters


def test_profiler_table_processing():
    """Test profiler table processing logic."""
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project-123456",
            "profiling": {
                "enabled": True,
                "use_sampling": True,
                "sample_size": 5000,
                "skip_stale_tables": True,
                "staleness_threshold_days": 30,
            },
        }
    )

    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test fresh table (should not be skipped)
    fresh_table = BigqueryTable(
        name="fresh_table",
        comment="Fresh table",
        created=datetime.now(timezone.utc) - timedelta(days=1),
        last_altered=datetime.now(timezone.utc) - timedelta(hours=1),
        size_in_bytes=1000000,
        rows_count=10000,
    )

    should_skip = profiler._should_skip_profiling_due_to_staleness(fresh_table)
    assert should_skip is False

    # Test stale table (should be skipped)
    stale_table = BigqueryTable(
        name="stale_table",
        comment="Stale table",
        created=datetime.now(timezone.utc) - timedelta(days=100),
        last_altered=datetime.now(timezone.utc) - timedelta(days=60),
        size_in_bytes=1000000,
        rows_count=10000,
    )

    should_skip = profiler._should_skip_profiling_due_to_staleness(stale_table)
    assert should_skip is True


def test_profiler_batch_kwargs_generation():
    """Test batch kwargs generation for different table types."""
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project-123456",
            "profiling": {
                "enabled": True,
                "use_sampling": True,
                "sample_size": 1000,
                "profiling_row_limit": 10000,
            },
        }
    )

    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Mock partition discovery to avoid external dependencies
    def mock_get_partition_filters(table, project, dataset, execute_func):
        return ["`date` = '2023-12-25'"]

    # Use patch context manager to avoid mypy method assignment error
    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        side_effect=mock_get_partition_filters,
    ):
        # Test small table (no sampling)
        small_table = BigqueryTable(
            name="small_table",
            comment="Small table",
            created=datetime.now(timezone.utc) - timedelta(days=1),
            last_altered=datetime.now(timezone.utc) - timedelta(hours=1),
            size_in_bytes=100000,
            rows_count=500,  # Less than sample_size
        )

        batch_kwargs = profiler.get_batch_kwargs(
            small_table, "test_dataset", "test-project-123456"
        )
        assert "custom_sql" in batch_kwargs
        assert "TABLESAMPLE" not in batch_kwargs["custom_sql"]
        assert "LIMIT" in batch_kwargs["custom_sql"]

        # Test large table (should use sampling)
        large_table = BigqueryTable(
            name="large_table",
            comment="Large table",
            created=datetime.now(timezone.utc) - timedelta(days=1),
            last_altered=datetime.now(timezone.utc) - timedelta(hours=1),
            size_in_bytes=10000000,
            rows_count=50000,  # Greater than sample_size
        )

        batch_kwargs = profiler.get_batch_kwargs(
            large_table, "test_dataset", "test-project-123456"
        )
        assert "custom_sql" in batch_kwargs
        assert "TABLESAMPLE SYSTEM" in batch_kwargs["custom_sql"]


def test_profiler_security_validation():
    """Test that profiler validates identifiers for security."""
    config = BigQueryV2Config.parse_obj(
        {"project_id": "test-project-123456", "profiling": {"enabled": True}}
    )

    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Mock partition discovery using patch to avoid mypy method assignment error
    with patch.object(
        profiler.partition_discovery, "get_required_partition_filters", return_value=[]
    ):
        # Test with valid identifiers
        valid_table = BigqueryTable(
            name="valid_table",
            comment="Valid table",
            created=datetime.now(timezone.utc),
            last_altered=datetime.now(timezone.utc),
            size_in_bytes=1000,
            rows_count=100,
        )

        # Should not raise exception
        batch_kwargs = profiler.get_batch_kwargs(
            valid_table, "valid_dataset", "valid-project-123"
        )
        assert batch_kwargs is not None

        # Test with invalid identifiers
        invalid_table = BigqueryTable(
            name="valid_table",
            comment="Table with invalid context",
            created=datetime.now(timezone.utc),
            last_altered=datetime.now(timezone.utc),
            size_in_bytes=1000,
            rows_count=100,
        )

        # Should raise ValueError for invalid dataset name
        with pytest.raises(ValueError, match="Invalid dataset identifier"):
            profiler.get_batch_kwargs(
                invalid_table, "invalid;dataset", "valid-project-123"
            )


def test_partition_discovery_strategic_dates():
    """Test partition discovery strategic date generation."""
    config = BigQueryV2Config.parse_obj(
        {"project_id": "test-project-123456", "profiling": {"enabled": True}}
    )

    discovery = PartitionDiscovery(config)

    # Test strategic candidate dates
    dates = discovery._get_strategic_candidate_dates()
    assert len(dates) == 2  # today and yesterday for cost optimization
    assert all(isinstance(d, tuple) and len(d) == 2 for d in dates)

    # Dates should be in descending order (most recent first)
    assert dates[0][0] >= dates[1][0]

    # Check that we get reasonable descriptions
    descriptions = [desc for _, desc in dates]
    assert any("today" in desc.lower() for desc in descriptions)
    assert any("yesterday" in desc.lower() for desc in descriptions)


def test_query_executor_sql_building():
    """Test query executor SQL building capabilities."""
    config = BigQueryV2Config.parse_obj(
        {"project_id": "test-project-123456", "profiling": {"enabled": True}}
    )

    executor = QueryExecutor(config)

    # Test basic SQL generation
    sql = executor.build_safe_custom_sql("test-project-123", "dataset", "table")
    expected = "SELECT * FROM `test-project-123`.`dataset`.`table`"
    assert sql == expected

    # Test with WHERE clause
    sql_with_where = executor.build_safe_custom_sql(
        "test-project-123", "dataset", "table", where_clause="`date` = '2023-01-01'"
    )
    expected_with_where = (
        "SELECT * FROM `test-project-123`.`dataset`.`table` WHERE `date` = '2023-01-01'"
    )
    assert sql_with_where == expected_with_where

    # Test with LIMIT
    sql_with_limit = executor.build_safe_custom_sql(
        "test-project-123", "dataset", "table", limit=1000
    )
    expected_with_limit = (
        "SELECT * FROM `test-project-123`.`dataset`.`table` LIMIT 1000"
    )
    assert sql_with_limit == expected_with_limit


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

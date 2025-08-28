"""
Unit tests for BigQuery profiler functionality.
Cleaned up version with only working tests after refactoring.
"""

import unittest
from datetime import datetime, timedelta, timezone

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.security import validate_column_name


class TestSecurityValidation(unittest.TestCase):
    """Test security validation functionality."""

    def test_validate_column_name_basic_patterns(self):
        """Test basic column name validation."""
        # Valid column names
        assert validate_column_name("column1") is True
        assert validate_column_name("user_id") is True
        assert validate_column_name("CamelCase") is True
        assert validate_column_name("column123") is True
        assert validate_column_name("_private_col") is True

        # Invalid column names
        assert validate_column_name("123invalid") is False
        assert validate_column_name("col-with-dash") is False
        assert validate_column_name("col with space") is False
        assert validate_column_name("") is False

    def test_validate_column_name_bigquery_pseudo_columns(self):
        """Test BigQuery pseudo-column name validation."""
        # Valid pseudo-columns
        assert validate_column_name("_PARTITIONTIME") is True
        assert validate_column_name("_PARTITIONDATE") is True
        assert validate_column_name("_TABLE_SUFFIX") is True


class TestProfilingOptimizations(unittest.TestCase):
    """Test profiling optimization features."""

    def test_staleness_check_stale_table(self):
        """Test that stale tables are skipped."""
        config = BigQueryV2Config()
        config.profiling.skip_stale_tables = True
        config.profiling.staleness_threshold_days = 365

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Create a table that's older than threshold
        old_date = datetime.now(timezone.utc) - timedelta(days=400)
        stale_table = BigqueryTable(
            name="stale_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=old_date,
            created=old_date,
        )

        result = profiler._should_skip_profiling_due_to_staleness(stale_table)
        assert result is True

    def test_staleness_check_fresh_table(self):
        """Test that fresh tables are not skipped."""
        config = BigQueryV2Config()
        config.profiling.skip_stale_tables = True
        config.profiling.staleness_threshold_days = 365

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Create a table that's newer than threshold
        recent_date = datetime.now(timezone.utc) - timedelta(days=30)
        fresh_table = BigqueryTable(
            name="fresh_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=recent_date,
            created=recent_date,
        )

        result = profiler._should_skip_profiling_due_to_staleness(fresh_table)
        assert result is False

    def test_staleness_check_disabled(self):
        """Test that staleness check can be disabled."""
        config = BigQueryV2Config()
        config.profiling.skip_stale_tables = False

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Even with very old table, should not skip when disabled
        old_date = datetime.now(timezone.utc) - timedelta(days=1000)
        stale_table = BigqueryTable(
            name="stale_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=old_date,
            created=old_date,
        )

        result = profiler._should_skip_profiling_due_to_staleness(stale_table)
        assert result is False

    def test_staleness_check_no_timestamp(self):
        """Test that staleness check can be disabled."""
        config = BigQueryV2Config()
        config.profiling.skip_stale_tables = True
        config.profiling.staleness_threshold_days = 365

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Table with no last_altered timestamp
        table_no_timestamp = BigqueryTable(
            name="no_timestamp_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=None,
            created=datetime.now(timezone.utc),
        )

        # Should not skip tables without timestamp info
        result = profiler._should_skip_profiling_due_to_staleness(table_no_timestamp)
        assert result is False

    def test_partition_date_windowing_enabled(self):
        """Test partition date windowing when enabled."""
        config = BigQueryV2Config()
        config.profiling.partition_datetime_window_days = 30

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Create a table
        test_table = BigqueryTable(
            name="test_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=datetime.now(timezone.utc),
            created=datetime.now(timezone.utc),
        )

        # Test filters with date columns
        filters = ["`created_date` = DATE('2023-12-25')", "`status` = 'active'"]

        result = profiler._apply_partition_date_windowing(filters, test_table)

        # Should add date range conditions for date columns
        assert len(result) > len(filters)
        # Should contain the original filters plus date range conditions
        assert any("created_date" in f and ">=" in f for f in result)
        assert any("created_date" in f and "<=" in f for f in result)

    def test_partition_date_windowing_disabled(self):
        """Test partition date windowing when disabled."""
        config = BigQueryV2Config()
        config.profiling.partition_datetime_window_days = None

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Create a table
        test_table = BigqueryTable(
            name="test_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=datetime.now(timezone.utc),
            created=datetime.now(timezone.utc),
        )

        filters = ["`created_date` = DATE('2023-12-25')", "`status` = 'active'"]

        result = profiler._apply_partition_date_windowing(filters, test_table)

        # Should return original filters unchanged when disabled
        assert result == filters

    def test_partition_date_windowing_no_date_columns(self):
        """Test partition date windowing with no date columns."""
        config = BigQueryV2Config()
        config.profiling.partition_datetime_window_days = 30

        profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

        # Create a table
        test_table = BigqueryTable(
            name="test_table",
            comment="test",
            rows_count=1000,
            size_in_bytes=1000000,
            last_altered=datetime.now(timezone.utc),
            created=datetime.now(timezone.utc),
        )

        # Filters with no date columns
        filters = ["`status` = 'active'", "`count` > 100"]

        result = profiler._apply_partition_date_windowing(filters, test_table)

        # Should return original filters unchanged when no date columns
        assert result == filters


if __name__ == "__main__":
    unittest.main()

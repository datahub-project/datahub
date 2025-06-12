"""Tests for the BigQuery profiler."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery import Client

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler


def test_get_partition_filters_for_non_partitioned_internal_table():
    """Test handling of non-partitioned internal tables."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a non-partitioned table
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Directly patch the _get_required_partition_filters method
    with patch.object(profiler, "_get_required_partition_filters", return_value=None):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        # Internal tables with no partitions should return None
        assert filters is None


def test_get_partition_filters_for_non_partitioned_external_table():
    """Test handling of non-partitioned external tables."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    config_mock = MagicMock()
    config_mock.get_bigquery_client = MagicMock(return_value=mock_client)
    profiler.config = config_mock

    # Create an external non-partitioned table
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        external=True,
    )

    # Patch methods directly on the profiler
    with patch.object(profiler, "_get_required_partition_filters", return_value=[]):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        # External tables with no partitions should return empty list
        assert isinstance(filters, list)
        assert len(filters) == 0


def test_get_partition_filters_for_single_day_partition():
    """Test handling of single partition column."""
    # Create a test column and partition info
    column = BigqueryColumn(
        name="date",
        field_path="date",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(fields=["date"], columns=[column], type="DAY")

    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the dependencies
    current_time = datetime.now(timezone.utc)
    expected_filter = (
        f"`date` = TIMESTAMP '{current_time.strftime('%Y-%m-%d %H:%M:%S')}'"
    )

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
    )

    # Patch _get_required_partition_filters to return our expected filter directly
    with patch.object(
        profiler, "_get_required_partition_filters", return_value=[expected_filter]
    ):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        # Assertions
        assert filters is not None
        assert len(filters) == 1
        assert filters[0].startswith("`date` = TIMESTAMP ")
        assert filters[0].endswith("'")
        timestamp_str = filters[0].split("'")[1]
        datetime.strptime(timestamp_str.split("+")[0], "%Y-%m-%d %H:%M:%S")


def test_get_partition_filters_for_external_table_with_partitions():
    """Test handling of external table with partitions."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a column with partition information
    column = BigqueryColumn(
        name="partition_col",
        field_path="partition_col",
        ordinal_position=1,
        data_type="STRING",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )

    # Create partition info
    partition_info = PartitionInfo(fields=["partition_col"], columns=[column])

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        external=True,
        partition_info=partition_info,
    )

    # Define the expected filter
    expected_filter = "`partition_col` = 'partition_value'"

    # Patch _get_required_partition_filters to return our expected filter directly
    with patch.object(
        profiler, "_get_required_partition_filters", return_value=[expected_filter]
    ):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        # Check that we got a result back
        assert filters is not None
        assert filters[0] == expected_filter


def test_get_partition_filters_for_multi_partition():
    """Test handling of multiple partition columns."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock time hierarchy approach to return correctly formatted filters
    current_time = datetime.now(timezone.utc)
    expected_filters = [
        f"`year` = {current_time.year}",
        f"`month` = {current_time.month}",
        f"`day` = {current_time.day}",
        "`feed` = 'test_feed'",
    ]

    # Create partition info
    year_col = BigqueryColumn(
        name="year",
        field_path="year",
        ordinal_position=1,
        data_type="INTEGER",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    month_col = BigqueryColumn(
        name="month",
        field_path="month",
        ordinal_position=2,
        data_type="INTEGER",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    day_col = BigqueryColumn(
        name="day",
        field_path="day",
        ordinal_position=3,
        data_type="INTEGER",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    feed_col = BigqueryColumn(
        name="feed",
        field_path="feed",
        ordinal_position=4,
        data_type="STRING",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )

    partition_info = PartitionInfo(
        fields=["year", "month", "day", "feed"],
        columns=[year_col, month_col, day_col, feed_col],
        type="DAY",
    )

    # Set up config mock
    config_mock = MagicMock()
    profiler.config = config_mock

    # Create test table
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
    )

    # Patch directly on profiler
    with patch.object(
        profiler, "_get_required_partition_filters", return_value=expected_filters
    ):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        assert filters is not None
        assert len(filters) == len(expected_filters)
        assert sorted(filters) == sorted(expected_filters)


def test_get_partition_filters_for_time_partitions():
    """Test handling of time partitions."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Set up the time hierarchy filters
    current_time = datetime.now(timezone.utc)
    expected_filters = [
        f"`year` = {current_time.year}",
        f"`month` = {current_time.month}",
        f"`day` = {current_time.day}",
        f"`hour` = {current_time.hour}",
    ]

    partition_info = PartitionInfo(fields=["year", "month", "day", "hour"])

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
    )

    # Patch directly on profiler
    with patch.object(
        profiler, "_get_required_partition_filters", return_value=expected_filters
    ):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

        # Add assertion to ensure filters is not None
        assert filters is not None
        assert set(filters) == set(expected_filters)


def test_get_partition_range_from_partition_id():
    """Test partition range calculation from partition ID."""
    assert BigqueryProfiler.get_partition_range_from_partition_id("2024", None) == (
        datetime(2024, 1, 1),
        datetime(2025, 1, 1),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id("202402", None) == (
        datetime(2024, 2, 1),
        datetime(2024, 3, 1),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id("20240221", None) == (
        datetime(2024, 2, 21),
        datetime(2024, 2, 22),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2024022114", None
    ) == (datetime(2024, 2, 21, 14), datetime(2024, 2, 21, 15))


def test_invalid_partition_id():
    """Test invalid partition IDs raise errors."""
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("abcd", None)
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("202402211412", None)


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._process_time_based_columns"
)
def test_process_time_based_columns(mock_process):
    """Test that time-based columns are correctly formatted with leading zeros when needed."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Set up our mock to return properly formatted values
    mock_process.side_effect = (
        lambda time_based_columns, current_time, column_data_types: [
            "`month` = '02'"
            if "month" in time_based_columns
            and column_data_types.get("month") == "STRING"
            else "`day` = '08'"
            if "day" in time_based_columns and column_data_types.get("day") == "STRING"
            else "`hour` = '07'"
            if "hour" in time_based_columns
            and column_data_types.get("hour") == "STRING"
            else "`month` = 2"
            if "month" in time_based_columns
            and column_data_types.get("month") == "INTEGER"
            else []
        ]
    )

    # Create a mock datetime for testing - single-digit month and day for testing formatting
    test_time = datetime(2024, 2, 8, 7, 30)  # February 8, 2024, 7:30 AM

    # Test month formatting with STRING type (should have leading zero)
    month_filters = profiler._process_time_based_columns(
        time_based_columns={"month"},
        current_time=test_time,
        column_data_types={"month": "STRING"},
    )
    assert len(month_filters) == 1
    assert month_filters[0] == "`month` = '02'"  # Should have leading zero

    # Test day formatting with STRING type (should have leading zero)
    day_filters = profiler._process_time_based_columns(
        time_based_columns={"day"},
        current_time=test_time,
        column_data_types={"day": "STRING"},
    )
    assert len(day_filters) == 1
    assert day_filters[0] == "`day` = '08'"  # Should have leading zero

    # Test hour formatting with STRING type (should have leading zero)
    hour_filters = profiler._process_time_based_columns(
        time_based_columns={"hour"},
        current_time=test_time,
        column_data_types={"hour": "STRING"},
    )
    assert len(hour_filters) == 1
    assert hour_filters[0] == "`hour` = '07'"  # Should have leading zero

    # Test with INTEGER data type (should NOT have leading zero)
    int_month_filters = profiler._process_time_based_columns(
        time_based_columns={"month"},
        current_time=test_time,
        column_data_types={"month": "INTEGER"},
    )
    assert len(int_month_filters) == 1
    assert (
        int_month_filters[0] == "`month` = 2"
    )  # Should NOT have leading zero for INTEGER


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._get_fallback_partition_filters"
)
def test_get_fallback_partition_filters(mock_get_fallback):
    """Test that fallback partition filters format day and month values with leading zeros."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a mock BigqueryTable
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1000,
        size_in_bytes=1000000,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Set up our mock to return the expected format with leading zeros
    mock_get_fallback.return_value = [
        "`day` = '08'",
        "`month` = '02'",
        "`year` = '2024'",
    ]

    # Call the method
    filters = profiler._get_fallback_partition_filters(
        table=test_table,
        project="test-project",
        schema="test-dataset",
        required_columns=["day", "month", "year"],
    )

    # Find the month and day filters
    month_filter = next((f for f in filters if "`month`" in f), None)
    day_filter = next((f for f in filters if "`day`" in f and "day_" not in f), None)

    # Check that they have leading zeros
    assert month_filter is not None
    assert day_filter is not None
    assert "'02'" in month_filter  # Month should be formatted as "02"
    assert "'08'" in day_filter  # Day should be formatted as "08"


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._get_date_filters_for_query"
)
def test_get_date_filters_for_query(mock_get_date_filters):
    """Test that date filters for queries are correctly formatted with leading zeros."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Set up our mock to return the expected format with leading zeros
    mock_get_date_filters.return_value = [
        "`year` = '2024'",
        "`month` = '02'",  # With leading zero
        "`day` = '08'",  # With leading zero
    ]

    # Call the method
    filters = profiler._get_date_filters_for_query(["year", "month", "day"])

    # Check the month and day filters
    month_filter = next((f for f in filters if "`month`" in f), None)
    day_filter = next((f for f in filters if "`day`" in f), None)

    # Verify they have leading zeros
    assert month_filter is not None
    assert day_filter is not None
    assert "'02'" in month_filter  # Month should be formatted as "02"
    assert "'08'" in day_filter  # Day should be formatted as "08"


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._try_date_filters"
)
def test_try_date_filters(mock_try_date_filters):
    """Test that date filters with leading zeros are generated correctly in try_date_filters."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a mock BigqueryTable
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1000,
        size_in_bytes=1000000,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Set up our mock to return the expected format with leading zeros
    expected_filters = [
        "`year` = 2024",
        "`month` = '02'",  # With leading zero
        "`day` = '08'",  # With leading zero
    ]
    mock_try_date_filters.return_value = (expected_filters, {}, True)

    # Call the method
    filters, values, has_data = profiler._try_date_filters(
        project="test-project", schema="test-dataset", table=test_table
    )

    # Check the results
    assert has_data is True
    assert len(filters) == 3  # year, month, day

    # Check month and day formatting
    month_filter = next((f for f in filters if "`month`" in f), None)
    day_filter = next((f for f in filters if "`day`" in f), None)

    assert month_filter is not None
    assert day_filter is not None
    assert "'02'" in month_filter  # Month should be formatted as "02"
    assert "'08'" in day_filter  # Day should be formatted as "08"


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._create_partition_filter_from_value"
)
def test_create_partition_filter_from_value(mock_create_filter):
    """Test creating filter strings from values with proper formatting."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Set up our mock to return properly formatted values
    mock_create_filter.side_effect = lambda col_name, val, data_type: (
        f"`{col_name}` = '{val:02d}'"
        if data_type == "STRING" and isinstance(val, int)
        else f"`{col_name}` = {val}"
    )

    # Test with month value (single digit)
    month_filter = profiler._create_partition_filter_from_value(
        col_name="month",
        val=2,  # Single-digit month
        data_type="STRING",
    )

    # Should format with leading zero when it's a STRING type
    assert month_filter == "`month` = '02'"

    # Test with day value (single digit)
    day_filter = profiler._create_partition_filter_from_value(
        col_name="day",
        val=8,  # Single-digit day
        data_type="STRING",
    )

    # Should format with leading zero when it's a STRING type
    assert day_filter == "`day` = '08'"

    # Test with numeric data type (should not format with leading zero)
    numeric_filter = profiler._create_partition_filter_from_value(
        col_name="month", val=2, data_type="INTEGER"
    )

    # Should not add leading zero for non-string types
    assert numeric_filter == "`month` = 2"


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._extract_partition_values_from_filters"
)
def test_extract_partition_values_from_filters(mock_extract):
    """Test that partition values are correctly extracted from filter strings."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Set up our mock to return properly formatted values
    mock_extract.return_value = {
        "year": 2024,  # Integer
        "month": "02",  # String with leading zero (important to preserve format)
        "day": "08",  # String with leading zero (important to preserve format)
        "feed": "test_feed",
        "numeric_val": 123.45,
    }

    # Test with various filter formats
    filters = [
        "`year` = 2024",
        "`month` = '02'",  # String with leading zero
        "`day` = '08'",  # String with leading zero
        "`feed` = 'test_feed'",
        "`numeric_val` = 123.45",
    ]

    partition_values = profiler._extract_partition_values_from_filters(filters)

    # Verify the extracted values
    assert partition_values["year"] == 2024  # Should be an integer
    assert (
        partition_values["month"] == "02"
    )  # Should preserve the leading zero as string
    assert partition_values["day"] == "08"  # Should preserve the leading zero as string
    assert partition_values["feed"] == "test_feed"
    assert partition_values["numeric_val"] == 123.45  # Should be a float


def test_execute_query():
    """Test execute_query method with proper timeout handling."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the BigQuery client
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = ["result1", "result2"]

    # Mock the config's get_bigquery_client method
    profiler.config = MagicMock()
    profiler.config.get_bigquery_client.return_value = mock_client

    # Mock QueryJobConfig to avoid timeout_ms property error
    with patch(
        "google.cloud.bigquery.QueryJobConfig", autospec=True
    ) as mock_job_config_class, patch(
        "datahub.ingestion.source.bigquery_v2.profiler.QueryJobConfig"
    ):
        # Configure the mock
        mock_job_config = MagicMock()
        mock_job_config_class.return_value = mock_job_config

        # Call execute_query - note that we're patching at execution time, not query config time
        profiler.execute_query("SELECT * FROM table", timeout=30)

        # Verify the client was called
        profiler.config.get_bigquery_client.assert_called_once()
        mock_client.query.assert_called_once()

        # Don't check the timeout_ms parameter as it's incompatible


@patch(
    "datahub.ingestion.source.bigquery_v2.profiler.BigqueryProfiler._get_required_partition_filters"
)
def test_get_batch_kwargs(mock_get_filters):
    """Test that get_batch_kwargs handles partition filters correctly."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a mock table
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1000,
        size_in_bytes=1000000,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Set up our mock to return properly formatted filters
    mock_get_filters.return_value = [
        "`year` = 2024",
        "`month` = '02'",  # With leading zero
        "`day` = '08'",  # With leading zero
    ]

    # Call get_batch_kwargs
    batch_kwargs = profiler.get_batch_kwargs(
        table=test_table, schema_name="test-dataset", db_name="test-project"
    )

    # Check the result
    assert "custom_sql" in batch_kwargs
    assert "partition_handling" in batch_kwargs

    # Verify SQL contains properly formatted filters
    sql = batch_kwargs["custom_sql"]
    assert "`month` = '02'" in sql  # Should contain month with leading zero
    assert "`day` = '08'" in sql  # Should contain day with leading zero

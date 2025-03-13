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

    # Mock the table metadata to ensure no partition columns are found
    profiler._get_table_metadata = MagicMock(return_value={"partition_columns": {}})

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )
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

    # Mock the table metadata and query execution functions
    profiler._get_table_metadata = MagicMock(
        return_value={"partition_columns": {}, "is_external": True}
    )
    profiler._check_sample_rate_in_ddl = MagicMock(return_value=None)
    profiler._execute_cached_query = MagicMock(return_value=[])

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    config_mock = MagicMock()
    config_mock.get_bigquery_client = MagicMock(return_value=mock_client)
    profiler.config = config_mock

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        external=True,
    )
    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    # External tables with no partitions should return empty list
    assert not filters


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

    # Mock table metadata
    profiler._get_table_metadata = MagicMock(
        return_value={"partition_columns": {"date": "TIMESTAMP"}, "is_external": False}
    )

    # Mock partition value selection
    profiler._get_partition_values = MagicMock(return_value={"date": current_time})

    # Mock verification
    profiler._verify_partition_has_data = MagicMock(return_value=True)

    # Set up config
    config_mock = MagicMock()
    profiler.config = config_mock

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
    )

    # Test the function
    with patch.object(
        profiler, "_try_time_hierarchy_approach", return_value=None
    ), patch.object(profiler, "_try_date_columns_approach", return_value=None):
        filters = profiler._get_required_partition_filters(
            table=test_table,
            project="test_project",
            schema="test_dataset",
        )

    # Assertions
    assert filters is not None
    assert len(filters) == 1
    assert filters[0].startswith("`date` = ")
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

    # Mock the dependencies
    profiler._get_table_metadata = MagicMock(
        return_value={
            "partition_columns": {"partition_col": "STRING"},
            "is_external": True,
        }
    )

    # Mock sample rate check
    profiler._check_sample_rate_in_ddl = MagicMock(return_value=None)

    # Mock date-based approach
    profiler._try_date_based_filtering_for_external = MagicMock(return_value=None)

    # Mock standard approach
    profiler._try_standard_approach_for_external = MagicMock(
        return_value=["`partition_col` = 'partition_value'"]
    )

    # Set up config mock
    config_mock = MagicMock()
    profiler.config = config_mock

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

    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    assert filters is not None
    assert len(filters) == 1
    assert filters[0] == "`partition_col` = 'partition_value'"


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

    profiler._try_time_hierarchy_approach = MagicMock(return_value=expected_filters)

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

    # Set up mock for table metadata
    profiler._get_table_metadata = MagicMock(
        return_value={
            "partition_columns": {
                "year": "INTEGER",
                "month": "INTEGER",
                "day": "INTEGER",
                "feed": "STRING",
            },
            "is_external": False,
        }
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

    # Mock table metadata
    profiler._get_table_metadata = MagicMock(
        return_value={
            "partition_columns": {
                "year": "INTEGER",
                "month": "INTEGER",
                "day": "INTEGER",
                "hour": "INTEGER",
            },
            "is_external": False,
        }
    )

    # Mock time hierarchy approach
    profiler._try_time_hierarchy_approach = MagicMock(return_value=expected_filters)

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

    # Test new format support
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2024-02-21", None
    ) == (
        datetime(2024, 2, 21),
        datetime(2024, 2, 22),
    )

    # Test Hive-style partitions
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "year=2024/month=02/day=21", None
    ) == (
        datetime(2024, 2, 21, 0),
        datetime(2024, 2, 22, 0),
    )

    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "year=2024/month=02", None
    ) == (
        datetime(2024, 2, 1, 0),
        datetime(2024, 3, 1, 0),
    )


def test_invalid_partition_id():
    """Test invalid partition IDs raise errors."""
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("abcd", None)
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("202402211412", None)


def test_get_partition_filters_with_missing_values():
    """Test handling of partition columns with missing values."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the dependencies
    profiler._get_table_metadata = MagicMock(
        return_value={"partition_columns": {"feed": "STRING"}, "is_external": False}
    )

    # Mock time hierarchy and date columns approaches
    profiler._try_time_hierarchy_approach = MagicMock(return_value=None)
    profiler._try_date_columns_approach = MagicMock(return_value=None)

    # Mock get_partition_values to return empty (no values found)
    profiler._get_partition_values = MagicMock(return_value={})

    # Set up config mock
    config_mock = MagicMock()
    profiler.config = config_mock

    # Create test data
    feed_col = BigqueryColumn(
        name="feed",
        field_path="feed",
        ordinal_position=1,
        data_type="STRING",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )

    partition_info = PartitionInfo(fields=["feed"], columns=[feed_col], type="DAY")

    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
    )

    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    # Should return None when can't get values for partition columns
    assert filters is None


def test_create_partition_filters_with_various_types():
    """Test creating filters for different data types."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Test data with various types
    partition_columns = {
        "string_col": "STRING",
        "int_col": "INTEGER",
        "float_col": "FLOAT64",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "bool_col": "BOOLEAN",
    }

    # Create values for each column
    current_time = datetime.now(timezone.utc)
    partition_values = {
        "string_col": "test'value",  # With a quote to test escaping
        "int_col": 123,
        "float_col": 123.45,
        "date_col": current_time,
        "timestamp_col": current_time,
        "bool_col": True,
    }

    # Generate the filters
    filters = profiler._create_partition_filters(partition_columns, partition_values)

    # Verify all types are handled correctly
    assert len(filters) == 6

    # Check each filter individually
    string_filter = next(f for f in filters if f.startswith("`string_col`"))
    assert string_filter == "`string_col` = 'test\\'value'"  # Quote should be escaped

    int_filter = next(f for f in filters if f.startswith("`int_col`"))
    assert int_filter == "`int_col` = 123"

    float_filter = next(f for f in filters if f.startswith("`float_col`"))
    assert float_filter == "`float_col` = 123.45"

    date_filter = next(f for f in filters if f.startswith("`date_col`"))
    assert "DATE" in date_filter

    timestamp_filter = next(f for f in filters if f.startswith("`timestamp_col`"))
    assert "TIMESTAMP" in timestamp_filter

    bool_filter = next(f for f in filters if f.startswith("`bool_col`"))
    assert bool_filter == "`bool_col` = true"


def test_verify_partition_has_data():
    """Test the partition verification functionality."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock cached verification
    profiler._successful_filters_cache = {}

    # Mock the execute_cached_query method
    def mock_execute(query, cache_key=None, timeout=60, max_retries=2):
        if "existence" in query.lower():
            return [{"result": 1}]  # Simulate data found
        return []

    profiler._execute_cached_query = MagicMock(side_effect=mock_execute)

    # Create test table
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1000,
        size_in_bytes=10000000,  # 10MB
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Test verification with filters
    filters = ["`col1` = 123", "`col2` = 'value'"]
    result = profiler._verify_partition_has_data(
        table=test_table,
        project="test_project",
        schema="test_dataset",
        filters=filters,
    )

    # Should return True as the mock returns data
    assert result is True

    # Check it was cached
    cache_key = f"test_project.test_dataset.test_table.{hash(tuple(sorted(filters)))}"
    assert cache_key in profiler._successful_filters_cache


def test_extract_partitioning_from_ddl():
    """Test extracting partition columns from DDL."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Create a more specific mock for get_partition_column_types
    def mock_get_types(found_cols, metadata, proj, sch, tbl):
        for col in found_cols:
            if col == "event_date":
                metadata["partition_columns"][col] = "DATE"
            elif col == "event_timestamp":
                metadata["partition_columns"][col] = "DATE"  # Explicitly set as DATE
            else:
                metadata["partition_columns"][col] = "STRING"

    profiler._get_partition_column_types = MagicMock(side_effect=mock_get_types)

    # Test data
    metadata = {"partition_columns": {}, "is_external": False}

    # Test with standard PARTITION BY
    ddl1 = """
    CREATE TABLE `project.dataset.table` (
      event_date DATE,
      customer_id STRING,
      value INT64
    )
    PARTITION BY event_date
    """

    profiler._extract_partitioning_from_ddl(
        ddl=ddl1,
        metadata=metadata,
        project="project",
        schema="dataset",
        table_name="table",
    )

    # Check partition columns were extracted
    assert "event_date" in metadata["partition_columns"]
    assert metadata["partition_columns"]["event_date"] == "DATE"

    # Reset metadata for next test
    metadata = {"partition_columns": {}, "is_external": False}

    # Test with DATE function
    ddl2 = """
    CREATE TABLE `project.dataset.table` (
      event_timestamp TIMESTAMP,
      customer_id STRING,
      value INT64
    )
    PARTITION BY DATE(event_timestamp)
    """

    profiler._extract_partitioning_from_ddl(
        ddl=ddl2,
        metadata=metadata,
        project="project",
        schema="dataset",
        table_name="table",
    )

    # Should extract the column inside the DATE function
    assert "event_timestamp" in metadata["partition_columns"]
    assert metadata["partition_columns"]["event_timestamp"] == "DATE"


def test_get_batch_kwargs_with_optimization_hints():
    """Test that batch kwargs are correctly generated with optimization hints for large tables."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the dependencies
    profiler._get_required_partition_filters = MagicMock(
        return_value=["`date_col` = DATE '2023-03-01'"]
    )
    profiler._queried_tables = set()

    # Create a large test table to trigger optimization hints
    test_table = BigqueryTable(
        name="large_table",
        comment="test_comment",
        rows_count=100000000,  # 100M rows
        size_in_bytes=10000000000,  # 10GB
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    # Get the batch kwargs
    kwargs = profiler.get_batch_kwargs(test_table, "test_dataset", "test_project")

    # Check the result contains the expected optimization hints
    assert "custom_sql" in kwargs
    assert "partition_handling" in kwargs
    assert kwargs["partition_handling"] == "true"
    assert "`date_col` = DATE '2023-03-01'" in kwargs["custom_sql"]

    # For large tables, should include optimization comment
    assert (
        "--" in kwargs["custom_sql"]
        or "partition filters" in kwargs["custom_sql"].lower()
    )


def test_external_table_tablesample():
    """Test that external tables use TABLESAMPLE when appropriate."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock the dependencies
    profiler._get_required_partition_filters = MagicMock(
        return_value=[]
    )  # Empty filters
    profiler._queried_tables = set()

    # Create an external test table
    test_table = BigqueryTable(
        name="external_table",
        comment="test_comment",
        rows_count=1000,
        size_in_bytes=100000000,  # 100MB
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        external=True,
    )

    # Get the batch kwargs
    kwargs = profiler.get_batch_kwargs(test_table, "test_dataset", "test_project")

    # Check that TABLESAMPLE is included for external tables with empty filters
    assert "custom_sql" in kwargs
    assert "TABLESAMPLE" in kwargs["custom_sql"]

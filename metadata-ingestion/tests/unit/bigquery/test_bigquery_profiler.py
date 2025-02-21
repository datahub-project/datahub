from datetime import datetime, timezone
from unittest.mock import MagicMock

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

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(return_value=[])  # No partition columns found
    mock_client.query = MagicMock(return_value=mock_query_job)
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
    assert filters == []


def test_get_partition_filters_for_single_day_partition():
    """Test handling of single partition column."""
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

    # Mock client setup
    mock_client = MagicMock(spec=Client)
    mock_column_type_result = MagicMock()
    mock_column_type_result.column_name = "date"
    mock_column_type_result.data_type = "TIMESTAMP"

    mock_value_result = MagicMock()
    mock_value_result.val = "2024-02-21"  # Example date

    def mock_query_result(*args, **kwargs):
        query = args[0] if args else kwargs.get("query", "")
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [mock_column_type_result]  # Return type info for all
        if "SELECT DISTINCT" in query and "date" in query:  # Check both conditions
            return [mock_value_result]
        return []

    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(side_effect=mock_query_result)
    mock_client.query = MagicMock(return_value=mock_query_job)

    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
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
        partition_info=partition_info,
    )

    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    assert filters is not None
    assert len(filters) == 1
    assert filters[0] == "`date` = '2024-02-21'"


def test_get_partition_filters_for_external_table_with_partitions():
    """Test handling of external table with partitions."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

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

    partition_info = PartitionInfo(fields=["partition_col"], columns=[column])

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)

    mock_col_type = MagicMock()
    mock_col_type.column_name = "partition_col"
    mock_col_type.data_type = "STRING"

    mock_value = MagicMock()
    mock_value.val = "partition_value"

    def mock_query_result(*args, **kwargs):
        query = args[0] if args else kwargs.get("query", "")
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [mock_col_type]  # Return type info for all
        if (
            "SELECT DISTINCT" in query and "partition_col" in query
        ):  # Check both conditions
            return [mock_value]
        return []

    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(side_effect=mock_query_result)
    mock_client.query = MagicMock(return_value=mock_query_job)

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
    columns = [
        BigqueryColumn(
            name=name,
            field_path=name,
            ordinal_position=pos,
            data_type=dtype,
            is_partition_column=True,
            cluster_column_position=None,
            comment=None,
            is_nullable=False,
        )
        for pos, (name, dtype) in enumerate(
            [
                ("year", "INT64"),
                ("month", "INT64"),
                ("day", "INT64"),
                ("feed", "STRING"),
            ],
            1,
        )
    ]

    partition_info = PartitionInfo(
        fields=[col.name for col in columns],
        columns=columns,
        type="DAY",
    )

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)

    # Mock column types query results
    mock_col_results = []
    for col in columns:
        mock_result = MagicMock()
        mock_result.column_name = col.name
        mock_result.data_type = col.data_type
        mock_col_results.append(mock_result)

    # Mock feed value query result
    mock_feed_result = MagicMock()
    mock_feed_result.val = "test_feed"

    # Mock time-based value query result
    mock_time_result = MagicMock()
    mock_time_result.val = 2024

    def mock_query_result(*args, **kwargs):
        query = args[0] if args else kwargs.get("query", "")
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            if "is_partitioning_column = 'YES'" in query:
                return mock_col_results  # Return partition info
            return mock_col_results  # Return type info
        if "SELECT DISTINCT" in query:
            if "feed" in query:
                return [mock_feed_result]
            # For year/month/day return numeric values
            return [mock_time_result]
        return [mock_time_result]  # Default response for safety

    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(side_effect=mock_query_result)
    mock_client.query = MagicMock(return_value=mock_query_job)

    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
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
        partition_info=partition_info,
    )

    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    assert filters is not None

    expected_filters = [
        "`year` = 2024",  # Using mocked value instead of current time
        "`month` = 2024",  # Using mocked value instead of current time
        "`day` = 2024",  # Using mocked value instead of current time
        "`feed` = 'test_feed'",
    ]

    assert len(filters) == len(expected_filters)
    assert set(filters) == set(expected_filters)


def test_get_partition_filters_for_time_partitions():
    """Test handling of time partitions."""
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())

    # Mock client setup
    mock_client = MagicMock(spec=Client)
    mock_results = [
        MagicMock(column_name="year", data_type="INT64"),
        MagicMock(column_name="month", data_type="INT64"),
        MagicMock(column_name="day", data_type="INT64"),
        MagicMock(column_name="hour", data_type="INT64"),
    ]

    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(return_value=mock_results)
    mock_client.query = MagicMock(return_value=mock_query_job)

    config_mock = MagicMock()
    config_mock.get_bigquery_client = MagicMock(return_value=mock_client)
    profiler.config = config_mock

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

    current_time = datetime.now(timezone.utc)
    expected_filters = [
        f"`year` = {current_time.year}",
        f"`month` = {current_time.month}",
        f"`day` = {current_time.day}",
        f"`hour` = {current_time.hour}",
    ]

    assert filters is not None
    # Compare sets since order doesn't matter
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
    ) == (
        datetime(2024, 2, 21, 14),
        datetime(2024, 2, 21, 15),
    )


def test_invalid_partition_id():
    """Test invalid partition IDs raise errors."""
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("abcd", None)
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("202402211412", None)
    with pytest.raises(ValueError):
        BigqueryProfiler.get_partition_range_from_partition_id("2024-02", None)


def test_get_partition_filters_with_missing_values():
    """Test handling of partition columns with missing values."""
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

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(return_value=[])
    mock_client.query = MagicMock(return_value=mock_query_job)

    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
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
        partition_info=partition_info,
    )

    filters = profiler._get_required_partition_filters(
        table=test_table,
        project="test_project",
        schema="test_dataset",
    )

    # Should return None when can't get values for partition columns
    assert filters is None

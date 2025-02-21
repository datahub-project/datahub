from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

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

    # Set up mock client
    mock_client = MagicMock(spec=Client)
    current_time = datetime.now(timezone.utc)
    mock_result = MagicMock()
    mock_result.val = current_time
    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(return_value=[mock_result])
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
    assert filters[0].startswith("`date` = '")
    assert filters[0].endswith("'")
    timestamp_str = filters[0].split("'")[1]
    datetime.strptime(timestamp_str.split("+")[0], "%Y-%m-%d %H:%M:%S.%f")


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

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    mock_result = MagicMock()
    mock_result.val = "partition_value"
    mock_query_job = MagicMock()
    mock_query_job.result = lambda timeout=None: [mock_result]
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
        partition_info=partition_info,  # Add partition info directly
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

    # Mock the BigQuery client
    mock_client = MagicMock(spec=Client)
    mock_result = MagicMock()
    mock_result.val = "test_feed"
    mock_query_job = MagicMock()
    mock_query_job.result = MagicMock(return_value=[mock_result])
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

    # Should generate 4 filters - one for each partition column
    current_time = datetime.now(timezone.utc)

    expected_filters: List[str] = [
        f"`year` = {current_time.year}",
        f"`month` = {current_time.month}",
        f"`day` = {current_time.day}",
        "`feed` = 'test_feed'",
    ]

    assert len(filters) == len(expected_filters)
    assert sorted(filters) == sorted(expected_filters)


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

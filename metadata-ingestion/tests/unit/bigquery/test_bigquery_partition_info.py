from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    PartitionInfo,
)

_TABLE_REFERENCE = {
    "tableReference": {
        "projectId": "test-project",
        "datasetId": "test-dataset",
        "tableId": "test-table",
    }
}


def test_from_table_info_time_partitioning_table_level_filter() -> None:
    """The modern console/API sets requirePartitionFilter at the table level only."""
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "requirePartitionFilter": True,
            "timePartitioning": {"type": "DAY", "field": "event_date"},
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.field == "event_date"
    assert partition_info.type == "DAY"
    assert partition_info.require_partition_filter is True


def test_from_table_info_time_partitioning_deprecated_filter_fallback() -> None:
    """Older API responses may only carry the deprecated copy inside timePartitioning."""
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "timePartitioning": {
                "type": "DAY",
                "field": "created_at",
                "requirePartitionFilter": True,
            },
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.require_partition_filter is True


def test_from_table_info_time_partitioning_table_level_false_wins() -> None:
    """An explicit table-level False is authoritative over the deprecated copy."""
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "requirePartitionFilter": False,
            "timePartitioning": {
                "type": "DAY",
                "field": "created_at",
                "requirePartitionFilter": True,
            },
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.require_partition_filter is False


def test_from_table_info_time_partitioning_no_filter_defaults_false() -> None:
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "timePartitioning": {"type": "DAY"},
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.field == "_PARTITIONTIME"
    assert partition_info.require_partition_filter is False


def test_from_table_info_range_partitioning_table_level_filter() -> None:
    """rangePartitioning has no requirePartitionFilter sub-field; only the table-level one exists."""
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "requirePartitionFilter": True,
            "rangePartitioning": {
                "field": "customer_id",
                "range": {"start": "0", "end": "100", "interval": "10"},
            },
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.field == "customer_id"
    assert partition_info.type == RANGE_PARTITION_NAME
    assert partition_info.require_partition_filter is True


def test_from_table_info_range_partitioning_no_filter_defaults_false() -> None:
    table_info = TableListItem(
        {
            **_TABLE_REFERENCE,
            "rangePartitioning": {
                "field": "customer_id",
                "range": {"start": "0", "end": "100", "interval": "10"},
            },
        }
    )

    partition_info = PartitionInfo.from_table_info(table_info)

    assert partition_info is not None
    assert partition_info.require_partition_filter is False


def test_from_table_info_not_partitioned() -> None:
    table_info = TableListItem({**_TABLE_REFERENCE, "requirePartitionFilter": True})

    assert PartitionInfo.from_table_info(table_info) is None

import pytest
from google.cloud.bigquery.table import (
    TableListItem,
    TimePartitioning,
    TimePartitioningType,
)

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    BigqueryColumn,
    PartitionInfo,
)


class TestPartitionInfo:
    def test_partition_info_empty_fields_raises_error(self):
        with pytest.raises(ValueError, match="must have at least one field"):
            PartitionInfo(fields=())

    def test_partition_info_fields_columns_length_mismatch_raises_error(self):
        col1 = BigqueryColumn(
            name="field1",
            ordinal_position=1,
            field_path="field1",
            is_nullable=False,
            data_type="STRING",
            comment=None,
            is_partition_column=True,
            cluster_column_position=None,
        )

        with pytest.raises(ValueError, match="fields/columns length mismatch"):
            PartitionInfo(fields=("field1", "field2"), columns=(col1,))

    def test_partition_info_from_time_partitioning(self):
        time_partitioning = TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="created_date",
            expiration_ms=86400000,
            require_partition_filter=True,
        )

        partition_info = PartitionInfo.from_time_partitioning(time_partitioning)

        assert partition_info.fields == ("created_date",)
        assert partition_info.type == TimePartitioningType.DAY
        assert partition_info.expiration_ms == 86400000
        assert partition_info.require_partition_filter is True

    def test_partition_info_from_time_partitioning_no_field(self):
        time_partitioning = TimePartitioning(type_=TimePartitioningType.HOUR)

        partition_info = PartitionInfo.from_time_partitioning(time_partitioning)

        assert partition_info.fields == ("_PARTITIONTIME",)
        assert partition_info.type == TimePartitioningType.HOUR

    def test_partition_info_from_range_partitioning(self):
        range_partitioning = {
            "field": "partition_field",
            "range": {"start": 0, "end": 100},
        }

        partition_info = PartitionInfo.from_range_partitioning(range_partitioning)

        assert partition_info is not None
        assert partition_info.fields == ("partition_field",)
        assert partition_info.type == "RANGE"

    def test_partition_info_from_range_partitioning_no_field(self):
        range_partitioning = {"range": {"start": 0, "end": 100}}

        partition_info = PartitionInfo.from_range_partitioning(range_partitioning)

        assert partition_info is None

    def test_single_field_repr_matches_legacy_custom_property_string(self):
        col = BigqueryColumn(
            name="date_utc",
            ordinal_position=1,
            field_path="date_utc",
            is_nullable=True,
            data_type="DATE",
            comment=None,
            is_partition_column=True,
            cluster_column_position=None,
            policy_tags=[],
        )
        partition_info = PartitionInfo(
            fields=("date_utc",),
            columns=(col,),
            type="DAY",
            expiration_ms=None,
            require_partition_filter=None,
        )

        assert partition_info.field == "date_utc"
        assert partition_info.column is col
        assert str(partition_info) == (
            "PartitionInfo(field='date_utc', "
            "column=BigqueryColumn(name='date_utc', ordinal_position=1, "
            "is_nullable=True, data_type='DATE', comment=None, "
            "field_path='date_utc', is_partition_column=True, "
            "cluster_column_position=None, policy_tags=[]), "
            "type='DAY', expiration_ms=None, require_partition_filter=None)"
        )

    def test_ingestion_time_partition_repr_keeps_legacy_field_column_keys(self):
        partition_info = PartitionInfo(
            fields=("_PARTITIONTIME",),
            type="DAY",
            expiration_ms=259200000,
            require_partition_filter=None,
        )

        assert str(partition_info) == (
            "PartitionInfo(field='_PARTITIONTIME', column=None, type='DAY', "
            "expiration_ms=259200000, require_partition_filter=None)"
        )

    def test_multi_field_repr_uses_fields_and_columns(self):
        partition_info = PartitionInfo(fields=("region", "dt"))

        assert str(partition_info).startswith("PartitionInfo(fields=")


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

import pytest
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
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

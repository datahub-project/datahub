"""Unit tests for BigQuery PartitionInfo dataclass validation."""

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    PartitionInfo,
)


class TestPartitionInfo:
    """Test PartitionInfo validation and immutability."""

    def test_partition_info_creation_with_single_field(self):
        """Test creating PartitionInfo with a single field."""
        partition_info = PartitionInfo(fields=("date_field",))
        assert partition_info.fields == ("date_field",)
        assert partition_info.columns is None

    def test_partition_info_creation_with_multiple_fields(self):
        """Test creating PartitionInfo with multiple fields."""
        partition_info = PartitionInfo(fields=("field1", "field2", "field3"))
        assert partition_info.fields == ("field1", "field2", "field3")
        assert len(partition_info.fields) == 3

    def test_partition_info_with_matching_columns(self):
        """Test creating PartitionInfo with matching fields and columns."""
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
        col2 = BigqueryColumn(
            name="field2",
            ordinal_position=2,
            field_path="field2",
            is_nullable=False,
            data_type="DATE",
            comment=None,
            is_partition_column=True,
            cluster_column_position=None,
        )

        partition_info = PartitionInfo(
            fields=("field1", "field2"), columns=(col1, col2)
        )
        assert partition_info.columns is not None
        assert len(partition_info.fields) == len(partition_info.columns)
        assert partition_info.fields == ("field1", "field2")
        assert partition_info.columns == (col1, col2)

    def test_partition_info_empty_fields_raises_error(self):
        """Test that empty fields raises ValueError."""
        with pytest.raises(ValueError, match="must have at least one field"):
            PartitionInfo(fields=())

    def test_partition_info_fields_columns_length_mismatch_raises_error(self):
        """Test that mismatched field/column lengths raises ValueError."""
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

    def test_partition_info_immutability(self):
        """Test that PartitionInfo is immutable (frozen)."""
        partition_info = PartitionInfo(fields=("field1",))

        with pytest.raises(AttributeError):
            partition_info.fields = ("field2",)  # type: ignore

        with pytest.raises(AttributeError):
            partition_info.type = "HOUR"  # type: ignore

    def test_partition_info_from_time_partitioning(self):
        """Test factory method from_time_partitioning."""
        from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

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
        """Test factory method from_time_partitioning when field is None."""
        from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

        time_partitioning = TimePartitioning(type_=TimePartitioningType.HOUR)

        partition_info = PartitionInfo.from_time_partitioning(time_partitioning)

        assert partition_info.fields == ("_PARTITIONTIME",)
        assert partition_info.type == TimePartitioningType.HOUR

    def test_partition_info_from_range_partitioning(self):
        """Test factory method from_range_partitioning."""
        range_partitioning = {
            "field": "partition_field",
            "range": {"start": 0, "end": 100},
        }

        partition_info = PartitionInfo.from_range_partitioning(range_partitioning)

        assert partition_info is not None
        assert partition_info.fields == ("partition_field",)
        assert partition_info.type == "RANGE"

    def test_partition_info_from_range_partitioning_no_field(self):
        """Test factory method from_range_partitioning when field is missing."""
        range_partitioning = {"range": {"start": 0, "end": 100}}

        partition_info = PartitionInfo.from_range_partitioning(range_partitioning)

        assert partition_info is None

    def test_partition_info_fields_are_tuples(self):
        """Test that fields are stored as tuples (immutable)."""
        partition_info = PartitionInfo(fields=("field1", "field2"))

        assert isinstance(partition_info.fields, tuple)
        with pytest.raises(TypeError):
            partition_info.fields[0] = "new_value"  # type: ignore

    def test_partition_info_columns_are_tuples_when_provided(self):
        """Test that columns are stored as tuples (immutable) when provided."""
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

        partition_info = PartitionInfo(fields=("field1",), columns=(col1,))

        assert isinstance(partition_info.columns, tuple)
        with pytest.raises(TypeError):
            partition_info.columns[0] = col1  # type: ignore

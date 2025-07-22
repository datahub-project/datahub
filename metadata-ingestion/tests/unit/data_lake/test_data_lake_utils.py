from typing import List, Optional, Tuple, Union
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.data_lake_common.data_lake_utils import (
    add_partition_columns_to_schema,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


class TestAddPartitionColumnsToSchema:
    def create_mock_path_spec(
        self, partition_result: Optional[List[Tuple[str, str]]] = None
    ) -> PathSpec:
        mock_path_spec = MagicMock(spec=PathSpec)
        mock_path_spec.get_partition_from_path.return_value = partition_result
        return mock_path_spec

    def create_schema_field(
        self,
        field_path: str,
        field_type: Union[StringTypeClass, NumberTypeClass] = StringTypeClass(),
    ) -> SchemaFieldClass:
        return SchemaFieldClass(
            fieldPath=field_path,
            nativeDataType="string",
            type=SchemaFieldDataTypeClass(field_type),
            nullable=False,
            recursive=False,
        )

    @pytest.mark.parametrize(
        "partition_keys,expected_field_paths",
        [
            # Simple partition
            ([("year", "2023")], ["year"]),
            # Multiple partitions
            ([("year", "2023"), ("month", "01")], ["year", "month"]),
            # Complex partition keys with underscores
            (
                [("partition_0", "value1"), ("partition_1", "value2")],
                ["partition_0", "partition_1"],
            ),
            # User's complex case
            (
                [("date", "2023-01-01"), ("region", "us-east"), ("category", "sales")],
                ["date", "region", "category"],
            ),
        ],
    )
    def test_add_partition_columns_basic(self, partition_keys, expected_field_paths):
        # Setup
        path_spec = self.create_mock_path_spec(partition_keys)
        fields = [
            self.create_schema_field("existing_field1"),
            self.create_schema_field("existing_field2"),
        ]
        original_field_count = len(fields)

        # Execute
        add_partition_columns_to_schema(path_spec, "/test/path", fields)

        # Assert
        assert len(fields) == original_field_count + len(expected_field_paths)

        # Check that partition fields were added correctly
        partition_fields = fields[original_field_count:]
        for i, expected_path in enumerate(expected_field_paths):
            field = partition_fields[i]
            assert field.fieldPath == expected_path
            assert field.isPartitioningKey is True
            assert field.nullable is False
            assert isinstance(field.type.type, StringTypeClass)
            assert field.nativeDataType == "string"

    @pytest.mark.parametrize(
        "existing_fields,expected_v2_format",
        [
            # v1 format only
            (["regular_field", "another_field"], False),
            # v2 format detected
            (["[version=2.0].[type=string].existing_field", "regular_field"], True),
            # Mixed with v2 present
            (["regular_field", "[version=2.0].[type=int].id", "another_field"], True),
            # Empty fields list
            ([], False),
        ],
    )
    def test_fieldpath_version_detection(self, existing_fields, expected_v2_format):
        path_spec = self.create_mock_path_spec([("year", "2023")])
        fields = [
            self.create_schema_field(field_path) for field_path in existing_fields
        ]
        original_field_count = len(fields)

        add_partition_columns_to_schema(path_spec, "/test/path", fields)

        if expected_v2_format:
            partition_field = fields[original_field_count]
            assert partition_field.fieldPath == "[version=2.0].[type=string].year"
        else:
            partition_field = fields[original_field_count]
            assert partition_field.fieldPath == "year"

    @pytest.mark.parametrize(
        "partition_result",
        [
            None,  # No partitions detected
            [],  # Empty partition list
        ],
    )
    def test_no_partitions_detected(self, partition_result):
        path_spec = self.create_mock_path_spec(partition_result)
        fields = [
            self.create_schema_field("existing_field1"),
            self.create_schema_field("existing_field2"),
        ]
        original_fields = fields.copy()

        add_partition_columns_to_schema(path_spec, "/test/path", fields)

        assert fields == original_fields

    def test_preserves_existing_fields(self):
        path_spec = self.create_mock_path_spec([("year", "2023")])
        original_field = self.create_schema_field("existing_field", NumberTypeClass())
        original_field.isPartitioningKey = False
        fields = [original_field]

        add_partition_columns_to_schema(path_spec, "/test/path", fields)

        assert fields[0] == original_field
        assert fields[0].fieldPath == "existing_field"
        assert isinstance(fields[0].type.type, NumberTypeClass)
        assert fields[0].isPartitioningKey is False

        assert len(fields) == 2
        assert fields[1].fieldPath == "year"
        assert fields[1].isPartitioningKey is True

    def test_empty_fields_list(self):
        path_spec = self.create_mock_path_spec([("year", "2023")])
        fields: List[SchemaFieldClass] = []

        add_partition_columns_to_schema(path_spec, "/test/path", fields)

        assert len(fields) == 1
        assert fields[0].fieldPath == "year"
        assert fields[0].isPartitioningKey is True

    def test_real_world_complex_partition_scenario(self):
        # This simulates the user's path spec:
        # /{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/{partition_key[2]}={partition[2]}/
        partition_keys = [
            ("date", "2023-12-01"),
            ("region", "us-east"),
            ("category", "sales"),
        ]

        path_spec = self.create_mock_path_spec(partition_keys)
        fields = [
            self.create_schema_field("customer_id"),
            self.create_schema_field("amount"),
            self.create_schema_field("transaction_date"),
        ]
        original_field_count = len(fields)

        add_partition_columns_to_schema(
            path_spec,
            "https://odedmdatacataloggold.blob.core.windows.net/settler/transactions/partitioned/date=2023-12-01/region=us-east/category=sales/data.parquet",
            fields,
        )

        assert len(fields) == original_field_count + 3

        partition_fields = fields[original_field_count:]
        expected_partitions = ["date", "region", "category"]

        for i, expected_name in enumerate(expected_partitions):
            field = partition_fields[i]
            assert field.fieldPath == expected_name
            assert field.isPartitioningKey is True
            assert field.nullable is False
            assert isinstance(field.type.type, StringTypeClass)

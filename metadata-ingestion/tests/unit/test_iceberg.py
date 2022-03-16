from typing import Any

import pytest
from iceberg.api import types as IcebergTypes
from iceberg.api.types.types import NestedField

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure.azure_common import AdlsSourceConfig
from datahub.ingestion.source.iceberg import IcebergSource, IcebergSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    FixedTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

FROZEN_TIME = "2020-04-14 07:00:00"


def iceberg_source() -> IcebergSource:
    adls: AdlsSourceConfig = AdlsSourceConfig(
        account_name="test", container_name="test"
    )
    return IcebergSource(
        ctx=PipelineContext(run_id="iceberg-source-test"),
        config=IcebergSourceConfig(adls=adls),
    )


# @freeze_time(FROZEN_TIME)
# def test_iceberg_string(tmp_path, pytestconfig):
#     column: NestedField = NestedField.required(1, "name", IcebergTypes.StringType.get(), "documentation")
#     iceberg_source_instance = iceberg_source()
#     schema_fields = iceberg_source_instance.get_schema_fields_for_column(column)
#     assert len(schema_fields) == 1
#     schema_field: SchemaField = schema_fields[0]
#     assert schema_field.description == column.doc
#     assert schema_field.nullable == column.is_optional
#     assert isinstance(schema_field.type.type, StringTypeClass)


@pytest.mark.parametrize(
    "iceberg_type, expected_schema_field_type",
    [
        (IcebergTypes.BinaryType.get(), BytesTypeClass),
        (IcebergTypes.BooleanType.get(), BooleanTypeClass),
        (IcebergTypes.DateType.get(), DateTypeClass),
        (
            IcebergTypes.DecimalType.of(3, 2),
            NumberTypeClass,
        ),
        (IcebergTypes.DoubleType.get(), NumberTypeClass),
        (IcebergTypes.FixedType.of_length(4), FixedTypeClass),
        (IcebergTypes.FloatType.get(), NumberTypeClass),
        (IcebergTypes.IntegerType.get(), NumberTypeClass),
        (IcebergTypes.LongType.get(), NumberTypeClass),
        (IcebergTypes.StringType.get(), StringTypeClass),
        (
            IcebergTypes.TimestampType.with_timezone(),
            TimeTypeClass,
        ),
        (
            IcebergTypes.TimestampType.without_timezone(),
            TimeTypeClass,
        ),
        (IcebergTypes.TimeType.get(), TimeTypeClass),
        (
            IcebergTypes.UUIDType.get(),
            StringTypeClass,
        ),  # Is this the right mapping or it should be a FixedType?
    ],
)
def test_iceberg_primitive_type_to_schema_field(
    iceberg_type: IcebergTypes.PrimitiveType, expected_schema_field_type: Any
) -> None:
    """Test converting a primitive typed Iceberg field to a SchemaField"""
    iceberg_source_instance = iceberg_source()
    for column in [
        NestedField.required(
            1, "required_field", iceberg_type, "required field documentation"
        ),
        NestedField.optional(
            1, "optional_field", iceberg_type, "optional field documentation"
        ),
    ]:
        schema_fields = iceberg_source_instance.get_schema_fields_for_column(column)
        assert len(schema_fields) == 1
        schema_field: SchemaField = schema_fields[0]
        assert schema_field.description == column.doc
        assert schema_field.nullable == column.is_optional
        assert isinstance(schema_field.type.type, expected_schema_field_type)


def test_list():
    column: NestedField = NestedField.required(
        1,
        "listField",
        IcebergTypes.ListType.of_required(2, IcebergTypes.StringType.get()),
        "documentation",
    )
    iceberg_source_instance = iceberg_source()
    schema_fields = iceberg_source_instance.get_schema_fields_for_column(column)
    assert len(schema_fields) == 1
    schema_field: SchemaField = schema_fields[0]
    assert schema_field.description == column.doc
    assert schema_field.nullable == column.is_optional
    assert isinstance(schema_field.type.type, ArrayTypeClass)


@pytest.mark.parametrize(
    "iceberg_type, expected_schema_field_type",
    [
        (IcebergTypes.BinaryType.get(), BytesTypeClass),
        (IcebergTypes.BooleanType.get(), BooleanTypeClass),
        (IcebergTypes.DateType.get(), DateTypeClass),
        (
            IcebergTypes.DecimalType.of(3, 2),
            NumberTypeClass,
        ),
        (IcebergTypes.DoubleType.get(), NumberTypeClass),
        (IcebergTypes.FixedType.of_length(4), FixedTypeClass),
        (IcebergTypes.FloatType.get(), NumberTypeClass),
        (IcebergTypes.IntegerType.get(), NumberTypeClass),
        (IcebergTypes.LongType.get(), NumberTypeClass),
        (IcebergTypes.StringType.get(), StringTypeClass),
        (
            IcebergTypes.TimestampType.with_timezone(),
            TimeTypeClass,
        ),
        (
            IcebergTypes.TimestampType.without_timezone(),
            TimeTypeClass,
        ),
        (IcebergTypes.TimeType.get(), TimeTypeClass),
        (
            IcebergTypes.UUIDType.get(),
            StringTypeClass,
        ),  # Is this the right mapping or it should be a FixedType?
    ],
)
def test_struct(iceberg_type, expected_schema_field_type):
    field1: NestedField = NestedField.required(
        11, "field1", iceberg_type, "field documentation"
    )
    column: NestedField = NestedField.required(
        1, "structField", IcebergTypes.StructType.of([field1]), "struct documentation"
    )
    iceberg_source_instance = iceberg_source()
    schema_fields = iceberg_source_instance.get_schema_fields_for_column(column)
    assert len(schema_fields) == 2
    struct_schema_field: SchemaField = schema_fields[0]
    assert struct_schema_field.description == column.doc
    assert struct_schema_field.nullable == column.is_optional
    assert isinstance(struct_schema_field.type.type, RecordTypeClass)

    schema_field: SchemaField = schema_fields[1]
    assert schema_field.description == field1.doc
    assert schema_field.nullable == field1.is_optional
    assert isinstance(schema_field.type.type, expected_schema_field_type)

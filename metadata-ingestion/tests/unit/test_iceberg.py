from typing import Any, Optional

import pytest
from iceberg.api import types as IcebergTypes
from iceberg.api.types.types import NestedField

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure.azure_common import AdlsSourceConfig
from datahub.ingestion.source.iceberg.iceberg import IcebergSource, IcebergSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import ArrayType, SchemaField
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


def with_iceberg_source() -> IcebergSource:
    adls: AdlsSourceConfig = AdlsSourceConfig(
        account_name="test", account_key="test", container_name="test"
    )
    return IcebergSource(
        ctx=PipelineContext(run_id="iceberg-source-test"),
        config=IcebergSourceConfig(adls=adls),
    )


def assert_field(
    schema_field: SchemaField,
    expected_description: Optional[str],
    expected_nullable: bool,
    expected_type: Any,
) -> None:
    assert (
        schema_field.description == expected_description
    ), f"Field description '{schema_field.description}' is different from expected description '{expected_description}'"
    assert (
        schema_field.nullable == expected_nullable
    ), f"Field nullable '{schema_field.nullable}' is different from expected nullable '{expected_nullable}'"
    assert isinstance(
        schema_field.type.type, expected_type
    ), f"Field type {schema_field.type.type} is different from expected type {expected_type}"


def test_adls_config_no_credential():
    """
    Test when no ADLS credential information is provided (SAS token, Account key).
    """
    with pytest.raises(ConfigurationError):
        AdlsSourceConfig(account_name="test", container_name="test")


def test_adls_config_with_sas_credential():
    """
    Test when a SAS token is used as an ADLS credential.
    """
    AdlsSourceConfig(account_name="test", sas_token="test", container_name="test")


def test_adls_config_with_key_credential():
    """
    Test when an account key is used as an ADLS credential.
    """
    AdlsSourceConfig(account_name="test", account_key="test", container_name="test")


def test_adls_config_with_client_secret_credential():
    """
    Test when a client secret is used as an ADLS credential.
    """
    AdlsSourceConfig(
        account_name="test",
        tenant_id="test",
        client_id="test",
        client_secret="test",
        container_name="test",
    )

    # Test when tenant_id is missing
    with pytest.raises(ConfigurationError):
        AdlsSourceConfig(
            account_name="test",
            client_id="test",
            client_secret="test",
            container_name="test",
        )

    # Test when client_id is missing
    with pytest.raises(ConfigurationError):
        AdlsSourceConfig(
            account_name="test",
            tenant_id="test",
            client_secret="test",
            container_name="test",
        )

    # Test when client_secret is missing
    with pytest.raises(ConfigurationError):
        AdlsSourceConfig(
            account_name="test",
            tenant_id="test",
            client_id="test",
            container_name="test",
        )


def test_config_for_tests():
    """
    Test valid iceberg source that will be used in unit tests.
    """
    with_iceberg_source()


def test_config_no_filesystem():
    """
    Test when a SAS token is used as an ADLS credential.
    """
    with pytest.raises(ConfigurationError):
        IcebergSource(
            ctx=PipelineContext(run_id="iceberg-source-test"),
            config=IcebergSourceConfig(),
        )


def test_config_multiple_filesystems():
    """
    Test when more than 1 filesystem is configured.
    """
    with pytest.raises(ConfigurationError):
        adls: AdlsSourceConfig = AdlsSourceConfig(
            account_name="test", container_name="test"
        )
        IcebergSource(
            ctx=PipelineContext(run_id="iceberg-source-test"),
            config=IcebergSourceConfig(adls=adls, localfs="/tmp"),
        )


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
        ),
    ],
)
def test_iceberg_primitive_type_to_schema_field(
    iceberg_type: IcebergTypes.PrimitiveType, expected_schema_field_type: Any
) -> None:
    """
    Test converting a primitive typed Iceberg field to a SchemaField
    """
    iceberg_source_instance = with_iceberg_source()
    for column in [
        NestedField.required(
            1, "required_field", iceberg_type, "required field documentation"
        ),
        NestedField.optional(
            1, "optional_field", iceberg_type, "optional field documentation"
        ),
    ]:
        schema_fields = iceberg_source_instance._get_schema_fields_for_column(column)
        assert (
            len(schema_fields) == 1
        ), f"Expected 1 field, but got {len(schema_fields)}"
        assert_field(
            schema_fields[0], column.doc, column.is_optional, expected_schema_field_type
        )


@pytest.mark.parametrize(
    "iceberg_type, expected_array_nested_type",
    [
        (IcebergTypes.BinaryType.get(), "bytes"),
        (IcebergTypes.BooleanType.get(), "boolean"),
        (IcebergTypes.DateType.get(), "date"),
        (
            IcebergTypes.DecimalType.of(3, 2),
            "decimal",
        ),
        (IcebergTypes.DoubleType.get(), "double"),
        (IcebergTypes.FixedType.of_length(4), "fixed"),
        (IcebergTypes.FloatType.get(), "float"),
        (IcebergTypes.IntegerType.get(), "int"),
        (IcebergTypes.LongType.get(), "long"),
        (IcebergTypes.StringType.get(), "string"),
        (
            IcebergTypes.TimestampType.with_timezone(),
            "timestamp-micros",
        ),
        (
            IcebergTypes.TimestampType.without_timezone(),
            "timestamp-micros",
        ),
        (IcebergTypes.TimeType.get(), "time-micros"),
        (
            IcebergTypes.UUIDType.get(),
            "uuid",
        ),
    ],
)
def test_iceberg_list_to_schema_field(
    iceberg_type: IcebergTypes.PrimitiveType, expected_array_nested_type: Any
) -> None:
    """
    Test converting a list typed Iceberg field to an ArrayType SchemaField, including the list nested type.
    """
    list_column: NestedField = NestedField.required(
        1,
        "listField",
        IcebergTypes.ListType.of_required(2, iceberg_type),
        "documentation",
    )
    iceberg_source_instance = with_iceberg_source()
    schema_fields = iceberg_source_instance._get_schema_fields_for_column(list_column)
    assert len(schema_fields) == 1, f"Expected 1 field, but got {len(schema_fields)}"
    assert_field(
        schema_fields[0], list_column.doc, list_column.is_optional, ArrayTypeClass
    )
    assert isinstance(
        schema_fields[0].type.type, ArrayType
    ), f"Field type {schema_fields[0].type.type} was expected to be {ArrayType}"
    arrayType: ArrayType = schema_fields[0].type.type
    assert arrayType.nestedType == [
        expected_array_nested_type
    ], f"List Field nested type {arrayType.nestedType} was expected to be {expected_array_nested_type}"


@pytest.mark.parametrize(
    "iceberg_type, expected_map_type",
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
        ),
    ],
)
def test_iceberg_map_to_schema_field(
    iceberg_type: IcebergTypes.PrimitiveType, expected_map_type: Any
) -> None:
    """
    Test converting a map typed Iceberg field to a MapType SchemaField, where the key is the same type as the value.
    """
    map_column: NestedField = NestedField.required(
        1,
        "mapField",
        IcebergTypes.MapType.of_required(11, 12, iceberg_type, iceberg_type),
        "documentation",
    )
    iceberg_source_instance = with_iceberg_source()
    schema_fields = iceberg_source_instance._get_schema_fields_for_column(map_column)
    # Converting an Iceberg Map type will be done by creating an array of struct(key, value) records.
    # The first field will be the array.
    assert len(schema_fields) == 3, f"Expected 3 fields, but got {len(schema_fields)}"
    assert_field(
        schema_fields[0], map_column.doc, map_column.is_optional, ArrayTypeClass
    )

    # The second field will be the key type
    assert_field(schema_fields[1], None, False, expected_map_type)

    # The third field will be the value type
    assert_field(schema_fields[2], None, True, expected_map_type)


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
        ),
    ],
)
def test_iceberg_struct_to_schema_field(
    iceberg_type: IcebergTypes.PrimitiveType, expected_schema_field_type: Any
) -> None:
    """
    Test converting a struct typed Iceberg field to a RecordType SchemaField.
    """
    field1: NestedField = NestedField.required(
        11, "field1", iceberg_type, "field documentation"
    )
    struct_column: NestedField = NestedField.required(
        1, "structField", IcebergTypes.StructType.of([field1]), "struct documentation"
    )
    iceberg_source_instance = with_iceberg_source()
    schema_fields = iceberg_source_instance._get_schema_fields_for_column(struct_column)
    assert len(schema_fields) == 2, f"Expected 2 fields, but got {len(schema_fields)}"
    assert_field(
        schema_fields[0], struct_column.doc, struct_column.is_optional, RecordTypeClass
    )
    assert_field(
        schema_fields[1], field1.doc, field1.is_optional, expected_schema_field_type
    )


def test_avro_decimal_bytes_nullable():
    """
    The following test exposes a problem with decimal (bytes) not preserving extra attributes like _nullable.  Decimal (fixed) and Boolean for example do.
    NOTE: This bug was by-passed by mapping the Decimal type to fixed instead of bytes.
    """
    import avro.schema

    decimal_avro_schema_string = """{"type": "record", "name": "__struct_", "fields": [{"type": {"type": "bytes", "precision": 3, "scale": 2, "logicalType": "decimal", "native_data_type": "decimal(3, 2)", "_nullable": false}, "name": "required_field", "doc": "required field documentation"}]}"""
    decimal_avro_schema = avro.schema.parse(decimal_avro_schema_string)
    print("\nDecimal (bytes)")
    print(
        f"Original avro schema string:                         {decimal_avro_schema_string}"
    )
    print(f"After avro parsing, _nullable attribute is missing:  {decimal_avro_schema}")

    decimal_fixed_avro_schema_string = """{"type": "record", "name": "__struct_", "fields": [{"type": {"type": "fixed", "logicalType": "decimal", "precision": 3, "scale": 2, "native_data_type": "decimal(3, 2)", "_nullable": false, "name": "bogusName", "size": 16}, "name": "required_field", "doc": "required field documentation"}]}"""
    decimal_fixed_avro_schema = avro.schema.parse(decimal_fixed_avro_schema_string)
    print("\nDecimal (fixed)")
    print(
        f"Original avro schema string:                           {decimal_fixed_avro_schema_string}"
    )
    print(
        f"After avro parsing, _nullable attribute is preserved:  {decimal_fixed_avro_schema}"
    )

    boolean_avro_schema_string = """{"type": "record", "name": "__struct_", "fields": [{"type": {"type": "boolean", "native_data_type": "boolean", "_nullable": false}, "name": "required_field", "doc": "required field documentation"}]}"""
    boolean_avro_schema = avro.schema.parse(boolean_avro_schema_string)
    print("\nBoolean")
    print(
        f"Original avro schema string:                           {boolean_avro_schema_string}"
    )
    print(
        f"After avro parsing, _nullable attribute is preserved:  {boolean_avro_schema}"
    )

import sys
import uuid
from decimal import Decimal
from typing import Any, Optional

import pytest
from pydantic import ValidationError

if sys.version_info >= (3, 8):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BinaryType,
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FixedType,
        FloatType,
        IcebergType,
        IntegerType,
        ListType,
        LongType,
        MapType,
        NestedField,
        PrimitiveType,
        StringType,
        StructType,
        TimestampType,
        TimestamptzType,
        TimeType,
        UUIDType,
    )

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.iceberg.iceberg import (
        IcebergProfiler,
        IcebergSource,
        IcebergSourceConfig,
    )
    from datahub.ingestion.source.iceberg.iceberg_common import IcebergCatalogConfig
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

    pytestmark = pytest.mark.skipif(
        sys.version_info < (3, 8), reason="requires python 3.8 or higher"
    )

    def with_iceberg_source() -> IcebergSource:
        catalog: IcebergCatalogConfig = IcebergCatalogConfig(
            name="test", type="rest", config={}
        )
        return IcebergSource(
            ctx=PipelineContext(run_id="iceberg-source-test"),
            config=IcebergSourceConfig(catalog=catalog),
        )

    def with_iceberg_profiler() -> IcebergProfiler:
        iceberg_source_instance = with_iceberg_source()
        return IcebergProfiler(
            iceberg_source_instance.report, iceberg_source_instance.config.profiling
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

    def test_config_no_catalog():
        """
        Test when no Iceberg catalog is provided.
        """
        with pytest.raises(ValidationError, match="catalog"):
            IcebergSourceConfig()  # type: ignore

    def test_config_catalog_not_configured():
        """
        Test when an Iceberg catalog is provided, but not properly configured.
        """
        with pytest.raises(ValidationError):
            IcebergCatalogConfig()  # type: ignore

        with pytest.raises(ValidationError, match="conf"):
            IcebergCatalogConfig(type="a type")  # type: ignore

        with pytest.raises(ValidationError, match="type"):
            IcebergCatalogConfig(conf={})  # type: ignore

    def test_config_for_tests():
        """
        Test valid iceberg source that will be used in unit tests.
        """
        with_iceberg_source()

    @pytest.mark.parametrize(
        "iceberg_type, expected_schema_field_type",
        [
            (BinaryType(), BytesTypeClass),
            (BooleanType(), BooleanTypeClass),
            (DateType(), DateTypeClass),
            (
                DecimalType(3, 2),
                NumberTypeClass,
            ),
            (DoubleType(), NumberTypeClass),
            (FixedType(4), FixedTypeClass),
            (FloatType(), NumberTypeClass),
            (IntegerType(), NumberTypeClass),
            (LongType(), NumberTypeClass),
            (StringType(), StringTypeClass),
            (
                TimestampType(),
                TimeTypeClass,
            ),
            (
                TimestamptzType(),
                TimeTypeClass,
            ),
            (TimeType(), TimeTypeClass),
            (
                UUIDType(),
                StringTypeClass,
            ),
        ],
    )
    def test_iceberg_primitive_type_to_schema_field(
        iceberg_type: PrimitiveType, expected_schema_field_type: Any
    ) -> None:
        """
        Test converting a primitive typed Iceberg field to a SchemaField
        """
        iceberg_source_instance = with_iceberg_source()
        for column in [
            NestedField(
                1, "required_field", iceberg_type, True, "required field documentation"
            ),
            NestedField(
                1, "optional_field", iceberg_type, False, "optional field documentation"
            ),
        ]:
            schema = Schema(column)
            schema_fields = iceberg_source_instance._get_schema_fields_for_schema(
                schema
            )
            assert (
                len(schema_fields) == 1
            ), f"Expected 1 field, but got {len(schema_fields)}"
            assert_field(
                schema_fields[0],
                column.doc,
                column.optional,
                expected_schema_field_type,
            )

    @pytest.mark.parametrize(
        "iceberg_type, expected_array_nested_type",
        [
            (BinaryType(), "bytes"),
            (BooleanType(), "boolean"),
            (DateType(), "date"),
            (
                DecimalType(3, 2),
                "decimal",
            ),
            (DoubleType(), "double"),
            (FixedType(4), "fixed"),
            (FloatType(), "float"),
            (IntegerType(), "int"),
            (LongType(), "long"),
            (StringType(), "string"),
            (
                TimestampType(),
                "timestamp-micros",
            ),
            (
                TimestamptzType(),
                "timestamp-micros",
            ),
            (TimeType(), "time-micros"),
            (
                UUIDType(),
                "uuid",
            ),
        ],
    )
    def test_iceberg_list_to_schema_field(
        iceberg_type: PrimitiveType, expected_array_nested_type: Any
    ) -> None:
        """
        Test converting a list typed Iceberg field to an ArrayType SchemaField, including the list nested type.
        """
        for list_column in [
            NestedField(
                1,
                "listField",
                ListType(2, iceberg_type, True),
                True,
                "required field, required element documentation",
            ),
            NestedField(
                1,
                "listField",
                ListType(2, iceberg_type, False),
                True,
                "required field, optional element documentation",
            ),
            NestedField(
                1,
                "listField",
                ListType(2, iceberg_type, True),
                False,
                "optional field, required element documentation",
            ),
            NestedField(
                1,
                "listField",
                ListType(2, iceberg_type, False),
                False,
                "optional field, optional element documentation",
            ),
        ]:
            iceberg_source_instance = with_iceberg_source()
            schema = Schema(list_column)
            schema_fields = iceberg_source_instance._get_schema_fields_for_schema(
                schema
            )
            assert (
                len(schema_fields) == 1
            ), f"Expected 1 field, but got {len(schema_fields)}"
            assert_field(
                schema_fields[0], list_column.doc, list_column.optional, ArrayTypeClass
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
            (BinaryType(), BytesTypeClass),
            (BooleanType(), BooleanTypeClass),
            (DateType(), DateTypeClass),
            (
                DecimalType(3, 2),
                NumberTypeClass,
            ),
            (DoubleType(), NumberTypeClass),
            (FixedType(4), FixedTypeClass),
            (FloatType(), NumberTypeClass),
            (IntegerType(), NumberTypeClass),
            (LongType(), NumberTypeClass),
            (StringType(), StringTypeClass),
            (
                TimestampType(),
                TimeTypeClass,
            ),
            (
                TimestamptzType(),
                TimeTypeClass,
            ),
            (TimeType(), TimeTypeClass),
            (
                UUIDType(),
                StringTypeClass,
            ),
        ],
    )
    def test_iceberg_map_to_schema_field(
        iceberg_type: PrimitiveType, expected_map_type: Any
    ) -> None:
        """
        Test converting a map typed Iceberg field to a MapType SchemaField, where the key is the same type as the value.
        """
        for map_column in [
            NestedField(
                1,
                "mapField",
                MapType(11, iceberg_type, 12, iceberg_type, True),
                True,
                "required field, required value documentation",
            ),
            NestedField(
                1,
                "mapField",
                MapType(11, iceberg_type, 12, iceberg_type, False),
                True,
                "required field, optional value documentation",
            ),
            NestedField(
                1,
                "mapField",
                MapType(11, iceberg_type, 12, iceberg_type, True),
                False,
                "optional field, required value documentation",
            ),
            NestedField(
                1,
                "mapField",
                MapType(11, iceberg_type, 12, iceberg_type, False),
                False,
                "optional field, optional value documentation",
            ),
        ]:
            iceberg_source_instance = with_iceberg_source()
            schema = Schema(map_column)
            schema_fields = iceberg_source_instance._get_schema_fields_for_schema(
                schema
            )
            # Converting an Iceberg Map type will be done by creating an array of struct(key, value) records.
            # The first field will be the array.
            assert (
                len(schema_fields) == 3
            ), f"Expected 3 fields, but got {len(schema_fields)}"
            assert_field(
                schema_fields[0], map_column.doc, map_column.optional, ArrayTypeClass
            )

            # The second field will be the key type
            assert_field(schema_fields[1], None, False, expected_map_type)

            # The third field will be the value type
            assert_field(
                schema_fields[2],
                None,
                not map_column.field_type.value_required,
                expected_map_type,
            )

    @pytest.mark.parametrize(
        "iceberg_type, expected_schema_field_type",
        [
            (BinaryType(), BytesTypeClass),
            (BooleanType(), BooleanTypeClass),
            (DateType(), DateTypeClass),
            (
                DecimalType(3, 2),
                NumberTypeClass,
            ),
            (DoubleType(), NumberTypeClass),
            (FixedType(4), FixedTypeClass),
            (FloatType(), NumberTypeClass),
            (IntegerType(), NumberTypeClass),
            (LongType(), NumberTypeClass),
            (StringType(), StringTypeClass),
            (
                TimestampType(),
                TimeTypeClass,
            ),
            (
                TimestamptzType(),
                TimeTypeClass,
            ),
            (TimeType(), TimeTypeClass),
            (
                UUIDType(),
                StringTypeClass,
            ),
        ],
    )
    def test_iceberg_struct_to_schema_field(
        iceberg_type: PrimitiveType, expected_schema_field_type: Any
    ) -> None:
        """
        Test converting a struct typed Iceberg field to a RecordType SchemaField.
        """
        field1 = NestedField(11, "field1", iceberg_type, True, "field documentation")
        struct_column = NestedField(
            1, "structField", StructType(field1), True, "struct documentation"
        )
        iceberg_source_instance = with_iceberg_source()
        schema = Schema(struct_column)
        schema_fields = iceberg_source_instance._get_schema_fields_for_schema(schema)
        assert (
            len(schema_fields) == 2
        ), f"Expected 2 fields, but got {len(schema_fields)}"
        assert_field(
            schema_fields[0], struct_column.doc, struct_column.optional, RecordTypeClass
        )
        assert_field(
            schema_fields[1], field1.doc, field1.optional, expected_schema_field_type
        )

    @pytest.mark.parametrize(
        "value_type, value, expected_value",
        [
            (BinaryType(), bytes([1, 2, 3, 4, 5]), "b'\\x01\\x02\\x03\\x04\\x05'"),
            (BooleanType(), True, "True"),
            (DateType(), 19543, "2023-07-05"),
            (DecimalType(3, 2), Decimal((0, (3, 1, 4), -2)), "3.14"),
            (DoubleType(), 3.4, "3.4"),
            (FixedType(4), bytes([1, 2, 3, 4]), "b'\\x01\\x02\\x03\\x04'"),
            (FloatType(), 3.4, "3.4"),
            (IntegerType(), 3, "3"),
            (LongType(), 4294967295000, "4294967295000"),
            (StringType(), "a string", "a string"),
            (
                TimestampType(),
                1688559488157000,
                "2023-07-05T12:18:08.157000",
            ),
            (
                TimestamptzType(),
                1688559488157000,
                "2023-07-05T12:18:08.157000+00:00",
            ),
            (TimeType(), 40400000000, "11:13:20"),
            (
                UUIDType(),
                uuid.UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
                "00010203-0405-0607-0809-0a0b0c0d0e0f",
            ),
        ],
    )
    def test_iceberg_profiler_value_render(
        value_type: IcebergType, value: Any, expected_value: Optional[str]
    ) -> None:
        iceberg_profiler_instance = with_iceberg_profiler()
        assert (
            iceberg_profiler_instance._render_value("a.dataset", value_type, value)
            == expected_value
        )

    def test_avro_decimal_bytes_nullable() -> None:
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
        print(
            f"After avro parsing, _nullable attribute is missing:  {decimal_avro_schema}"
        )

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

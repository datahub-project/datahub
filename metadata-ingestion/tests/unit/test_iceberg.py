import uuid
from collections import defaultdict
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)
from unittest import TestCase
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    RESTError,
    ServerError,
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
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
from typing_extensions import Never

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.iceberg.iceberg import (
    IcebergProfiler,
    IcebergSource,
    IcebergSourceConfig,
    ToAvroSchemaIcebergVisitor,
)
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

MCPS_PER_TABLE = 7  # assuming no profiling
MCPS_PER_NAMESPACE = 5


def with_iceberg_source(processing_threads: int = 1, **kwargs: Any) -> IcebergSource:
    catalog = {"test": {"type": "rest"}}
    config = IcebergSourceConfig(
        catalog=catalog, processing_threads=processing_threads, **kwargs
    )
    return IcebergSource(
        ctx=PipelineContext(run_id="iceberg-source-test"),
        config=config,
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
    assert schema_field.description == expected_description, (
        f"Field description '{schema_field.description}' is different from expected description '{expected_description}'"
    )
    assert schema_field.nullable == expected_nullable, (
        f"Field nullable '{schema_field.nullable}' is different from expected nullable '{expected_nullable}'"
    )
    assert isinstance(schema_field.type.type, expected_type), (
        f"Field type {schema_field.type.type} is different from expected type {expected_type}"
    )


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
    # When no catalog configurationis provided, the config should be invalid
    with pytest.raises(ValidationError, match="type"):
        IcebergSourceConfig(catalog={})  # type: ignore

    # When a catalog name is provided without configuration, the config should be invalid
    with pytest.raises(ValidationError):
        IcebergSourceConfig(catalog={"test": {}})


def test_config_deprecated_catalog_configuration():
    """
    Test when a deprecated Iceberg catalog configuration is provided, it should be converted to the current scheme.
    """
    deprecated_config = {
        "name": "test",
        "type": "rest",
        "config": {"uri": "http://a.uri.test", "another_prop": "another_value"},
    }
    migrated_config = IcebergSourceConfig(catalog=deprecated_config)
    assert migrated_config.catalog["test"] is not None
    assert migrated_config.catalog["test"]["type"] == "rest"
    assert migrated_config.catalog["test"]["uri"] == "http://a.uri.test"
    assert migrated_config.catalog["test"]["another_prop"] == "another_value"


def test_config_for_tests():
    """
    Test valid iceberg source that will be used in unit tests.
    """
    with_iceberg_source()


def test_config_support_nested_dicts():
    """
    Test that Iceberg source supports nested dictionaries inside its configuration, as allowed by pyiceberg.
    """
    catalog = {
        "test": {
            "type": "rest",
            "nested_dict": {
                "nested_key": "nested_value",
                "nested_array": ["a1", "a2"],
                "subnested_dict": {"subnested_key": "subnested_value"},
            },
        }
    }
    test_config = IcebergSourceConfig(catalog=catalog)
    assert isinstance(test_config.catalog["test"]["nested_dict"], Dict)
    assert test_config.catalog["test"]["nested_dict"]["nested_key"] == "nested_value"
    assert isinstance(test_config.catalog["test"]["nested_dict"]["nested_array"], List)
    assert test_config.catalog["test"]["nested_dict"]["nested_array"][0] == "a1"
    assert isinstance(
        test_config.catalog["test"]["nested_dict"]["subnested_dict"], Dict
    )
    assert (
        test_config.catalog["test"]["nested_dict"]["subnested_dict"]["subnested_key"]
        == "subnested_value"
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
        schema_fields = iceberg_source_instance._get_schema_fields_for_schema(schema)
        assert len(schema_fields) == 1, (
            f"Expected 1 field, but got {len(schema_fields)}"
        )
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
        schema_fields = iceberg_source_instance._get_schema_fields_for_schema(schema)
        assert len(schema_fields) == 1, (
            f"Expected 1 field, but got {len(schema_fields)}"
        )
        assert_field(
            schema_fields[0], list_column.doc, list_column.optional, ArrayTypeClass
        )
        assert isinstance(schema_fields[0].type.type, ArrayType), (
            f"Field type {schema_fields[0].type.type} was expected to be {ArrayType}"
        )
        arrayType: ArrayType = schema_fields[0].type.type
        assert arrayType.nestedType == [expected_array_nested_type], (
            f"List Field nested type {arrayType.nestedType} was expected to be {expected_array_nested_type}"
        )


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
        schema_fields = iceberg_source_instance._get_schema_fields_for_schema(schema)
        # Converting an Iceberg Map type will be done by creating an array of struct(key, value) records.
        # The first field will be the array.
        assert len(schema_fields) == 3, (
            f"Expected 3 fields, but got {len(schema_fields)}"
        )
        assert_field(
            schema_fields[0], map_column.doc, map_column.optional, ArrayTypeClass
        )

        # The second field will be the key type
        assert_field(schema_fields[1], None, False, expected_map_type)

        # The third field will be the value type
        assert isinstance(map_column.field_type, MapType)
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
    assert len(schema_fields) == 2, f"Expected 2 fields, but got {len(schema_fields)}"
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


def test_visit_timestamp_ns() -> None:
    """
    Test the visit_timestamp_ns method for handling nanosecond precision timestamps.
    This method was added in pyiceberg 0.10.0 to support nanosecond precision timestamps.
    """
    visitor = ToAvroSchemaIcebergVisitor()

    # Create a mock type object that behaves like TimestampNsType from pyiceberg 0.10.0+
    # The string representation follows pyiceberg's pattern: "timestampns"
    class MockTimestampNsType:
        def __str__(self) -> str:
            return "timestampns"

    mock_type = MockTimestampNsType()
    result = visitor.visit_timestamp_ns(mock_type)

    # Verify the Avro schema structure
    assert result["type"] == "long"
    assert result["logicalType"] == "timestamp-micros"
    assert result["native_data_type"] == "timestampns"


def test_visit_timestamptz_ns() -> None:
    """
    Test the visit_timestamptz_ns method for handling nanosecond precision timestamps with timezone.
    This method was added in pyiceberg 0.10.0 to support nanosecond precision timestamps with timezone.
    """
    visitor = ToAvroSchemaIcebergVisitor()

    # Create a mock type object that behaves like TimestamptzNsType from pyiceberg 0.10.0+
    # The string representation follows pyiceberg's pattern: "timestamptzns"
    class MockTimestamptzNsType:
        def __str__(self) -> str:
            return "timestamptzns"

    mock_type = MockTimestamptzNsType()
    result = visitor.visit_timestamptz_ns(mock_type)

    # Verify the Avro schema structure
    assert result["type"] == "long"
    assert result["logicalType"] == "timestamp-micros"
    assert result["native_data_type"] == "timestamptzns"


def test_visit_unknown() -> None:
    """
    Test the visit_unknown method for handling unknown/unsupported types.
    This is a fallback method for types that don't have specific visitor implementations.
    """
    visitor = ToAvroSchemaIcebergVisitor()

    # Create a mock type object representing an unknown type
    class MockUnknownType:
        def __str__(self) -> str:
            return "unknown_custom_type"

    mock_type = MockUnknownType()
    result = visitor.visit_unknown(mock_type)

    # Verify the Avro schema structure - unknown types are mapped to string
    assert result["type"] == "string"
    assert result["native_data_type"] == "unknown_custom_type"


class MockCatalog:
    def __init__(
        self,
        tables: Mapping[
            str,
            Mapping[
                str,
                Callable[[Catalog], Table],
            ],
        ],
        namespace_properties: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        """

        :param tables: Dictionary containing namespaces as keys and dictionaries containing names of tables (keys) and
                       their metadata as values
        """
        self.tables = tables
        self.namespace_properties = (
            namespace_properties if namespace_properties else defaultdict(dict)
        )

    def list_namespaces(self) -> Iterable[Tuple[str]]:
        return [*[(key,) for key in self.tables]]

    def list_tables(self, namespace: str) -> Iterable[Tuple[str, str]]:
        return [(namespace[0], table) for table in self.tables[namespace[0]]]

    def load_table(self, dataset_path: Tuple[str, str]) -> Table:
        table_callable = self.tables[dataset_path[0]][dataset_path[1]]

        # Passing self as a mock catalog, despite it not being fully valid.
        # This makes the mocking setup simpler.
        return table_callable(self)  # type: ignore

    def load_namespace_properties(self, namespace: Tuple[str, ...]) -> Dict[str, str]:
        return self.namespace_properties[namespace[0]]


class MockCatalogExceptionListingTables(MockCatalog):
    def list_tables(self, namespace: str) -> Iterable[Tuple[str, str]]:
        if namespace == ("no_such_namespace",):
            raise NoSuchNamespaceError()
        if namespace == ("rest_error",):
            raise RESTError()
        if namespace == ("generic_exception",):
            raise Exception()
        return super().list_tables(namespace)


class MockCatalogExceptionListingNamespaces(MockCatalog):
    def list_namespaces(self) -> Iterable[Tuple[str]]:
        raise Exception("Test exception")


class MockCatalogExceptionRetrievingNamespaceProperties(MockCatalog):
    def load_namespace_properties(self, namespace: Tuple[str, ...]) -> Dict[str, str]:
        if namespace == ("no_such_namespace",):
            raise NoSuchNamespaceError()
        if namespace == ("rest_error",):
            raise RESTError()
        if namespace == ("generic_exception",):
            raise Exception()
        return super().load_namespace_properties(namespace)


def test_exception_while_listing_namespaces() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingNamespaces({})
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wus = [*source.get_workunits_internal()]
        assert len(wus) == 0
        assert source.report.failures.total_elements == 1


def test_known_exception_while_retrieving_namespace_properties() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionRetrievingNamespaceProperties(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "no_such_namespace": {},
            "rest_error": {},
            "namespaceB": {
                "table2": lambda catalog: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespaceC": {
                "table4": lambda catalog: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "namespaceD": {
                "table5": lambda catalog: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 5 tables (6 MCPs each) and 4 namespaces (4 MCPs each), we will not ingest namespaces at all if we fail to get their properties
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
            "urn:li:container:74727446a56420d80ff3b1abf2a18087",
            "urn:li:container:3f9a24213cca64ab22e409d1b9a94789",
            "urn:li:container:38a0583b0305ec5066cb708199f6848c",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.warnings.total_elements == 2
        assert source.report.failures.total_elements == 0
        assert source.report.tables_scanned == 5


def test_unknown_exception_while_retrieving_namespace_properties() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionRetrievingNamespaceProperties(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "generic_exception": {},
            "namespaceB": {
                "table2": lambda catalog: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespaceC": {
                "table4": lambda catalog: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "namespaceD": {
                "table5": lambda catalog: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 5 tables (6 MCPs each) and 4 namespaces (4 MCPs each), despite exception
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
            "urn:li:container:74727446a56420d80ff3b1abf2a18087",
            "urn:li:container:3f9a24213cca64ab22e409d1b9a94789",
            "urn:li:container:38a0583b0305ec5066cb708199f6848c",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.warnings.total_elements == 0
        assert source.report.failures.total_elements == 1
        assert source.report.tables_scanned == 5


def test_known_exception_while_listing_tables() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingTables(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "no_such_namespace": {},
            "rest_error": {},
            "namespaceB": {
                "table2": lambda catalog: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespaceC": {
                "table4": lambda catalog: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "namespaceD": {
                "table5": lambda catalog: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 5 tables (6 MCPs each) and 6 namespaces (4 MCPs each), despite exception
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
            "urn:li:container:9cb5e87ec392b231720f23bf00d6f6aa",
            "urn:li:container:74727446a56420d80ff3b1abf2a18087",
            "urn:li:container:3f9a24213cca64ab22e409d1b9a94789",
            "urn:li:container:38a0583b0305ec5066cb708199f6848c",
            "urn:li:container:7b510fcb61d4977da0b1707e533999d8",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.warnings.total_elements == 2
        assert source.report.failures.total_elements == 0
        assert source.report.tables_scanned == 5


def test_unknown_exception_while_listing_tables() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingTables(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "generic_exception": {},
            "namespaceB": {
                "table2": lambda catalog: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespaceC": {
                "table4": lambda catalog: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "namespaceD": {
                "table5": lambda catalog: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 5 tables (6 MCPs each) and 5 namespaces (4 MCPs each), despite exception
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
            "urn:li:container:be99158f9640329f4394315e1d8dacf3",
            "urn:li:container:74727446a56420d80ff3b1abf2a18087",
            "urn:li:container:3f9a24213cca64ab22e409d1b9a94789",
            "urn:li:container:38a0583b0305ec5066cb708199f6848c",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.warnings.total_elements == 0
        assert source.report.failures.total_elements == 1
        assert source.report.tables_scanned == 5


def test_proper_run_with_multiple_namespaces() -> None:
    source = with_iceberg_source(processing_threads=3)
    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                )
            },
            "namespaceB": {},
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 1 table (6 MCPs) and 2 namespaces (4 MCPs each), despite exception
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:74727446a56420d80ff3b1abf2a18087",
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )


def test_filtering() -> None:
    source = with_iceberg_source(
        processing_threads=1,
        table_pattern=AllowDenyPattern(deny=[".*abcd.*"]),
        namespace_pattern=AllowDenyPattern(allow=["namespace1"]),
    )
    mock_catalog = MockCatalog(
        {
            "namespace1": {
                "table_xyz": lambda catalog: Table(
                    identifier=("namespace1", "table_xyz"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/table_xyz",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace1/table_xyz",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "JKLtable": lambda catalog: Table(
                    identifier=("namespace1", "JKLtable"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/JKLtable",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace1/JKLtable",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table_abcd": lambda catalog: Table(
                    identifier=("namespace1", "table_abcd"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/table_abcd",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace1/table_abcd",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "aaabcd": lambda catalog: Table(
                    identifier=("namespace1", "aaabcd"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/aaabcd",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace1/aaabcd",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespace2": {
                "foo": lambda catalog: Table(
                    identifier=("namespace2", "foo"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace2/foo",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace2/foo",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "bar": lambda catalog: Table(
                    identifier=("namespace2", "bar"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace2/bar",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace2/bar",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
            "namespace3": {
                "sales": lambda catalog: Table(
                    identifier=("namespace3", "sales"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace3/sales",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace3/sales",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "products": lambda catalog: Table(
                    identifier=("namespace2", "bar"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace3/products",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespace3/products",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 2 tables (6 MCPs each) and 1 namespace (4 MCPs)
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespace1.table_xyz,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespace1.JKLtable,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:075fc8fdac17b0eb3482e73052e875f1",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.tables_scanned == 2


def test_handle_expected_exceptions() -> None:
    source = with_iceberg_source(processing_threads=3)

    def _raise_no_such_property_exception(_: Catalog) -> Never:
        raise NoSuchPropertyException()

    def _raise_no_such_iceberg_table_exception(_: Catalog) -> Never:
        raise NoSuchIcebergTableError()

    def _raise_file_not_found_error(_: Catalog) -> Never:
        raise FileNotFoundError()

    def _raise_no_such_table_exception(_: Catalog) -> Never:
        raise NoSuchTableError()

    def _raise_server_error(_: Catalog) -> Never:
        raise ServerError()

    def _raise_rest_error(_: Catalog) -> Never:
        raise RESTError()

    def _raise_os_error(_: Catalog) -> Never:
        raise OSError()

    def _raise_fileio_error(_: Catalog) -> Never:
        raise ValueError("Could not initialize FileIO: abc.dummy.fileio")

    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table2": lambda catalog: Table(
                    identifier=("namespaceA", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceA", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table4": lambda catalog: Table(
                    identifier=("namespaceA", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table5": _raise_no_such_property_exception,
                "table6": _raise_no_such_table_exception,
                "table7": _raise_file_not_found_error,
                "table8": _raise_no_such_iceberg_table_exception,
                "table9": _raise_server_error,
                "table10": _raise_fileio_error,
                "table11": _raise_rest_error,
                "table12": _raise_os_error,
            }
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 4 tables (6 MCPs each) and 1 namespace (4 MCPs)
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table4,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
        ] * MCPS_PER_NAMESPACE
        assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert (
            source.report.warnings.total_elements == 7
        )  # ServerError and RESTError exceptions are caught together
        assert source.report.failures.total_elements == 0
        assert source.report.tables_scanned == 4


def test_handle_unexpected_exceptions() -> None:
    source = with_iceberg_source(processing_threads=3)

    def _raise_exception(_: Catalog) -> Never:
        raise Exception()

    def _raise_other_value_error_exception(_: Catalog) -> Never:
        raise ValueError("Other value exception")

    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda catalog: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table2": lambda catalog: Table(
                    identifier=("namespaceA", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table2",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table3": lambda catalog: Table(
                    identifier=("namespaceA", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table3",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table4": lambda catalog: Table(
                    identifier=("namespaceA", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                        current_schema_id=0,
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table4",
                    io=PyArrowFileIO(),
                    catalog=catalog,
                ),
                "table5": _raise_exception,
                "table6": _raise_other_value_error_exception,
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        # ingested 4 tables (6 MCPs each) and 1 namespace (4 MCPs)
        expected_wu_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table4,PROD)",
        ] * MCPS_PER_TABLE + [
            "urn:li:container:390e031441265aae5b7b7ae8d51b0c1f",
        ] * MCPS_PER_NAMESPACE
        # assert len(wu) == len(expected_wu_urns)
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            urns.append(unit.metadata.entityUrn)
        TestCase().assertCountEqual(
            urns,
            expected_wu_urns,
        )
        assert source.report.warnings.total_elements == 0
        assert source.report.failures.total_elements == 1
        assert source.report.tables_scanned == 4
        # Needed to make sure all failures are recognized properly
        failures = [f for f in source.report.failures]
        TestCase().assertCountEqual(
            failures[0].context,
            [
                "('namespaceA', 'table6') <class 'ValueError'>: Other value exception",
                "('namespaceA', 'table5') <class 'Exception'>: ",
            ],
        )


def test_ingesting_namespace_properties() -> None:
    source = with_iceberg_source(processing_threads=2)
    custom_properties = {"prop1": "foo", "prop2": "bar"}
    mock_catalog = MockCatalog(
        tables={
            "namespaceA": {},  # mapped to urn:li:container:390e031441265aae5b7b7ae8d51b0c1f
            "namespaceB": {},  # mapped to urn:li:container:74727446a56420d80ff3b1abf2a18087
        },
        namespace_properties={"namespaceA": {}, "namespaceB": custom_properties},
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        aspects: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeProposalWrapper)
            mcpw: MetadataChangeProposalWrapper = unit.metadata
            assert mcpw.entityUrn
            assert mcpw.aspectName
            aspects[mcpw.entityUrn][mcpw.aspectName] = mcpw.aspect
        assert (
            aspects["urn:li:container:390e031441265aae5b7b7ae8d51b0c1f"][
                "containerProperties"
            ].customProperties
            == {}
        )
        assert (
            aspects["urn:li:container:74727446a56420d80ff3b1abf2a18087"][
                "containerProperties"
            ].customProperties
            == custom_properties
        )


class TestGlueCatalogRoleAssumption:
    """
    This class tests logic we have to workaround PyIceberg library bug, which causes it to not assume indicated IAM role
    when connecting to a Glue catalog
    """

    @pytest.fixture(autouse=True)
    def mock_load_catalog(self):
        """
        get_catalog function, which we are testing in this class, would call load_catalog, which would in turn
        make a call to boto3.Session, it would bloat our tests, therefore we are mocking it for all of them
        """
        with patch("datahub.ingestion.source.iceberg.iceberg_common.load_catalog"):
            yield

    @pytest.fixture
    def mock_boto3_session(self):
        """Fixture to mock boto3.Session and return configured mocks.

        Returns:
            tuple: (mock_boto3_session, mock_sts_client) for use in tests
        """
        with patch(
            "datahub.ingestion.source.iceberg.iceberg_common.boto3.Session"
        ) as mock_boto3_session:
            mock_session_instance = mock_boto3_session.return_value
            mock_sts = mock_session_instance.client.return_value
            yield mock_boto3_session, mock_sts

    def test_no_role_assumption(self):
        """Test that when no role ARN is provided, no role assumption occurs."""
        catalog_config = {
            "test_glue": {
                "type": "glue",
                "s3.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        with patch(
            "datahub.ingestion.source.iceberg.iceberg_common.boto3"
        ) as mock_boto3:
            config.get_catalog()

            # To assume role we first need a boto3 Session object, since we are not getting it, there is guarantee
            # we are not assuming role neither
            mock_boto3.Session.assert_not_called()

    def test_same_role_no_assumption(self, mock_boto3_session):
        """Test that when current role matches target role, no assumption occurs."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": "arn:aws:iam::123456789012:role/MyRole",
                "s3.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/MyRole/session-name",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        config.get_catalog()
        mock_sts.get_caller_identity.assert_called_once()

        # Should NOT call assume_role since we're already in the target role
        mock_sts.assume_role.assert_not_called()

    def test_same_role_name_different_account(self, mock_boto3_session):
        """Test that when current role name matches but account differs, assumption occurs."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": "arn:aws:iam::123456789012:role/MyRole",
                "s3.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::345678249436:assumed-role/MyRole/session",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEBYaDH...",
                "Expiration": "2024-01-01T00:00:00Z",
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                "Arn": "arn:aws:sts::123456789012:assumed-role/MyRole/session",
            },
        }

        config.get_catalog()
        mock_sts.get_caller_identity.assert_called_once()

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/MyRole",
            RoleSessionName="session",
            DurationSeconds=43200,
        )

        # Verify credentials were updated in catalog config
        updated_config = config.catalog["test_glue"]
        assert updated_config["glue.access-key-id"] == "ASIAIOSFODNN7EXAMPLE"
        assert (
            updated_config["glue.secret-access-key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY"
        )
        assert updated_config["glue.session-token"] == "FwoGZXIvYXdzEBYaDH..."

    def test_different_role_assumption(self, mock_boto3_session):
        """Test successful role assumption when current role differs from target."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": "arn:aws:iam::123456789012:role/TargetRole",
                "s3.region": "us-west-2",
                "glue.access-key-id": "AKIAIOSFODNN7EXAMPLE",
                "glue.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/CurrentRole/session",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEBYaDH...",
                "Expiration": "2024-01-01T00:00:00Z",
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                "Arn": "arn:aws:sts::123456789012:assumed-role/TargetRole/session",
            },
        }

        config.get_catalog()

        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TargetRole",
            RoleSessionName="session",
            DurationSeconds=43200,
        )

        updated_config = config.catalog["test_glue"]
        assert updated_config["glue.access-key-id"] == "ASIAIOSFODNN7EXAMPLE"
        assert (
            updated_config["glue.secret-access-key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY"
        )
        assert updated_config["glue.session-token"] == "FwoGZXIvYXdzEBYaDH..."

    def test_fallback_duration(self, mock_boto3_session):
        """Test role assumption falls back to default duration on ClientError."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": "arn:aws:iam::123456789012:role/TargetRole",
                "s3.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/CurrentRole/session",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.exceptions.ClientError = ClientError

        # First call with long duration fails, second succeeds
        mock_sts.assume_role.side_effect = [
            ClientError(
                {
                    "Error": {
                        "Code": "ValidationError",
                        "Message": "DurationSeconds exceeds maximum",
                    }
                },
                "AssumeRole",
            ),
            {
                "Credentials": {
                    "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
                    "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY",
                    "SessionToken": "FwoGZXIvYXdzEBYaDH...",
                    "Expiration": "2024-01-01T00:00:00Z",
                },
                "AssumedRoleUser": {
                    "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                    "Arn": "arn:aws:sts::123456789012:assumed-role/TargetRole/session",
                },
            },
        ]

        config.get_catalog()

        # Should call assume_role twice: once with long duration, once without
        assert mock_sts.assume_role.call_count == 2

        # First call with long duration
        assert mock_sts.assume_role.call_args_list[0] == (
            (),
            {
                "RoleArn": "arn:aws:iam::123456789012:role/TargetRole",
                "RoleSessionName": "session",
                "DurationSeconds": 43200,
            },
        )

        # Second call without duration (default)
        assert mock_sts.assume_role.call_args_list[1] == (
            (),
            {
                "RoleArn": "arn:aws:iam::123456789012:role/TargetRole",
                "RoleSessionName": "session",
            },
        )

    def test_glue_catalog_role_assumption_with_aws_role_arn_property(
        self, mock_boto3_session
    ):
        """Test that client.role-arn property is also recognized for role assumption."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "client.role-arn": "arn:aws:iam::123456789012:role/TargetRole",
                "client.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/CurrentRole/session",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEBYaDH...",
                "Expiration": "2024-01-01T00:00:00Z",
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                "Arn": "arn:aws:sts::123456789012:assumed-role/TargetRole/session",
            },
        }

        config.get_catalog()

        mock_sts.assume_role.assert_called_once()

        updated_config = config.catalog["test_glue"]
        assert updated_config["glue.access-key-id"] == "ASIAIOSFODNN7EXAMPLE"
        assert (
            updated_config["glue.secret-access-key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY"
        )
        assert updated_config["glue.session-token"] == "FwoGZXIvYXdzEBYaDH..."

    def test_glue_catalog_role_assumption_non_assumed_role_identity(
        self, mock_boto3_session
    ):
        """Test role assumption when current identity is not an assumed role (e.g., IAM user)."""
        mock_session, mock_sts = mock_boto3_session

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": "arn:aws:iam::123456789012:role/TargetRole",
                "s3.region": "us-west-2",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123456789012:user/my-user",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEBYaDH...",
                "Expiration": "2024-01-01T00:00:00Z",
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                "Arn": "arn:aws:sts::123456789012:assumed-role/TargetRole/session",
            },
        }

        config.get_catalog()

        mock_sts.assume_role.assert_called_once()

        updated_config = config.catalog["test_glue"]
        assert updated_config["glue.access-key-id"] == "ASIAIOSFODNN7EXAMPLE"
        assert (
            updated_config["glue.secret-access-key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY"
        )
        assert updated_config["glue.session-token"] == "FwoGZXIvYXdzEBYaDH..."

    def test_glue_catalog_with_all_credential_parameters(self, mock_boto3_session):
        """Test that all credential parameters are passed correctly to boto3 Session."""
        mock_session, mock_sts = mock_boto3_session
        role_to_assume = "arn:aws:iam::123456789012:role/TargetRole"

        catalog_config = {
            "test_glue": {
                "type": "glue",
                "glue.role-arn": role_to_assume,
                "glue.region": "us-west-2",
                "glue.profile-name": "my-profile",
                "glue.access-key-id": "AKIAIOSFODNN7EXAMPLE",
                "glue.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "glue.session-token": "FwoGZXIvYXdzEB...",
            }
        }
        config = IcebergSourceConfig(catalog=catalog_config)

        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/CurrentRole/session",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Account": "123456789012",
        }

        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIAIOSFODNN7EXAMPLE2",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY2",
                "SessionToken": "FwoGZXIvYXdzEBYaDH2...",
                "Expiration": "2024-01-01T00:00:00Z",
            },
            "AssumedRoleUser": {
                "AssumedRoleId": "AROA3XFRBF535PLBIFPI4:session",
                "Arn": "arn:aws:sts::123456789012:assumed-role/TargetRole/session",
            },
        }

        config.get_catalog()

        mock_session.assert_called_once_with(
            profile_name="my-profile",
            region_name="us-west-2",
            botocore_session=None,
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_session_token="FwoGZXIvYXdzEB...",
        )

        mock_sts.assume_role.assert_called_once_with(
            RoleArn=role_to_assume,
            RoleSessionName="session",
            DurationSeconds=43200,
        )

        updated_config = config.catalog["test_glue"]
        assert updated_config["glue.access-key-id"] == "ASIAIOSFODNN7EXAMPLE2"
        assert (
            updated_config["glue.secret-access-key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYZEXAMPLEKEY2"
        )
        assert updated_config["glue.session-token"] == "FwoGZXIvYXdzEBYaDH2..."

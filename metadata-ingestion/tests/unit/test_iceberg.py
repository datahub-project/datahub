import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from unittest import TestCase
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
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

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.iceberg.iceberg import (
    IcebergProfiler,
    IcebergSource,
    IcebergSourceConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import ArrayType, SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DatasetSnapshotClass,
    DateTypeClass,
    FixedTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


def with_iceberg_source(processing_threads: int = 1, **kwargs: Any) -> IcebergSource:
    catalog = {"test": {"type": "rest"}}
    return IcebergSource(
        ctx=PipelineContext(run_id="iceberg-source-test"),
        config=IcebergSourceConfig(
            catalog=catalog, processing_threads=processing_threads, **kwargs
        ),
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


class MockCatalog:
    def __init__(self, tables: Dict[str, Dict[str, Callable[[], Table]]]):
        """

        :param tables: Dictionary containing namespaces as keys and dictionaries containing names of tables (keys) and
                       their metadata as values
        """
        self.tables = tables

    def list_namespaces(self) -> Iterable[Tuple[str]]:
        return [*[(key,) for key in self.tables.keys()]]

    def list_tables(self, namespace: str) -> Iterable[Tuple[str, str]]:
        return [(namespace[0], table) for table in self.tables[namespace[0]].keys()]

    def load_table(self, dataset_path: Tuple[str, str]) -> Table:
        return self.tables[dataset_path[0]][dataset_path[1]]()


class MockCatalogExceptionListingTables(MockCatalog):
    def list_tables(self, namespace: str) -> Iterable[Tuple[str, str]]:
        if namespace == ("no_such_namespace",):
            raise NoSuchNamespaceError()
        if namespace == ("generic_exception",):
            raise Exception()
        return super().list_tables(namespace)


class MockCatalogExceptionListingNamespaces(MockCatalog):
    def list_namespaces(self) -> Iterable[Tuple[str]]:
        raise Exception()


def test_exception_while_listing_namespaces() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingNamespaces({})
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog, pytest.raises(Exception):
        get_catalog.return_value = mock_catalog
        [*source.get_workunits_internal()]


def test_known_exception_while_listing_tables() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingTables(
        {
            "namespaceA": {
                "table1": lambda: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
            "no_such_namespace": {},
            "namespaceB": {
                "table2": lambda: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table3": lambda: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
            },
            "namespaceC": {
                "table4": lambda: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
            "namespaceD": {
                "table5": lambda: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        assert len(wu) == 5  # ingested 5 tables, despite exception
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeEvent)
            assert isinstance(unit.metadata.proposedSnapshot, DatasetSnapshotClass)
            urns.append(unit.metadata.proposedSnapshot.urn)
        TestCase().assertCountEqual(
            urns,
            [
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
            ],
        )
        assert source.report.warnings.total_elements == 1
        assert source.report.failures.total_elements == 0
        assert source.report.tables_scanned == 5


def test_unknown_exception_while_listing_tables() -> None:
    source = with_iceberg_source(processing_threads=2)
    mock_catalog = MockCatalogExceptionListingTables(
        {
            "namespaceA": {
                "table1": lambda: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
            "generic_exception": {},
            "namespaceB": {
                "table2": lambda: Table(
                    identifier=("namespaceB", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table2",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table3": lambda: Table(
                    identifier=("namespaceB", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceB/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceB/table3",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
            },
            "namespaceC": {
                "table4": lambda: Table(
                    identifier=("namespaceC", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceC/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceC/table4",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
            "namespaceD": {
                "table5": lambda: Table(
                    identifier=("namespaceD", "table5"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table5",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table5",
                    io=PyArrowFileIO(),
                    catalog=None,
                )
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        assert len(wu) == 5  # ingested 5 tables, despite exception
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeEvent)
            assert isinstance(unit.metadata.proposedSnapshot, DatasetSnapshotClass)
            urns.append(unit.metadata.proposedSnapshot.urn)
        TestCase().assertCountEqual(
            urns,
            [
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table2,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceB.table3,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceC.table4,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceD.table5,PROD)",
            ],
        )
        assert source.report.warnings.total_elements == 0
        assert source.report.failures.total_elements == 1
        assert source.report.tables_scanned == 5


def test_proper_run_with_multiple_namespaces() -> None:
    source = with_iceberg_source(processing_threads=3)
    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=None,
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
        assert len(wu) == 1  # only one table processed as an MCE
        assert isinstance(wu[0].metadata, MetadataChangeEvent)
        assert isinstance(wu[0].metadata.proposedSnapshot, DatasetSnapshotClass)
        snapshot: DatasetSnapshotClass = wu[0].metadata.proposedSnapshot
        assert (
            snapshot.urn
            == "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)"
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
                "table_xyz": lambda: Table(
                    identifier=("namespace1", "table_xyz"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/table_xyz",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace1/table_xyz",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "JKLtable": lambda: Table(
                    identifier=("namespace1", "JKLtable"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/JKLtable",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace1/JKLtable",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table_abcd": lambda: Table(
                    identifier=("namespace1", "table_abcd"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/table_abcd",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace1/table_abcd",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "aaabcd": lambda: Table(
                    identifier=("namespace1", "aaabcd"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace1/aaabcd",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace1/aaabcd",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
            },
            "namespace2": {
                "foo": lambda: Table(
                    identifier=("namespace2", "foo"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace2/foo",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace2/foo",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "bar": lambda: Table(
                    identifier=("namespace2", "bar"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace2/bar",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace2/bar",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
            },
            "namespace3": {
                "sales": lambda: Table(
                    identifier=("namespace3", "sales"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace3/sales",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace3/sales",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "products": lambda: Table(
                    identifier=("namespace2", "bar"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespace3/products",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespace3/products",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
            },
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        assert len(wu) == 2
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeEvent)
            assert isinstance(unit.metadata.proposedSnapshot, DatasetSnapshotClass)
            urns.append(unit.metadata.proposedSnapshot.urn)
        TestCase().assertCountEqual(
            urns,
            [
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespace1.table_xyz,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespace1.JKLtable,PROD)",
            ],
        )
        assert source.report.tables_scanned == 2


def test_handle_expected_exceptions() -> None:
    source = with_iceberg_source(processing_threads=3)

    def _raise_no_such_property_exception():
        raise NoSuchPropertyException()

    def _raise_no_such_iceberg_table_exception():
        raise NoSuchIcebergTableError()

    def _raise_file_not_found_error():
        raise FileNotFoundError()

    def _raise_no_such_table_exception():
        raise NoSuchTableError()

    def _raise_server_error():
        raise ServerError()

    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table2": lambda: Table(
                    identifier=("namespaceA", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table2",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table3": lambda: Table(
                    identifier=("namespaceA", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table3",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table4": lambda: Table(
                    identifier=("namespaceA", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table4",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table5": _raise_no_such_property_exception,
                "table6": _raise_no_such_table_exception,
                "table7": _raise_file_not_found_error,
                "table8": _raise_no_such_iceberg_table_exception,
                "table9": _raise_server_error,
            }
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        assert len(wu) == 4
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeEvent)
            assert isinstance(unit.metadata.proposedSnapshot, DatasetSnapshotClass)
            urns.append(unit.metadata.proposedSnapshot.urn)
        TestCase().assertCountEqual(
            urns,
            [
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table2,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table3,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table4,PROD)",
            ],
        )
        assert source.report.warnings.total_elements == 5
        assert source.report.failures.total_elements == 0
        assert source.report.tables_scanned == 4


def test_handle_unexpected_exceptions() -> None:
    source = with_iceberg_source(processing_threads=3)

    def _raise_exception():
        raise Exception()

    mock_catalog = MockCatalog(
        {
            "namespaceA": {
                "table1": lambda: Table(
                    identifier=("namespaceA", "table1"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table1",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table1",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table2": lambda: Table(
                    identifier=("namespaceA", "table2"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table2",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table2",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table3": lambda: Table(
                    identifier=("namespaceA", "table3"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table3",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table3",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table4": lambda: Table(
                    identifier=("namespaceA", "table4"),
                    metadata=TableMetadataV2(
                        partition_specs=[PartitionSpec(spec_id=0)],
                        location="s3://abcdefg/namespaceA/table4",
                        last_column_id=0,
                        schemas=[Schema(schema_id=0)],
                    ),
                    metadata_location="s3://abcdefg/namespaceA/table4",
                    io=PyArrowFileIO(),
                    catalog=None,
                ),
                "table5": _raise_exception,
            }
        }
    )
    with patch(
        "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
    ) as get_catalog:
        get_catalog.return_value = mock_catalog
        wu: List[MetadataWorkUnit] = [*source.get_workunits_internal()]
        assert len(wu) == 4
        urns = []
        for unit in wu:
            assert isinstance(unit.metadata, MetadataChangeEvent)
            assert isinstance(unit.metadata.proposedSnapshot, DatasetSnapshotClass)
            urns.append(unit.metadata.proposedSnapshot.urn)
        TestCase().assertCountEqual(
            urns,
            [
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table2,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table3,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:iceberg,namespaceA.table4,PROD)",
            ],
        )
        assert source.report.warnings.total_elements == 0
        assert source.report.failures.total_elements == 1
        assert source.report.tables_scanned == 4

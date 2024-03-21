from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
)
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column


def test_get_avro_schema_for_hive_column():
    schema_fields = get_schema_fields_for_hive_column("test", "int")
    assert schema_fields[0].type.type == NumberTypeClass()
    # Len will be the struct + 2 key there which should remain after the deduplication
    assert len(schema_fields) == 1


def test_get_avro_schema_for_struct_hive_column():
    schema_fields = get_schema_fields_for_hive_column("test", "struct<test:int>")
    assert schema_fields[0].type.type == RecordTypeClass()
    assert len(schema_fields) == 2


def test_get_avro_schema_for_struct_hive_with_duplicate_column():
    schema_fields = get_schema_fields_for_hive_column(
        "test", "struct<test:int, test2:int, test:int>"
    )
    assert schema_fields[0].type.type == RecordTypeClass()
    # Len will be the struct + 2 key there which should remain after the deduplication
    assert len(schema_fields) == 3


def test_get_avro_schema_for_struct_hive_with_duplicate_column2():
    invalid_schema: str = "struct!test:intdsfs, test2:int, test:int>"
    schema_fields = get_schema_fields_for_hive_column("test", invalid_schema)
    assert len(schema_fields) == 1
    assert schema_fields[0].type.type == NullTypeClass()
    assert schema_fields[0].fieldPath == "test"
    assert schema_fields[0].nativeDataType == invalid_schema


def test_get_avro_schema_for_null_type_hive_column():
    schema_fields = get_schema_fields_for_hive_column(
        hive_column_name="test", hive_column_type="unknown"
    )
    assert schema_fields[0].type.type == NullTypeClass()
    assert len(schema_fields) == 1

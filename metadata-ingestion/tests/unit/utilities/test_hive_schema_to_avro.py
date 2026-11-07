import json

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
)
from datahub.utilities.hive_schema_to_avro import (
    get_column_descriptions_from_type_json,
    get_schema_fields_for_hive_column,
)


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


def test_list_type_treated_as_array():
    # Iceberg uses `list<T>` as a synonym for `array<T>`
    schema_fields = get_schema_fields_for_hive_column("engines", "list<string>")
    assert isinstance(schema_fields[0].type.type, ArrayTypeClass)


def test_list_of_structs_inside_struct():
    # Iceberg: struct field whose type is list<struct<...>>.
    schema_fields = get_schema_fields_for_hive_column(
        "col", "struct<items:list<struct<name:string,value:int>>>"
    )
    assert isinstance(schema_fields[0].type.type, RecordTypeClass)
    assert not any(isinstance(f.type.type, NullTypeClass) for f in schema_fields), (
        "no field should fall back to NullType"
    )


def test_get_column_descriptions_from_type_json_nested_struct_and_array():
    type_json = json.dumps(
        {
            "name": "details",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "col_a",
                        "type": "string",
                        "nullable": True,
                        "metadata": {"comment": "Column A comment"},
                    },
                    {
                        "name": "items",
                        "type": {
                            "type": "array",
                            "elementType": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "item_id",
                                        "type": "string",
                                        "nullable": True,
                                        "metadata": {"comment": "Item identifier"},
                                    },
                                    {
                                        "name": "item_count",
                                        "type": "integer",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                ],
                            },
                            "containsNull": True,
                        },
                        "nullable": True,
                        "metadata": {"comment": "List of items"},
                    },
                ],
            },
            "nullable": True,
            "metadata": {"comment": "Details record"},
        }
    )

    descriptions = get_column_descriptions_from_type_json(type_json)
    assert descriptions == {
        "details": "Details record",
        "details.col_a": "Column A comment",
        "details.items": "List of items",
        "details.items.item_id": "Item identifier",
    }


def test_get_column_descriptions_from_type_json_map_and_invalid_inputs():
    map_type_json = json.dumps(
        {
            "name": "attributes",
            "type": {
                "type": "map",
                "keyType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "code",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "Key code"},
                        }
                    ],
                },
                "valueType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "label",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "Attribute label"},
                        },
                        {
                            "name": "code",
                            "type": "string",
                            "nullable": True,
                            "metadata": {},
                        },
                    ],
                },
                "valueContainsNull": True,
            },
            "nullable": True,
            "metadata": {},
        }
    )
    assert get_column_descriptions_from_type_json(map_type_json) == {
        "attributes.label": "Attribute label"
    }
    assert get_column_descriptions_from_type_json("") == {}
    assert get_column_descriptions_from_type_json("not-json") == {}
    assert (
        get_column_descriptions_from_type_json(
            '{"name":"col_a","type":"integer","nullable":true,"metadata":{}}'
        )
        == {}
    )

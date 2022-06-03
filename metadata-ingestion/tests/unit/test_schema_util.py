import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Type

import pytest

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    DateTypeClass,
    NumberTypeClass,
    SchemaField,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)

SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE = """
{
  "type": "record",
  "name": "TestRecord",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my.field",
      "type": ["null", "string"],
      "doc": "some.doc"
    }
  ]
}
"""

SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION = """
{
  "type": "record",
  "name": "TestRecord",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my.field",
      "type": ["string", "null"],
      "doc": "some.doc"
    }
  ]
}
"""

SCHEMA_WITH_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE = """
{
  "type": "record",
  "name": "TestRecord",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my.field",
      "type": "null",
      "doc": "some.doc"
    }
  ]
}
"""

SCHEMA_WITH_OPTIONAL_FIELD_VIA_FIXED_TYPE: str = json.dumps(
    {
        "type": "record",
        "name": "__struct_",
        "fields": [
            {
                "name": "value",
                "type": {
                    "type": "fixed",
                    "name": "__fixed_d9d2d051916045d9975d6c573aaabb89",
                    "size": 4,
                    "native_data_type": "fixed[4]",
                    "_nullable": True,
                },
            },
        ],
    }
)


def log_field_paths(fields: List[SchemaField]) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')


def assert_field_paths_are_unique(fields: List[SchemaField]) -> None:
    avro_fields_paths = [
        f.fieldPath for f in fields if re.match(".*[^]]$", f.fieldPath)
    ]

    if avro_fields_paths:
        assert len(avro_fields_paths) == len(set(avro_fields_paths))


def assert_field_paths_match(
    fields: List[SchemaField], expected_field_paths: List[str]
) -> None:
    log_field_paths(fields)
    assert len(fields) == len(expected_field_paths)
    for f, efp in zip(fields, expected_field_paths):
        assert f.fieldPath == efp
    assert_field_paths_are_unique(fields)


@pytest.mark.parametrize(
    "schema",
    [
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE,
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION,
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE,
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_FIXED_TYPE,
    ],
    ids=[
        "optional_field_via_union_type",
        "optional_field_via_union_null_not_first",
        "optional_field_via_primitive",
        "optional_field_via_fixed",
    ],
)
def test_avro_schema_to_mce_fields_events_with_nullable_fields(schema):
    fields = avro_schema_to_mce_fields(schema)
    assert 1 == len(fields)
    assert fields[0].nullable


def test_avro_schema_to_mce_fields_sample_events_with_different_field_types():
    schema = """
{
  "type": "record",
  "name": "R",
  "namespace": "some.namespace",
  "fields": [
    {
      "name": "a_map_of_longs_field",
      "type": {
        "type": "map",
        "values": "long"
      }
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=R].[type=map].[type=long].a_map_of_longs_field",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_record_with_two_fields():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "not.relevant.namespace",
  "fields": [
    {
      "name": "a",
      "type": "string",
      "doc": "some.doc"
    },
    {
      "name": "b",
      "type": "string",
      "doc": "some.doc"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=name].[type=string].a",
        "[version=2.0].[type=name].[type=string].b",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = """
{
  "type": "string"
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = ["[version=2.0].[type=string]"]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_schema_namespacing():
    schema = """
{
  "type": "record",
  "name": "name",
  "namespace": "should.not.show.up.namespace",
  "fields": [
    {
      "name": "aStringField",
      "type": "string",
      "doc": "some docs",
      "default": "this is custom, default value"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=name].[type=string].aStringField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_with_default():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "not.relevant.namespace",
  "fields": [
    {
      "name": "aStringField",
      "type": "string",
      "doc": "some docs",
      "default": "this is custom, default value"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=name].[type=string].aStringField",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    description = fields[0].description
    assert description and "custom, default value" in description


def test_avro_schema_with_recursion():
    schema = """
{
    "type": "record",
    "name": "TreeNode",
    "fields": [
        {
            "name": "value",
            "type": "long"
        },
        {
            "name": "children",
            "type": { "type": "array", "items": "TreeNode" }
        }
    ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=TreeNode].[type=long].value",
        "[version=2.0].[type=TreeNode].[type=array].[type=TreeNode].children",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_sample_payment_schema_to_mce_fields_with_nesting():
    schema = """
{
  "type": "record",
  "name": "Payment",
  "namespace": "some.event.namespace",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "name","type": "string","default": ""},
    {"name": "phoneNumber",
     "type": [{
         "type": "record",
         "name": "PhoneNumber",
         "fields": [{
             "name": "areaCode",
             "type": "string",
             "default": ""
             }, {
             "name": "countryCode",
             "type": "string",
             "default": ""
             }, {
             "name": "prefix",
             "type": "string",
             "default": ""
             }, {
             "name": "number",
             "type": "string",
             "default": ""
             }]
         },
         "null"
     ],
     "default": "null"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=Payment].[type=string].id",
        "[version=2.0].[type=Payment].[type=double].amount",
        "[version=2.0].[type=Payment].[type=string].name",
        "[version=2.0].[type=Payment].[type=PhoneNumber].phoneNumber",
        "[version=2.0].[type=Payment].[type=PhoneNumber].phoneNumber.[type=string].areaCode",
        "[version=2.0].[type=Payment].[type=PhoneNumber].phoneNumber.[type=string].countryCode",
        "[version=2.0].[type=Payment].[type=PhoneNumber].phoneNumber.[type=string].prefix",
        "[version=2.0].[type=Payment].[type=PhoneNumber].phoneNumber.[type=string].number",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_with_nesting_across_records():
    schema = """
[
    {
        "type": "record",
        "name": "Address",
        "fields": [
            {"name": "streetAddress", "type": "string"},
            {"name": "city", "type": "string"}
        ]
    },
    {
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "firstname", "type": "string"},
            {"name": "lastname", "type": "string" },
            {"name": "address", "type": "Address"}
        ]
    }
]
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=union]",
        "[version=2.0].[type=union].[type=Address].[type=string].streetAddress",
        "[version=2.0].[type=union].[type=Address].[type=string].city",
        "[version=2.0].[type=union].[type=Person].[type=string].firstname",
        "[version=2.0].[type=union].[type=Person].[type=string].lastname",
        "[version=2.0].[type=union].[type=Person].[type=Address].address",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_simple_record_with_primitive_types():
    schema = """
    {
        "type": "record",
        "name": "Simple",
        "namespace": "com.linkedin",
        "fields": [
            {"name": "stringField", "type": "string", "doc": "string field"},
            {"name": "booleanField", "type": "boolean" },
            {"name": "intField", "type": "int" },
            {
                "name": "enumField",
                "type": {
                    "type": "enum",
                    "name": "MyTestEnumField",
                    "symbols": [ "TEST", "TEST1" ],
                    "symbolDoc": {
                        "TEST": "test enum",
                        "TEST1": "test1 enum"
                    }
                }
            }
        ]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=Simple].[type=string].stringField",
        "[version=2.0].[type=Simple].[type=boolean].booleanField",
        "[version=2.0].[type=Simple].[type=int].intField",
        "[version=2.0].[type=Simple].[type=enum].enumField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_simple_nested_record_with_a_string_field_for_key_schema():
    schema = """
    {
        "type": "record",
        "name": "SimpleNested",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "nestedRcd",
            "type": {
                "type": "record",
                "name": "InnerRcd",
                "fields": [{
                    "name": "aStringField",
                     "type": "string"
                } ]
            }
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema, True)
    expected_field_paths: List[str] = [
        "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd",
        "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd.[type=string].aStringField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_union_with_nested_record_of_union():
    schema = """
    {
        "type": "record",
        "name": "UnionSample",
        "namespace": "com.linkedin",
        "fields": [
            {
                "name": "aUnion",
                "type": [
                    "boolean",
                    {
                        "type": "record",
                        "name": "Rcd",
                        "fields": [
                            {
                                "name": "aNullableStringField",
                                "type": ["null", "string"]
                            }
                        ]
                    }
                ]
            }
        ]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=UnionSample].[type=union].aUnion",
        "[version=2.0].[type=UnionSample].[type=union].[type=boolean].aUnion",
        "[version=2.0].[type=UnionSample].[type=union].[type=Rcd].aUnion",
        "[version=2.0].[type=UnionSample].[type=union].[type=Rcd].aUnion.[type=string].aNullableStringField",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert isinstance(fields[3].type.type, StringTypeClass)
    assert fields[0].nativeDataType == "union"
    assert fields[1].nativeDataType == "boolean"
    assert fields[2].nativeDataType == "Rcd"
    assert fields[3].nativeDataType == "string"


def test_nested_arrays():
    schema = """
{
    "type": "record",
    "name": "NestedArray",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "ar",
        "type": {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "name": "Foo",
                        "fields": [ {
                            "name": "a",
                            "type": "long"
                        } ]
                    }
                ]
            }
        }
    } ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths: List[str] = [
        "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar",
        "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar.[type=long].a",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_map_of_union_of_int_and_record_of_union():
    schema = """
    {
        "type": "record",
        "name": "MapSample",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "aMap",
            "type": {
                "type": "map",
                "values": [
                    "int",
                    {
                        "type": "record",
                        "name": "Rcd",
                        "fields": [{
                            "name": "aUnion",
                            "type": ["null", "string", "int"]
                        }]
                    }
               ]
            }
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=MapSample].[type=map].[type=union].aMap",
        "[version=2.0].[type=MapSample].[type=map].[type=union].[type=int].aMap",
        "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap",
        "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].aUnion",
        "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].[type=string].aUnion",
        "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].[type=int].aUnion",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_recursive_avro():
    schema = """
    {
        "type": "record",
        "name": "Recursive",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "r",
            "type": {
                "type": "record",
                "name": "R",
                "fields": [
                    { "name" : "anIntegerField", "type" : "int" },
                    { "name": "aRecursiveField", "type": "com.linkedin.R"}
                ]
            }
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [
        "[version=2.0].[type=Recursive].[type=R].r",
        "[version=2.0].[type=Recursive].[type=R].r.[type=int].anIntegerField",
        "[version=2.0].[type=Recursive].[type=R].r.[type=R].aRecursiveField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_needs_disambiguation_nested_union_of_records_with_same_field_name():
    schema = """
    {
        "type": "record",
        "name": "ABFooUnion",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "a",
            "type": [ {
                "type": "record",
                "name": "A",
                "fields": [{ "name": "f", "type": "string" } ]
                }, {
                "type": "record",
                "name": "B",
                "fields": [{ "name": "f", "type": "string" } ]
                }, {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "Foo",
                            "fields": [{ "name": "f", "type": "long" }]
                        }
                    ]
                }
        }]
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths: List[str] = [
        "[version=2.0].[type=ABFooUnion].[type=union].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_mce_avro_parses_okay():
    """This test helps to exercise the complexity in parsing and catch unexpected regressions."""
    schema = Path(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "src",
            "datahub",
            "metadata",
            "schema.avsc",
        )
    ).read_text()
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields)
    # Ensure that all the paths corresponding to the AVRO fields are unique.
    assert_field_paths_are_unique(fields)
    log_field_paths(fields)


def test_key_schema_handling():
    """Tests key schema handling"""
    schema = """
    {
        "type": "record",
        "name": "ABFooUnion",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "a",
            "type": [ {
                "type": "record",
                "name": "A",
                "fields": [{ "name": "f", "type": "string" } ]
                }, {
                "type": "record",
                "name": "B",
                "fields": [{ "name": "f", "type": "string" } ]
                }, {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "Foo",
                            "fields": [{ "name": "f", "type": "long" }]
                        }
                    ]
                }
        }]
        }]
    }
"""
    fields: List[SchemaField] = avro_schema_to_mce_fields(schema, is_key_schema=True)
    expected_field_paths: List[str] = [
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=A].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=B].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    for f in fields:
        assert f.isPartOfKey


def test_logical_types_bare():
    schema: str = """
{
    "type": "record",
    "name": "test_logical_types",
    "fields":  [
        {"name": "decimal_logical", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2},
        {"name": "uuid_logical", "type": "string", "logicalType": "uuid"},
        {"name": "date_logical", "type": "int", "logicalType": "date"},
        {"name": "time_millis_logical", "type": "int", "logicalType": "time-millis"},
        {"name": "time_micros_logical", "type": "long", "logicalType": "time-micros"},
        {"name": "timestamp_millis_logical", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "timestamp_micros_logical", "type": "long", "logicalType": "timestamp-micros"}
    ]
}
    """
    fields: List[SchemaField] = avro_schema_to_mce_fields(schema, is_key_schema=False)
    # validate field paths
    expected_field_paths: List[str] = [
        "[version=2.0].[type=test_logical_types].[type=bytes].decimal_logical",
        "[version=2.0].[type=test_logical_types].[type=string].uuid_logical",
        "[version=2.0].[type=test_logical_types].[type=int].date_logical",
        "[version=2.0].[type=test_logical_types].[type=int].time_millis_logical",
        "[version=2.0].[type=test_logical_types].[type=long].time_micros_logical",
        "[version=2.0].[type=test_logical_types].[type=long].timestamp_millis_logical",
        "[version=2.0].[type=test_logical_types].[type=long].timestamp_micros_logical",
    ]
    assert_field_paths_match(fields, expected_field_paths)

    # validate field types.
    expected_types: List[Type] = [
        NumberTypeClass,
        StringTypeClass,
        DateTypeClass,
        TimeTypeClass,
        TimeTypeClass,
        TimeTypeClass,
        TimeTypeClass,
    ]
    assert expected_types == [type(field.type.type) for field in fields]


def test_logical_types_fully_specified_in_type():
    schema: Dict = {
        "type": "record",
        "name": "test",
        "fields": [
            {
                "name": "name",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 3,
                    "scale": 2,
                    "native_data_type": "decimal(3, 2)",
                    "_nullable": True,
                },
            }
        ],
    }
    fields: List[SchemaField] = avro_schema_to_mce_fields(
        json.dumps(schema), default_nullable=True
    )
    assert len(fields) == 1
    assert "[version=2.0].[type=test].[type=bytes].name" == fields[0].fieldPath
    assert isinstance(fields[0].type.type, NumberTypeClass)


def test_ignore_exceptions():
    malformed_schema: str = """
  "name": "event_ts",
  "type": "long",
  "logicalType": "timestamp-millis",
  "tags": [
    "business-timestamp"
  ]
"""
    fields: List[SchemaField] = avro_schema_to_mce_fields(malformed_schema)
    assert not fields

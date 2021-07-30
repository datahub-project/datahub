import os
import pytest
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from pathlib import Path

SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE = """
{
  "type": "record",
  "name": "some.event.name",
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
  "name": "some.event.name",
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
  "name": "some.event.name",
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


@pytest.mark.parametrize(
    "schema",
    [
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE,
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION,
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE,
    ],
    ids=[
        "optional_field_via_union_type",
        "optional_field_via_union_null_not_first",
        "optional_field_via_primitive",
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
  "name": "some.event.name",
  "namespace": "some.namespace",
  "fields": [
    {
      "name": "some.field.name",
      "type": {
        "type": "map",
        "values": "long"
      }
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    assert 1 == len(fields)
    assert fields[0].fieldPath == "some.field.name"


def test_avro_schema_to_mce_fields_record_with_two_fields():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my.field.A",
      "type": "string",
      "doc": "some.doc"
    },
    {
      "name": "my.field.B",
      "type": "string",
      "doc": "some.doc"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 2
    assert fields[0].fieldPath == "my.field.A"
    assert fields[1].fieldPath == "my.field.B"


def test_avro_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = """
{
  "type": "string"
}
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1
    assert fields[0].fieldPath == ""


def test_avro_schema_to_mce_fields_with_default():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my.field",
      "type": "string",
      "doc": "some docs",
      "default": "this is custom, default value"
    }
  ]
}
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1
    assert fields[0].fieldPath == "my.field"
    assert fields[0].description and "custom, default value" in fields[0].description


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
    assert len(fields) == 2
    assert fields[0].fieldPath == "value"
    assert fields[1].fieldPath == "children"


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
    assert len(fields) == 7
    field_paths = ["id", "amount", "name", "phoneNumber.areaCode", "phoneNumber.countryCode", "phoneNumber.prefix",
                   "phoneNumber.number"]
    for i, f in enumerate(fields):
        assert f.fieldPath == field_paths[i]


def test_avro_schema_to_mce_fields_with_nesting_across_records():
    schema = """
[
    {
        "type": "record",
        "name": "Address",
        "fields": [
            {"name": "streetaddress", "type": "string"},
            {"name": "city", "type": "string"}
        ]
    },
    {
        "type": "record",
        "name": "person",
        "fields": [
            {"name": "firstname", "type": "string"},
            {"name": "lastname", "type": "string" },
            {"name": "address", "type": "Address"}
        ]
    }
]
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 5
    expected_field_paths = ["streetaddress", "city", "firstname", "lastname", "address"]
    for i, f in enumerate(fields):
        assert f.fieldPath == expected_field_paths[i]


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
                    "name": "EnumField",
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
    assert len(fields) == 4
    field_paths = ["stringField", "booleanField", "intField", "enumField"]
    for i, f in enumerate(fields):
        assert f.fieldPath == field_paths[i]


def test_simple_nested_record_with_a_string_field():
    schema = """
    {
        "type": "record",
        "name": "Simple",
        "namespace": "com.linkedin",
        "fields": [{
            "name": "nestedRcd",
            "type": {
                "type": "record",
                "name": "NestedRcd",
                "fields": [{
                    "name": "aStringField",
                     "type": "string"
                } ]
            }
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1
    assert fields[0].fieldPath == "nestedRcd.aStringField"


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
    assert len(fields) == 1
    assert fields[0].fieldPath == "aUnion.aNullableStringField"


def test_nested_arrays():
    schema = """
{
    "type": "record",
    "name": "nestedArrays",
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
    assert len(fields) == 1
    assert fields[0].fieldPath == "ar.a"


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
                            "type": ["null", "string"]
                        }]
                    }
               ]
            }
        }]
    }
"""
    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1
    assert fields[0].fieldPath == "aMap.aUnion"


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
    assert len(fields) == 2
    assert fields[0].fieldPath == "r.anIntegerField"
    assert fields[1].fieldPath == "r.aRecursiveField"


def test_mce_avro_parses_okay():
    # This helps to exercise the complexity in parsing and catch unexpected regressions.
    schema = Path(
        os.path.join(os.path.dirname(__file__), "..", "..", "src", "datahub", "metadata", "schema.avsc")).read_text()
    fields = avro_schema_to_mce_fields(schema)

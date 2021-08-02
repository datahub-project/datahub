import os
from pathlib import Path

import pytest

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields

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
    assert 1 == len(fields)
    assert fields[0].fieldPath == "[type=map]a_map_of_longs_field"


def test_avro_schema_to_mce_fields_record_with_two_fields():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "some.event.namespace",
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
    assert len(fields) == 2
    assert fields[0].fieldPath == "[type=string]a"
    assert fields[1].fieldPath == "[type=string]b"


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
    assert fields[0].fieldPath == "[type=string]my.field"
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
    assert fields[0].fieldPath == "[type=long]value"
    assert fields[1].fieldPath == "[type=array]children"


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
    assert len(fields) == 8
    field_paths = [
        "[type=string]id",
        "[type=double]amount",
        "[type=string]name",
        "[type=union]phoneNumber",
        "[type=union]phoneNumber.[type=union][member=PhoneNumber].[type=string]areaCode",
        "[type=union]phoneNumber.[type=union][member=PhoneNumber].[type=string]countryCode",
        "[type=union]phoneNumber.[type=union][member=PhoneNumber].[type=string]prefix",
        "[type=union]phoneNumber.[type=union][member=PhoneNumber].[type=string]number",
    ]
    for i, f in enumerate(fields):
        assert f.fieldPath == field_paths[i]


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
    assert len(fields) == 5
    expected_field_paths = [
        "[type=union][member=Address].[type=string]streetAddress",
        "[type=union][member=Address].[type=string]city",
        "[type=union][member=Person].[type=string]firstname",
        "[type=union][member=Person].[type=string]lastname",
        "[type=union][member=Person].[type=Address]address",
    ]
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
    field_paths = [
        "[type=string]stringField",
        "[type=boolean]booleanField",
        "[type=int]intField",
        "[type=enum]enumField",
    ]
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
    assert len(fields) == 2
    assert fields[0].fieldPath == "[type=NestedRcd]nestedRcd"
    assert fields[1].fieldPath == "[type=NestedRcd]nestedRcd.[type=string]aStringField"


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
    assert len(fields) == 2
    assert fields[0].fieldPath == "[type=union]aUnion"
    assert (
        fields[1].fieldPath
        == "[type=union]aUnion.[type=union][member=Rcd].[type=union]aNullableStringField"
    )


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
    assert len(fields) == 2
    assert fields[0].fieldPath == "[type=array]ar"
    assert (
        fields[1].fieldPath
        == "[type=array]ar.[type=array].[type=array].[type=union][member=Foo].[type=long]a"
    )


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
    assert len(fields) == 2
    assert fields[0].fieldPath == "[type=map]aMap"
    assert (
        fields[1].fieldPath
        == "[type=map]aMap.[value=union].[type=union][member=Rcd].[type=union]aUnion"
    )


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
    assert len(fields) == 3
    assert fields[0].fieldPath == "[type=R]r"
    assert fields[1].fieldPath == "[type=R]r.[type=int]anIntegerField"
    assert fields[2].fieldPath == "[type=R]r.[type=R]aRecursiveField"


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
    assert len(fields) == 4
    assert fields[0].fieldPath == "[type=union]a"
    assert fields[1].fieldPath == "[type=union]a.[type=union][member=A].[type=string]f"
    assert fields[2].fieldPath == "[type=union]a.[type=union][member=B].[type=string]f"
    assert (
        fields[3].fieldPath
        == "[type=union]a.[type=union][member=array].[type=array].[type=array].[type=union][member=Foo].[type=long]f"
    )


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
    # Ensure that all fields have a unique path.
    assert len({f.fieldPath for f in fields}) == len(fields)

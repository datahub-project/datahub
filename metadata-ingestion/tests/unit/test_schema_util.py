import logging
import os
from pathlib import Path
from typing import List

import pytest

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

logger = logging.getLogger(__name__)

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


def log_field_paths(fields: List[SchemaField]) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')


def assret_field_paths_match(
    fields: List[SchemaField], expected_field_paths: List[str]
) -> None:
    log_field_paths(fields)
    assert len(fields) == len(expected_field_paths)
    for f, efp in zip(fields, expected_field_paths):
        assert f.fieldPath == efp


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
    assert 2 == len(fields)
    assert fields[1].nullable


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
    expected_field_paths = ["[type=name]", "[type=name].[type=map]a_map_of_longs_field"]
    assret_field_paths_match(fields, expected_field_paths)


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
    expected_field_paths = [
        "[type=name]",
        "[type=name].[type=string]a",
        "[type=name].[type=string]b",
    ]
    assret_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = """
{
  "type": "string"
}
"""
    fields = avro_schema_to_mce_fields(schema)
    expected_field_paths = [""]
    assret_field_paths_match(fields, expected_field_paths)


def test_avro_schema_to_mce_fields_with_default():
    schema = """
{
  "type": "record",
  "name": "some.event.name",
  "namespace": "some.event.namespace",
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
    expected_field_paths = ["[type=name]", "[type=name].[type=string]aStringField"]
    assret_field_paths_match(fields, expected_field_paths)
    assert fields[1].description and "custom, default value" in fields[1].description


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
        "[type=TreeNode]",
        "[type=TreeNode].[type=long]value",
        "[type=TreeNode].[type=array]children",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=Payment]",
        "[type=Payment].[type=string]id",
        "[type=Payment].[type=double]amount",
        "[type=Payment].[type=string]name",
        "[type=Payment].[type=PhoneNumber]phoneNumber",
        "[type=Payment].[type=PhoneNumber]phoneNumber.[type=PhoneNumber]",
        "[type=Payment].[type=PhoneNumber]phoneNumber.[type=PhoneNumber].[type=string]areaCode",
        "[type=Payment].[type=PhoneNumber]phoneNumber.[type=PhoneNumber].[type=string]countryCode",
        "[type=Payment].[type=PhoneNumber]phoneNumber.[type=PhoneNumber].[type=string]prefix",
        "[type=Payment].[type=PhoneNumber]phoneNumber.[type=PhoneNumber].[type=string]number",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=union]",
        "[type=union].[type=Address]",
        "[type=union].[type=Address].[type=string]streetAddress",
        "[type=union].[type=Address].[type=string]city",
        "[type=union].[type=Person]",
        "[type=union].[type=Person].[type=string]firstname",
        "[type=union].[type=Person].[type=string]lastname",
        "[type=union].[type=Person].[type=Address]address",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
    expected_field_paths = [
        "[type=Simple]",
        "[type=Simple].[type=string]stringField",
        "[type=Simple].[type=boolean]booleanField",
        "[type=Simple].[type=int]intField",
        "[type=Simple].[type=enum]enumField",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[key=True].[type=SimpleNested]",
        "[key=True].[type=SimpleNested].[type=InnerRcd]nestedRcd",
        "[key=True].[type=SimpleNested].[type=InnerRcd]nestedRcd.[type=string]aStringField",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=UnionSample]",
        "[type=UnionSample].[type=union]aUnion",
        "[type=UnionSample].[type=union]aUnion.[type=Rcd]",
        "[type=UnionSample].[type=union]aUnion.[type=Rcd].[type=string]aNullableStringField",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=NestedArray]",
        "[type=NestedArray].[type=array]ar",
        "[type=NestedArray].[type=array]ar.[type=array]",
        "[type=NestedArray].[type=array]ar.[type=array].[type=Foo]",
        "[type=NestedArray].[type=array]ar.[type=array].[type=Foo].[type=long]a",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
    expected_field_paths = [
        "[type=MapSample]",
        "[type=MapSample].[type=map]aMap",
        "[type=MapSample].[type=map]aMap.[type=union]",
        "[type=MapSample].[type=map]aMap.[type=union].[type=Rcd]",
        "[type=MapSample].[type=map]aMap.[type=union].[type=Rcd].[type=string]aUnion",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=Recursive]",
        "[type=Recursive].[type=R]r",
        "[type=Recursive].[type=R]r.[type=int]anIntegerField",
        "[type=Recursive].[type=R]r.[type=R]aRecursiveField",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[type=ABFooUnion]",
        "[type=ABFooUnion].[type=union]a",
        "[type=ABFooUnion].[type=union]a.[type=A]",
        "[type=ABFooUnion].[type=union]a.[type=A].[type=string]f",
        "[type=ABFooUnion].[type=union]a.[type=B]",
        "[type=ABFooUnion].[type=union]a.[type=B].[type=string]f",
        "[type=ABFooUnion].[type=union]a.[type=array]",
        "[type=ABFooUnion].[type=union]a.[type=array].[type=array]",
        "[type=ABFooUnion].[type=union]a.[type=array].[type=array].[type=Foo]",
        "[type=ABFooUnion].[type=union]a.[type=array].[type=array].[type=Foo].[type=long]f",
    ]
    assret_field_paths_match(fields, expected_field_paths)


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
        "[key=True].[type=ABFooUnion]",
        "[key=True].[type=ABFooUnion].[type=union]a",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=A]",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=A].[type=string]f",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=B]",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=B].[type=string]f",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=array]",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=array].[type=array]",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=array].[type=array].[type=Foo]",
        "[key=True].[type=ABFooUnion].[type=union]a.[type=array].[type=array].[type=Foo].[type=long]f",
    ]
    assret_field_paths_match(fields, expected_field_paths)
    for f in fields:
        assert f.isPartOfKey

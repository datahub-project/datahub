import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Union

import pytest

from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NumberTypeClass,
    SchemaField,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    MapTypeClass,
    RecordTypeClass,
    UnionTypeClass,
)

logger = logging.getLogger(__name__)

SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE = {
    "type": "object",
    "title": "TestRecord",
    "properties": {
        "my.field": {
            "type": "string",
            "description": "some.doc",
        }
    },
}

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


def log_field_paths(fields: Iterable[SchemaField]) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')


def assert_field_paths_are_unique(fields: Iterable[SchemaField]) -> None:
    avro_fields_paths = [
        f.fieldPath for f in fields if re.match(".*[^]]$", f.fieldPath)
    ]

    if avro_fields_paths:
        assert len(avro_fields_paths) == len(set(avro_fields_paths))


def assert_field_paths_match(
    fields: Iterable[SchemaField], expected_field_paths: Union[List[str], List[Dict]]
) -> None:
    log_field_paths(fields)
    assert len([f for f in fields]) == len(expected_field_paths)
    for f, efp in zip(fields, expected_field_paths):
        if isinstance(efp, dict):
            assert f.fieldPath == efp["path"]
            assert isinstance(f.type.type, efp["type"])
        else:
            assert f.fieldPath == efp
    assert_field_paths_are_unique(fields)


def json_schema_to_schema_fields(schema):
    return list(JsonSchemaTranslator.get_fields_from_schema(schema))


@pytest.mark.parametrize(
    "schema",
    [
        SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE,
    ],
    ids=[
        "optional_field_via_union_type",
    ],
)
def test_json_schema_to_events_with_nullable_fields(schema):
    fields = json_schema_to_schema_fields(schema)
    assert 1 == len(fields)
    assert fields[0].nullable


def test_json_schema_to_mce_fields_sample_events_with_different_field_types():
    schema = {
        "type": "object",
        "title": "R",
        "namespace": "some.namespace",
        "properties": {
            "a_map_of_longs_field": {
                "type": "object",
                "additionalProperties": {"type": "integer"},
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {
            "path": "[version=2.0].[type=R].[type=map].[type=integer].a_map_of_longs_field",
            "type": MapTypeClass,
        }
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_json_schema_to_record_with_two_fields():
    schema = {
        "type": "object",
        "title": "some_event_name",
        "namespace": "not.relevant.namespace",
        "properties": {
            "a": {"type": "string", "description": "some.doc"},
            "b": {"type": "string", "description": "some.doc"},
        },
    }
    fields = json_schema_to_schema_fields(schema)
    expected_field_paths = [
        {
            "path": "[version=2.0].[type=some_event_name].[type=string].a",
            "type": StringTypeClass,
        },
        {
            "path": "[version=2.0].[type=some_event_name].[type=string].b",
            "type": StringTypeClass,
        },
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_json_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = {"type": "string"}
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {"path": "[version=2.0].[type=string]", "type": StringTypeClass}
    ]
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


def test_json_schema_with_recursion():
    schema = {
        "type": "object",
        "title": "TreeNode",
        "properties": {
            "value": {"type": "integer"},
            "children": {"type": "array", "items": {"$ref": "#"}},
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {
            "path": "[version=2.0].[type=TreeNode].[type=integer].value",
            "type": NumberTypeClass,
        },
        {
            "path": "[version=2.0].[type=TreeNode].[type=array].[type=TreeNode].children",
            "type": RecordTypeClass,
        },
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
    {"name": "amount", "type": "double", "doc": "amountDoc"},
    {"name": "name","type": "string","default": ""},
    {"name": "phoneNumber",
     "type": [{
         "type": "record",
         "name": "PhoneNumber",
         "doc": "testDoc",
         "fields": [{
             "name": "areaCode",
             "type": "string",
             "doc": "areaCodeDoc",
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
    },
    {"name": "address",
     "type": [{
         "type": "record",
         "name": "Address",
         "fields": [{
             "name": "street",
             "type": "string",
             "default": ""
             }]
         },
         "null"
     ],
      "doc": "addressDoc",
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
        "[version=2.0].[type=Payment].[type=Address].address",
        "[version=2.0].[type=Payment].[type=Address].address.[type=string].street",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert fields[1].description == "amountDoc"
    assert fields[3].description == "testDoc\nField default value: null"
    assert fields[4].description == "areaCodeDoc\nField default value: "
    assert fields[8].description == "addressDoc\nField default value: null"


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
    schema = {
        "type": "object",
        "title": "Simple",
        "namespace": "com.linkedin",
        "properties": {
            "stringField": {"type": "string", "description": "string field"},
            "booleanField": {"type": "boolean"},
            "intField": {"type": "integer"},
            "enumField": {"title": "MyTestEnumField", "enum": ["TEST", "TEST1"]},
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        "[version=2.0].[type=Simple].[type=string].stringField",
        "[version=2.0].[type=Simple].[type=boolean].booleanField",
        "[version=2.0].[type=Simple].[type=integer].intField",
        "[version=2.0].[type=Simple].[type=enum].enumField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_simple_nested_record_with_a_string_field_for_key_schema():
    schema = {
        "type": "object",
        "title": "SimpleNested",
        "namespace": "com.linkedin",
        "properties": {
            "nestedRcd": {
                "type": "object",
                "title": "InnerRcd",
                "properties": {"aStringField": {"type": "string"}},
            }
        },
    }
    fields = list(
        JsonSchemaTranslator.get_fields_from_schema(schema, is_key_schema=True)
    )
    expected_field_paths: List[str] = [
        "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd",
        "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd.[type=string].aStringField",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_union_with_nested_record_of_union():
    schema = {
        "type": "object",
        "title": "UnionSample",
        "namespace": "com.linkedin",
        "properties": {
            "aUnion": {
                "oneOf": [
                    {"type": "boolean"},
                    {
                        "type": "object",
                        "title": "Rcd",
                        "properties": {"aNullableStringField": {"type": "string"}},
                    },
                ]
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {
            "path": "[version=2.0].[type=UnionSample].[type=union].aUnion",
            "type": UnionTypeClass,
        },
        {
            "path": "[version=2.0].[type=UnionSample].[type=union].[type=boolean].aUnion",
            "type": UnionTypeClass,
        },
        {
            "path": "[version=2.0].[type=UnionSample].[type=union].[type=Rcd].aUnion",
            "type": UnionTypeClass,
        },
        {
            "path": "[version=2.0].[type=UnionSample].[type=union].[type=Rcd].aUnion.[type=string].aNullableStringField",
            "type": StringTypeClass,
        },
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert isinstance(fields[3].type.type, StringTypeClass)
    assert fields[0].nativeDataType == "union(oneOf)"
    assert fields[1].nativeDataType == "boolean"
    assert fields[2].nativeDataType == "Rcd"
    assert fields[3].nativeDataType == "string"


def test_nested_arrays():
    schema = {
        "type": "object",
        "title": "NestedArray",
        "namespace": "com.linkedin",
        "properties": {
            "ar": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "title": "Foo",
                        "properties": {"a": {"type": "integer"}},
                    },
                },
            }
        },
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar",
        "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar.[type=integer].a",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_map_of_union_of_int_and_record_of_union():
    schema = {
        "type": "object",
        "title": "MapSample",
        "namespace": "com.linkedin",
        "properties": {
            "aMap": {
                "type": "object",
                "additionalProperties": {
                    "oneOf": [
                        {"type": "integer"},
                        {
                            "type": "object",
                            "title": "Rcd",
                            "properties": {
                                "aUnion": {
                                    "oneOf": [{"type": "string"}, {"type": "integer"}]
                                }
                            },
                        },
                    ]
                },
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].aMap",
            "type": MapTypeClass,
        },
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].[type=integer].aMap",
            "type": MapTypeClass,
        },
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap",
            "type": MapTypeClass,
        },
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].aUnion",
            "type": UnionTypeClass,
        },
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].[type=string].aUnion",
            "type": UnionTypeClass,
        },
        {
            "path": "[version=2.0].[type=MapSample].[type=map].[type=union].[type=Rcd].aMap.[type=union].[type=integer].aUnion",
            "type": UnionTypeClass,
        },
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_recursive_json():
    schema = {
        "type": "object",
        "title": "Recursive",
        "namespace": "com.linkedin",
        "properties": {
            "r": {
                "type": "object",
                "title": "R",
                "properties": {
                    "anIntegerField": {"type": "integer"},
                    "aRecursiveField": {"$ref": "#/properties/r"},
                },
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        "[version=2.0].[type=Recursive].[type=R].r",
        "[version=2.0].[type=Recursive].[type=R].r.[type=integer].anIntegerField",
        "[version=2.0].[type=Recursive].[type=R].r.[type=R].aRecursiveField",
    ]
    assert fields[2].recursive
    assert isinstance(fields[2].type.type, RecordTypeClass)
    assert fields[2].nativeDataType == "R"
    assert_field_paths_match(fields, expected_field_paths)


def test_needs_disambiguation_nested_union_of_records_with_same_field_name():
    schema = {
        "type": "object",
        "title": "ABFooUnion",
        "namespace": "com.linkedin",
        "properties": {
            "a": {
                "oneOf": [
                    {
                        "type": "object",
                        "title": "A",
                        "properties": {"f": {"type": "string"}},
                    },
                    {
                        "type": "object",
                        "title": "B",
                        "properties": {"f": {"type": "string"}},
                    },
                    {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "title": "Foo",
                                "properties": {"f": {"type": "integer"}},
                            },
                        },
                    },
                ]
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=ABFooUnion].[type=union].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=integer].f",
    ]
    assert_field_paths_match(fields, expected_field_paths)


def test_datahub_json_schemas_parses_okay(tmp_path):
    """This is more like an integration test that helps us exercise the complexity in parsing and catch unexpected regressions."""

    json_path: Path = Path(os.path.dirname(__file__)) / Path(
        "../../../../metadata-models/src/generatedJsonSchema/json/"
    )
    pipeline = Pipeline.create(
        config_dict={
            "source": {
                "type": "json-schema",
                "config": {
                    "path": str(json_path),
                    "platform": "schemaregistry",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/json_schema_test.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    logger.info(f"Wrote file to {tmp_path}/json_schema_test.json")


def test_key_schema_handling():
    """Tests key schema handling"""
    schema = {
        "type": "object",
        "title": "ABFooUnion",
        "properties": {
            "a": {
                "oneOf": [
                    {
                        "type": "object",
                        "title": "A",
                        "properties": {"f": {"type": "string"}},
                    },
                    {
                        "type": "object",
                        "title": "B",
                        "properties": {"f": {"type": "string"}},
                    },
                    {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "title": "Foo",
                                "properties": {"f": {"type": "number"}},
                            },
                        },
                    },
                ]
            }
        },
    }
    fields: List[SchemaField] = list(
        JsonSchemaTranslator.get_fields_from_schema(schema, is_key_schema=True)
    )
    expected_field_paths: List[str] = [
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=A].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=B].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=number].f",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    for f in fields:
        assert f.isPartOfKey


def test_ignore_exceptions():
    malformed_schema = {
        "name": "event_ts",
        "type": "long",
        "logicalType": "timestamp-millis",
        "tags": ["business-timestamp"],
    }
    fields: List[SchemaField] = list(
        JsonSchemaTranslator.get_fields_from_schema(malformed_schema)
    )
    assert not fields

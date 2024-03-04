import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Union

import pytest

from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NumberTypeClass,
    SchemaField,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
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
            "type": ["string", "null"],
            "description": "some.doc",
        }
    },
}


def log_field_paths(fields: Iterable[SchemaField]) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')


def assert_field_paths_are_unique(fields: Iterable[SchemaField]) -> None:
    fields_paths = [f.fieldPath for f in fields if re.match(".*[^]]$", f.fieldPath)]

    if fields_paths:
        assert len(fields_paths) == len(set(fields_paths))


def assert_fields_are_valid(fields: Iterable[SchemaField]) -> None:
    for f in fields:
        f.validate()
        assert isinstance(f.nativeDataType, str)


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
    assert_fields_are_valid(fields)


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
    assert_fields_are_valid(fields)


def test_json_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = {"type": "string"}
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        {"path": "[version=2.0].[type=string]", "type": StringTypeClass}
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)


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
            "path": "[version=2.0].[type=TreeNode].[type=array].children",
            "type": ArrayTypeClass,
        },
        {
            "path": "[version=2.0].[type=TreeNode].[type=array].children.[type=TreeNode].TreeNode",
            "type": RecordTypeClass,
        },
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)


def test_json_sample_payment_schema_to_schema_fields_with_nesting():
    schema = {
        "type": "object",
        "title": "Payment",
        "namespace": "some.event.namespace",
        "properties": {
            "id": {"type": "string"},
            "amount": {"type": "number", "description": "amountDoc"},
            "name": {"type": "string", "default": ""},
            "phoneNumber": {
                "type": "object",
                "title": "PhoneNumber",
                "description": "testDoc",
                "properties": {
                    "areaCode": {
                        "type": "string",
                        "description": "areaCodeDoc",
                        "default": "",
                    },
                    "countryCode": {"type": "string", "default": ""},
                    "prefix": {"type": "string", "default": ""},
                    "number": {"type": "string", "default": ""},
                },
                "default": "null",
            },
            "address": {
                "type": "object",
                "title": "Address",
                "properties": {"street": {"type": "string", "default": ""}},
                "description": "addressDoc",
                "default": "null",
            },
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        "[version=2.0].[type=Payment].[type=string].id",
        "[version=2.0].[type=Payment].[type=number].amount",
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
    assert_fields_are_valid(fields)


def test_json_schema_to_schema_fields_with_nesting_across_records():
    schema = {
        "definitions": {
            "Address": {
                "type": "object",
                "title": "Address",
                "properties": {
                    "streetAddress": {"type": "string"},
                    "city": {"type": "string"},
                },
            }
        },
        "oneOf": [
            {"$ref": "#/definitions/Address"},
            {
                "type": "object",
                "title": "Person",
                "properties": {
                    "firstname": {"type": "string"},
                    "lastname": {"type": "string"},
                    "address": {"$ref": "#/definitions/Address"},
                },
            },
        ],
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths = [
        "[version=2.0].[type=union].[type=Address].[type=string].streetAddress",
        "[version=2.0].[type=union].[type=Address].[type=string].city",
        "[version=2.0].[type=union].[type=Person].[type=string].firstname",
        "[version=2.0].[type=union].[type=Person].[type=string].lastname",
        "[version=2.0].[type=union].[type=Person].[type=Address].address",
        "[version=2.0].[type=union].[type=Person].[type=Address].address.[type=string].streetAddress",
        "[version=2.0].[type=union].[type=Person].[type=Address].address.[type=string].city",
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
        "[version=2.0].[type=NestedArray].[type=array].ar",
        "[version=2.0].[type=NestedArray].[type=array].ar.[type=array].array",
        "[version=2.0].[type=NestedArray].[type=array].ar.[type=array].array.[type=Foo].Foo",
        "[version=2.0].[type=NestedArray].[type=array].ar.[type=array].array.[type=Foo].Foo.[type=integer].a",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert isinstance(fields[0].type.type, ArrayTypeClass)


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
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].a",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array.[type=Foo].Foo",
        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array.[type=Foo].Foo.[type=integer].f",
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
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].a",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array.[type=Foo].Foo",
        "[version=2.0].[key=True].[type=ABFooUnion].[type=union].[type=array].a.[type=array].array.[type=Foo].Foo.[type=number].f",
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


SCHEMA_WITH_ARRAY_TYPE = {
    "title": "Administrative-Unit",
    "type": "object",
    "properties": {
        "Identifier": {"type": ["integer"]},
        "ValidFrom": {"format": "date", "type": ["string"]},
        "ValidTo": {"format": "date", "type": ["string", "null"]},
        "Level": {"minimum": 1, "maximum": 3, "type": ["integer"]},
        "Parent": {"type": ["integer", "null"]},
        "Name_en": {"type": ["string", "null"]},
        "Name_fr": {"type": ["string", "null"]},
        "Name_de": {"type": ["string", "null"]},
        "Name_it": {"type": ["string", "null"]},
        "ABBREV_1_Text_en": {"type": ["string", "null"]},
        "ABBREV_1_Text_fr": {"type": ["string", "null"]},
        "ABBREV_1_Text_de": {"type": ["string", "null"]},
        "ABBREV_1_Text_it": {"type": ["string", "null"]},
        "ABBREV_1_Text": {"type": ["string", "null"]},
        "CODE_OFS_1_Text_en": {"type": ["integer", "null"]},
        "CODE_OFS_1_Text_fr": {"type": ["integer", "null"]},
        "CODE_OFS_1_Text_de": {"type": ["integer", "null"]},
        "CODE_OFS_1_Text_it": {"type": ["integer", "null"]},
        "CODE_OFS_1_Text": {"type": ["integer", "null"]},
    },
}


def test_array_handling():
    schema = SCHEMA_WITH_ARRAY_TYPE
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=Administrative-Unit].[type=integer].Identifier",
        "[version=2.0].[type=Administrative-Unit].[type=string(date)].ValidFrom",
        "[version=2.0].[type=Administrative-Unit].[type=string(date)].ValidTo",
        "[version=2.0].[type=Administrative-Unit].[type=integer].Level",
        "[version=2.0].[type=Administrative-Unit].[type=integer].Parent",
        "[version=2.0].[type=Administrative-Unit].[type=string].Name_en",
        "[version=2.0].[type=Administrative-Unit].[type=string].Name_fr",
        "[version=2.0].[type=Administrative-Unit].[type=string].Name_de",
        "[version=2.0].[type=Administrative-Unit].[type=string].Name_it",
        "[version=2.0].[type=Administrative-Unit].[type=string].ABBREV_1_Text_en",
        "[version=2.0].[type=Administrative-Unit].[type=string].ABBREV_1_Text_fr",
        "[version=2.0].[type=Administrative-Unit].[type=string].ABBREV_1_Text_de",
        "[version=2.0].[type=Administrative-Unit].[type=string].ABBREV_1_Text_it",
        "[version=2.0].[type=Administrative-Unit].[type=string].ABBREV_1_Text",
        "[version=2.0].[type=Administrative-Unit].[type=integer].CODE_OFS_1_Text_en",
        "[version=2.0].[type=Administrative-Unit].[type=integer].CODE_OFS_1_Text_fr",
        "[version=2.0].[type=Administrative-Unit].[type=integer].CODE_OFS_1_Text_de",
        "[version=2.0].[type=Administrative-Unit].[type=integer].CODE_OFS_1_Text_it",
        "[version=2.0].[type=Administrative-Unit].[type=integer].CODE_OFS_1_Text",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)


def test_simple_array():
    schema = {
        "type": "object",
        "title": "ObjectWithArray",
        "namespace": "com.linkedin",
        "properties": {"ar": {"type": "array", "items": {"type": "string"}}},
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=ObjectWithArray].[type=array].ar",
        "[version=2.0].[type=ObjectWithArray].[type=array].ar.[type=string].string",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert isinstance(fields[0].type.type, ArrayTypeClass)


def test_simple_object():
    schema = {
        "type": "object",
        "title": "Object With Object",
        "namespace": "io.datahubproject",
        "properties": {"inner": {"type": "object"}},
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=Object With Object].[type=object].inner",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert isinstance(fields[0].type.type, RecordTypeClass)


def test_required_field():
    schema = {
        "$id": "test",
        "$schema": "http://json-schema.org/draft-06/schema#",
        "properties": {
            "a_str": {
                "description": "Example String",
                "type": "string",
            },
            "b_str": {"type": ["string", "null"]},
        },
        "required": ["b_str"],
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=object].[type=string].a_str",
        "[version=2.0].[type=object].[type=string].b_str",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)
    assert fields[0].nullable is False
    assert fields[1].nullable is True
    assert json.loads(fields[1].jsonProps or "{}")["required"] is True
    assert json.loads(fields[0].jsonProps or "{}")["required"] is False


def test_non_str_enums():
    schema = {
        "$id": "test",
        "$schema": "http://json-schema.org/draft-06/schema#",
        "properties": {"bar": {"description": "Mixed enum", "enum": ["baz", 1, None]}},
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = ["[version=2.0].[type=object].[type=enum].bar"]
    assert_field_paths_match(fields, expected_field_paths)
    assert fields[0].description == 'One of: "baz", 1, null'


def test_const_description_pulled_correctly():
    schema = {
        "$id": "test",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {"bar": {"type": "string", "const": "not_defined"}},
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = ["[version=2.0].[type=object].[type=string].bar"]
    assert_field_paths_match(fields, expected_field_paths)
    assert fields[0].description == "Const value: not_defined"


def test_anyof_with_properties():
    # We expect the event / timestamp fields to be included in both branches of the anyOf.

    schema = {
        "$id": "test",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": False,
        "anyOf": [{"required": ["anonymousId"]}, {"required": ["userId"]}],
        "properties": {
            "anonymousId": {
                "description": "A temporary user id, used before a user logs in.",
                "format": "uuid",
                "type": "string",
            },
            "userId": {
                "description": "Unique user id.",
                "type": "string",
            },
            "event": {"description": "Unique name of the event.", "type": "string"},
            "timestamp": {
                "description": "Timestamp of when the message itself took place.",
                "type": "string",
            },
        },
        "required": ["event"],
        "type": "object",
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=union].[type=union_0].[type=string(uuid)].anonymousId",
        "[version=2.0].[type=union].[type=union_0].[type=string].userId",
        "[version=2.0].[type=union].[type=union_0].[type=string].event",
        "[version=2.0].[type=union].[type=union_0].[type=string].timestamp",
        "[version=2.0].[type=union].[type=union_1].[type=string(uuid)].anonymousId",
        "[version=2.0].[type=union].[type=union_1].[type=string].userId",
        "[version=2.0].[type=union].[type=union_1].[type=string].event",
        "[version=2.0].[type=union].[type=union_1].[type=string].timestamp",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)

    # In the first one, the anonymousId is required, but the userId is not.
    assert json.loads(fields[0].jsonProps or "{}")["required"] is True
    assert json.loads(fields[1].jsonProps or "{}")["required"] is False

    # In the second one, the userId is required, but the anonymousId is not.
    assert json.loads(fields[4].jsonProps or "{}")["required"] is False
    assert json.loads(fields[5].jsonProps or "{}")["required"] is True

    # The event field is required in both branches.
    assert json.loads(fields[2].jsonProps or "{}")["required"] is True
    assert json.loads(fields[6].jsonProps or "{}")["required"] is True

    # The timestamp field is not required in either branch.
    assert json.loads(fields[3].jsonProps or "{}")["required"] is False
    assert json.loads(fields[7].jsonProps or "{}")["required"] is False


def test_top_level_trival_allof():
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "event-wrapper",
        "type": "object",
        "allOf": [
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "event",
                "properties": {
                    "userId": {
                        "description": "Unique user id.",
                        "type": "string",
                    },
                    "event": {
                        "description": "Unique name of the event.",
                        "type": "string",
                    },
                    "timestamp": {
                        "description": "Timestamp of when the message itself took place.",
                        "type": "string",
                    },
                },
                "required": ["event"],
                "type": "object",
                "additionalProperties": False,
            },
        ],
        "properties": {
            "extra-top-level-property": {
                "type": "string",
            },
        },
    }

    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=object].[type=string].extra-top-level-property",
        "[version=2.0].[type=object].[type=string].userId",
        "[version=2.0].[type=object].[type=string].event",
        "[version=2.0].[type=object].[type=string].timestamp",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)

    assert json.loads(fields[0].jsonProps or "{}")["required"] is False
    assert json.loads(fields[1].jsonProps or "{}")["required"] is False
    assert json.loads(fields[2].jsonProps or "{}")["required"] is True
    assert json.loads(fields[3].jsonProps or "{}")["required"] is False


def test_description_extraction():
    schema = {
        "$id": "test",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
            "bar": {
                "type": "array",
                "items": {"type": "string"},
                "description": "XYZ",
            }
        },
    }
    fields = list(JsonSchemaTranslator.get_fields_from_schema(schema))
    expected_field_paths: List[str] = [
        "[version=2.0].[type=object].[type=array].bar",
        "[version=2.0].[type=object].[type=array].bar.[type=string].string",
    ]
    assert_field_paths_match(fields, expected_field_paths)
    assert_fields_are_valid(fields)
    # Additional check for the description extraction
    array_field = next(
        field
        for field in fields
        if field.fieldPath == "[version=2.0].[type=object].[type=array].bar"
    )
    assert array_field.description == "XYZ"

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

SCHEMA_WITH_MAP_TYPE_FIELD = """
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

SCHEMA_WITH_TOP_LEVEL_PRIMITIVE_FIELD = """
{
  "type": "string"
}
"""

SCHEMA_WITH_TWO_FIELD_RECORD = """
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

SCHEMA_WITH_DEFAULT_VALUE = """
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
    schema = SCHEMA_WITH_MAP_TYPE_FIELD
    fields = avro_schema_to_mce_fields(schema)
    assert 1 == len(fields)


def test_avro_schema_to_mce_fields_record_with_two_fields():
    schema = SCHEMA_WITH_TWO_FIELD_RECORD

    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 2


def test_avro_schema_to_mce_fields_toplevel_isnt_a_record():
    schema = SCHEMA_WITH_TOP_LEVEL_PRIMITIVE_FIELD

    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1


def test_avro_schema_to_mce_fields_with_default():
    schema = SCHEMA_WITH_DEFAULT_VALUE

    fields = avro_schema_to_mce_fields(schema)
    assert len(fields) == 1
    assert fields[0].description and "custom, default value" in fields[0].description

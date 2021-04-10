import unittest

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


class SchemaUtilTest(unittest.TestCase):
    def test_avro_schema_to_mce_fields_events_with_nullable_fields(self):

        examples = [
            SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE,
            SCHEMA_WITH_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION,
            SCHEMA_WITH_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE,
        ]

        for schema in examples:
            fields = avro_schema_to_mce_fields(schema)
            self.assertEqual(1, len(fields))
            self.assertTrue(fields[0].nullable)

    def test_avro_schema_to_mce_fields_sample_events_with_different_field_types(self):

        examples = [SCHEMA_WITH_MAP_TYPE_FIELD]

        for schema in examples:
            fields = avro_schema_to_mce_fields(schema)
            self.assertEqual(1, len(fields))

    def test_avro_schema_to_mce_fields_record_with_two_fields(self):

        examples = [SCHEMA_WITH_TWO_FIELD_RECORD]

        for schema in examples:
            fields = avro_schema_to_mce_fields(schema)
            self.assertEqual(2, len(fields))

    def test_avro_schema_to_mce_fields_toplevel_isnt_a_record(self):

        examples = [SCHEMA_WITH_TOP_LEVEL_PRIMITIVE_FIELD]

        for schema in examples:
            fields = avro_schema_to_mce_fields(schema)
            self.assertEqual(1, len(fields))

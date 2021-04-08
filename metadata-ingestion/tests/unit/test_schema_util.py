import unittest

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields

EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_UNION_TYPE = """
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

EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION = """
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

EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE = """
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


class SchemaUtilTest(unittest.TestCase):
    def test_avro_schema_to_mce_fields_events_with_nullable_fields(self):

        events = [
            EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_UNION_TYPE,
            EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_UNION_TYPE_NULL_ISNT_FIRST_IN_UNION,
            EXAMPLE_EVENT_OPTIONAL_FIELD_VIA_PRIMITIVE_TYPE,
        ]

        for event in events:
            fields = avro_schema_to_mce_fields(event)
            self.assertEqual(1, len(fields))
            self.assertTrue(fields[0].nullable)

    def test_avro_schema_to_mce_fields_sample_events_with_different_field_types(self):

        EXAMPLES = [SCHEMA_WITH_MAP_TYPE_FIELD]

        for schema in EXAMPLES:
            fields = avro_schema_to_mce_fields(schema)
            self.assertEqual(1, len(fields))

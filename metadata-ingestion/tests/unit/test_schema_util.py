import unittest

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields


class SchemaUtilTest(unittest.TestCase):
    def test_avro_schema_to_mce_fields(self):
        avro_schema_string = """
{
  "type": "record",
  "name": "SomeEventName",
  "namespace": "some.event.namespace",
  "fields": [
    {
      "name": "my_field",
      "type": {
        "type": "string"
      },
      "default": "some.default.value",
      "doc": "some doc"
    }
  ]
}
"""

        fields = avro_schema_to_mce_fields(avro_schema_string)

        self.assertEquals(1, len(fields))
        self.assertTrue(fields[0].nullable)

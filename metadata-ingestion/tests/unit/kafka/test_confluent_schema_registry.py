import unittest
from unittest.mock import patch

from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
)

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.ingestion.source.kafka.kafka import KafkaSourceConfig, KafkaSourceReport


class ConfluentSchemaRegistryTest(unittest.TestCase):
    def test_get_schema_str_replace_confluent_ref_avro(self):
        schema_str_orig = """
        {
          "fields": [
            {
              "name": "my_field1",
              "type": "TestTopic1"
            }
          ],
          "name": "TestTopic1Val",
          "namespace": "io.acryl",
          "type": "record"
        }
        """
        schema_str_ref = """
        {
          "doc": "Sample schema to help you get started.",
          "fields": [
            {
              "doc": "The int type is a 32-bit signed integer.",
              "name": "my_field1",
              "type": "int"
            }
          ],
          "name": "TestTopic1",
          "namespace": "io.acryl",
          "type": "record"
        }
    """

        schema_str_final = (
            """
    {
      "fields": [
        {
          "name": "my_field1",
          "type": """
            + schema_str_ref
            + """
            }
          ],
          "name": "TestTopic1Val",
          "namespace": "io.acryl",
          "type": "record"
        }
        """
        )

        kafka_source_config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, KafkaSourceReport()
        )

        def new_get_latest_version(subject_name: str) -> RegisteredSchema:
            return RegisteredSchema(
                schema_id="schema_id_1",
                schema=Schema(schema_str=schema_str_ref, schema_type="AVRO"),
                subject="test",
                version=1,
            )

        with patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = (
                confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                    # The external reference would match by name.
                    schema=Schema(
                        schema_str=schema_str_orig,
                        schema_type="AVRO",
                        references=[
                            SchemaReference(
                                name="TestTopic1", subject="schema_subject_1", version=1
                            )
                        ],
                    )
                )
            )
            assert schema_str == ConfluentSchemaRegistry._compact_schema(
                schema_str_final
            )

        with patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = (
                confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                    # The external reference would match by subject.
                    schema=Schema(
                        schema_str=schema_str_orig,
                        schema_type="AVRO",
                        references=[
                            SchemaReference(
                                name="schema_subject_1", subject="TestTopic1", version=1
                            )
                        ],
                    )
                )
            )
            assert schema_str == ConfluentSchemaRegistry._compact_schema(
                schema_str_final
            )


if __name__ == "__main__":
    unittest.main()

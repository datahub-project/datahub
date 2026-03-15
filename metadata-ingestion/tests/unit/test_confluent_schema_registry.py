import json
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
    @staticmethod
    def _make_registry() -> ConfluentSchemaRegistry:
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        return ConfluentSchemaRegistry.create(kafka_source_config, KafkaSourceReport())

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

        confluent_schema_registry = self._make_registry()

        def new_get_latest_version(subject_name: str) -> RegisteredSchema:
            return RegisteredSchema(
                guid=None,
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

    def test_get_schema_fields_with_hyphenated_namespace(self):
        """Test that schemas with hyphens in namespace (e.g., from Debezium CDC)
        are parsed successfully by default, matching Java Schema Registry client behavior."""
        debezium_schema_str = json.dumps(
            {
                "type": "record",
                "name": "Value",
                "namespace": "my-debezium-topic.public.users",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )

        confluent_schema_registry = self._make_registry()

        schema = Schema(schema_str=debezium_schema_str, schema_type="AVRO")
        fields = confluent_schema_registry._get_schema_fields(
            topic="my-debezium-topic.public.users",
            schema=schema,
            is_key_schema=False,
        )

        assert len(fields) == 2
        field_names = [f.fieldPath for f in fields]
        assert any("id" in name for name in field_names)
        assert any("name" in name for name in field_names)

    def test_get_schemas_from_confluent_ref_protobuf_recurses_and_uses_ref_versions(
        self,
    ):
        confluent_schema_registry = self._make_registry()

        main_schema = Schema(
            schema_str='syntax = "proto3"; import "child.proto"; message Root { Child child = 1; }',
            schema_type="PROTOBUF",
            references=[SchemaReference(name="child.proto", subject="child", version=2)],
        )
        child_schema = Schema(
            schema_str='syntax = "proto3"; import "grandchild.proto"; message Child { Grandchild grandchild = 1; }',
            schema_type="PROTOBUF",
            references=[
                SchemaReference(name="grandchild.proto", subject="grandchild", version=7)
            ],
        )
        grandchild_schema = Schema(
            schema_str='syntax = "proto3"; message Grandchild { string value = 1; }',
            schema_type="PROTOBUF",
            references=[],
        )

        requested_versions = []

        def new_get_version(subject_name: str, version: int) -> RegisteredSchema:
            requested_versions.append((subject_name, version))
            schema_map = {
                ("child", 2): child_schema,
                ("grandchild", 7): grandchild_schema,
            }
            return RegisteredSchema(
                guid=None,
                schema_id=f"{subject_name}-{version}",
                schema=schema_map[(subject_name, version)],
                subject=subject_name,
                version=version,
            )

        with patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_version",
            new_get_version,
        ), patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_latest_version",
            side_effect=AssertionError(
                "protobuf references should use versioned lookups"
            ),
        ):
            schemas = confluent_schema_registry.get_schemas_from_confluent_ref_protobuf(
                main_schema
            )

        assert requested_versions == [("child", 2), ("grandchild", 7)]
        assert [schema.name for schema in schemas] == ["grandchild.proto", "child.proto"]


if __name__ == "__main__":
    unittest.main()

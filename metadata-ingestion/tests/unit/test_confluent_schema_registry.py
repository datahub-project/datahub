from unittest.mock import patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
)

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.ingestion.source.kafka.kafka import KafkaSourceConfig, KafkaSourceReport


class TestConfluentSchemaRegistry:
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

        kafka_source_config = KafkaSourceConfig.model_validate(
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
                guid=None,
                schema_id=1,
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

    def test_get_schema_str_replace_confluent_ref_avro_with_none_schema_str(self):
        """Test that ValueError is raised when schema.schema_str is None (confluent-kafka >= 2.13.0)"""
        kafka_source_config = KafkaSourceConfig.model_validate(
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

        # Schema with None schema_str should raise ValueError
        schema_with_none = Schema(schema_str=None, schema_type="AVRO")
        with pytest.raises(ValueError, match="Schema string cannot be None"):
            confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                schema=schema_with_none
            )

    def test_get_schema_str_replace_confluent_ref_avro_with_none_subject(self):
        """Test that schema references with None subject are skipped (confluent-kafka >= 2.13.0)"""
        schema_str_orig = """
        {
          "fields": [{"name": "my_field1", "type": "int"}],
          "name": "TestTopic1Val",
          "namespace": "io.acryl",
          "type": "record"
        }
        """

        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, report
        )

        # Schema reference with None subject should be skipped with a warning
        schema_with_none_ref = Schema(
            schema_str=schema_str_orig,
            schema_type="AVRO",
            references=[SchemaReference(name="TestTopic1", subject=None, version=1)],
        )

        result = confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
            schema=schema_with_none_ref
        )

        # Should return the compacted schema since the reference was skipped
        assert result is not None

    def test_get_schemas_from_confluent_ref_protobuf_with_none_attributes(self):
        """Test that protobuf schema references with None attributes are skipped (confluent-kafka >= 2.13.0)"""
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, report
        )

        # Schema with reference that has None subject
        schema_with_none_ref = Schema(
            schema_str="syntax = 'proto3';",
            schema_type="PROTOBUF",
            references=[SchemaReference(name="RefName", subject=None, version=1)],
        )

        result = confluent_schema_registry.get_schemas_from_confluent_ref_protobuf(
            schema=schema_with_none_ref
        )

        # Should return empty list since the reference was skipped
        assert result == []

        # Also test with None name
        confluent_schema_registry2 = ConfluentSchemaRegistry.create(
            kafka_source_config, KafkaSourceReport()
        )
        schema_with_none_name = Schema(
            schema_str="syntax = 'proto3';",
            schema_type="PROTOBUF",
            references=[SchemaReference(name=None, subject="RefSubject", version=1)],
        )

        result2 = confluent_schema_registry2.get_schemas_from_confluent_ref_protobuf(
            schema=schema_with_none_name
        )

        assert result2 == []

    def test_get_schemas_from_confluent_ref_json_with_none_attributes(self):
        """Test that JSON schema references with None attributes are skipped (confluent-kafka >= 2.13.0)"""
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, report
        )

        # Schema with reference that has None version
        schema_with_none_version = Schema(
            schema_str='{"type": "object"}',
            schema_type="JSON",
            references=[
                SchemaReference(name="RefName", subject="RefSubject", version=None)
            ],
        )

        result = confluent_schema_registry.get_schemas_from_confluent_ref_json(
            schema=schema_with_none_version,
            name="test-schema",
            subject="test-subject",
        )

        # Should return just the main schema since the reference was skipped
        assert len(result) == 1
        assert result[0].name == "test-schema"

    def test_get_schemas_from_confluent_ref_json_with_none_schema_str(self):
        """Test that ValueError is raised when JSON schema.schema_str is None (confluent-kafka >= 2.13.0)"""
        kafka_source_config = KafkaSourceConfig.model_validate(
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

        # Schema with None schema_str should raise ValueError
        schema_with_none = Schema(schema_str=None, schema_type="JSON")
        with pytest.raises(ValueError, match="Schema string cannot be None"):
            confluent_schema_registry.get_schemas_from_confluent_ref_json(
                schema=schema_with_none,
                name="test-schema",
                subject="test-subject",
            )

import json
import unittest
from typing import List
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
                schema_id="schema_id_1",
                guid=None,
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

    def test_init_subjects_connectivity_failure_is_reported(self):
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()

        with patch(
            "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.get_subjects.side_effect = OSError(
                "registry unreachable"
            )
            registry = ConfluentSchemaRegistry.create(kafka_source_config, report)

        # A registry we can't reach at startup must be tallied, not swallowed.
        assert report.schema_registry_connectivity_failures == 1
        assert registry.get_subjects() == []

    def test_batch_connectivity_failure_is_reported(self):
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()
        registry = ConfluentSchemaRegistry.create(kafka_source_config, report)

        with patch.object(
            registry,
            "_get_schema_and_fields",
            side_effect=OSError("registry unreachable"),
        ):
            result = registry.get_schema_and_fields_batch(["topic-a"])

        # Connectivity failures must be tallied and the topic left schemaless.
        assert report.schema_registry_connectivity_failures == 1
        assert result["topic-a"].schema is None
        assert result["topic-a"].fields == []


class SubjectForTopicTest(unittest.TestCase):
    """Which registry subject a topic resolves to.

    Companion topics (`.RETRY`, `.DLT`, environment suffixes) produce subjects that
    all begin with the parent topic's name, so resolving on prefix alone is
    order-dependent and can hand a topic another topic's schema.
    """

    @staticmethod
    def _registry(subjects: List[str], **config: object) -> ConfluentSchemaRegistry:
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                **config,
            }
        )
        with patch(
            "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.get_subjects.return_value = subjects
            return ConfluentSchemaRegistry.create(
                kafka_source_config, KafkaSourceReport()
            )

    def test_exact_subject_wins_over_companion_topic_listed_first(self):
        # The registry lists the .RETRY companion before the topic's own subject.
        registry = self._registry(
            [
                "orders.RETRY-value",
                "orders.DLT-value",
                "orders-value",
            ]
        )
        assert registry._get_subject_for_topic("orders", False) == "orders-value"

    def test_companion_topics_keep_their_own_subjects(self):
        registry = self._registry(
            ["orders-value", "orders.RETRY-value", "orders.DLT-value"]
        )
        assert (
            registry._get_subject_for_topic("orders.RETRY", False)
            == "orders.RETRY-value"
        )
        assert (
            registry._get_subject_for_topic("orders.DLT", False) == "orders.DLT-value"
        )

    def test_companion_subject_is_not_served_to_a_schemaless_topic(self):
        # "orders" has no subject of its own. Handing it the .RETRY schema would be
        # worse than reporting it schemaless, which is what the caller warns about.
        registry = self._registry(["orders.RETRY-value"])
        assert registry._get_subject_for_topic("orders", False) is None

    def test_environment_suffixed_topics_do_not_collide(self):
        # Case (c) from the naming-strategy comment.
        registry = self._registry(["a.b.c.d.qa-value", "a.b.c.d-value"])
        assert registry._get_subject_for_topic("a.b.c.d", False) == "a.b.c.d-value"
        assert (
            registry._get_subject_for_topic("a.b.c.d.qa", False) == "a.b.c.d.qa-value"
        )

    def test_topic_record_name_strategy_still_resolves(self):
        # TopicRecordNameStrategy joins the record name with "-", so it is still
        # reachable when the topic has no TopicNameStrategy subject.
        registry = self._registry(["orders-io.acryl.Order-value"])
        assert (
            registry._get_subject_for_topic("orders", False)
            == "orders-io.acryl.Order-value"
        )

    def test_key_schemas_resolve_independently(self):
        registry = self._registry(["orders.RETRY-key", "orders-key", "orders-value"])
        assert registry._get_subject_for_topic("orders", True) == "orders-key"
        assert registry._get_subject_for_topic("orders", False) == "orders-value"

    def test_topic_subject_map_still_overrides(self):
        registry = self._registry(
            ["orders-value"],
            topic_subject_map={"orders-value": "explicitly.mapped-value"},
        )
        assert (
            registry._get_subject_for_topic("orders", False)
            == "explicitly.mapped-value"
        )

    def test_disable_topic_record_naming_strategy_is_exact_only(self):
        registry = self._registry(
            ["orders-io.acryl.Order-value"],
            disable_topic_record_naming_strategy=True,
        )
        assert registry._get_subject_for_topic("orders", False) is None


if __name__ == "__main__":
    unittest.main()

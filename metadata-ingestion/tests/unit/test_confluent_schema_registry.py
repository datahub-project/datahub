import json
import unittest
from hashlib import md5
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.ingestion.source.kafka import KafkaSourceConfig, KafkaSourceReport
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    KafkaSchemaClass,
    OwnershipClass,
    SchemaMetadataClass,
)


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
            schema_str = confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                # The external reference would match by name.
                schema=Schema(
                    schema_str=schema_str_orig,
                    schema_type="AVRO",
                    references=[
                        dict(name="TestTopic1", subject="schema_subject_1", version=1)
                    ],
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
            schema_str = confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                # The external reference would match by subject.
                schema=Schema(
                    schema_str=schema_str_orig,
                    schema_type="AVRO",
                    references=[
                        dict(name="schema_subject_1", subject="TestTopic1", version=1)
                    ],
                )
            )
            assert schema_str == ConfluentSchemaRegistry._compact_schema(
                schema_str_final
            )

    def test_get_schema_metadata_uses_get_aspects_from_schema(self):
        """Check that get_schema_metadata remains implemented although deprecated."""
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

        schema_metadata_aspect = SchemaMetadataClass(
            schemaName="test",
            platform="urn:li:platform:test",
            version=0,
            hash="test hash",
            platformSchema=KafkaSchemaClass("test scheama"),
            fields=[],
        )

        def new_get_aspects_from_schema(topic: str, platform_urn: str) -> List[Any]:
            return [DatasetPropertiesClass(name="test"), schema_metadata_aspect]

        with patch.object(
            confluent_schema_registry,
            "get_aspects_from_schema",
            new_get_aspects_from_schema,
        ):
            assert (
                confluent_schema_registry.get_schema_metadata(
                    topic="test", platform_urn="urn:li:platform:test"
                )
                == schema_metadata_aspect
            )

    @patch(
        "datahub.ingestion.source.confluent_schema_registry.confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient",
        autospec=True,
    )
    def test_get_aspects_from_schema_avro(self, mock_schema_registry_client):
        key_schema_str = json.dumps(
            {
                "type": "record",
                "name": "Topic1Key",
                "namespace": "test.acryl",
                "fields": [{"name": "t1key", "type": "string"}],
            }
        )
        value_schema_str = json.dumps(
            {
                "type": "record",
                "name": "TestTopic1Val",
                "doc": "test dataset description",
                "owner": "@test-team",
                "tags": ["tag1", "tag2"],
                "has_tag3": True,
                "has_term1": True,
                "namespace": "test.acryl",
                "fields": [
                    {
                        "name": "field1",
                        "type": "string",
                        "doc": "field1 description",
                        "tags": ["tag1", "tag2"],
                        "has_tag3": True,
                        "has_term1": True,
                    }
                ],
            }
        )

        topic_subject_schema_map: Dict[
            str, Tuple[RegisteredSchema, RegisteredSchema]
        ] = {
            # TopicNameStrategy is used for subject
            "topic1": (
                RegisteredSchema(
                    schema_id="schema_id_2",
                    schema=Schema(
                        schema_str=key_schema_str,
                        schema_type="AVRO",
                    ),
                    subject="topic1-key",
                    version=1,
                ),
                RegisteredSchema(
                    schema_id="schema_id_1",
                    schema=Schema(
                        schema_str=value_schema_str,
                        schema_type="AVRO",
                    ),
                    subject="topic1-value",
                    version=1,
                ),
            ),
        }

        # Mock the schema registry client
        # - mock get_subjects: all subjects in topic_subject_schema_map
        mock_schema_registry_client.return_value.get_subjects.return_value = [
            v.subject for v in chain(*topic_subject_schema_map.values())
        ]

        # - mock get_latest_version
        def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
            for registered_schema in chain(*topic_subject_schema_map.values()):
                if registered_schema.subject == subject_name:
                    return registered_schema
            return None

        mock_schema_registry_client.return_value.get_latest_version = (
            mock_get_latest_version
        )

        kafka_source_config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "meta_mapping": {
                    "owner": {
                        "match": "^@(.*)",
                        "operation": "add_owner",
                        "config": {
                            "owner_type": "group",
                        },
                    },
                    "has_tag3": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {
                            "tag": "tag3",
                        },
                    },
                    "has_term1": {
                        "match": True,
                        "operation": "add_term",
                        "config": {
                            "term": "term1",
                        },
                    },
                },
                "field_meta_mapping": {
                    "has_tag3": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {
                            "tag": "tag3",
                        },
                    },
                    "has_term1": {
                        "match": True,
                        "operation": "add_term",
                        "config": {
                            "term": "term1",
                        },
                    },
                },
            }
        )
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, KafkaSourceReport()
        )

        aspects = confluent_schema_registry.get_aspects_from_schema(
            topic="topic1", platform_urn="urn:li:platform:test"
        )

        # Check dataset properties aspect
        dataset_properties_aspect = [
            asp for asp in aspects if isinstance(asp, DatasetPropertiesClass)
        ][0]
        assert dataset_properties_aspect.description == "test dataset description"

        # Check owners aspect
        owners_aspect = [asp for asp in aspects if isinstance(asp, OwnershipClass)][0]
        assert owners_aspect.owners[0].owner == "urn:li:corpGroup:test-team"

        # Check terms aspects
        terms_aspect = [asp for asp in aspects if isinstance(asp, GlossaryTermsClass)][
            0
        ]
        terms_assoc_values = {term_assoc.urn for term_assoc in terms_aspect.terms}
        assert terms_assoc_values == {"urn:li:glossaryTerm:term1"}

        # Check tags aspect
        tags_aspect = [asp for asp in aspects if isinstance(asp, GlobalTagsClass)][0]
        tags_assoc_values = {tag_assoc.tag for tag_assoc in tags_aspect.tags}
        # Should include tags from `schema_tags_field` and meta_mapping
        assert tags_assoc_values == {
            "urn:li:tag:kafka:tag1",
            "urn:li:tag:kafka:tag2",
            "urn:li:tag:kafka:tag3",
        }

        # Check schema metadata aspect
        schema_metadata_aspect: SchemaMetadataClass = [
            asp for asp in aspects if isinstance(asp, SchemaMetadataClass)
        ][0]
        assert schema_metadata_aspect.schemaName == "topic1"
        assert schema_metadata_aspect.platform == "urn:li:platform:test"
        assert (
            schema_metadata_aspect.hash
            == md5((value_schema_str + key_schema_str).encode()).hexdigest()
        )
        assert isinstance(schema_metadata_aspect.platformSchema, KafkaSchemaClass)
        assert schema_metadata_aspect.platformSchema.documentSchema == value_schema_str
        assert len(schema_metadata_aspect.fields) == 2
        field = schema_metadata_aspect.fields[1]  # the value field
        assert isinstance(field.globalTags, GlobalTagsClass)
        fieldTags = {tag_assoc.tag for tag_assoc in field.globalTags.tags}
        assert fieldTags == {
            "urn:li:tag:kafka:tag1",
            "urn:li:tag:kafka:tag2",
            "urn:li:tag:kafka:tag3",
        }
        assert isinstance(field.glossaryTerms, GlossaryTermsClass)
        fieldTerms = {term_assoc.urn for term_assoc in field.glossaryTerms.terms}
        assert fieldTerms == {"urn:li:glossaryTerm:term1"}


if __name__ == "__main__":
    unittest.main()

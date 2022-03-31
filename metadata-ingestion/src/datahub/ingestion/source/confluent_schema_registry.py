import json
import logging
from hashlib import md5
from typing import List, Optional, Tuple

import confluent_kafka
from confluent_kafka.schema_registry.schema_registry_client import Schema

from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.kafka import KafkaSourceConfig, KafkaSourceReport
from datahub.ingestion.source.kafka_schema_registry_base import KafkaSchemaRegistryBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)

logger = logging.getLogger(__name__)


class ConfluentSchemaRegistry(KafkaSchemaRegistryBase):
    """
    This is confluent schema registry specific implementation of datahub.ingestion.source.kafka import SchemaRegistry
    It knows how to get SchemaMetadata of a topic from ConfluentSchemaRegistry
    """

    def __init__(
        self, source_config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> None:
        self.source_config: KafkaSourceConfig = source_config
        self.report: KafkaSourceReport = report
        # Use the fully qualified name for SchemaRegistryClient to make it mock patchable for testing.
        self.schema_registry_client = (
            confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient(
                {
                    "url": source_config.connection.schema_registry_url,
                    **source_config.connection.schema_registry_config,
                }
            )
        )
        self.known_schema_registry_subjects: List[str] = []
        try:
            self.known_schema_registry_subjects.extend(
                self.schema_registry_client.get_subjects()
            )
        except Exception as e:
            logger.warning(f"Failed to get subjects from schema registry: {e}")

    @classmethod
    def create(
        cls, source_config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> "ConfluentSchemaRegistry":
        return cls(source_config, report)

    def _get_subject_for_topic(self, topic: str, is_key_schema: bool) -> Optional[str]:
        subject_key_suffix: str = "-key" if is_key_schema else "-value"
        # For details on schema registry subject name strategy,
        # see: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work

        # User-provided subject for the topic overrides the rest, regardless of the subject name strategy.
        # However, it is a must when the RecordNameStrategy is used as the schema registry subject name strategy.
        # The subject name format for RecordNameStrategy is: <fully-qualified record name>-<key/value> (cannot be inferred from topic name).
        subject_key: str = topic + subject_key_suffix
        if subject_key in self.source_config.topic_subject_map:
            return self.source_config.topic_subject_map[subject_key]

        # Subject name format when the schema registry subject name strategy is
        #  (a) TopicNameStrategy(default strategy): <topic name>-<key/value>
        #  (b) TopicRecordNameStrategy: <topic name>-<fully-qualified record name>-<key/value>
        for subject in self.known_schema_registry_subjects:
            if subject.startswith(topic) and subject.endswith(subject_key_suffix):
                return subject
        return None

    @staticmethod
    def _compact_schema(schema_str: str) -> str:
        # Eliminate all white-spaces for a compact representation.
        return json.dumps(json.loads(schema_str), separators=(",", ":"))

    def get_schema_str_replace_confluent_ref_avro(
        self, schema: Schema, schema_seen: Optional[set] = None
    ) -> str:
        if not schema.references:
            return self._compact_schema(schema.schema_str)

        if schema_seen is None:
            schema_seen = set()
        schema_str = self._compact_schema(schema.schema_str)
        for schema_ref in schema.references:
            ref_subject = schema_ref["subject"]
            if ref_subject in schema_seen:
                continue

            if ref_subject not in self.known_schema_registry_subjects:
                logger.warning(
                    f"{ref_subject} is not present in the list of registered subjects with schema registry!"
                )

            reference_schema = self.schema_registry_client.get_latest_version(
                subject_name=ref_subject
            )
            schema_seen.add(ref_subject)
            logger.debug(
                f"ref for {ref_subject} is {reference_schema.schema.schema_str}"
            )
            # Replace only external type references with the reference schema recursively.
            # NOTE: The type pattern is dependent on _compact_schema.
            avro_type_kwd = '"type"'
            ref_name = schema_ref["name"]
            # Try by name first
            pattern_to_replace = f'{avro_type_kwd}:"{ref_name}"'
            if pattern_to_replace not in schema_str:
                # Try by subject
                pattern_to_replace = f'{avro_type_kwd}:"{ref_subject}"'
                if pattern_to_replace not in schema_str:
                    logger.warning(
                        f"Not match for external schema type: {{name:{ref_name}, subject:{ref_subject}}} in schema:{schema_str}"
                    )
                else:
                    logger.debug(
                        f"External schema matches by subject, {pattern_to_replace}"
                    )
            else:
                logger.debug(f"External schema matches by name, {pattern_to_replace}")
            schema_str = schema_str.replace(
                pattern_to_replace,
                f"{avro_type_kwd}:{self.get_schema_str_replace_confluent_ref_avro(reference_schema.schema, schema_seen)}",
            )
        return schema_str

    def _get_schema_and_fields(
        self, topic: str, is_key_schema: bool
    ) -> Tuple[Optional[Schema], List[SchemaField]]:
        schema: Optional[Schema] = None
        schema_type_str: str = "key" if is_key_schema else "value"
        topic_subject: Optional[str] = self._get_subject_for_topic(
            topic=topic, is_key_schema=is_key_schema
        )
        if topic_subject is not None:
            logger.debug(
                f"The {schema_type_str} schema subject:'{topic_subject}' is found for topic:'{topic}'."
            )
            try:
                registered_schema = self.schema_registry_client.get_latest_version(
                    subject_name=topic_subject
                )
                schema = registered_schema.schema
            except Exception as e:
                logger.warning(
                    f"For topic: {topic}, failed to get {schema_type_str} schema from schema registry using subject:'{topic_subject}': {e}."
                )
                self.report.report_warning(
                    topic,
                    f"failed to get {schema_type_str} schema from schema registry using subject:'{topic_subject}': {e}.",
                )
        else:
            logger.debug(
                f"For topic: {topic}, the schema registry subject for the {schema_type_str} schema is not found."
            )
            if not is_key_schema:
                # Value schema is always expected. Report a warning.
                self.report.report_warning(
                    topic,
                    f"The schema registry subject for the {schema_type_str} schema is not found."
                    f" The topic is either schema-less, or no messages have been written to the topic yet.",
                )

        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if schema is not None:
            fields = self._get_schema_fields(
                topic=topic, schema=schema, is_key_schema=is_key_schema
            )
        return (schema, fields)

    def _get_schema_fields(
        self, topic: str, schema: Schema, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if schema.schema_type == "AVRO":
            cleaned_str: str = self.get_schema_str_replace_confluent_ref_avro(schema)
            # "value.id" or "value.[type=string]id"
            fields = schema_util.avro_schema_to_mce_fields(
                cleaned_str, is_key_schema=is_key_schema
            )
        else:
            self.report.report_warning(
                topic,
                f"Parsing kafka schema type {schema.schema_type} is currently not implemented",
            )
        return fields

    def _get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        # Process the value schema
        schema, fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=False
        )  # type: Tuple[Optional[Schema], List[SchemaField]]

        # Process the key schema
        key_schema, key_fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=True
        )  # type:Tuple[Optional[Schema], List[SchemaField]]

        # Create the schemaMetadata aspect.
        if schema is not None or key_schema is not None:
            # create a merged string for the combined schemas and compute an md5 hash across
            schema_as_string = (schema.schema_str if schema is not None else "") + (
                key_schema.schema_str if key_schema is not None else ""
            )
            md5_hash: str = md5(schema_as_string.encode()).hexdigest()

            return SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema.schema_str if schema is not None else "",
                    keySchema=key_schema.schema_str if key_schema else None,
                ),
                fields=key_fields + fields,
            )
        return None

    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        logger.debug(f"Inside _get_schema_metadata {topic} {platform_urn}")
        # Process the value schema
        schema, fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=False
        )  # type: Tuple[Optional[Schema], List[SchemaField]]

        # Process the key schema
        key_schema, key_fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=True
        )  # type:Tuple[Optional[Schema], List[SchemaField]]

        # Create the schemaMetadata aspect.
        if schema is not None or key_schema is not None:
            # create a merged string for the combined schemas and compute an md5 hash across
            schema_as_string = (schema.schema_str if schema is not None else "") + (
                key_schema.schema_str if key_schema is not None else ""
            )
            md5_hash = md5(schema_as_string.encode()).hexdigest()

            return SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema.schema_str if schema is not None else "",
                    keySchema=key_schema.schema_str if key_schema else None,
                ),
                fields=key_fields + fields,
            )
        return None

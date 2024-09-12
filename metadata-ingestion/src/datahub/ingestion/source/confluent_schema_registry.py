import json
import logging
from dataclasses import dataclass
from hashlib import md5
from typing import Any, List, Optional, Set, Tuple

import avro.schema
import jsonref
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
    SchemaRegistryClient,
)

from datahub.ingestion.extractor import protobuf_util, schema_util
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.extractor.protobuf_util import ProtobufSchema
from datahub.ingestion.source.kafka import KafkaSourceConfig, KafkaSourceReport
from datahub.ingestion.source.kafka_schema_registry_base import KafkaSchemaRegistryBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import OwnershipSourceTypeClass
from datahub.utilities.mapping import OperationProcessor

logger = logging.getLogger(__name__)


@dataclass
class JsonSchemaWrapper:
    name: str
    subject: str
    content: str
    references: List[Any]


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
        self.schema_registry_client = SchemaRegistryClient(
            {
                "url": source_config.connection.schema_registry_url,
                **source_config.connection.schema_registry_config,
            }
        )
        self.known_schema_registry_subjects: List[str] = []
        try:
            self.known_schema_registry_subjects.extend(
                self.schema_registry_client.get_subjects()
            )
        except Exception as e:
            logger.warning(f"Failed to get subjects from schema registry: {e}")

        self.field_meta_processor = OperationProcessor(
            self.source_config.field_meta_mapping,
            self.source_config.tag_prefix,
            OwnershipSourceTypeClass.SERVICE,
            self.source_config.strip_user_ids_from_email,
            match_nested_props=True,
        )

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
        #  there's a third case
        #  (c) TopicNameStrategy differing by environment name suffixes.
        #       e.g "a.b.c.d-value" and "a.b.c.d.qa-value"
        #       For such instances, the wrong schema registry entries could picked by the previous logic.
        for subject in self.known_schema_registry_subjects:
            if (
                self.source_config.disable_topic_record_naming_strategy
                and subject == subject_key
            ):
                return subject
            if (
                (not self.source_config.disable_topic_record_naming_strategy)
                and subject.startswith(topic)
                and subject.endswith(subject_key_suffix)
            ):
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
            ref_subject = schema_ref.subject
            if ref_subject in schema_seen:
                continue

            if ref_subject not in self.known_schema_registry_subjects:
                logger.warning(
                    f"{ref_subject} is not present in the list of registered subjects with schema registry!"
                )

            reference_schema = self.schema_registry_client.get_latest_version(
                subject_name=ref_subject,
            )
            schema_seen.add(ref_subject)
            logger.debug(
                f"ref for {ref_subject} is {reference_schema.schema.schema_str}"
            )
            # Replace only external type references with the reference schema recursively.
            # NOTE: The type pattern is dependent on _compact_schema.
            avro_type_kwd = '"type"'
            ref_name = schema_ref.name
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

    def get_schemas_from_confluent_ref_protobuf(
        self, schema: Schema, schema_seen: Optional[Set[str]] = None
    ) -> List[ProtobufSchema]:
        all_schemas: List[ProtobufSchema] = []

        if schema_seen is None:
            schema_seen = set()

        schema_ref: SchemaReference
        for schema_ref in schema.references:
            ref_subject: str = schema_ref.subject
            if ref_subject in schema_seen:
                continue
            reference_schema: RegisteredSchema = (
                self.schema_registry_client.get_latest_version(ref_subject)
            )
            schema_seen.add(ref_subject)
            all_schemas.append(
                ProtobufSchema(
                    name=schema_ref.name, content=reference_schema.schema.schema_str
                )
            )
        return all_schemas

    def get_schemas_from_confluent_ref_json(
        self,
        schema: Schema,
        name: str,
        subject: str,
        schema_seen: Optional[Set[str]] = None,
    ) -> List[JsonSchemaWrapper]:
        """Recursively get all the referenced schemas and their references starting from this schema"""
        all_schemas: List[JsonSchemaWrapper] = []
        if schema_seen is None:
            schema_seen = set()

        schema_ref: SchemaReference
        for schema_ref in schema.references:
            ref_subject: str = schema_ref.subject
            if ref_subject in schema_seen:
                continue
            reference_schema: RegisteredSchema = (
                self.schema_registry_client.get_version(
                    subject_name=ref_subject, version=schema_ref.version
                )
            )
            schema_seen.add(ref_subject)
            all_schemas.extend(
                self.get_schemas_from_confluent_ref_json(
                    reference_schema.schema,
                    name=schema_ref.name,
                    subject=ref_subject,
                    schema_seen=schema_seen,
                )
            )
        all_schemas.append(
            JsonSchemaWrapper(
                name=name,
                subject=subject,
                content=schema.schema_str,
                references=schema.references,
            )
        )
        return all_schemas

    def _get_schema_and_fields(
        self, topic: str, is_key_schema: bool, is_subject: bool
    ) -> Tuple[Optional[Schema], List[SchemaField]]:
        schema: Optional[Schema] = None
        kafka_entity = "subject" if is_subject else "topic"

        # if provided schema as topic, assuming it as value subject
        schema_type_str: Optional[str] = "value"
        topic_subject: Optional[str] = None
        if not is_subject:
            schema_type_str = "key" if is_key_schema else "value"
            topic_subject = self._get_subject_for_topic(
                topic=topic, is_key_schema=is_key_schema
            )
        else:
            topic_subject = topic

        if topic_subject is not None:
            logger.debug(
                f"The {schema_type_str} schema subject:'{topic_subject}' is found for {kafka_entity}: '{topic}'."
            )
            try:
                registered_schema = self.schema_registry_client.get_latest_version(
                    subject_name=topic_subject
                )
                schema = registered_schema.schema
            except Exception as e:
                self.report.warning(
                    title="Failed to get subject schema from schema registry",
                    message=f"Failed to get {kafka_entity} {schema_type_str or ''} schema from schema registry",
                    context=(
                        f"{topic}: {topic_subject}" if not is_subject else topic_subject
                    ),
                    exc=e,
                )
        else:
            logger.debug(
                f"For {kafka_entity}: {topic}, the schema registry subject for the {schema_type_str} schema is not found."
            )
            if not is_key_schema:
                # Value schema is always expected. Report a warning.
                self.report.warning(
                    title="Unable to find a matching subject name for the topic in the schema registry",
                    message=f"The {kafka_entity} {schema_type_str or ''} is either schema-less, or no messages have been written to the {kafka_entity} yet. "
                    "If this is unexpected, check the topic_subject_map and topic_naming related configs.",
                    context=topic,
                )

        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if schema is not None:
            fields = self._get_schema_fields(
                topic=topic,
                schema=schema,
                is_key_schema=is_key_schema,
            )
        return (schema, fields)

    def _load_json_schema_with_resolved_references(
        self, schema: Schema, name: str, subject: str
    ) -> dict:
        imported_json_schemas: List[
            JsonSchemaWrapper
        ] = self.get_schemas_from_confluent_ref_json(schema, name=name, subject=subject)
        schema_dict = json.loads(schema.schema_str)
        reference_map = {}
        for imported_schema in imported_json_schemas:
            reference_schema = json.loads(imported_schema.content)
            if "title" not in reference_schema:
                reference_schema["title"] = imported_schema.subject
            reference_map[imported_schema.name] = reference_schema

        jsonref_schema = jsonref.loads(
            json.dumps(schema_dict), loader=lambda x: reference_map.get(x)
        )
        return jsonref_schema

    def _get_schema_fields(
        self, topic: str, schema: Schema, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if schema.schema_type == "AVRO":
            cleaned_str: str = self.get_schema_str_replace_confluent_ref_avro(schema)
            avro_schema = avro.schema.parse(cleaned_str)

            # "value.id" or "value.[type=string]id"
            fields = schema_util.avro_schema_to_mce_fields(
                avro_schema,
                is_key_schema=is_key_schema,
                meta_mapping_processor=(
                    self.field_meta_processor
                    if self.source_config.enable_meta_mapping
                    else None
                ),
                schema_tags_field=self.source_config.schema_tags_field,
                tag_prefix=self.source_config.tag_prefix,
            )

        elif schema.schema_type == "PROTOBUF":
            imported_schemas: List[
                ProtobufSchema
            ] = self.get_schemas_from_confluent_ref_protobuf(schema)
            base_name: str = topic.replace(".", "_")
            fields = protobuf_util.protobuf_schema_to_mce_fields(
                ProtobufSchema(
                    (
                        f"{base_name}-key.proto"
                        if is_key_schema
                        else f"{base_name}-value.proto"
                    ),
                    schema.schema_str,
                ),
                imported_schemas,
                is_key_schema=is_key_schema,
            )
        elif schema.schema_type == "JSON":
            base_name = topic.replace(".", "_")
            canonical_name = (
                f"{base_name}-key" if is_key_schema else f"{base_name}-value"
            )
            jsonref_schema = self._load_json_schema_with_resolved_references(
                schema=schema,
                name=canonical_name,
                subject=f"{topic}-key" if is_key_schema else f"{topic}-value",
            )
            fields = list(
                JsonSchemaTranslator.get_fields_from_schema(
                    jsonref_schema, is_key_schema=is_key_schema
                )
            )
        elif not self.source_config.ignore_warnings_on_schema_type:
            self.report.report_warning(
                topic,
                f"Parsing kafka schema type {schema.schema_type} is currently not implemented",
            )
        return fields

    def _get_schema_metadata(
        self, topic: str, platform_urn: str, is_subject: bool
    ) -> Optional[SchemaMetadata]:

        # Process the value schema
        schema, fields = self._get_schema_and_fields(
            topic=topic,
            is_key_schema=False,
            is_subject=is_subject,
        )  # type: Tuple[Optional[Schema], List[SchemaField]]

        # Process the key schema
        key_schema, key_fields = self._get_schema_and_fields(
            topic=topic,
            is_key_schema=True,
            is_subject=is_subject,
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
                    documentSchema=schema.schema_str if schema else "",
                    documentSchemaType=schema.schema_type if schema else None,
                    keySchema=key_schema.schema_str if key_schema else None,
                    keySchemaType=key_schema.schema_type if key_schema else None,
                ),
                fields=key_fields + fields,
            )
        return None

    def get_schema_metadata(
        self, topic: str, platform_urn: str, is_subject: bool
    ) -> Optional[SchemaMetadata]:
        logger.debug(f"Inside get_schema_metadata {topic} {platform_urn}")

        # Process the value schema
        schema, fields = self._get_schema_and_fields(
            topic=topic,
            is_key_schema=False,
            is_subject=is_subject,
        )  # type: Tuple[Optional[Schema], List[SchemaField]]

        # Process the key schema
        key_schema, key_fields = self._get_schema_and_fields(
            topic=topic,
            is_key_schema=True,
            is_subject=is_subject,
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
                    documentSchema=schema.schema_str if schema else "",
                    documentSchemaType=schema.schema_type if schema else None,
                    keySchema=key_schema.schema_str if key_schema else None,
                    keySchemaType=key_schema.schema_type if key_schema else None,
                ),
                fields=key_fields + fields,
            )
        return None

    def get_subjects(self) -> List[str]:
        return self.known_schema_registry_subjects

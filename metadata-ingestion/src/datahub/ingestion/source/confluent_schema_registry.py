import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import avro.schema
import jsonref
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
    SchemaRegistryClient,
)

from datahub.ingestion.extractor import protobuf_util, schema_util
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.extractor.protobuf_util import ProtobufSchema
from datahub.ingestion.source.kafka.kafka import KafkaSourceConfig, KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_config import SchemalessFallback
from datahub.ingestion.source.kafka.kafka_schema_registry_base import (
    KafkaSchemaRegistryBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    NumberTypeClass,
    OwnershipSourceTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from datahub.utilities.mapping import OperationProcessor

logger = logging.getLogger(__name__)

# Type aliases for better type safety
MessageValue = Union[str, int, float, bool, Dict[str, Any], List[Any], None]


@dataclass
class FieldAnalysis:
    """Analysis of a field from message samples."""

    types: Set[str]
    sample_values: List[str]

    def __init__(self) -> None:
        self.types = set()
        self.sample_values = []


FieldInfo = Dict[str, FieldAnalysis]


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
                # Value schema is always expected. Check if we should fallback or warn.
                if self.source_config.schemaless_fallback.enabled:
                    logger.info(
                        f"Schema registry subject not found for {kafka_entity}: {topic}. "
                        f"Falling back to schema-less processing to infer schema from message data."
                    )
                    # Set a flag to indicate this topic should use schemaless fallback
                    self.report.report_topic_scanned(f"{topic}_schemaless_fallback")
                else:
                    self.report.warning(
                        title="Unable to find a matching subject name for the topic in the schema registry",
                        message=f"The {kafka_entity} {schema_type_str or ''} is either schema-less, or no messages have been written to the {kafka_entity} yet. "
                        "If this is unexpected, check the topic_subject_map and topic_naming related configs. "
                        "Consider enabling 'schemaless_fallback.enabled' to automatically infer schema from message data.",
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
        elif (
            self.source_config.schemaless_fallback.enabled
            and not is_key_schema
            and not is_subject
        ):
            # Attempt to infer schema from message data when no schema registry entry exists
            fields = self._infer_schema_from_messages(topic)

        return (schema, fields)

    def get_schema_and_fields_batch(
        self, topics: List[str], is_key_schema: bool = False
    ) -> Dict[str, Tuple[Optional[Schema], List[SchemaField]]]:
        """
        Get schemas and fields for multiple topics in batch, using parallel processing for schema-less fallback.
        This is the main entry point for batch schema processing.
        """
        results = {}
        topics_needing_fallback = []

        # First, try to get schemas from schema registry
        for topic in topics:
            try:
                schema, fields = self._get_schema_and_fields(
                    topic, is_key_schema, is_subject=False
                )
                results[topic] = (schema, fields)

                # If no schema was found and fallback is enabled, add to fallback list
                if (
                    schema is None
                    and not fields
                    and self.source_config.schemaless_fallback.enabled
                    and not is_key_schema
                ):
                    topics_needing_fallback.append(topic)

            except Exception as e:
                logger.warning(f"Failed to get schema for topic {topic}: {e}")
                if self.source_config.schemaless_fallback.enabled and not is_key_schema:
                    topics_needing_fallback.append(topic)
                else:
                    results[topic] = (None, [])

        # Process topics needing fallback in batch (parallel if enabled)
        if topics_needing_fallback:
            logger.info(
                f"Processing {len(topics_needing_fallback)} topics with schema-less fallback"
            )
            fallback_results = self.infer_schemas_batch(topics_needing_fallback)

            # Update results with fallback schemas
            for topic, inferred_fields in fallback_results.items():
                results[topic] = (None, inferred_fields)

        return results

    def infer_schemas_batch(self, topics: List[str]) -> Dict[str, List[SchemaField]]:
        """
        Infer schemas for multiple topics in parallel for improved performance.
        Returns a dictionary mapping topic names to their inferred schema fields.
        """
        if not topics:
            return {}

        fallback_config = self.source_config.schemaless_fallback
        results = {}

        # Process topics in parallel if max_workers > 1
        if fallback_config.max_workers > 1 and len(topics) > 1:
            logger.info(
                f"Processing {len(topics)} topics in parallel for schema inference"
            )
            parallel_results = self._infer_schemas_parallel(topics, fallback_config)
            results.update(parallel_results)
        else:
            # Sequential processing
            logger.debug(
                f"Processing {len(topics)} topics sequentially for schema inference"
            )
            for topic in topics:
                try:
                    schema_fields = self._infer_schema_from_messages(topic)
                    results[topic] = schema_fields
                except Exception as e:
                    logger.warning(f"Failed to infer schema for topic {topic}: {e}")
                    results[topic] = []

        return results

    def _infer_schemas_parallel(
        self, topics: List[str], fallback_config: SchemalessFallback
    ) -> Dict[str, List[SchemaField]]:
        """
        Process multiple topics in parallel using ThreadPoolExecutor.
        """
        results = {}

        # Intelligent worker calculation based on system resources and configuration
        cpu_count = os.cpu_count() or 4  # Fallback to 4 if cpu_count() returns None

        # Use the smaller of: configured max, number of topics, or 2x CPU cores (reasonable for I/O bound work)
        max_workers = min(
            fallback_config.max_workers,
            len(topics),
            cpu_count
            * 2,  # I/O bound work can benefit from more threads than CPU cores
        )

        logger.info(
            f"Using {max_workers} parallel workers for {len(topics)} topics (CPU cores: {cpu_count})"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_topic = {
                executor.submit(
                    self._infer_schema_from_messages_internal, topic, fallback_config
                ): topic
                for topic in topics
            }

            # Collect results as they complete
            for future in as_completed(future_to_topic):
                topic = future_to_topic[future]
                try:
                    schema_fields = future.result()
                    results[topic] = schema_fields
                    logger.debug(f"Completed schema inference for topic {topic}")
                except Exception as e:
                    logger.warning(f"Failed to infer schema for topic {topic}: {e}")
                    results[topic] = []

        logger.info(f"Completed parallel schema inference for {len(topics)} topics")
        return results

    def _infer_schema_from_messages_internal(
        self, topic: str, fallback_config: SchemalessFallback
    ) -> List[SchemaField]:
        """
        Internal method for schema inference without caching logic (used by parallel processing).
        """
        try:
            # Use optimized message sampling
            sample_messages = self._sample_topic_messages_optimized(
                topic, fallback_config
            )

            if not sample_messages:
                logger.debug(f"Skipping empty topic {topic} for schema inference")
                return []

            # Infer schema fields from the sample data
            inferred_fields = self._extract_fields_from_samples(topic, sample_messages)

            # Return the inferred fields directly (no caching needed)

            logger.debug(
                f"Successfully inferred {len(inferred_fields)} schema fields from message data for topic {topic}"
            )

            return inferred_fields

        except Exception as e:
            logger.warning(
                f"Failed to infer schema from messages for topic {topic}: {e}. "
                f"Topic will be processed without schema information."
            )
            return []

    def _sample_topic_messages_optimized(
        self, topic: str, fallback_config: SchemalessFallback
    ) -> List[Dict[str, MessageValue]]:
        """
        Optimized message sampling with hybrid strategy: try latest first, fallback to earliest.
        """
        strategy = fallback_config.sample_strategy.lower()

        if strategy == "hybrid":
            # Try latest first for speed
            logger.debug(f"Trying 'latest' sampling for topic {topic}")
            messages = self._sample_messages_with_strategy(
                topic, fallback_config, "latest"
            )

            if not messages:
                logger.debug(
                    f"No recent messages found, trying 'earliest' for topic {topic}"
                )
                messages = self._sample_messages_with_strategy(
                    topic, fallback_config, "earliest"
                )

            return messages
        elif strategy == "latest":
            return self._sample_messages_with_strategy(topic, fallback_config, "latest")
        else:  # earliest or any other value
            return self._sample_messages_with_strategy(
                topic, fallback_config, "earliest"
            )

    def _sample_messages_with_strategy(
        self,
        topic: str,
        fallback_config: SchemalessFallback,
        offset_strategy: str,
    ) -> List[Dict[str, MessageValue]]:
        """
        Sample messages from a topic with the specified offset strategy.
        """
        start_time = time.time()

        try:
            # Create a consumer with optimized settings for fast sampling
            consumer_config = {
                "bootstrap.servers": self.source_config.connection.bootstrap,
                "group.id": f"datahub-schema-inference-{topic}-{offset_strategy}-{int(time.time())}",
                "auto.offset.reset": offset_strategy,
                "enable.auto.commit": False,
                "fetch.min.bytes": 1,  # Don't wait for large batches
                "fetch.wait.max.ms": 100,  # Short wait time
                "session.timeout.ms": 6000,  # Shorter session timeout
                **self.source_config.connection.consumer_config,
            }

            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])

            messages: List[Dict[str, MessageValue]] = []
            attempts = 0

            # For 'latest' strategy, use shorter timeout since we expect recent activity
            timeout_seconds = (
                fallback_config.sample_timeout_seconds * 0.3  # 30% of normal timeout
                if offset_strategy == "latest"
                else fallback_config.sample_timeout_seconds
            )

            # For 'latest' strategy, reduce poll attempts since we're looking for recent messages
            max_attempts = (
                10  # Cap at 10 for latest
                if offset_strategy == "latest"
                else 20  # Standard attempts for earliest
            )

            while (
                len(messages) < 5  # Sample 5 messages
                and attempts < max_attempts
                and (time.time() - start_time) < timeout_seconds
            ):
                msg = consumer.poll(timeout=0.5)  # Short poll timeout
                attempts += 1

                if msg is None:
                    continue
                if msg.error():
                    continue

                try:
                    # Try to decode the message value
                    value = msg.value()
                    if value is None:
                        continue

                    # Try JSON decoding first (most common case)
                    if isinstance(value, bytes):
                        try:
                            decoded_value = json.loads(value.decode("utf-8"))
                            if isinstance(decoded_value, dict):
                                messages.append(decoded_value)
                                continue
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass

                        # If JSON fails, try to create a simple structure
                        try:
                            messages.append(
                                {"raw_value": value.decode("utf-8", errors="replace")}
                            )
                        except Exception:
                            messages.append({"raw_value": str(value)})
                    elif isinstance(value, dict):
                        messages.append(value)
                    else:
                        messages.append({"value": str(value)})

                except Exception as e:
                    logger.debug(f"Failed to process message for schema inference: {e}")
                    continue

            consumer.close()

            elapsed_time = time.time() - start_time
            logger.debug(
                f"Sampled {len(messages)} messages from topic {topic} using '{offset_strategy}' strategy "
                f"in {elapsed_time:.2f}s ({attempts} poll attempts)"
            )

            return messages

        except Exception as e:
            logger.debug(
                f"Failed to sample messages from topic {topic} with '{offset_strategy}' strategy: {e}"
            )
            return []

    def _infer_schema_from_messages(self, topic: str) -> List[SchemaField]:
        """
        Infer schema fields from actual message data when no schema registry entry exists.
        This provides a fallback mechanism for schema-less topics.
        """
        fallback_config = self.source_config.schemaless_fallback

        try:
            # Use optimized message sampling
            sample_messages = self._sample_topic_messages_optimized(
                topic, fallback_config
            )

            if not sample_messages:
                logger.debug(f"Skipping empty topic {topic} for schema inference")
                return []

            # Infer schema fields from the sample data
            inferred_fields = self._extract_fields_from_samples(topic, sample_messages)

            # Return the inferred fields directly (no caching needed)

            logger.info(
                f"Successfully inferred {len(inferred_fields)} schema fields from message data for topic {topic}"
            )

            return inferred_fields

        except Exception as e:
            logger.warning(
                f"Failed to infer schema from messages for topic {topic}: {e}. "
                f"Topic will be processed without schema information."
            )
            return []

    def _sample_topic_messages(
        self, topic: str, max_messages: int = 10
    ) -> List[Dict[str, MessageValue]]:
        """
        Sample messages from a Kafka topic for schema inference.
        """

        try:
            # Create a consumer for sampling
            consumer_config = {
                "bootstrap.servers": self.source_config.connection.bootstrap,
                "group.id": f"datahub-schema-inference-{topic}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                **self.source_config.connection.consumer_config,
            }

            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])

            messages: List[Dict[str, MessageValue]] = []
            attempts = 0
            max_attempts = 50  # Try up to 50 polls

            while len(messages) < max_messages and attempts < max_attempts:
                msg = consumer.poll(timeout=1.0)
                attempts += 1

                if msg is None:
                    continue
                if msg.error():
                    continue

                try:
                    # Try to decode the message value
                    value = msg.value()
                    if value is None:
                        continue

                    # Try JSON decoding first
                    if isinstance(value, bytes):
                        try:
                            decoded_value = json.loads(value.decode("utf-8"))
                            if isinstance(decoded_value, dict):
                                messages.append(decoded_value)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            # If JSON fails, try to create a simple structure
                            try:
                                messages.append(
                                    {
                                        "raw_value": value.decode(
                                            "utf-8", errors="replace"
                                        )
                                    }
                                )
                            except Exception:
                                messages.append({"raw_value": str(value)})
                    elif isinstance(value, dict):
                        messages.append(value)
                    else:
                        messages.append({"value": str(value)})

                except Exception as e:
                    logger.debug(f"Failed to process message for schema inference: {e}")
                    continue

            consumer.close()
            return messages

        except Exception as e:
            logger.debug(f"Failed to sample messages from topic {topic}: {e}")
            return []

    def _extract_fields_from_samples(
        self, topic: str, sample_messages: List[Dict[str, MessageValue]]
    ) -> List[SchemaField]:
        """
        Extract schema fields from sample message data.
        """

        # Collect all unique field paths and their types from samples
        field_info: FieldInfo = {}

        for message in sample_messages[
            :50
        ]:  # Limit to first 50 messages for performance
            if not isinstance(message, dict):
                continue

            # Flatten the message to get all field paths
            from datahub.ingestion.source.kafka.kafka_profiler import flatten_json

            try:
                flattened = flatten_json(message, max_depth=5)  # Reasonable depth limit

                for field_path, value in flattened.items():
                    if field_path not in field_info:
                        field_info[field_path] = FieldAnalysis()

                    # Determine the type of this value
                    if value is None:
                        field_info[field_path].types.add("null")
                    elif isinstance(value, bool):
                        field_info[field_path].types.add("boolean")
                    elif isinstance(value, int):
                        field_info[field_path].types.add("long")
                    elif isinstance(value, float):
                        field_info[field_path].types.add("double")
                    elif isinstance(value, str):
                        field_info[field_path].types.add("string")
                    elif isinstance(value, (list, tuple)):
                        field_info[field_path].types.add("array")
                    elif isinstance(value, dict):
                        field_info[field_path].types.add("record")
                    else:
                        field_info[field_path].types.add("string")  # Default to string

                    # Keep a few sample values
                    if len(field_info[field_path].sample_values) < 3:
                        field_info[field_path].sample_values.append(str(value))

            except Exception as e:
                logger.debug(
                    f"Failed to process message sample for schema inference: {e}"
                )
                continue

        # Convert field info to SchemaField objects
        schema_fields = []
        for field_path, info in field_info.items():
            try:
                # Determine the best type for this field
                types = info.types

                if "double" in types or "float" in types:
                    data_type = SchemaFieldDataTypeClass(type=NumberTypeClass())
                    native_type = "double"
                elif "long" in types or "int" in types:
                    data_type = SchemaFieldDataTypeClass(type=NumberTypeClass())
                    native_type = "long"
                elif "boolean" in types:
                    data_type = SchemaFieldDataTypeClass(type=BooleanTypeClass())
                    native_type = "boolean"
                elif "array" in types:
                    data_type = SchemaFieldDataTypeClass(
                        type=ArrayTypeClass(nestedType=["string"])
                    )
                    native_type = "array"
                else:
                    data_type = SchemaFieldDataTypeClass(type=StringTypeClass())
                    native_type = "string"

                # Create the schema field
                schema_field = SchemaField(
                    fieldPath=field_path,
                    type=data_type,
                    nativeDataType=native_type,
                    description=f"Inferred from message data. Sample values: {', '.join(info.sample_values[:3])}",
                    nullable=("null" in types),
                    recursive=False,
                )

                schema_fields.append(schema_field)

            except Exception as e:
                logger.debug(f"Failed to create schema field for {field_path}: {e}")
                continue

        return schema_fields

    def _load_json_schema_with_resolved_references(
        self, schema: Schema, name: str, subject: str
    ) -> dict:
        imported_json_schemas: List[JsonSchemaWrapper] = (
            self.get_schemas_from_confluent_ref_json(schema, name=name, subject=subject)
        )
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
            imported_schemas: List[ProtobufSchema] = (
                self.get_schemas_from_confluent_ref_protobuf(schema)
            )
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

    def build_schema_metadata(
        self,
        topic: str,
        platform_urn: str,
        schema: Optional[Schema],
        fields: List[SchemaField],
    ) -> Optional[SchemaMetadata]:
        """Build SchemaMetadata from pre-fetched schema and fields data."""
        return self.build_schema_metadata_with_key(
            topic, platform_urn, schema, fields, None, []
        )

    def build_schema_metadata_with_key(
        self,
        topic: str,
        platform_urn: str,
        value_schema: Optional[Schema],
        value_fields: List[SchemaField],
        key_schema: Optional[Schema],
        key_fields: List[SchemaField],
    ) -> Optional[SchemaMetadata]:
        """Build SchemaMetadata from pre-fetched value and key schema data."""
        if (
            value_schema is not None
            or key_schema is not None
            or value_fields
            or key_fields
        ):
            # Create a hash from both schemas
            value_schema_str = (
                value_schema.schema_str if value_schema is not None else ""
            )
            key_schema_str = key_schema.schema_str if key_schema is not None else ""
            schema_as_string = value_schema_str + key_schema_str
            md5_hash = md5(schema_as_string.encode()).hexdigest()

            return SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=value_schema.schema_str if value_schema else "",
                    documentSchemaType=value_schema.schema_type
                    if value_schema
                    else None,
                    keySchema=key_schema.schema_str if key_schema else None,
                    keySchemaType=key_schema.schema_type if key_schema else None,
                ),
                fields=key_fields + value_fields,  # Combine key and value fields
            )
        return None

    def get_subjects(self) -> List[str]:
        return self.known_schema_registry_subjects

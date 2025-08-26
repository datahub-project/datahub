"""
Enhanced Schema Resolution for Kafka Topics

This module provides comprehensive schema resolution strategies for Kafka topics,
including multiple fallback approaches before resorting to schema inference from message data.

Schema Resolution Strategy (in order of preference):
1. TopicNameStrategy: Direct lookup using topic name
2. RecordNameStrategy: Extract record name from message and lookup
3. TopicRecordNameStrategy: Combine topic + record name for lookup
4. TopicSubjectMap: User-defined topic-to-subject mappings
5. Schema Inference: Infer schema from message data as final fallback
"""

import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from datahub.ingestion.source.kafka.kafka_config import KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_schema_inference import KafkaSchemaInference
from datahub.ingestion.source.kafka.kafka_utils import MessageValue
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

logger = logging.getLogger(__name__)


@dataclass
class SchemaResolutionResult:
    """Result of schema resolution attempt."""

    schema: Optional[Schema]
    fields: List[SchemaField]
    resolution_method: str
    subject_name: Optional[str] = None
    record_name: Optional[str] = None


@dataclass
class RecordNameExtractionResult:
    """Result of record name extraction from message data."""

    record_name: Optional[str]
    namespace: Optional[str] = None
    full_name: Optional[str] = None


class KafkaSchemaResolver:
    """
    Comprehensive schema resolver for Kafka topics with multiple fallback strategies.

    This class implements a sophisticated schema resolution approach that tries multiple
    strategies before falling back to schema inference from message data.
    """

    def __init__(
        self,
        source_config: KafkaSourceConfig,
        schema_registry_client: SchemaRegistryClient,
        known_subjects: List[str],
        max_workers: int = 5 * (os.cpu_count() or 4),
    ):
        """Initialize the schema resolver."""
        self.source_config = source_config
        self.schema_registry_client = schema_registry_client
        self.known_subjects = set(known_subjects)
        self.max_workers = max_workers

        # Initialize schema inference as final fallback
        self.schema_inference = None
        if source_config.schemaless_fallback.enabled:
            self.schema_inference = KafkaSchemaInference(
                bootstrap_servers=source_config.connection.bootstrap,
                consumer_config=source_config.connection.consumer_config,
                fallback_config=source_config.schemaless_fallback,
                max_workers=max_workers,
            )

    def resolve_schemas_batch(
        self, topics: List[str], is_key_schema: bool = False
    ) -> Dict[str, SchemaResolutionResult]:
        """
        Resolve schemas for multiple topics using comprehensive strategy.

        Args:
            topics: List of topic names
            is_key_schema: Whether to resolve key schemas (vs value schemas)

        Returns:
            Dictionary mapping topic names to schema resolution results
        """
        results = {}
        topics_needing_inference = []

        for topic in topics:
            # Try all registry-based strategies first
            result = self._resolve_from_registry(topic, is_key_schema)

            if result.schema or result.fields:
                results[topic] = result
            else:
                # Mark for schema inference as final fallback
                topics_needing_inference.append(topic)

        # Batch process topics that need schema inference
        if topics_needing_inference and self.schema_inference:
            logger.info(
                f"Attempting schema inference for {len(topics_needing_inference)} topics "
                f"after registry resolution failed"
            )
            inferred_schemas = self.schema_inference.infer_schemas_batch(
                topics_needing_inference
            )

            for topic in topics_needing_inference:
                fields = inferred_schemas.get(topic, [])
                results[topic] = SchemaResolutionResult(
                    schema=None,
                    fields=fields,
                    resolution_method="schema_inference" if fields else "none",
                )
        else:
            # No schema inference available or needed
            for topic in topics_needing_inference:
                results[topic] = SchemaResolutionResult(
                    schema=None,
                    fields=[],
                    resolution_method="none",
                )

        return results

    def _resolve_from_registry(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
        """
        Try to resolve schema from registry using multiple strategies.

        Strategy order:
        1. TopicNameStrategy: <topic>-key/value
        2. TopicSubjectMap: User-defined mappings
        3. RecordNameStrategy: Extract from messages and try <record_name>-key/value
        4. TopicRecordNameStrategy: Try <topic>-<record_name>-key/value
        """

        # Strategy 1: TopicNameStrategy (most common)
        result = self._try_topic_name_strategy(topic, is_key_schema)
        if result.schema or result.fields:
            return result

        # Strategy 2: TopicSubjectMap (user-defined override)
        result = self._try_topic_subject_map(topic, is_key_schema)
        if result.schema or result.fields:
            return result

        # Strategy 3 & 4: RecordNameStrategy and TopicRecordNameStrategy
        # These require extracting record names from message data
        result = self._try_record_name_strategies(topic, is_key_schema)
        if result.schema or result.fields:
            return result

        return SchemaResolutionResult(
            schema=None,
            fields=[],
            resolution_method="registry_failed",
        )

    def _try_topic_name_strategy(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
        """Try TopicNameStrategy: <topic>-key/value"""
        suffix = "-key" if is_key_schema else "-value"
        subject_name = f"{topic}{suffix}"

        if subject_name in self.known_subjects:
            try:
                registered_schema = self.schema_registry_client.get_latest_version(
                    subject_name
                )
                if registered_schema and registered_schema.schema:
                    return SchemaResolutionResult(
                        schema=registered_schema.schema,
                        fields=[],  # Fields will be extracted by the caller
                        resolution_method="topic_name_strategy",
                        subject_name=subject_name,
                    )
            except Exception as e:
                logger.debug(f"TopicNameStrategy failed for {subject_name}: {e}")

        return SchemaResolutionResult(
            schema=None, fields=[], resolution_method="topic_name_failed"
        )

    def _try_topic_subject_map(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
        """Try user-defined topic-to-subject mappings"""
        suffix = "-key" if is_key_schema else "-value"
        topic_key = f"{topic}{suffix}"

        subject_name = self.source_config.topic_subject_map.get(topic_key)
        if subject_name and subject_name in self.known_subjects:
            try:
                registered_schema = self.schema_registry_client.get_latest_version(
                    subject_name
                )
                if registered_schema and registered_schema.schema:
                    return SchemaResolutionResult(
                        schema=registered_schema.schema,
                        fields=[],
                        resolution_method="topic_subject_map",
                        subject_name=subject_name,
                    )
            except Exception as e:
                logger.debug(f"TopicSubjectMap failed for {subject_name}: {e}")

        return SchemaResolutionResult(
            schema=None, fields=[], resolution_method="subject_map_failed"
        )

    def _try_record_name_strategies(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
        """
        Try RecordNameStrategy and TopicRecordNameStrategy by extracting record names from messages.

        This requires sampling a few messages from the topic to extract record names,
        then trying to find matching subjects in the registry.
        """
        if not self.schema_inference:
            return SchemaResolutionResult(
                schema=None, fields=[], resolution_method="no_inference_available"
            )

        try:
            # Sample a few messages to extract potential record names
            record_names = self._extract_record_names_from_topic(topic, is_key_schema)

            if not record_names:
                return SchemaResolutionResult(
                    schema=None, fields=[], resolution_method="no_record_names_found"
                )

            suffix = "-key" if is_key_schema else "-value"

            # Try RecordNameStrategy: <record_name>-key/value
            for record_name in record_names:
                subject_name = f"{record_name}{suffix}"
                if subject_name in self.known_subjects:
                    try:
                        registered_schema = (
                            self.schema_registry_client.get_latest_version(subject_name)
                        )
                        if registered_schema and registered_schema.schema:
                            return SchemaResolutionResult(
                                schema=registered_schema.schema,
                                fields=[],
                                resolution_method="record_name_strategy",
                                subject_name=subject_name,
                                record_name=record_name,
                            )
                    except Exception as e:
                        logger.debug(
                            f"RecordNameStrategy failed for {subject_name}: {e}"
                        )

            # Try TopicRecordNameStrategy: <topic>-<record_name>-key/value
            for record_name in record_names:
                subject_name = f"{topic}-{record_name}{suffix}"
                if subject_name in self.known_subjects:
                    try:
                        registered_schema = (
                            self.schema_registry_client.get_latest_version(subject_name)
                        )
                        if registered_schema and registered_schema.schema:
                            return SchemaResolutionResult(
                                schema=registered_schema.schema,
                                fields=[],
                                resolution_method="topic_record_name_strategy",
                                subject_name=subject_name,
                                record_name=record_name,
                            )
                    except Exception as e:
                        logger.debug(
                            f"TopicRecordNameStrategy failed for {subject_name}: {e}"
                        )

        except Exception as e:
            logger.warning(f"Record name extraction failed for topic {topic}: {e}")

        return SchemaResolutionResult(
            schema=None, fields=[], resolution_method="record_name_strategies_failed"
        )

    def _extract_record_names_from_topic(
        self, topic: str, is_key_schema: bool
    ) -> Set[str]:
        """
        Extract potential record names from a small sample of messages in the topic.

        This method samples a few messages and tries to extract Avro record names
        or Protobuf message names that could be used for RecordNameStrategy.
        """
        record_names: Set[str] = set()

        try:
            # Use the schema inference infrastructure to sample messages
            if not self.schema_inference:
                return record_names
            sample_messages = self.schema_inference._sample_topic_messages(topic)

            # Use the configured fallback config to determine how many messages to sample
            fallback_config = (
                self.source_config.schema_resolution
                if self.source_config.schema_resolution.enabled
                else self.source_config.schemaless_fallback
            )

            # Limit to configured number of samples for efficiency
            sample_messages = sample_messages[: fallback_config.max_messages_per_topic]

            for message_value in sample_messages:
                if message_value is None:
                    continue

                # For schema resolution, we only work with value schemas for now
                # Key schema resolution would need a separate sampling approach
                if is_key_schema:
                    continue

                # Try to extract record name from the message
                record_name_result = self._extract_record_name_from_message(
                    message_value
                )
                if record_name_result.record_name:
                    record_names.add(record_name_result.record_name)
                if record_name_result.full_name:
                    record_names.add(record_name_result.full_name)

        except Exception as e:
            logger.debug(f"Failed to extract record names from topic {topic}: {e}")

        return record_names

    def _extract_record_name_from_message(
        self, message_value: MessageValue
    ) -> RecordNameExtractionResult:
        """
        Extract record name from a single message.

        This method attempts to parse Avro or Protobuf messages to extract
        the record/message name that could be used for schema registry lookup.
        """
        if not isinstance(message_value, bytes) or len(message_value) < 5:
            return RecordNameExtractionResult(record_name=None)

        try:
            # Check for Confluent Schema Registry magic byte
            if message_value[0] == 0:
                # This is a schema registry serialized message
                # Bytes 1-4 contain the schema ID
                schema_id = int.from_bytes(message_value[1:5], byteorder="big")

                # Try to get the schema from the registry using the ID
                try:
                    schema = self.schema_registry_client.get_by_id(schema_id)
                    if schema and schema.schema_str:
                        return self._extract_record_name_from_schema(schema.schema_str)
                except Exception as e:
                    logger.debug(f"Failed to get schema for ID {schema_id}: {e}")

            # If not a schema registry message, try to parse as raw Avro/Protobuf
            # This is more complex and may not always work, but we can try
            return self._extract_record_name_from_raw_message(message_value)

        except Exception as e:
            logger.debug(f"Failed to extract record name from message: {e}")
            return RecordNameExtractionResult(record_name=None)

    def _extract_record_name_from_schema(
        self, schema_str: str
    ) -> RecordNameExtractionResult:
        """Extract record name from a schema string (Avro JSON or Protobuf)."""
        try:
            # Try parsing as Avro schema
            import json

            schema_dict = json.loads(schema_str)

            if isinstance(schema_dict, dict):
                name = schema_dict.get("name")
                namespace = schema_dict.get("namespace")

                if name:
                    full_name = f"{namespace}.{name}" if namespace else name
                    return RecordNameExtractionResult(
                        record_name=name,
                        namespace=namespace,
                        full_name=full_name,
                    )
        except json.JSONDecodeError:
            # Not JSON, might be Protobuf - try to extract message name
            # This is more complex and would require protobuf parsing
            pass
        except Exception as e:
            logger.debug(f"Failed to parse schema for record name: {e}")

        return RecordNameExtractionResult(record_name=None)

    def _extract_record_name_from_raw_message(
        self, message_value: bytes
    ) -> RecordNameExtractionResult:
        """
        Extract record name from raw message data.

        This is a best-effort attempt and may not always work,
        especially for complex or encrypted messages.
        """
        # This is quite complex to implement reliably without knowing the exact schema
        # For now, return None - this could be enhanced in the future
        return RecordNameExtractionResult(record_name=None)

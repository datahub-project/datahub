import json
import logging
import os
import struct
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from datahub.ingestion.source.kafka.kafka_config import KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_constants import (
    CONFLUENT_MAGIC_BYTE,
    CONFLUENT_WIRE_HEADER_LENGTH,
    DEFAULT_CPU_COUNT_FALLBACK,
    DEFAULT_MAX_WORKERS_MULTIPLIER,
    STRATEGY_NAME_RECORD_NAME,
    STRATEGY_NAME_TOPIC_RECORD_NAME,
    ResolutionMethod,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_schema_inference import KafkaSchemaInference
from datahub.ingestion.source.kafka.kafka_utils import MessageValue
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

logger = logging.getLogger(__name__)


@dataclass
class SchemaResolutionResult:
    schema: Optional[Schema]
    fields: List[SchemaField]
    # Diagnostic label only; use is_resolved to test for success.
    resolution_method: ResolutionMethod
    subject_name: Optional[str] = None
    record_name: Optional[str] = None

    @property
    def is_resolved(self) -> bool:
        return self.schema is not None or bool(self.fields)


@dataclass
class RecordNameExtractionResult:
    record_name: Optional[str]
    namespace: Optional[str] = None
    full_name: Optional[str] = None


class KafkaSchemaResolver:
    def __init__(
        self,
        source_config: KafkaSourceConfig,
        schema_registry_client: SchemaRegistryClient,
        known_subjects: List[str],
        max_workers: int = DEFAULT_MAX_WORKERS_MULTIPLIER
        * (os.cpu_count() or DEFAULT_CPU_COUNT_FALLBACK),
        report: Optional[KafkaSourceReport] = None,
    ):
        self.source_config = source_config
        self.schema_registry_client = schema_registry_client
        self.known_subjects = set(known_subjects)
        self.max_workers = max_workers
        self.report = report

        # Initialize schema inference as final fallback
        self.schema_inference = None
        if source_config.schema_resolution.enabled:
            self.schema_inference = KafkaSchemaInference(
                bootstrap_servers=source_config.connection.bootstrap,
                consumer_config=source_config.connection.consumer_config,
                fallback_config=source_config.schema_resolution,
                max_workers=max_workers,
                report=report,
            )

    def _note_registry_error(self, subject_name: str, error: Exception) -> None:
        # KeyError/ValueError just mean "subject not registered" (expected while probing).
        # OSError means the registry was unreachable — operators must see that, not lose it.
        if isinstance(error, OSError) and self.report is not None:
            self.report.schema_registry_connectivity_failures += 1
            self.report.report_warning(
                "schema-registry",
                f"Schema registry was unreachable while resolving subject "
                f"{subject_name}: {error}",
            )

    def resolve_schemas_batch(
        self, topics: List[str], is_key_schema: bool = False
    ) -> Dict[str, SchemaResolutionResult]:
        results = {}
        topics_needing_inference = []

        for topic in topics:
            # Try all registry-based strategies first
            result = self._resolve_from_registry(topic, is_key_schema)

            if result.is_resolved:
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
                    resolution_method=ResolutionMethod.SCHEMA_INFERENCE
                    if fields
                    else ResolutionMethod.NONE,
                )
        else:
            # No schema inference available or needed
            for topic in topics_needing_inference:
                results[topic] = SchemaResolutionResult(
                    schema=None,
                    fields=[],
                    resolution_method=ResolutionMethod.NONE,
                )

        return results

    def _resolve_from_registry(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
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
            resolution_method=ResolutionMethod.REGISTRY_FAILED,
        )

    def _try_topic_name_strategy(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
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
                        fields=[],
                        resolution_method=ResolutionMethod.TOPIC_NAME_STRATEGY,
                        subject_name=subject_name,
                    )
            except (KeyError, ValueError, OSError) as e:
                logger.debug(f"TopicNameStrategy failed for {subject_name}: {e}")
                self._note_registry_error(subject_name, e)

        return SchemaResolutionResult(
            schema=None,
            fields=[],
            resolution_method=ResolutionMethod.TOPIC_NAME_FAILED,
        )

    def _try_topic_subject_map(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
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
                        resolution_method=ResolutionMethod.TOPIC_SUBJECT_MAP,
                        subject_name=subject_name,
                    )
            except (KeyError, ValueError, OSError) as e:
                logger.debug(f"TopicSubjectMap failed for {subject_name}: {e}")
                self._note_registry_error(subject_name, e)

        return SchemaResolutionResult(
            schema=None,
            fields=[],
            resolution_method=ResolutionMethod.SUBJECT_MAP_FAILED,
        )

    def _try_subject_name_with_record_names(
        self,
        record_names: Set[str],
        subject_format: str,
        resolution_method: ResolutionMethod,
        strategy_name: str,
    ) -> Optional[SchemaResolutionResult]:
        # Only hits the registry for subjects already known to exist; returns on first match.
        for record_name in record_names:
            subject_name = subject_format.format(record_name=record_name)
            if subject_name in self.known_subjects:
                try:
                    registered_schema = self.schema_registry_client.get_latest_version(
                        subject_name
                    )
                    if registered_schema and registered_schema.schema:
                        logger.debug(
                            f"{strategy_name} succeeded for subject {subject_name} (record: {record_name})"
                        )
                        return SchemaResolutionResult(
                            schema=registered_schema.schema,
                            fields=[],
                            resolution_method=resolution_method,
                            subject_name=subject_name,
                            record_name=record_name,
                        )
                except (KeyError, ValueError, OSError) as e:
                    logger.debug(f"{strategy_name} failed for {subject_name}: {e}")
                    self._note_registry_error(subject_name, e)
        return None

    def _try_record_name_strategies(
        self, topic: str, is_key_schema: bool
    ) -> SchemaResolutionResult:
        if not self.schema_inference:
            return SchemaResolutionResult(
                schema=None,
                fields=[],
                resolution_method=ResolutionMethod.NO_INFERENCE_AVAILABLE,
            )

        try:
            # Sample a few messages to extract potential record names
            record_names = self._extract_record_names_from_topic(topic, is_key_schema)

            if not record_names:
                return SchemaResolutionResult(
                    schema=None,
                    fields=[],
                    resolution_method=ResolutionMethod.NO_RECORD_NAMES_FOUND,
                )

            suffix = "-key" if is_key_schema else "-value"

            # Try RecordNameStrategy: <record_name>-key/value
            result = self._try_subject_name_with_record_names(
                record_names,
                subject_format=f"{{record_name}}{suffix}",
                resolution_method=ResolutionMethod.RECORD_NAME_STRATEGY,
                strategy_name=STRATEGY_NAME_RECORD_NAME,
            )
            if result:
                return result

            # Try TopicRecordNameStrategy: <topic>-<record_name>-key/value
            result = self._try_subject_name_with_record_names(
                record_names,
                subject_format=f"{topic}-{{record_name}}{suffix}",
                resolution_method=ResolutionMethod.TOPIC_RECORD_NAME_STRATEGY,
                strategy_name=STRATEGY_NAME_TOPIC_RECORD_NAME,
            )
            if result:
                return result

        except (ValueError, KeyError, AttributeError, TypeError) as e:
            # Catch expected schema parsing/extraction errors, let critical errors propagate
            logger.warning(f"Record name extraction failed for topic {topic}: {e}")

        return SchemaResolutionResult(
            schema=None,
            fields=[],
            resolution_method=ResolutionMethod.RECORD_NAME_STRATEGIES_FAILED,
        )

    def _extract_record_names_from_topic(
        self, topic: str, is_key_schema: bool
    ) -> Set[str]:
        record_names: Set[str] = set()

        try:
            # Use the schema inference infrastructure to sample messages
            if not self.schema_inference:
                return record_names
            sample_messages = self.schema_inference.sample_topic_messages(topic)

            # Use the configured fallback config to determine how many messages to sample
            fallback_config = self.source_config.schema_resolution

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

        except (ValueError, KeyError, OSError) as e:
            logger.debug(f"Failed to extract record names from topic {topic}: {e}")

        return record_names

    def _extract_record_name_from_message(
        self, message_value: MessageValue
    ) -> RecordNameExtractionResult:
        if (
            not isinstance(message_value, bytes)
            or len(message_value) < CONFLUENT_WIRE_HEADER_LENGTH
        ):
            return RecordNameExtractionResult(record_name=None)

        try:
            # Check for Confluent Schema Registry magic byte
            if message_value[0] == CONFLUENT_MAGIC_BYTE:
                # This is a schema registry serialized message
                # Bytes 1-4 contain the schema ID
                schema_id = int.from_bytes(
                    message_value[1:CONFLUENT_WIRE_HEADER_LENGTH], byteorder="big"
                )

                # Try to get the schema from the registry using the ID
                try:
                    schema = self.schema_registry_client.get_by_id(schema_id)
                    if schema and schema.schema_str:
                        return self._extract_record_name_from_schema(schema.schema_str)
                except (KeyError, ValueError, OSError) as e:
                    logger.debug(f"Failed to get schema for ID {schema_id}: {e}")
                    self._note_registry_error(f"schema-id-{schema_id}", e)

            logger.debug(
                "Message is not schema registry format and raw parsing is not implemented"
            )
            return RecordNameExtractionResult(record_name=None)

        except (ValueError, struct.error) as e:
            logger.debug(f"Failed to extract record name from message: {e}")
            return RecordNameExtractionResult(record_name=None)

    def _extract_record_name_from_schema(
        self, schema_str: str
    ) -> RecordNameExtractionResult:
        try:
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
            logger.debug("Schema is not valid JSON - skipping record name extraction")

        return RecordNameExtractionResult(record_name=None)

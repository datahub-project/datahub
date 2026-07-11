import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Union

from confluent_kafka import Consumer

from datahub.ingestion.source.kafka.kafka_config import SchemaResolutionFallback
from datahub.ingestion.source.kafka.kafka_constants import (
    DEFAULT_CPU_COUNT_FALLBACK,
    DEFAULT_MAX_WORKERS_MULTIPLIER,
    DEFAULT_SESSION_TIMEOUT_MS,
    OffsetResetStrategy,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_utils import (
    MessageValue,
    process_kafka_message_for_sampling,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


@dataclass
class FieldAnalysis:
    types: Set[str] = field(default_factory=set)
    sample_values: List[str] = field(default_factory=list)


FieldInfo = Dict[str, FieldAnalysis]


class KafkaSchemaInference:
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_config: Dict[str, Union[str, int, float, bool]],
        fallback_config: SchemaResolutionFallback,
        max_workers: int = DEFAULT_MAX_WORKERS_MULTIPLIER
        * (os.cpu_count() or DEFAULT_CPU_COUNT_FALLBACK),
        report: Optional[KafkaSourceReport] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.consumer_config = consumer_config
        self.fallback_config = fallback_config
        self.max_workers = max_workers
        self.report = report

    def _note_inference_failure(self, topic: str, exc: Exception) -> None:
        if self.report is not None:
            self.report.schema_inference_sampling_failures += 1
            self.report.report_warning(
                "schema-inference",
                f"Failed to infer schema for topic {topic}; it will be processed "
                f"without schema information: {exc}",
            )

    def infer_schemas_batch(self, topics: List[str]) -> Dict[str, List[SchemaField]]:
        if not topics:
            return {}

        results = {}

        # Process topics in parallel if max_workers > 1
        if self.max_workers > 1 and len(topics) > 1:
            logger.info(
                f"Processing {len(topics)} topics in parallel for schema inference"
            )
            parallel_results = self._infer_schemas_parallel(topics)
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
                    self._note_inference_failure(topic, e)
                    results[topic] = []

        return results

    def _infer_schemas_parallel(
        self, topics: List[str]
    ) -> Dict[str, List[SchemaField]]:
        results = {}

        cpu_count = os.cpu_count() or DEFAULT_CPU_COUNT_FALLBACK

        # Use the smaller of: configured max, number of topics, or 2x CPU cores (reasonable for I/O bound work)
        max_workers = min(
            self.max_workers,
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
                executor.submit(self._infer_schema_from_messages, topic): topic
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
                    self._note_inference_failure(topic, e)
                    results[topic] = []

        logger.info(f"Completed parallel schema inference for {len(topics)} topics")
        return results

    def sample_topic_messages(self, topic: str) -> List[MessageValue]:
        strategy = self.fallback_config.offset_reset_strategy

        if strategy == OffsetResetStrategy.HYBRID:
            # Try latest first for speed
            logger.debug(f"Trying 'latest' sampling for topic {topic}")
            messages = self._sample_messages_with_strategy(
                topic, OffsetResetStrategy.LATEST
            )

            if not messages:
                logger.debug(
                    f"No recent messages found, trying 'earliest' for topic {topic}"
                )
                messages = self._sample_messages_with_strategy(
                    topic, OffsetResetStrategy.EARLIEST
                )

            return messages
        elif strategy == OffsetResetStrategy.LATEST:
            return self._sample_messages_with_strategy(
                topic, OffsetResetStrategy.LATEST
            )
        else:  # earliest
            return self._sample_messages_with_strategy(
                topic, OffsetResetStrategy.EARLIEST
            )

    def _sample_messages_with_strategy(
        self, topic: str, offset_strategy: OffsetResetStrategy
    ) -> List[MessageValue]:
        start_time = time.time()

        # Create a consumer with optimized settings for fast sampling
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"datahub-schema-inference-{topic}-{offset_strategy}-{int(time.time())}",
            "auto.offset.reset": str(offset_strategy),
            "enable.auto.commit": False,
            "fetch.min.bytes": 1,  # Don't wait for large batches
            "fetch.wait.max.ms": 100,  # Short wait time
            "session.timeout.ms": DEFAULT_SESSION_TIMEOUT_MS,  # Shorter session timeout
            **self.consumer_config,
        }

        try:
            consumer = Consumer(consumer_config)
        except Exception as e:
            logger.debug(
                f"Failed to create consumer for topic {topic} with '{offset_strategy}' strategy: {e}"
            )
            if self.report is not None:
                self.report.schema_inference_sampling_failures += 1
                self.report.report_warning(
                    "schema-inference",
                    f"Could not create a consumer to sample topic {topic} for schema "
                    f"inference ({offset_strategy}): {e}",
                )
            return []

        try:
            consumer.subscribe([topic])

            messages: List[MessageValue] = []
            attempts = 0
            decode_failures = 0

            # For 'latest' strategy, use shorter timeout since we expect recent activity
            timeout_seconds = (
                self.fallback_config.sample_timeout_seconds
                * 0.3  # 30% of normal timeout
                if offset_strategy == OffsetResetStrategy.LATEST
                else self.fallback_config.sample_timeout_seconds
            )

            # For 'latest' strategy, reduce poll attempts since we're looking for recent messages
            max_attempts = (
                10  # Cap at 10 for latest
                if offset_strategy == OffsetResetStrategy.LATEST
                else 20  # Standard attempts for earliest
            )

            while (
                len(messages) < self.fallback_config.max_messages_per_topic
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

                    processed_message = process_kafka_message_for_sampling(value)
                    messages.append(processed_message)

                except Exception as e:
                    decode_failures += 1
                    logger.debug(f"Failed to process message for schema inference: {e}")
                    continue

            elapsed_time = time.time() - start_time
            logger.debug(
                f"Sampled {len(messages)} messages from topic {topic} using '{offset_strategy}' strategy "
                f"in {elapsed_time:.2f}s ({attempts} poll attempts)"
            )

            # Every polled message failed to decode: surface it so the topic doesn't
            # silently fall back to schemaless with nothing in the report.
            if not messages and decode_failures > 0 and self.report is not None:
                self.report.schema_inference_message_decode_failures += decode_failures
                self.report.report_warning(
                    "schema-inference",
                    f"All {decode_failures} sampled message(s) for topic {topic} failed "
                    f"to decode ({offset_strategy}); it will be treated as schemaless.",
                )

            return messages

        except Exception as e:
            logger.debug(
                f"Failed to sample messages from topic {topic} with '{offset_strategy}' strategy: {e}"
            )
            if self.report is not None:
                self.report.schema_inference_sampling_failures += 1
                self.report.report_warning(
                    "schema-inference",
                    f"Failed to sample topic {topic} for schema inference "
                    f"({offset_strategy}): {e}",
                )
            return []
        finally:
            # Always close so a flaky broker can't leak connections and
            # consumer-group slots across the ThreadPoolExecutor workers.
            consumer.close()

    def _infer_schema_from_messages(self, topic: str) -> List[SchemaField]:
        try:
            sample_messages = self.sample_topic_messages(topic)

            if not sample_messages:
                logger.debug(f"Skipping empty topic {topic} for schema inference")
                return []

            inferred_fields = self._extract_fields_from_samples(topic, sample_messages)

            if not inferred_fields and self.report is not None:
                # We had samples but every one failed to yield a field; without
                # this the topic would go schemaless with no report signal.
                self.report.schema_inference_no_fields += 1
                self.report.report_warning(
                    "schema-inference",
                    f"Sampled {len(sample_messages)} message(s) for topic {topic} "
                    f"but inferred no fields; treating it as schemaless.",
                )
            else:
                logger.info(
                    f"Inferred {len(inferred_fields)} schema fields from message data for topic {topic}"
                )

            return inferred_fields

        except Exception as e:
            self._note_inference_failure(topic, e)
            return []

    def _extract_fields_from_samples(
        self, topic: str, sample_messages: List[MessageValue]
    ) -> List[SchemaField]:
        # Collect all unique field paths and their types from samples
        field_info: FieldInfo = {}

        for message in sample_messages:
            if not isinstance(message, dict):
                continue

            try:
                # For schema inference, we need to preserve original types, so we'll flatten manually
                # instead of using the string-converting flatten_json function
                flattened = self._flatten_for_schema_inference(message)

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

    def _flatten_for_schema_inference(
        self,
        obj: Union[dict, list],
        parent_key: str = "",
        max_depth: int = 5,
        current_depth: int = 0,
    ) -> Dict[str, Union[str, int, float, bool, list, dict, None]]:
        # Like flatten_json but keeps original Python types for schema inference.
        result = {}

        if current_depth >= max_depth:
            return {parent_key or "truncated": obj}

        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{parent_key}.{key}" if parent_key else key

                if isinstance(value, (dict, list)) and current_depth < max_depth - 1:
                    # Recursively flatten nested structures
                    nested = self._flatten_for_schema_inference(
                        value, new_key, max_depth, current_depth + 1
                    )
                    result.update(nested)
                else:
                    # Keep the original type
                    result[new_key] = value

        elif isinstance(obj, list):
            # For arrays, we'll create a single field representing the array type
            if parent_key:
                result[parent_key] = obj
            else:
                result["item"] = obj
        else:
            # Primitive value
            result[parent_key or "value"] = obj

        return result

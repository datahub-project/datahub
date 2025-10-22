import base64
import concurrent.futures
import io
import json
import logging
import random
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

from datahub.ingestion.source.kafka.kafka_schema_inference import (
    KafkaSchemaInference,
)
from datahub.ingestion.source.kafka.kafka_utils import (
    decode_kafka_message_value,
)

if TYPE_CHECKING:
    from confluent_kafka.schema_registry.schema_registry_client import Schema

    from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

import avro.io
import avro.schema
import confluent_kafka
import confluent_kafka.admin
from confluent_kafka.admin import (
    AdminClient,
    ConfigEntry,
    ConfigResource,
    TopicMetadata,
)
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.configuration.kafka_consumer_config import CallableConsumerConfig
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.registry import import_path
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kafka.kafka_config import KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_profiler import (
    KafkaProfiler,
    clean_field_path,
    flatten_json,
)
from datahub.ingestion.source.kafka.kafka_schema_registry_base import (
    KafkaSchemaRegistryBase,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    KafkaSchemaClass,
    OwnershipSourceTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class KafkaTopicConfigKeys(StrEnum):
    MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas"
    RETENTION_SIZE_CONFIG = "retention.bytes"
    RETENTION_TIME_CONFIG = "retention.ms"
    CLEANUP_POLICY_CONFIG = "cleanup.policy"
    MAX_MESSAGE_SIZE_CONFIG = "max.message.bytes"
    UNCLEAN_LEADER_ELECTION_CONFIG = "unclean.leader.election.enable"


def get_kafka_consumer(
    connection: KafkaConsumerConnectionConfig,
) -> confluent_kafka.Consumer:
    consumer = confluent_kafka.Consumer(
        {
            "group.id": "datahub-kafka-ingestion",
            "bootstrap.servers": connection.bootstrap,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            **connection.consumer_config,
        }
    )

    if CallableConsumerConfig.is_callable_config(connection.consumer_config):
        # As per documentation, we need to explicitly call the poll method to make sure OAuth callback gets executed
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
        logger.debug("Initiating polling for kafka consumer")
        consumer.poll(timeout=30)
        logger.debug("Initiated polling for kafka consumer")

    return consumer


def get_kafka_admin_client(
    connection: KafkaConsumerConnectionConfig,
) -> AdminClient:
    client = AdminClient(
        {
            "group.id": "datahub-kafka-ingestion",
            "bootstrap.servers": connection.bootstrap,
            **connection.consumer_config,
        }
    )
    if CallableConsumerConfig.is_callable_config(connection.consumer_config):
        # As per documentation, we need to explicitly call the poll method to make sure OAuth callback gets executed
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
        logger.debug("Initiating polling for kafka admin client")
        client.poll(timeout=30)
        logger.debug("Initiated polling for kafka admin client")
    return client


@dataclass
class KafkaSourceReport(StaleEntityRemovalSourceReport):
    topics_scanned: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)


class KafkaConnectionTest:
    def __init__(self, config_dict: dict):
        self.config = KafkaSourceConfig.parse_obj_allow_extras(config_dict)
        self.report = KafkaSourceReport()
        self.consumer: confluent_kafka.Consumer = get_kafka_consumer(
            self.config.connection
        )

    def get_connection_test(self) -> TestConnectionReport:
        capability_report = {
            SourceCapability.SCHEMA_METADATA: self.schema_registry_connectivity(),
        }
        return TestConnectionReport(
            basic_connectivity=self.basic_connectivity(),
            capability_report={
                k: v for k, v in capability_report.items() if v is not None
            },
        )

    def basic_connectivity(self) -> CapabilityReport:
        try:
            self.consumer.list_topics(timeout=10)
            return CapabilityReport(capable=True)
        except Exception as e:
            return CapabilityReport(capable=False, failure_reason=str(e))

    def schema_registry_connectivity(self) -> CapabilityReport:
        try:
            SchemaRegistryClient(
                {
                    "url": self.config.connection.schema_registry_url,
                    **self.config.connection.schema_registry_config,
                }
            ).get_subjects()
            return CapabilityReport(capable=True)
        except Exception as e:
            return CapabilityReport(capable=False, failure_reason=str(e))


@platform_name("Kafka")
@config_class(KafkaSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Set dataset description to top level doc field for Avro schema",
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "For multiple Kafka clusters, use the platform_instance configuration",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Schemas associated with each topic are extracted from the schema registry. Avro and Protobuf (certified), JSON (incubating). Schema references are supported.",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration `profiling.enabled.`",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Not supported. If you use Kafka Connect, the kafka-connect source can generate lineage.",
    supported=False,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Not supported",
    supported=False,
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class KafkaSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Topics from the Kafka broker
    - Schemas associated with each topic from the schema registry (Avro, Protobuf and JSON schemas are supported)
    """

    platform: str = "kafka"

    @classmethod
    def create_schema_registry(
        cls, config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> KafkaSchemaRegistryBase:
        try:
            schema_registry_class: Type = import_path(config.schema_registry_class)
            return schema_registry_class.create(config, report)
        except Exception as e:
            logger.debug(e, exc_info=e)
            raise ImportError(config.schema_registry_class) from e

    def __init__(self, config: KafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: KafkaSourceConfig = config
        self.consumer: confluent_kafka.Consumer = get_kafka_consumer(
            self.source_config.connection
        )
        self.init_kafka_admin_client()
        self.report: KafkaSourceReport = KafkaSourceReport()
        self.schema_registry_client: KafkaSchemaRegistryBase = (
            KafkaSource.create_schema_registry(config, self.report)
        )

        if self.source_config.schemaless_fallback.enabled:
            self.schema_inference = KafkaSchemaInference(
                bootstrap_servers=self.source_config.connection.bootstrap,
                consumer_config=self.source_config.connection.consumer_config,
                fallback_config=self.source_config.schemaless_fallback,
                max_workers=self.source_config.profiling.max_workers,
            )
        if self.source_config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.source_config.domain],
                graph=self.ctx.graph,
            )

        self.meta_processor = OperationProcessor(
            self.source_config.meta_mapping,
            self.source_config.tag_prefix,
            OwnershipSourceTypeClass.SERVICE,
            self.source_config.strip_user_ids_from_email,
            match_nested_props=True,
        )

    def init_kafka_admin_client(self) -> None:
        try:
            # TODO: Do we require separate config than existing consumer_config ?
            self.admin_client = get_kafka_admin_client(self.source_config.connection)
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report.report_warning(
                "kafka-admin-client",
                f"Failed to create Kafka Admin Client due to error {e}.",
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return KafkaConnectionTest(config_dict).get_connection_test()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "KafkaSource":
        config: KafkaSourceConfig = KafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        topics = self.consumer.list_topics(
            timeout=self.source_config.connection.client_timeout_seconds
        ).topics
        extra_topic_details = self.fetch_extra_topic_details(topics.keys())

        # Filter allowed topics upfront
        allowed_topics = [
            (topic, topic_detail)
            for topic, topic_detail in topics.items()
            if self.source_config.topic_patterns.allowed(topic)
        ]

        # Report scanned and dropped topics
        for topic, _ in topics.items():
            self.report.report_topic_scanned(topic)
            if not self.source_config.topic_patterns.allowed(topic):
                self.report.report_dropped(topic)

        logger.info(f"Processing {len(allowed_topics)} allowed topics")

        # Batch schema retrieval (PARALLEL) - eliminates the main bottleneck
        topic_names = [topic for topic, _ in allowed_topics]
        logger.info(f"Retrieving schemas for {len(topic_names)} topics in batch")

        # Get both value and key schemas in parallel
        topic_value_schemas = self.schema_registry_client.get_schema_and_fields_batch(
            topic_names, is_key_schema=False
        )
        topic_key_schemas = self.schema_registry_client.get_schema_and_fields_batch(
            topic_names, is_key_schema=True
        )

        # Handle comprehensive schema resolution for topics without schemas
        # Support both new and old config names for backward compatibility
        schema_resolution_enabled = (
            self.source_config.schema_resolution.enabled
            or self.source_config.schemaless_fallback.enabled
        )
        if schema_resolution_enabled:
            topics_needing_resolution = [
                topic
                for topic in topic_names
                if topic_value_schemas.get(topic, (None, []))[0] is None
                and not topic_value_schemas.get(topic, (None, []))[1]
            ]

            if topics_needing_resolution:
                logger.info(
                    f"Processing {len(topics_needing_resolution)} topics with comprehensive schema resolution"
                )

                # Initialize schema resolver if not already done
                if not hasattr(self, "_schema_resolver"):
                    from datahub.ingestion.source.kafka.schema_resolution import (
                        KafkaSchemaResolver,
                    )

                    self._schema_resolver = KafkaSchemaResolver(
                        source_config=self.source_config,
                        schema_registry_client=self.schema_registry_client.get_schema_registry_client(),
                        known_subjects=self.schema_registry_client.get_subjects(),
                        max_workers=self.source_config.profiling.max_workers,
                    )

                # Resolve value schemas
                value_resolution_results = self._schema_resolver.resolve_schemas_batch(
                    topics_needing_resolution, is_key_schema=False
                )

                # Resolve key schemas
                key_resolution_results = self._schema_resolver.resolve_schemas_batch(
                    topics_needing_resolution, is_key_schema=True
                )

                # Update results with resolved schemas
                for topic in topics_needing_resolution:
                    value_result = value_resolution_results.get(topic)
                    key_result = key_resolution_results.get(topic)

                    # Update value schema
                    if value_result and (value_result.schema or value_result.fields):
                        topic_value_schemas[topic] = (
                            value_result.schema,
                            value_result.fields,
                        )
                        logger.info(
                            f"Resolved value schema for topic {topic} using {value_result.resolution_method}"
                        )
                    elif topic not in topic_value_schemas:
                        topic_value_schemas[topic] = (None, [])

                    # Update key schema
                    if key_result and (key_result.schema or key_result.fields):
                        topic_key_schemas[topic] = (
                            key_result.schema,
                            key_result.fields,
                        )
                        logger.info(
                            f"Resolved key schema for topic {topic} using {key_result.resolution_method}"
                        )
                    elif topic not in topic_key_schemas:
                        topic_key_schemas[topic] = (None, [])

        # Process topics sequentially (fast since schemas are pre-fetched) and collect profiling tasks
        profiling_tasks = []

        for topic, topic_detail in allowed_topics:
            try:
                value_schema, value_fields = topic_value_schemas.get(topic, (None, []))
                key_schema, key_fields = topic_key_schemas.get(topic, (None, []))

                # Extract metadata work units
                yield from self._extract_record_with_schemas(
                    topic,
                    False,
                    topic_detail,
                    extra_topic_details.get(topic),
                    value_schema,
                    value_fields,
                    key_schema,
                    key_fields,
                )

                # Collect profiling task if profiling is enabled and we have schema information
                if (
                    self.source_config.profiling.enabled
                    and self.source_config.is_profiling_enabled()
                ):
                    # Check if we have any schema information (from registry or inference)
                    has_schema_info = (
                        value_schema is not None
                        or key_schema is not None
                        or value_fields
                        or key_fields
                    )

                    if not has_schema_info:
                        logger.debug(
                            f"Skipping profiling for topic {topic} - no schema information available "
                            f"(not in schema registry and schemaless fallback failed/disabled)"
                        )
                        continue

                    # Build dataset URN
                    dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=topic,
                        platform_instance=self.source_config.platform_instance,
                        env=self.source_config.env,
                    )

                    # Build schema metadata (we know we have schema info at this point)
                    schema_metadata = (
                        self.schema_registry_client.build_schema_metadata_with_key(
                            topic,
                            make_data_platform_urn(self.platform),
                            value_schema,
                            value_fields,
                            key_schema,
                            key_fields,
                        )
                    )

                    # Collect samples for profiling (we have schema context for better profiling)
                    samples = self.get_sample_messages(topic)
                    if samples:
                        profiling_tasks.append(
                            (dataset_urn, topic, samples, schema_metadata)
                        )
                        logger.debug(
                            f"Added profiling task for topic {topic} with {len(samples)} samples"
                        )
                    else:
                        logger.debug(
                            f"No samples collected for topic {topic}, skipping profiling"
                        )

            except Exception as e:
                logger.warning(f"Failed to extract topic {topic}", exc_info=True)
                self.report.report_warning(
                    "topic", f"Exception while extracting topic {topic}: {e}"
                )

        # Process all profiling tasks in parallel
        if profiling_tasks:
            logger.info(
                f"Processing {len(profiling_tasks)} profiling tasks in parallel"
            )
            yield from self.generate_profiles_in_parallel(profiling_tasks)

        if self.source_config.ingest_schemas_as_entities:
            # Get all subjects from schema registry and ingest them as SCHEMA DatasetSubTypes
            for subject in self.schema_registry_client.get_subjects():
                try:
                    yield from self._extract_record(
                        subject, True, topic_detail=None, extra_topic_config=None
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to extract subject {subject}", exc_info=True
                    )
                    self.report.report_warning(
                        "subject", f"Exception while extracting topic {subject}: {e}"
                    )

    def get_sample_messages(self, topic: str) -> Optional[List[Dict[str, Any]]]:
        """Get sample messages from Kafka topic using configured strategy and optimizations."""

        logger.info(
            f"Collecting samples from topic {topic} using {self.source_config.profiling.sampling_strategy} strategy"
        )
        samples: List[Dict[str, Any]] = []

        # Pre-fetch and cache schema metadata to avoid repeated parsing
        schema_metadata = self.schema_registry_client.get_schema_metadata(
            topic, make_data_platform_urn(self.platform), False
        )

        try:
            # Get metadata for all partitions
            topic_metadata = self.consumer.list_topics(topic).topics[topic]
            partitions = [
                confluent_kafka.TopicPartition(topic, p)
                for p in topic_metadata.partitions
            ]

            if not partitions:
                self.report.report_warning(
                    "profiling", f"No partitions found for topic {topic}"
                )
                return samples

            # Calculate optimal sample distribution
            total_sample_size = self.source_config.profiling.sample_size
            partition_sample_size = max(
                total_sample_size // len(partitions),
                min(
                    50, total_sample_size
                ),  # Minimum 50 messages per partition or total if smaller
            )

            # Get watermark offsets for all partitions
            watermarks = {}
            for partition in partitions:
                low, high = self.consumer.get_watermark_offsets(partition)
                watermarks[partition.partition] = (low, high)

            # Different sampling approaches based on strategy
            strategy = self.source_config.profiling.sampling_strategy

            if strategy == "latest":
                self._get_latest_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                )
            elif strategy == "random":
                self._get_random_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                )
            elif strategy == "stratified":
                self._get_stratified_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                )
            elif strategy == "full":
                self._get_full_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                )
            else:
                logger.warning(
                    f"Unrecognized sampling strategy: {strategy}, using 'latest'"
                )
                self._get_latest_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                )

            logger.info(f"Collected {len(samples)} samples from topic {topic}")

            # Return samples directly (no caching needed)

        except Exception as e:
            self.report.report_warning(
                "profiling", f"Failed to collect samples from {topic}: {str(e)}"
            )
        finally:
            try:
                self.consumer.unassign()
            except Exception as e:
                self.report.report_warning(
                    "profiling", f"Failed to unassign consumer: {str(e)}"
                )

        return samples

    def _get_latest_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized version that uses batch processing and pre-cached schema."""
        # Set offsets to read from end of partitions
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:  # Empty partition
                continue

            # Start from calculated position at the end
            start_offset = max(low, high - partition_sample_size)
            partition.offset = start_offset

        self._read_messages_in_batches(partitions, samples, topic, schema_metadata)

    def _get_random_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized random sampling with batch processing."""
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:  # Empty partition
                continue

            range_size = high - low
            if range_size <= partition_sample_size:
                # If range is smaller than sample size, read everything
                partition.offset = low
            else:
                # Pick a random starting point
                random_start = low + random.randint(
                    0, range_size - partition_sample_size
                )
                partition.offset = random_start

        self._read_messages_in_batches(partitions, samples, topic, schema_metadata)

    def _get_stratified_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized stratified sampling using batch processing where possible."""
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:  # Empty partition
                continue

            range_size = high - low
            if range_size <= partition_sample_size:
                # If range is smaller than sample size, read everything in batch
                partition.offset = low
                self.consumer.assign([partition])
                self._read_messages_in_batches(
                    [partition], samples, topic, schema_metadata
                )
            else:
                # For stratified sampling, we still need to read individual messages at specific offsets
                # But we can optimize by reducing the number of seeks
                num_samples = min(partition_sample_size, range_size)
                stride = range_size / num_samples

                # Read multiple messages in smaller batches to reduce seeks
                for i in range(0, num_samples, 10):  # Process in batches of 10
                    batch_end = min(i + 10, num_samples)
                    start_offset = low + int(i * stride)

                    partition.offset = start_offset
                    self.consumer.assign([partition])

                    # Read a small batch from this position
                    for _ in range(batch_end - i):
                        msg = self.consumer.poll(timeout=1.0)
                        if msg and not msg.error():
                            self._process_message_to_sample(
                                msg, samples, topic, schema_metadata
                            )

    def _get_full_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized full sampling with batch processing."""
        # Start from beginning for all partitions
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:  # Empty partition
                continue

            partition.offset = low

        self._read_messages_in_batches(partitions, samples, topic, schema_metadata)

    def _read_messages_in_batches(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized batch processing with pre-cached schema metadata."""
        self.consumer.assign(partitions)

        # Read until we have enough samples or time out
        end_time = datetime.now() + timedelta(
            seconds=float(self.source_config.profiling.max_sample_time_seconds)
        )
        batch_size = self.source_config.profiling.batch_size
        total_needed = self.source_config.profiling.sample_size

        # Process messages in larger batches for better performance
        while len(samples) < total_needed and datetime.now() < end_time:
            # Use consume() method for better batch processing
            batch_count = min(batch_size, total_needed - len(samples))
            messages = self.consumer.consume(num_messages=batch_count, timeout=2.0)

            if not messages:
                break

            # Process the batch of messages
            for msg in messages:
                if msg.error():
                    self.report.report_warning(
                        "profiling",
                        f"Error while consuming from {topic}: {msg.error()}",
                    )
                    continue

                self._process_message_to_sample(msg, samples, topic, schema_metadata)

                # Early exit if we have enough samples
                if len(samples) >= total_needed:
                    break

    def _process_message_to_sample(
        self,
        msg: confluent_kafka.Message,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
        """Optimized message processing with pre-cached schema metadata."""
        try:
            key = msg.key() if callable(msg.key) else msg.key
            value = msg.value() if callable(msg.value) else msg.value

            # Use pre-cached schema metadata to avoid repeated lookups
            processed_key = self._process_message_part(
                key, "key", topic, schema_metadata, is_key=True
            )
            processed_value = self._process_message_part(
                value, "value", topic, schema_metadata, is_key=False
            )

            # Start with metadata
            sample = {
                "offset": msg.offset(),
                "timestamp": datetime.fromtimestamp(
                    msg.timestamp()[1] / 1000.0
                    if msg.timestamp()[1] > 1e10
                    else msg.timestamp()[1]
                ).isoformat(),
            }

            # Add key with proper field path
            if processed_key is not None:
                if isinstance(processed_key, dict):
                    # For complex keys, prefix fields with "key."
                    for k, v in processed_key.items():
                        sample[f"key.{k}"] = v
                else:
                    # For simple keys, use "key" field
                    sample["key"] = processed_key

            # Add value fields
            if processed_value is not None:
                if isinstance(processed_value, dict):
                    sample.update(processed_value)
                else:
                    sample["value"] = processed_value

            samples.append(sample)

        except Exception as e:
            self.report.report_warning(
                "profiling", f"Failed to process message: {str(e)}"
            )

    def _process_message_part(
        self,
        data: Any,
        prefix: str,
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
        is_key: bool = False,
    ) -> Optional[Any]:
        """Optimized message part processing with pre-cached schema metadata."""
        if data is None:
            return None

        if isinstance(data, bytes):
            try:
                # Use pre-cached schema metadata instead of fetching it again
                if schema_metadata and isinstance(
                    schema_metadata.platformSchema, KafkaSchemaClass
                ):
                    schema_str = (
                        schema_metadata.platformSchema.keySchema
                        if is_key
                        else schema_metadata.platformSchema.documentSchema
                    )

                    if schema_str:
                        try:
                            # Check if this is Avro data (has magic byte)
                            if len(data) > 5 and data[0] == 0:  # Magic byte check
                                schema = avro.schema.parse(schema_str)
                                decoder = avro.io.BinaryDecoder(io.BytesIO(data[5:]))
                                reader = avro.io.DatumReader(schema)
                                decoded_value = reader.read(decoder)

                                if isinstance(decoded_value, (dict, list)):
                                    # Flatten complex structures with recursion protection
                                    if isinstance(decoded_value, list):
                                        decoded_value = {"item": decoded_value}
                                    # Type cast for flatten_json compatibility
                                    flattened_data = cast(Dict[str, Any], decoded_value)
                                    return flatten_json(
                                        flattened_data,
                                        max_depth=self.source_config.profiling.nested_field_max_depth,
                                    )
                                return decoded_value
                        except Exception as e:
                            # Enhanced error handling for specific Avro issues
                            error_msg = str(e)
                            if "InvalidAvroBinaryEncoding" in error_msg:
                                logger.debug(
                                    f"Invalid Avro binary encoding for topic {topic}: {error_msg}"
                                )
                                # Skip this message but continue processing
                                return {
                                    "avro_decode_error": "Invalid binary encoding",
                                    "raw_data_length": len(data),
                                }
                            elif "SchemaResolutionException" in error_msg:
                                logger.debug(
                                    f"Avro schema resolution error for topic {topic}: {error_msg}"
                                )
                                # Skip this message but continue processing
                                return {
                                    "avro_schema_error": "Schema resolution failed",
                                    "raw_data_length": len(data),
                                }
                            else:
                                logger.warning(
                                    f"Avro decode error for topic {topic}: {error_msg}"
                                )
                                self.report.report_warning(
                                    "avro_decode_error",
                                    f"Failed to decode Avro message for topic {topic}: {error_msg}",
                                )
                                return {
                                    "avro_decode_error": "General decode error",
                                    "raw_data_length": len(data),
                                }

                    return decode_kafka_message_value(
                        data,
                        topic,
                        flatten_json_func=flatten_json,
                        max_depth=self.source_config.profiling.nested_field_max_depth,
                    )

            except Exception as e:
                logger.warning(f"Failed to process message part: {e}")
                return base64.b64encode(data).decode("utf-8")

        return data

    def _process_sample_data(
        self,
        samples: List[Dict[str, Any]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Dict[str, Any]:
        """Process sample data to extract field information from both key and value schemas."""
        all_keys: Set[str] = set()
        field_sample_map: Dict[str, List[str]] = {}

        # Initialize from schema if available
        if schema_metadata is not None and isinstance(
            schema_metadata.platformSchema, KafkaSchemaClass
        ):
            # Handle all schema fields (both key and value)
            for schema_field in schema_metadata.fields or []:
                field_path = schema_field.fieldPath
                if field_path not in field_sample_map:
                    field_sample_map[field_path] = []
                    all_keys.add(field_path)

        # Process samples
        for sample in samples:
            # Process each field in the sample
            for field_name, value in sample.items():
                if field_name not in ["offset", "timestamp"]:
                    # For sample data, we need to map the simplified field names back to full paths
                    matching_schema_field = None
                    if schema_metadata and schema_metadata.fields:
                        clean_field = clean_field_path(field_name, preserve_types=False)

                        # Find matching schema field by comparing the end of the path
                        for schema_field in schema_metadata.fields:
                            if (
                                clean_field_path(
                                    schema_field.fieldPath, preserve_types=False
                                )
                                == clean_field
                            ):
                                matching_schema_field = schema_field
                                break

                    # Use the full path from schema if found, otherwise use original field name
                    field_path = (
                        matching_schema_field.fieldPath
                        if matching_schema_field
                        else field_name
                    )

                    if field_path not in field_sample_map:
                        field_sample_map[field_path] = []
                        all_keys.add(field_path)
                    field_sample_map[field_path].append(str(value))

        return {"all_keys": all_keys, "field_sample_map": field_sample_map}

    def create_profiling_wu(
        self,
        entity_urn: str,
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Create samples work unit incorporating both schema fields and sample values."""
        # Only proceed if profiling is enabled (respects operation_config)
        if not self.source_config.is_profiling_enabled():
            if self.source_config.profiling.report_dropped_profiles:
                self.report.report_warning(
                    "Profiling not enabled for topic (respecting operation_config)",
                    topic,
                )
            return

        samples = self.get_sample_messages(topic)
        if not samples:
            if self.source_config.profiling.report_dropped_profiles:
                self.report.report_warning("No samples collected for topic", topic)
            return

        logger.info(f"Collected {len(samples)} samples for topic {topic}.")

        # Respect sample size limit if configured
        if self.source_config.profiling.limit:
            samples = samples[: self.source_config.profiling.limit]

        # Apply offset if configured
        if self.source_config.profiling.offset:
            samples = samples[self.source_config.profiling.offset :]

        # Use static method for potential parallelization
        dataset_profile = KafkaProfiler.profile_topic(
            topic, samples, schema_metadata, self.source_config.profiling
        )

        if dataset_profile:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=dataset_profile,
            ).as_workunit()

    def generate_profiles_in_parallel(
        self,
        profiling_tasks: List[
            Tuple[str, str, List[Dict[str, Any]], Optional[SchemaMetadataClass]]
        ],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate profiles for multiple topics in parallel using ThreadPoolExecutor.

        Args:
            profiling_tasks: List of tuples (entity_urn, topic_name, samples, schema_metadata)
        """
        if not profiling_tasks:
            return

        max_workers = min(
            self.source_config.profiling.max_workers or 1, len(profiling_tasks)
        )

        logger.info(
            f"Profiling {len(profiling_tasks)} topics with {max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all profiling tasks
            future_to_task: Dict[Future, Tuple[str, str]] = {}

            for entity_urn, topic_name, samples, schema_metadata in profiling_tasks:
                future = executor.submit(
                    KafkaProfiler.profile_topic,
                    topic_name,
                    samples,
                    schema_metadata,
                    self.source_config.profiling,
                )
                future_to_task[future] = (entity_urn, topic_name)

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_task):
                entity_urn, topic_name = future_to_task[future]
                try:
                    dataset_profile = future.result()
                    if dataset_profile:
                        yield MetadataChangeProposalWrapper(
                            entityUrn=entity_urn,
                            aspect=dataset_profile,
                        ).as_workunit()
                        logger.debug(f"Successfully profiled topic: {topic_name}")
                    else:
                        logger.warning(f"No profile generated for topic: {topic_name}")

                except Exception as e:
                    logger.error(f"Failed to profile topic {topic_name}: {e}")
                    if self.source_config.profiling.report_dropped_profiles:
                        self.report.report_warning(
                            "profiling",
                            f"Failed to profile topic {topic_name}: {str(e)}",
                        )

    def get_dataset_description(
        self,
        dataset_name: str,
        dataset_snapshot: DatasetSnapshotClass,
        custom_props: Dict[str, str],
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> DatasetSnapshotClass:
        AVRO = "AVRO"
        description: Optional[str] = None
        external_url: Optional[str] = None
        if (
            schema_metadata is not None
            and isinstance(schema_metadata.platformSchema, KafkaSchemaClass)
            and schema_metadata.platformSchema.documentSchemaType == AVRO
        ):
            # Point to note:
            # In Kafka documentSchema and keySchema both contains "doc" field.
            # DataHub Dataset "description" field is mapped to documentSchema's "doc" field.

            avro_schema = avro.schema.parse(
                schema_metadata.platformSchema.documentSchema
            )
            description = getattr(avro_schema, "doc", None)
            # set the tags
            all_tags: List[str] = []
            try:
                schema_tags = cast(
                    Iterable[str],
                    avro_schema.other_props.get(
                        self.source_config.schema_tags_field, []
                    ),
                )
                for tag in schema_tags:
                    all_tags.append(self.source_config.tag_prefix + tag)
            except TypeError:
                pass

            if self.source_config.enable_meta_mapping:
                meta_aspects = self.meta_processor.process(avro_schema.other_props)

                meta_owners_aspects = meta_aspects.get(Constants.ADD_OWNER_OPERATION)
                if meta_owners_aspects:
                    dataset_snapshot.aspects.append(meta_owners_aspects)

                meta_terms_aspect = meta_aspects.get(Constants.ADD_TERM_OPERATION)
                if meta_terms_aspect:
                    dataset_snapshot.aspects.append(meta_terms_aspect)

                # Create the tags aspect
                meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
                if meta_tags_aspect:
                    all_tags += [
                        tag_association.tag[len("urn:li:tag:") :]
                        for tag_association in meta_tags_aspect.tags
                    ]

            if all_tags:
                dataset_snapshot.aspects.append(
                    mce_builder.make_global_tag_aspect_with_tag_list(all_tags)
                )

        if self.source_config.external_url_base:
            # Remove trailing slash from base URL if present
            base_url = self.source_config.external_url_base.rstrip("/")
            external_url = f"{base_url}/{dataset_name}"

        dataset_properties = DatasetPropertiesClass(
            name=dataset_name,
            customProperties=custom_props,
            description=description,
            externalUrl=external_url,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        return dataset_snapshot

    def _extract_record_with_schemas(
        self,
        topic: str,
        is_subject: bool,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
        value_schema: Optional["Schema"],
        value_fields: List["SchemaField"],
        key_schema: Optional["Schema"],
        key_fields: List["SchemaField"],
    ) -> Iterable[MetadataWorkUnit]:
        """Extract record with pre-fetched schema data to avoid sequential schema lookups."""
        kafka_entity = "subject" if is_subject else "topic"

        logger.debug(f"extracting schema metadata from kafka entity = {kafka_entity}")

        platform_urn = make_data_platform_urn(self.platform)

        # Build schema metadata from pre-fetched data
        schema_metadata = None
        if (
            value_schema is not None
            or key_schema is not None
            or value_fields
            or key_fields
        ):
            schema_metadata = (
                self.schema_registry_client.build_schema_metadata_with_key(
                    topic,
                    platform_urn,
                    value_schema,
                    value_fields,
                    key_schema,
                    key_fields,
                )
            )

        # topic can have no associated subject, but still it can be ingested without schema
        # for schema ingestion, ingest only if it has valid schema
        if is_subject:
            if schema_metadata is None:
                return
            dataset_name = schema_metadata.schemaName
        else:
            dataset_name = topic

        # Create the default dataset snapshot for the topic
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],  # we append to this list later on
        )

        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        # Attach browsePaths aspect
        browse_path_str = f"/{self.source_config.env.lower()}/{self.platform}"
        if self.source_config.platform_instance:
            browse_path_str += f"/{self.source_config.platform_instance}"
        browse_path = BrowsePathsClass([browse_path_str])
        dataset_snapshot.aspects.append(browse_path)

        # build custom properties for topic, schema properties may be added as needed
        custom_props: Dict[str, str] = {}
        if not is_subject:
            custom_props = self.build_custom_properties(
                topic, topic_detail, extra_topic_config
            )
            schema_name: Optional[str] = (
                self.schema_registry_client._get_subject_for_topic(
                    topic, is_key_schema=False
                )
            )
            if schema_name is not None:
                custom_props["Schema Name"] = schema_name

        # Attach DatasetPropertiesClass
        dataset_snapshot = self.get_dataset_description(
            dataset_name, dataset_snapshot, custom_props, schema_metadata
        )

        # Attach dataPlatformInstance aspect if configured
        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )

        # Emit the dataset snapshot
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=f"kafka-{kafka_entity}", mce=mce)

        # Add the subtype aspect marking this as a "topic" or "schema"
        typeName = DatasetSubTypes.SCHEMA if is_subject else DatasetSubTypes.TOPIC
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[typeName]),
        ).as_workunit()

        domain_urn: Optional[str] = None

        # Emit domains aspect MCPW
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )

    def _extract_record(
        self,
        topic: str,
        is_subject: bool,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
    ) -> Iterable[MetadataWorkUnit]:
        kafka_entity = "subject" if is_subject else "topic"

        logger.debug(f"extracting schema metadata from kafka entity = {kafka_entity}")

        platform_urn = make_data_platform_urn(self.platform)

        schema_metadata = self.schema_registry_client.get_schema_metadata(
            topic, platform_urn, is_subject
        )

        # topic can have no associated subject, but still it can be ingested without schema
        # for schema ingestion, ingest only if it has valid schema
        if is_subject:
            if schema_metadata is None:
                return
            dataset_name = schema_metadata.schemaName
        else:
            dataset_name = topic

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],  # we append to this list later on
        )

        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        browse_path_str = f"/{self.source_config.env.lower()}/{self.platform}"
        if self.source_config.platform_instance:
            browse_path_str += f"/{self.source_config.platform_instance}"
        browse_path = BrowsePathsClass([browse_path_str])
        dataset_snapshot.aspects.append(browse_path)

        # build custom properties for topic, schema properties may be added as needed
        custom_props: Dict[str, str] = {}
        if not is_subject:
            custom_props = self.build_custom_properties(
                topic, topic_detail, extra_topic_config
            )
            schema_name: Optional[str] = (
                self.schema_registry_client._get_subject_for_topic(
                    topic, is_key_schema=False
                )
            )
            if schema_name is not None:
                custom_props["Schema Name"] = schema_name

        dataset_snapshot = self.get_dataset_description(
            dataset_name=dataset_name,
            dataset_snapshot=dataset_snapshot,
            custom_props=custom_props,
            schema_metadata=schema_metadata,
        )

        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=f"kafka-{kafka_entity}", mce=mce)

        typeName = DatasetSubTypes.SCHEMA if is_subject else DatasetSubTypes.TOPIC
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[typeName]),
        ).as_workunit()

        domain_urn: Optional[str] = None

        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )

    def build_custom_properties(
        self,
        topic: str,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
    ) -> Dict[str, str]:
        custom_props: Dict[str, str] = {}
        self.update_custom_props_with_topic_details(topic, topic_detail, custom_props)
        self.update_custom_props_with_topic_config(
            topic, extra_topic_config, custom_props
        )
        return custom_props

    def update_custom_props_with_topic_details(
        self,
        topic: str,
        topic_detail: Optional[TopicMetadata],
        custom_props: Dict[str, str],
    ) -> None:
        if topic_detail is None or topic_detail.partitions is None:
            logger.info(
                f"Partitions and Replication Factor not available for topic {topic}"
            )
            return

        custom_props["Partitions"] = str(len(topic_detail.partitions))
        replication_factor: Optional[int] = None
        for _, p_meta in topic_detail.partitions.items():
            if replication_factor is None or len(p_meta.replicas) > replication_factor:
                replication_factor = len(p_meta.replicas)

        if replication_factor is not None:
            custom_props["Replication Factor"] = str(replication_factor)

    def update_custom_props_with_topic_config(
        self,
        topic: str,
        topic_config: Optional[Dict[str, ConfigEntry]],
        custom_props: Dict[str, str],
    ) -> None:
        if topic_config is None:
            return

        for config_key in KafkaTopicConfigKeys:
            try:
                if config_key in topic_config and topic_config[config_key] is not None:
                    config_value = topic_config[config_key].value
                    custom_props[config_key] = (
                        config_value
                        if isinstance(config_value, str)
                        else json.dumps(config_value)
                    )
            except Exception as e:
                logger.info(f"{config_key} is not available for topic due to error {e}")

    def get_report(self) -> KafkaSourceReport:
        return self.report

    def close(self) -> None:
        if self.consumer:
            self.consumer.close()
        # Cleanup any resources when source is closed
        super().close()

    def _get_config_value_if_present(
        self, config_dict: Dict[str, ConfigEntry], key: str
    ) -> Any:
        return

    def fetch_extra_topic_details(self, topics: List[str]) -> Dict[str, dict]:
        extra_topic_details = {}

        if not hasattr(self, "admin_client"):
            logger.debug(
                "Kafka Admin Client missing. Not fetching config details for topics."
            )
        else:
            try:
                extra_topic_details = self.fetch_topic_configurations(topics)
            except Exception as e:
                logger.debug(e, exc_info=e)
                logger.warning(f"Failed to fetch config details due to error {e}.")
        return extra_topic_details

    def fetch_topic_configurations(self, topics: List[str]) -> Dict[str, dict]:
        logger.info("Fetching config details for all topics")
        configs: Dict[ConfigResource, concurrent.futures.Future] = (
            self.admin_client.describe_configs(
                resources=[
                    ConfigResource(ConfigResource.Type.TOPIC, t) for t in topics
                ],
                request_timeout=self.source_config.connection.client_timeout_seconds,
            )
        )
        logger.debug("Waiting for config details futures to complete")
        concurrent.futures.wait(configs.values())
        logger.debug("Config details futures completed")

        topic_configurations: Dict[str, dict] = {}
        for config_resource, config_result_future in configs.items():
            self.process_topic_config_result(
                config_resource, config_result_future, topic_configurations
            )
        return topic_configurations

    def process_topic_config_result(
        self,
        config_resource: ConfigResource,
        config_result_future: concurrent.futures.Future,
        topic_configurations: dict,
    ) -> None:
        try:
            topic_configurations[config_resource.name] = config_result_future.result()
        except Exception as e:
            logger.warning(
                f"Config details for topic {config_resource.name} not fetched due to error {e}"
            )
        else:
            logger.info(
                f"Config details for topic {config_resource.name} fetched successfully"
            )

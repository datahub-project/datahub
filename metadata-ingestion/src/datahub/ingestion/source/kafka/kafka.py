import concurrent.futures
import io
import json
import logging
import random
import threading
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from datahub.ingestion.source.kafka.kafka_constants import (
    CONFLUENT_MAGIC_BYTE,
    CONFLUENT_WIRE_HEADER_LENGTH,
    DEFAULT_CONSUMER_TIMEOUT_SECONDS,
    SCHEMA_TYPE_AVRO,
)
from datahub.ingestion.source.kafka.kafka_schema_inference import (
    KafkaSchemaInference,
)
from datahub.ingestion.source.kafka.kafka_utils import (
    decode_kafka_message_value,
)
from datahub.ingestion.source.kafka.schema_resolution import (
    KafkaSchemaResolver,
)

if TYPE_CHECKING:
    from confluent_kafka.schema_registry.schema_registry_client import Schema

    from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

import avro.errors
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
from datahub.configuration.kafka_consumer_config import KafkaOAuthCallbackResolver
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kafka.kafka_config import KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_profiler import (
    KafkaProfiler,
    flatten_json,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_schema_registry_base import (
    KafkaSchemaRegistryBase,
    SchemaAndFields,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DatasetProfileClass,
    KafkaSchemaClass,
    OwnershipSourceTypeClass,
    SchemaMetadataClass,
    StatusClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

# Suppress Avro logical type warnings - these are expected when schemas use custom logical types
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="avro.schema",
    message=".*Unknown .*, using .*",
)


def _needs_schema_resolution(schema_and_fields: Optional[SchemaAndFields]) -> bool:
    return schema_and_fields is None or (
        schema_and_fields.schema is None and not schema_and_fields.fields
    )


class KafkaConnectivityError(Exception):
    pass


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

    if KafkaOAuthCallbackResolver.is_callable_config(connection.consumer_config):
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
    if KafkaOAuthCallbackResolver.is_callable_config(connection.consumer_config):
        # As per documentation, we need to explicitly call the poll method to make sure OAuth callback gets executed
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
        logger.debug("Initiating polling for kafka admin client")
        client.poll(timeout=30)
        logger.debug("Initiated polling for kafka admin client")
    return client


def validate_kafka_connectivity(connection: KafkaConsumerConnectionConfig) -> None:
    logger.info(f"Validating connectivity to Kafka at {connection.bootstrap}")
    consumer = get_kafka_consumer(connection)
    try:
        # Get cluster metadata with a short timeout.
        # By passing an empty string as the topic parameter, we get cluster metadata
        # without enumerating all topics, which is much faster for large clusters.
        metadata = consumer.list_topics(
            topic="", timeout=max(10, connection.client_timeout_seconds)
        )

        # Verify we got valid metadata with at least one broker
        if not metadata or not metadata.brokers:
            raise KafkaConnectivityError("No brokers found in cluster metadata")

        broker_count = len(metadata.brokers)
        logger.info(
            f"Successfully connected to Kafka cluster with {broker_count} broker(s)"
        )
    except KafkaConnectivityError:
        # Re-raise our own exceptions
        raise
    except Exception as e:
        separator = "=" * 80
        error_msg = (
            f"\n{separator}\n"
            f"[FATAL] Failed to connect to Kafka\n"
            f"  Bootstrap servers: {connection.bootstrap}\n"
            f"  Error: {str(e)}\n"
            f"{separator}\n"
        )
        logger.error(error_msg)
        raise KafkaConnectivityError(error_msg) from e
    finally:
        # Always close the consumer, even on the error/timeout paths.
        consumer.close()


class KafkaConnectionTest:
    def __init__(self, config_dict: dict):
        self.config = KafkaSourceConfig.parse_obj_allow_extras(config_dict)
        self.report = KafkaSourceReport()
        self.consumer: confluent_kafka.Consumer = get_kafka_consumer(
            self.config.connection
        )

    def get_connection_test(self) -> TestConnectionReport:
        try:
            capability_report = {
                SourceCapability.SCHEMA_METADATA: self.schema_registry_connectivity(),
            }
            return TestConnectionReport(
                basic_connectivity=self.basic_connectivity(),
                capability_report={
                    k: v for k, v in capability_report.items() if v is not None
                },
            )
        finally:
            # The consumer is only needed for the connectivity check; always close it.
            self.consumer.close()

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
    "Optionally enabled via configuration `profiling.enabled`.",
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
        # Profiling runs across a thread pool; report counters and warnings are
        # read-modify-write, so serialize every mutation from a worker thread.
        self._report_lock = threading.Lock()
        self._schema_resolver: Optional[KafkaSchemaResolver] = None
        self.schema_registry_client: KafkaSchemaRegistryBase = (
            KafkaSource.create_schema_registry(config, self.report)
        )

        if self.source_config.schema_resolution.enabled:
            self.schema_inference = KafkaSchemaInference(
                bootstrap_servers=self.source_config.connection.bootstrap,
                consumer_config=self.source_config.connection.consumer_config,
                fallback_config=self.source_config.schema_resolution,
                max_workers=self.source_config.profiling.max_workers,
                report=self.report,
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
        config: KafkaSourceConfig = KafkaSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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

        # Fetch every topic's schema up front so per-topic processing below has no
        # further registry round-trips.
        topic_names = [topic for topic, _ in allowed_topics]
        logger.info(f"Retrieving schemas for {len(topic_names)} topics in batch")

        topic_value_schemas = self.schema_registry_client.get_schema_and_fields_batch(
            topic_names, is_key_schema=False
        )
        topic_key_schemas = self.schema_registry_client.get_schema_and_fields_batch(
            topic_names, is_key_schema=True
        )

        # Handle comprehensive schema resolution for topics without schemas
        if self.source_config.schema_resolution.enabled:
            self._resolve_missing_schemas(
                topic_names, topic_value_schemas, topic_key_schemas
            )

        # Process topics sequentially (schemas are pre-fetched) and collect profiling tasks
        collection_tasks: List[Tuple[str, str, Optional[SchemaMetadataClass]]] = []

        for topic, topic_detail in allowed_topics:
            try:
                value_sf = topic_value_schemas.get(topic) or SchemaAndFields()
                key_sf = topic_key_schemas.get(topic) or SchemaAndFields()
                value_schema, value_fields = value_sf.schema, value_sf.fields
                key_schema, key_fields = key_sf.schema, key_sf.fields

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

                    dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=topic,
                        platform_instance=self.source_config.platform_instance,
                        env=self.source_config.env,
                    )

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

                    collection_tasks.append((dataset_urn, topic, schema_metadata))
                    logger.debug(f"Queued profiling task for topic {topic}")

            except Exception as e:
                logger.warning(f"Failed to extract topic {topic}", exc_info=True)
                self.report.report_warning(
                    "topic", f"Exception while extracting topic {topic}: {e}"
                )

        # Collect samples and compute profiles in parallel — each worker owns its consumer
        if collection_tasks:
            yield from self.generate_profiles_in_parallel(collection_tasks)

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

    def get_sample_messages(
        self,
        topic: str,
        consumer: Optional[confluent_kafka.Consumer] = None,
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Get sample messages from a topic using the configured sampling strategy."""

        _consumer = consumer if consumer is not None else self.consumer

        logger.info(
            f"Collecting samples from topic {topic} using {self.source_config.profiling.sampling_strategy} strategy"
        )
        samples: List[Dict[str, Any]] = []

        # Use provided schema_metadata or fetch it
        if schema_metadata is None:
            schema_metadata = self.schema_registry_client.get_schema_metadata(
                topic, make_data_platform_urn(self.platform), False
            )

        try:
            # Get metadata for all partitions
            topic_metadata = _consumer.list_topics(
                topic, timeout=self.source_config.connection.client_timeout_seconds
            ).topics[topic]
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
                min(50, total_sample_size),
            )

            # Get watermark offsets for all partitions
            watermarks = {}
            for partition in partitions:
                low, high = _consumer.get_watermark_offsets(partition)
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
                    consumer=_consumer,
                )
            elif strategy == "random":
                self._get_random_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                    consumer=_consumer,
                )
            elif strategy == "stratified":
                self._get_stratified_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                    consumer=_consumer,
                )
            elif strategy == "full":
                self._get_full_samples(
                    partitions,
                    watermarks,
                    partition_sample_size,
                    samples,
                    topic,
                    schema_metadata,
                    consumer=_consumer,
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
                    consumer=_consumer,
                )

            logger.info(f"Collected {len(samples)} samples from topic {topic}")

        except (
            confluent_kafka.KafkaException,
            ValueError,
            TypeError,
            KeyError,
            json.JSONDecodeError,
        ) as e:
            self.report.report_warning(
                "profiling", f"Failed to collect samples from {topic}: {str(e)}"
            )
        finally:
            try:
                _consumer.unassign()
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
        consumer: Optional[confluent_kafka.Consumer] = None,
    ) -> None:
        total_available = 0
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:
                continue
            start_offset = max(low, high - partition_sample_size)
            partition.offset = start_offset
            total_available += high - start_offset

        self._read_messages_in_batches(
            partitions,
            samples,
            topic,
            schema_metadata,
            consumer=consumer,
            max_available=total_available,
        )

    def _get_random_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
        consumer: Optional[confluent_kafka.Consumer] = None,
    ) -> None:
        total_available = 0
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:
                continue
            range_size = high - low
            if range_size <= partition_sample_size:
                partition.offset = low
                total_available += range_size
            else:
                random_start = low + random.randint(
                    0, range_size - partition_sample_size
                )
                partition.offset = random_start
                total_available += partition_sample_size

        self._read_messages_in_batches(
            partitions,
            samples,
            topic,
            schema_metadata,
            consumer=consumer,
            max_available=total_available,
        )

    def _get_stratified_samples(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        watermarks: Dict[int, Tuple[int, int]],
        partition_sample_size: int,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
        consumer: Optional[confluent_kafka.Consumer] = None,
    ) -> None:
        _consumer = consumer or self.consumer
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:
                continue

            range_size = high - low
            if range_size <= partition_sample_size:
                partition.offset = low
                self._read_messages_in_batches(
                    [partition],
                    samples,
                    topic,
                    schema_metadata,
                    consumer=_consumer,
                    max_available=range_size,
                )
            else:
                num_samples = min(partition_sample_size, range_size)
                stride = range_size / num_samples

                # Single assign for the partition; seek to each stratum start to avoid
                # the cost of N/10 redundant assign() round-trips to the broker.
                first_tp = confluent_kafka.TopicPartition(
                    partition.topic, partition.partition, low
                )
                _consumer.assign([first_tp])

                for i in range(0, num_samples, 10):
                    batch_end = min(i + 10, num_samples)
                    start_offset = low + int(i * stride)
                    seek_tp = confluent_kafka.TopicPartition(
                        partition.topic, partition.partition, start_offset
                    )
                    _consumer.seek(seek_tp)
                    for _ in range(batch_end - i):
                        msg = _consumer.poll(timeout=1.0)
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
        consumer: Optional[confluent_kafka.Consumer] = None,
    ) -> None:
        total_available = 0
        for partition in partitions:
            low, high = watermarks[partition.partition]
            if high <= low:
                continue
            partition.offset = low
            total_available += high - low

        self._read_messages_in_batches(
            partitions,
            samples,
            topic,
            schema_metadata,
            consumer=consumer,
            max_available=total_available,
        )

    def _read_messages_in_batches(
        self,
        partitions: List[confluent_kafka.TopicPartition],
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
        consumer: Optional[confluent_kafka.Consumer] = None,
        max_available: Optional[int] = None,
    ) -> None:
        _consumer = consumer or self.consumer
        _consumer.assign(partitions)

        end_time = datetime.now() + timedelta(
            seconds=float(self.source_config.profiling.max_sample_time_seconds)
        )
        batch_size = self.source_config.profiling.batch_size
        # Clamp to the number of messages actually available so we don't block
        # on consume() waiting for messages that will never arrive.
        total_needed = self.source_config.profiling.sample_size
        if max_available is not None:
            total_needed = min(total_needed, max_available)
        samples_attempted = 0
        samples_failed = 0

        while len(samples) < total_needed and datetime.now() < end_time:
            batch_count = min(batch_size, total_needed - len(samples))
            messages = _consumer.consume(
                num_messages=batch_count, timeout=DEFAULT_CONSUMER_TIMEOUT_SECONDS
            )

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

                before_count = len(samples)
                samples_attempted += 1
                self._process_message_to_sample(msg, samples, topic, schema_metadata)
                if len(samples) == before_count:
                    samples_failed += 1

                # Early exit if we have enough samples
                if len(samples) >= total_needed:
                    break

        # Log sample processing stats
        if samples_failed > 0:
            failure_rate = (
                samples_failed / samples_attempted if samples_attempted > 0 else 0.0
            )
            logger.info(
                f"Sample processing: {len(samples)}/{samples_attempted} succeeded ({failure_rate:.1%} failed)"
            )

    def _process_message_to_sample(
        self,
        msg: confluent_kafka.Message,
        samples: List[Dict[str, Any]],
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> None:
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

        except (
            ValueError,
            TypeError,
            UnicodeDecodeError,
            KeyError,
            AttributeError,
        ) as e:
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
        """Decode one message part (key or value) using pre-fetched schema metadata."""
        if data is None:
            return None

        if isinstance(data, bytes):
            try:
                if schema_metadata and isinstance(
                    schema_metadata.platformSchema, KafkaSchemaClass
                ):
                    schema_type = (
                        schema_metadata.platformSchema.keySchemaType
                        if is_key
                        else schema_metadata.platformSchema.documentSchemaType
                    )

                    schema_str = (
                        schema_metadata.platformSchema.keySchema
                        if is_key
                        else schema_metadata.platformSchema.documentSchema
                    )

                    if schema_str and schema_type == SCHEMA_TYPE_AVRO:
                        try:
                            if (
                                len(data) > CONFLUENT_WIRE_HEADER_LENGTH
                                and data[0] == CONFLUENT_MAGIC_BYTE
                            ):
                                schema = avro.schema.parse(schema_str)
                                decoder = avro.io.BinaryDecoder(
                                    io.BytesIO(data[CONFLUENT_WIRE_HEADER_LENGTH:])
                                )
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
                        except (avro.errors.AvroException, EOFError, ValueError) as e:
                            # Skip undecodable messages; never emit a synthetic field,
                            # which would pollute the profile as a fake topic column.
                            with self._report_lock:
                                self.report.profiling_avro_decode_failures += 1
                                self.report.report_warning(
                                    "avro-decode",
                                    f"Skipped a message for topic {topic} that failed Avro "
                                    f"decoding ({type(e).__name__}): {e}",
                                )
                            return None

                    decoded = decode_kafka_message_value(
                        data,
                        topic,
                        flatten_json_func=flatten_json,
                        max_depth=self.source_config.profiling.nested_field_max_depth,
                    )
                    if decoded is None:
                        # Binary/undecodable payload; skip rather than pollute the
                        # profile with a synthetic field.
                        with self._report_lock:
                            self.report.profiling_samples_skipped += 1
                        return None
                    return decoded

            except Exception as e:
                # Skip the message rather than emitting a base64 blob, which would
                # pollute the profile as a synthetic field value.
                with self._report_lock:
                    self.report.profiling_samples_skipped += 1
                    self.report.report_warning(
                        "profiling",
                        f"Skipped a message for topic {topic} that failed to decode: {e}",
                    )
                return None

        return data

    def _collect_and_profile_topic(
        self,
        entity_urn: str,
        topic: str,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> Optional[Tuple[str, DatasetProfileClass]]:
        """Thread-safe: creates its own consumer, collects samples, runs profiling."""
        consumer = get_kafka_consumer(self.source_config.connection)
        try:
            samples = self.get_sample_messages(
                topic, consumer=consumer, schema_metadata=schema_metadata
            )
        finally:
            try:
                consumer.close()
            except Exception:
                pass

        if not samples:
            logger.debug(f"No samples collected for topic {topic}, skipping profiling")
            with self._report_lock:
                self.report.profiling_topics_dropped += 1
                if self.source_config.profiling.report_dropped_profiles:
                    self.report.report_warning(
                        "profiling",
                        f"No samples collected for topic {topic}; profile dropped.",
                    )
            return None

        profile = KafkaProfiler.profile_topic(
            topic,
            samples,
            schema_metadata,
            self.source_config.profiling,
            report=self.report,
            report_lock=self._report_lock,
        )
        with self._report_lock:
            if profile:
                self.report.profiling_topics_profiled += 1
            else:
                self.report.profiling_topics_dropped += 1
        return (entity_urn, profile) if profile else None

    def generate_profiles_in_parallel(
        self,
        collection_tasks: List[Tuple[str, str, Optional[SchemaMetadataClass]]],
    ) -> Iterable[MetadataWorkUnit]:
        if not collection_tasks:
            return

        max_workers = min(
            self.source_config.profiling.max_workers or 1, len(collection_tasks)
        )

        logger.info(
            f"Collecting and profiling {len(collection_tasks)} topics with {max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task: Dict[Future, Tuple[str, str]] = {}

            for entity_urn, topic_name, schema_metadata in collection_tasks:
                future = executor.submit(
                    self._collect_and_profile_topic,
                    entity_urn,
                    topic_name,
                    schema_metadata,
                )
                future_to_task[future] = (entity_urn, topic_name)

            for future in concurrent.futures.as_completed(future_to_task):
                entity_urn, topic_name = future_to_task[future]
                try:
                    result = future.result()
                    if result:
                        urn, dataset_profile = result
                        yield MetadataChangeProposalWrapper(
                            entityUrn=urn,
                            aspect=dataset_profile,
                        ).as_workunit()
                        logger.debug(f"Successfully profiled topic: {topic_name}")
                    else:
                        logger.warning(f"No profile generated for topic: {topic_name}")

                except Exception as e:
                    # A topic that throws is a dropped profile; always tally it so the
                    # final profiled/dropped counts stay correct.
                    self.report.profiling_topics_dropped += 1
                    self.report.report_warning(
                        "profiling",
                        f"Failed to profile topic {topic_name}: {str(e)}",
                    )

    def _resolve_missing_schemas(
        self,
        topic_names: List[str],
        topic_value_schemas: Dict[str, SchemaAndFields],
        topic_key_schemas: Dict[str, SchemaAndFields],
    ) -> None:
        # Fills topic_value_schemas / topic_key_schemas in place for topics the
        # registry returned nothing for, using the fallback resolver.
        topics_needing_resolution = [
            topic
            for topic in topic_names
            if _needs_schema_resolution(topic_value_schemas.get(topic))
        ]
        if not topics_needing_resolution:
            return

        logger.info(
            f"Processing {len(topics_needing_resolution)} topics with comprehensive schema resolution"
        )

        if self._schema_resolver is None:
            self._schema_resolver = KafkaSchemaResolver(
                source_config=self.source_config,
                schema_registry_client=self.schema_registry_client.get_schema_registry_client(),
                known_subjects=self.schema_registry_client.get_subjects(),
                max_workers=self.source_config.profiling.max_workers,
                report=self.report,
            )

        value_resolution_results = self._schema_resolver.resolve_schemas_batch(
            topics_needing_resolution, is_key_schema=False
        )
        key_resolution_results = self._schema_resolver.resolve_schemas_batch(
            topics_needing_resolution, is_key_schema=True
        )

        value_schemas_resolved = 0
        key_schemas_resolved = 0

        for topic in topics_needing_resolution:
            value_result = value_resolution_results.get(topic)
            key_result = key_resolution_results.get(topic)

            if value_result and (value_result.schema or value_result.fields):
                topic_value_schemas[topic] = SchemaAndFields(
                    schema=value_result.schema,
                    fields=value_result.fields,
                )
                logger.debug(
                    f"Resolved value schema for topic {topic} using {value_result.resolution_method}"
                )
                value_schemas_resolved += 1
            elif topic not in topic_value_schemas:
                topic_value_schemas[topic] = SchemaAndFields()

            if key_result and (key_result.schema or key_result.fields):
                topic_key_schemas[topic] = SchemaAndFields(
                    schema=key_result.schema,
                    fields=key_result.fields,
                )
                logger.debug(
                    f"Resolved key schema for topic {topic} using {key_result.resolution_method}"
                )
                key_schemas_resolved += 1
            elif topic not in topic_key_schemas:
                topic_key_schemas[topic] = SchemaAndFields()

        logger.info(
            f"Schema resolution complete: {value_schemas_resolved} value schemas and {key_schemas_resolved} key schemas resolved for {len(topics_needing_resolution)} topics"
        )

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
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        # Build schema metadata from pre-fetched data to avoid sequential lookups.
        platform_urn = make_data_platform_urn(self.platform)
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
        yield from self._emit_dataset(
            topic, is_subject, topic_detail, extra_topic_config, schema_metadata
        )

    def _extract_record(
        self,
        topic: str,
        is_subject: bool,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        platform_urn = make_data_platform_urn(self.platform)
        schema_metadata = self.schema_registry_client.get_schema_metadata(
            topic, platform_urn, is_subject
        )
        yield from self._emit_dataset(
            topic, is_subject, topic_detail, extra_topic_config, schema_metadata
        )

    def _emit_dataset(
        self,
        topic: str,
        is_subject: bool,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        kafka_entity = "subject" if is_subject else "topic"
        logger.debug(f"extracting schema metadata from kafka entity = {kafka_entity}")

        # topic can have no associated subject, but still it can be ingested without schema
        # for schema ingestion, ingest only if it has valid schema
        if is_subject:
            if schema_metadata is None:
                return
            dataset_name = schema_metadata.schemaName
        else:
            dataset_name = topic

        extra_aspects: List = [StatusClass(removed=False)]

        if schema_metadata is not None:
            extra_aspects.append(schema_metadata)

        # Source emits [env, platform]. The auto_browse_path_v2 processor prepends
        # the platform instance URN when configured, resulting in final path:
        # [platform_instance_urn, env, platform]
        browse_path_entries = [
            BrowsePathEntryClass(id=self.source_config.env.lower()),
            BrowsePathEntryClass(id=self.platform),
        ]
        extra_aspects.append(BrowsePathsV2Class(path=browse_path_entries))

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

        description: Optional[str] = None
        external_url: Optional[str] = None
        all_tags: List[str] = []

        # In Kafka both documentSchema and keySchema contain a "doc" field; the
        # dataset description maps to documentSchema's "doc".
        if (
            schema_metadata is not None
            and isinstance(schema_metadata.platformSchema, KafkaSchemaClass)
            and schema_metadata.platformSchema.documentSchemaType == SCHEMA_TYPE_AVRO
        ):
            avro_schema = avro.schema.parse(
                schema_metadata.platformSchema.documentSchema,
                validate_names=False,
            )
            description = getattr(avro_schema, "doc", None)

            try:
                schema_tags = cast(
                    Iterable[str],
                    avro_schema.other_props.get(
                        self.source_config.schema_tags_field, []
                    ),
                )
                for tag in schema_tags:
                    all_tags.append(self.source_config.tag_prefix + tag)
            except TypeError as e:
                self.report.warning(
                    message=f"Unable to extract tags from schema field '{self.source_config.schema_tags_field}': {e}. Expected an array of strings.",
                    context=dataset_name,
                    title="Unable to extract tags from schema field",
                    exc=e,
                )

            if self.source_config.enable_meta_mapping:
                meta_aspects = self.meta_processor.process(avro_schema.other_props)

                meta_owners_aspect = meta_aspects.get(Constants.ADD_OWNER_OPERATION)
                if meta_owners_aspect:
                    extra_aspects.append(meta_owners_aspect)

                meta_terms_aspect = meta_aspects.get(Constants.ADD_TERM_OPERATION)
                if meta_terms_aspect:
                    extra_aspects.append(meta_terms_aspect)

                meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
                if meta_tags_aspect:
                    all_tags += [
                        tag_association.tag[len("urn:li:tag:") :]
                        for tag_association in meta_tags_aspect.tags
                    ]

        if self.source_config.external_url_base:
            base_url = self.source_config.external_url_base.rstrip("/")
            external_url = f"{base_url}/{dataset_name}"

        domain_urn: Optional[str] = None
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        subtype = DatasetSubTypes.SCHEMA if is_subject else DatasetSubTypes.TOPIC
        tag_urns = [make_tag_urn(tag) for tag in all_tags] if all_tags else None
        yield Dataset(
            platform=self.platform,
            name=dataset_name,
            display_name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            subtype=subtype,
            description=description,
            external_url=external_url,
            custom_properties=custom_props if custom_props else None,
            tags=tag_urns,
            domain=domain_urn,
            extra_aspects=extra_aspects,
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
                self.report.report_warning(
                    "topic-config",
                    f"Failed to fetch topic configuration details (partitions, "
                    f"retention, cleanup policy, etc.) due to error {e}.",
                )
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

import concurrent.futures
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Type, Union, cast

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
    make_domain_urn,
    make_tag_urn,
)
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
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    KafkaSchemaClass,
    OwnershipSourceTypeClass,
    StatusClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class KafkaConnectivityError(Exception):
    """Exception raised when Kafka connectivity validation fails."""

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
    """
    Validate connectivity to Kafka by creating a consumer and checking broker availability.

    This uses a lightweight check that retrieves cluster metadata without listing all topics,
    making it suitable for clusters with thousands of topics.

    Args:
        connection: Kafka consumer connection configuration

    Raises:
        KafkaConnectivityError: If unable to connect to Kafka
    """
    logger.info(f"Validating connectivity to Kafka at {connection.bootstrap}")
    try:
        consumer = get_kafka_consumer(connection)
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
        consumer.close()
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
    "Not supported",
    supported=False,
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

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        topics = self.consumer.list_topics(
            timeout=self.source_config.connection.client_timeout_seconds
        ).topics
        extra_topic_details = self.fetch_extra_topic_details(topics.keys())

        for topic, topic_detail in topics.items():
            self.report.report_topic_scanned(topic)
            if self.source_config.topic_patterns.allowed(topic):
                try:
                    yield from self._extract_record(
                        topic, False, topic_detail, extra_topic_details.get(topic)
                    )
                except Exception as e:
                    logger.warning(f"Failed to extract topic {topic}", exc_info=True)
                    self.report.report_warning(
                        "topic", f"Exception while extracting topic {topic}: {e}"
                    )
            else:
                self.report.report_dropped(topic)

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

    def _extract_record(
        self,
        topic: str,
        is_subject: bool,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        AVRO = "AVRO"

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

        # Extract metadata from AVRO schema if available
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
                    context=topic,
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

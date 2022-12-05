import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Type

import confluent_kafka
import pydantic

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
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
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.registry import import_path
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_schema_registry_base import KafkaSchemaRegistryBase
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    SubTypesClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


class KafkaSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigBase):
    env: str = DEFAULT_ENV
    # TODO: inline the connection config
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    topic_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^_.*"])
    domain: Dict[str, AllowDenyPattern] = pydantic.Field(
        default={},
        description="A map of domain names to allow deny patterns. Domains can be urn-based (`urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810`) or bare (`13ae4d85-d955-49fc-8474-9004c663a810`).",
    )
    topic_subject_map: Dict[str, str] = pydantic.Field(
        default={},
        description="Provides the mapping for the `key` and the `value` schemas of a topic to the corresponding schema registry subject name. Each entry of this map has the form `<topic_name>-key`:`<schema_registry_subject_name_for_key_schema>` and `<topic_name>-value`:`<schema_registry_subject_name_for_value_schema>` for the key and the value schemas associated with the topic, respectively. This parameter is mandatory when the [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work) is used as the subject naming strategy in the kafka schema registry. NOTE: When provided, this overrides the default subject name resolution even when the `TopicNameStrategy` or the `TopicRecordNameStrategy` are used.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
    schema_registry_class: str = pydantic.Field(
        default="datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry",
        description="The fully qualified implementation class(custom) that implements the KafkaSchemaRegistryBase interface.",
    )
    ignore_warnings_on_schema_type: bool = pydantic.Field(
        default=False,
        description="Disables warnings reported for non-AVRO/Protobuf value or key schemas if set.",
    )

    @pydantic.root_validator
    def validate_platform_instance(cls: "KafkaSourceConfig", values: Dict) -> Dict:
        stateful_ingestion = values.get("stateful_ingestion")
        if (
            stateful_ingestion
            and stateful_ingestion.enabled
            and not values.get("platform_instance")
        ):
            raise ConfigurationError(
                "Enabling kafka stateful ingestion requires to specify a platform instance."
            )
        return values


@dataclass
class KafkaSourceReport(StaleEntityRemovalSourceReport):
    topics_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)


@platform_name("Kafka")
@config_class(KafkaSourceConfig)
@support_status(SupportStatus.CERTIFIED)
class KafkaSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - Topics from the Kafka broker
    - Schemas associated with each topic from the schema registry (only Avro schemas are currently supported)
    """

    platform: str = "kafka"

    @classmethod
    def create_schema_registry(
        cls, config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> KafkaSchemaRegistryBase:
        try:
            schema_registry_class: Type = import_path(config.schema_registry_class)
            return schema_registry_class.create(config, report)
        except (ImportError, AttributeError):
            raise ImportError(config.schema_registry_class)

    def __init__(self, config: KafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: KafkaSourceConfig = config
        self.consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(
            {
                "group.id": "test",
                "bootstrap.servers": self.source_config.connection.bootstrap,
                **self.source_config.connection.consumer_config,
            }
        )
        self.report: KafkaSourceReport = KafkaSourceReport()
        self.schema_registry_client: KafkaSchemaRegistryBase = (
            KafkaSource.create_schema_registry(config, self.report)
        )
        if self.source_config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.source_config.domain],
                graph=self.ctx.graph,
            )
        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=KafkaCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def get_platform_instance_id(self) -> str:
        assert self.source_config.platform_instance is not None
        return self.source_config.platform_instance

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "KafkaSource":
        config: KafkaSourceConfig = KafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        topics = self.consumer.list_topics().topics
        for t in topics:
            self.report.report_topic_scanned(t)
            if self.source_config.topic_patterns.allowed(t):
                yield from self._extract_record(t)
                # add topic to checkpoint
                topic_urn = make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=t,
                    platform_instance=self.source_config.platform_instance,
                    env=self.source_config.env,
                )
                self.stale_entity_removal_handler.add_entity_to_state(
                    type="topic", urn=topic_urn
                )
            else:
                self.report.report_dropped(t)
        # Clean up stale entities.
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def _extract_record(self, topic: str) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"topic = {topic}")

        # 1. Create the default dataset snapshot for the topic.
        dataset_name = topic
        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],  # we append to this list later on
        )

        # 2. Attach schemaMetadata aspect (pass control to SchemaRegistry)
        schema_metadata = self.schema_registry_client.get_schema_metadata(
            topic, platform_urn
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        # 3. Attach browsePaths aspect
        browse_path_suffix = (
            f"{self.source_config.platform_instance}/{topic}"
            if self.source_config.platform_instance
            else topic
        )
        browse_path = BrowsePathsClass(
            [f"/{self.source_config.env.lower()}/{self.platform}/{browse_path_suffix}"]
        )
        dataset_snapshot.aspects.append(browse_path)

        # 4. Attach dataPlatformInstance aspect.
        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )

        # 5. Emit the datasetSnapshot MCE
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=f"kafka-{topic}", mce=mce)
        self.report.report_workunit(wu)
        yield wu

        # 5. Add the subtype aspect marking this as a "topic"
        subtype_wu = MetadataWorkUnit(
            id=f"{topic}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["topic"]),
            ),
        )
        self.report.report_workunit(subtype_wu)
        yield subtype_wu

        domain_urn: Optional[str] = None

        # 6. Emit domains aspect MCPW
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type="dataset",
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> KafkaSourceReport:
        return self.report

    def close(self) -> None:
        if self.consumer:
            self.consumer.close()
        super().close()

import logging
from typing import Iterable, List, Optional, Type

import jpype
import jpype.imports
import requests

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_connect.common import (
    CONNECTOR_CLASS,
    SINK,
    SOURCE,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_platform_instance,
    transform_connector_config,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BIGQUERY_SINK_CONNECTOR_CLASS,
    S3_SINK_CONNECTOR_CLASS,
    SNOWFLAKE_SINK_CONNECTOR_CLASS,
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DEBEZIUM_SOURCE_CONNECTOR_PREFIX,
    JDBC_SOURCE_CONNECTOR_CLASS,
    MONGO_SOURCE_CONNECTOR_CLASS,
    ConfigDrivenSourceConnector,
    ConfluentJDBCSourceConnector,
    DebeziumSourceConnector,
    MongoSourceConnector,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@platform_name("Kafka Connect")
@config_class(KafkaConnectSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class KafkaConnectSource(StatefulIngestionSourceBase):
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport
    platform: str = "kafka-connect"

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Test the connection
        if self.config.username is not None and self.config.password is not None:
            logger.info(
                f"Connecting to {self.config.connect_uri} with Authentication..."
            )
            self.session.auth = (self.config.username, self.config.password)

        test_response = self.session.get(f"{self.config.connect_uri}/connectors")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")
        if not jpype.isJVMStarted():
            jpype.startJVM()

    def get_connectors_manifest(self) -> Iterable[ConnectorManifest]:
        """Get Kafka Connect connectors manifest using REST API.
        Enrich with lineages metadata.
        """

        connector_response = self.session.get(
            f"{self.config.connect_uri}/connectors",
        )

        payload = connector_response.json()

        for connector_name in payload:
            connector_url = f"{self.config.connect_uri}/connectors/{connector_name}"
            connector_manifest = self._get_connector_manifest(
                connector_name, connector_url
            )
            if connector_manifest is None or not self.config.connector_patterns.allowed(
                connector_manifest.name
            ):
                self.report.report_dropped(connector_name)
                continue

            if self.config.provided_configs:
                transform_connector_config(
                    connector_manifest.config, self.config.provided_configs
                )
            connector_manifest.url = connector_url
            connector_manifest.topic_names = self._get_connector_topics(connector_name)
            connector_class_value = connector_manifest.config.get(CONNECTOR_CLASS) or ""

            class_type: Type[BaseConnector] = BaseConnector

            # Populate Source Connector metadata
            if connector_manifest.type == SOURCE:
                connector_manifest.tasks = self._get_connector_tasks(connector_name)

                # JDBC source connector lineages
                if connector_class_value == JDBC_SOURCE_CONNECTOR_CLASS:
                    class_type = ConfluentJDBCSourceConnector
                elif connector_class_value.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX):
                    class_type = DebeziumSourceConnector
                elif connector_class_value == MONGO_SOURCE_CONNECTOR_CLASS:
                    class_type = MongoSourceConnector
                elif any(
                    [
                        connector.connector_name == connector_manifest.name
                        for connector in self.config.generic_connectors
                    ]
                ):
                    class_type = ConfigDrivenSourceConnector
                else:
                    self.report.report_dropped(connector_manifest.name)
                    self.report.warning(
                        "Lineage for Source Connector not supported. "
                        "Please refer to Kafka Connect docs to use `generic_connectors` config.",
                        context=f"{connector_manifest.name} of type {connector_class_value}",
                    )
                    continue
            elif connector_manifest.type == SINK:
                if connector_class_value == BIGQUERY_SINK_CONNECTOR_CLASS:
                    class_type = BigQuerySinkConnector
                elif connector_class_value == S3_SINK_CONNECTOR_CLASS:
                    class_type = ConfluentS3SinkConnector
                elif connector_class_value == SNOWFLAKE_SINK_CONNECTOR_CLASS:
                    class_type = SnowflakeSinkConnector
                else:
                    self.report.report_dropped(connector_manifest.name)
                    self.report.warning(
                        "Lineage for Sink Connector not supported.",
                        context=f"{connector_manifest.name} of type {connector_class_value}",
                    )

            connector_class = class_type(connector_manifest, self.config, self.report)
            connector_manifest.lineages = connector_class.extract_lineages()
            connector_manifest.flow_property_bag = (
                connector_class.extract_flow_property_bag()
            )

            yield connector_manifest

    def _get_connector_manifest(
        self, connector_name: str, connector_url: str
    ) -> Optional[ConnectorManifest]:
        try:
            connector_response = self.session.get(connector_url)
            connector_response.raise_for_status()
        except Exception as e:
            self.report.warning(
                "Failed to get connector details", connector_name, exc=e
            )
            return None
        manifest = connector_response.json()
        connector_manifest = ConnectorManifest(**manifest)
        return connector_manifest

    def _get_connector_tasks(self, connector_name: str) -> dict:
        try:
            response = self.session.get(
                f"{self.config.connect_uri}/connectors/{connector_name}/tasks",
            )
            response.raise_for_status()
        except Exception as e:
            self.report.warning(
                "Error getting connector tasks", context=connector_name, exc=e
            )
            return {}

        return response.json()

    def _get_connector_topics(self, connector_name: str) -> List[str]:
        try:
            response = self.session.get(
                f"{self.config.connect_uri}/connectors/{connector_name}/topics",
            )
            response.raise_for_status()
        except Exception as e:
            self.report.warning(
                "Error getting connector topics", context=connector_name, exc=e
            )
            return []

        return response.json()[connector_name]["topics"]

    def construct_flow_workunit(self, connector: ConnectorManifest) -> MetadataWorkUnit:
        connector_name = connector.name
        connector_type = connector.type
        connector_class = connector.config.get(CONNECTOR_CLASS)
        flow_property_bag = connector.flow_property_bag
        # connector_url = connector.url  # NOTE: this will expose connector credential when used
        flow_urn = builder.make_data_flow_urn(
            self.platform,
            connector_name,
            self.config.env,
            self.config.platform_instance,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=models.DataFlowInfoClass(
                name=connector_name,
                description=f"{connector_type.capitalize()} connector using `{connector_class}` plugin.",
                customProperties=flow_property_bag,
                # externalUrl=connector_url, # NOTE: this will expose connector credential when used
            ),
        ).as_workunit()

    def construct_job_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        flow_urn = builder.make_data_flow_urn(
            self.platform,
            connector_name,
            self.config.env,
            self.config.platform_instance,
        )

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform
                job_property_bag = lineage.job_property_bag

                source_platform_instance = get_platform_instance(
                    self.config, connector_name, source_platform
                )
                target_platform_instance = get_platform_instance(
                    self.config, connector_name, target_platform
                )

                job_id = self.get_job_id(lineage, connector, self.config)
                job_urn = builder.make_data_job_urn_with_flow(flow_urn, job_id)

                inlets = (
                    [
                        self.make_lineage_dataset_urn(
                            source_platform, source_dataset, source_platform_instance
                        )
                    ]
                    if source_dataset
                    else []
                )
                outlets = [
                    self.make_lineage_dataset_urn(
                        target_platform, target_dataset, target_platform_instance
                    )
                ]

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInfoClass(
                        name=f"{connector_name}:{job_id}",
                        type="COMMAND",
                        customProperties=job_property_bag,
                    ),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInputOutputClass(
                        inputDatasets=inlets,
                        outputDatasets=outlets,
                    ),
                ).as_workunit()

    def get_job_id(
        self,
        lineage: KafkaConnectLineage,
        connector: ConnectorManifest,
        config: KafkaConnectSourceConfig,
    ) -> str:
        connector_class = connector.config.get(CONNECTOR_CLASS)

        # Note - This block is only to maintain backward compatibility of Job URN
        if (
            connector_class
            and connector.type == SOURCE
            and (
                "JdbcSourceConnector" in connector_class
                or connector_class.startswith("io.debezium.connector")
            )
            and lineage.source_dataset
            and config.connect_to_platform_map
            and config.connect_to_platform_map.get(connector.name)
            and config.connect_to_platform_map[connector.name].get(
                lineage.source_platform
            )
        ):
            return f"{config.connect_to_platform_map[connector.name][lineage.source_platform]}.{lineage.source_dataset}"

        return (
            lineage.source_dataset
            if lineage.source_dataset
            else f"unknown_source.{lineage.target_dataset}"
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for connector in self.get_connectors_manifest():
            yield self.construct_flow_workunit(connector)
            yield from self.construct_job_workunits(connector)
            self.report.report_connector_scanned(connector.name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report

    def make_lineage_dataset_urn(
        self, platform: str, name: str, platform_instance: Optional[str]
    ) -> str:
        if self.config.convert_lineage_urns_to_lowercase:
            name = name.lower()

        return builder.make_dataset_urn_with_platform_instance(
            platform, name, platform_instance, self.config.env
        )

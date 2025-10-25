import logging
from typing import Dict, Iterable, List, Optional, Type

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
    BIGQUERY_SINK_CLOUD,
    BIGQUERY_SINK_CONNECTOR_CLASS,
    CLOUD_JDBC_SOURCE_CLASSES,
    CLOUD_OTHER_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    DEBEZIUM_SOURCE_CONNECTOR_PREFIX,
    JDBC_SOURCE_CONNECTOR_CLASS,
    MONGO_SOURCE_CONNECTOR_CLASS,
    MYSQL_SINK_CLOUD,
    MYSQL_SINK_CONNECTOR_CLASS,
    POSTGRES_SINK_CLOUD,
    S3_SINK_CLOUD,
    S3_SINK_CONNECTOR_CLASS,
    SINK,
    SNOWFLAKE_SINK_CLOUD,
    SNOWFLAKE_SINK_CONNECTOR_CLASS,
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
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
    MySqlSinkConnector,
    PostgresSinkConnector,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
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


# Connector class to implementation mapping for clean, extensible detection
SOURCE_CONNECTOR_MAPPING = {
    # OSS Platform JDBC source
    JDBC_SOURCE_CONNECTOR_CLASS: ConfluentJDBCSourceConnector,
    # Confluent Cloud JDBC sources
    **{cls: ConfluentJDBCSourceConnector for cls in CLOUD_JDBC_SOURCE_CLASSES},
    # MongoDB source
    MONGO_SOURCE_CONNECTOR_CLASS: MongoSourceConnector,
    # Cloud other sources (Datagen, etc.)
    **{cls: ConfigDrivenSourceConnector for cls in CLOUD_OTHER_SOURCE_CLASSES},
}

SINK_CONNECTOR_MAPPING = {
    # BigQuery sinks
    BIGQUERY_SINK_CONNECTOR_CLASS: BigQuerySinkConnector,
    BIGQUERY_SINK_CLOUD: BigQuerySinkConnector,
    # S3 sinks
    S3_SINK_CONNECTOR_CLASS: ConfluentS3SinkConnector,
    S3_SINK_CLOUD: ConfluentS3SinkConnector,
    # Snowflake sinks
    SNOWFLAKE_SINK_CONNECTOR_CLASS: SnowflakeSinkConnector,
    SNOWFLAKE_SINK_CLOUD: SnowflakeSinkConnector,
    # PostgreSQL sinks
    POSTGRES_SINK_CLOUD: PostgresSinkConnector,
    # MySQL sinks
    MYSQL_SINK_CONNECTOR_CLASS: MySqlSinkConnector,
    MYSQL_SINK_CLOUD: MySqlSinkConnector,
}


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
            connector_manifest.topic_names = self._get_connector_topics(
                connector_name=connector_name,
                config=connector_manifest.config,
                connector_type=connector_manifest.type,
            )
            connector_class_value = connector_manifest.config.get(CONNECTOR_CLASS) or ""

            class_type: Type[BaseConnector] = BaseConnector

            # Determine connector implementation class using clean mapping approach
            if connector_manifest.type == SOURCE:
                connector_manifest.tasks = self._get_connector_tasks(connector_name)
                source_class_type = self._get_source_connector_class(
                    connector_class_value, connector_manifest
                )

                if source_class_type is None:
                    self.report.report_dropped(connector_manifest.name)
                    self.report.warning(
                        "Lineage for Source Connector not supported. "
                        "Please refer to Kafka Connect docs to use `generic_connectors` config.",
                        context=f"{connector_manifest.name} of type {connector_class_value}",
                    )
                    continue

                class_type = source_class_type

            elif connector_manifest.type == SINK:
                sink_class_type = self._get_sink_connector_class(connector_class_value)

                if sink_class_type is None:
                    self.report.report_dropped(connector_manifest.name)
                    self.report.warning(
                        "Lineage for Sink Connector not supported.",
                        context=f"{connector_manifest.name} of type {connector_class_value}",
                    )
                    continue

                class_type = sink_class_type

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

    def _get_source_connector_class(
        self, connector_class_value: str, connector_manifest: ConnectorManifest
    ) -> Optional[Type[BaseConnector]]:
        """
        Determine the appropriate source connector implementation class.

        Uses mapping-based approach for clean, extensible connector detection.
        """
        # 1. Direct mapping lookup (most common cases)
        if connector_class_value in SOURCE_CONNECTOR_MAPPING:
            return SOURCE_CONNECTOR_MAPPING[connector_class_value]

        # 2. Debezium connector detection (prefix-based)
        if connector_class_value.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX):
            return DebeziumSourceConnector

        # 3. Generic connector configuration (user-defined)
        if any(
            connector.connector_name == connector_manifest.name
            for connector in self.config.generic_connectors
        ):
            return ConfigDrivenSourceConnector

        # 4. No supported connector found
        return None

    def _get_sink_connector_class(
        self, connector_class_value: str
    ) -> Optional[Type[BaseConnector]]:
        """
        Determine the appropriate sink connector implementation class.

        Uses mapping-based approach for clean, extensible connector detection.
        """
        return SINK_CONNECTOR_MAPPING.get(connector_class_value)

    def _get_connector_topics(
        self, connector_name: str, config: Dict[str, str], connector_type: str
    ) -> List[str]:
        try:
            response = self.session.get(
                f"{self.config.connect_uri}/connectors/{connector_name}/topics",
            )
            response.raise_for_status()
            processed_topics = response.json()[connector_name]["topics"]
        except Exception as e:
            self.report.warning(
                "Error getting connector topics via API, attempting to parse from config",
                context=connector_name,
                exc=e,
            )
            # Fallback to parsing topics from configuration (needed for Confluent Cloud)
            processed_topics = self._parse_topics_from_config(config, connector_type)

        if connector_type == SINK:
            try:
                return SinkTopicFilter().filter_stale_topics(processed_topics, config)
            except Exception as e:
                self.report.warning(
                    title="Error parsing sink conector topics configuration",
                    message="Some stale lineage tasks might show up for connector",
                    context=connector_name,
                    exc=e,
                )
                return processed_topics
        else:
            return processed_topics

    def _parse_topics_from_config(
        self, config: Dict[str, str], connector_type: str
    ) -> List[str]:
        """Parse topics from connector configuration when topics endpoint is not available.

        This is particularly useful for Confluent Cloud where the topics endpoint is not supported.
        """
        topics = []

        if connector_type == SINK:
            # For sink connectors, check topics or topics.regex
            topics_config = config.get("topics")
            if topics_config:
                topics = [
                    topic.strip() for topic in topics_config.split(",") if topic.strip()
                ]
            else:
                # If using topics.regex, we can't determine exact topics from config alone
                # This would require additional logic or external topic discovery
                topics_regex = config.get("topics.regex")
                if topics_regex:
                    self.report.warning(
                        "Connector uses topics.regex - cannot determine exact topics from config alone",
                        context=f"regex: {topics_regex}",
                    )
        else:
            # For source connectors, topics are typically generated based on table names
            # This is handled by the specific connector implementations
            pass

        return topics

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


class SinkTopicFilter:
    """Helper class to filter Kafka Connect topics based on configuration."""

    def filter_stale_topics(
        self,
        processed_topics: List[str],
        sink_config: Dict[str, str],
    ) -> List[str]:
        """
        Kafka-connect's /topics API returns the set of topic names the connector has been using
        since its creation or since the last time its set of active topics was reset. This means-
        if a topic was ever used by a connector, it will be returned, even if it is no longer used.
        To remove these stale topics from the list, we double-check the list returned by the API
        against the sink connector's config.
        Sink connectors configure exactly one of `topics` or `topics.regex`
        https://kafka.apache.org/documentation/#sinkconnectorconfigs_topics

        Args:
            processed_topics: List of topics currently being processed
            sink_config: Configuration dictionary for the sink connector

        Returns:
            List of filtered topics that match the configuration

        Raises:
            ValueError: If sink connector configuration is missing both 'topics' and 'topics.regex' fields

        """
        # Absence of topics config is a defensive NOOP,
        # although this should never happen in real world
        if not self.has_topic_config(sink_config):
            logger.warning(
                f"Found sink without topics config {sink_config.get(CONNECTOR_CLASS)}"
            )
            return processed_topics

        # Handle explicit topic list
        if sink_config.get("topics"):
            return self._filter_by_topic_list(processed_topics, sink_config["topics"])
        else:
            # Handle regex pattern
            return self._filter_by_topic_regex(
                processed_topics, sink_config["topics.regex"]
            )

    def has_topic_config(self, sink_config: Dict[str, str]) -> bool:
        """Check if sink config has either topics or topics.regex."""
        return bool(sink_config.get("topics") or sink_config.get("topics.regex"))

    def _filter_by_topic_list(
        self, processed_topics: List[str], topics_config: str
    ) -> List[str]:
        """Filter topics based on explicit topic list from config."""
        config_topics = [
            topic.strip() for topic in topics_config.split(",") if topic.strip()
        ]
        return [topic for topic in processed_topics if topic in config_topics]

    def _filter_by_topic_regex(
        self, processed_topics: List[str], regex_pattern: str
    ) -> List[str]:
        """Filter topics based on regex pattern from config."""
        from java.util.regex import Pattern

        regex_matcher = Pattern.compile(regex_pattern)

        return [
            topic
            for topic in processed_topics
            if regex_matcher.matcher(topic).matches()
        ]

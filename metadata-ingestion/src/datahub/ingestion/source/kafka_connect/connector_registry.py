"""
Connector registry to resolve connector types without circular imports.

This module provides a clean separation between common utilities and connector implementations.
"""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    MYSQL_SINK_CLOUD,
    POSTGRES_SINK_CLOUD,
    SINK,
    SNOWFLAKE_SINK_CLOUD,
    SOURCE,
    BaseConnector,
    ConnectorManifest,
    GenericConnectorConfig,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_platform_instance,
)

if TYPE_CHECKING:
    from datahub.ingestion.api.common import PipelineContext
    from datahub.sql_parsing.schema_resolver import SchemaResolver

logger = logging.getLogger(__name__)


class ConnectorRegistry:
    """
    Simple registry for connector types that avoids circular import issues.

    This registry handles the mapping between connector class names and their
    corresponding lineage extraction implementations.
    """

    @staticmethod
    def create_schema_resolver(
        ctx: Optional["PipelineContext"],
        config: KafkaConnectSourceConfig,
        connector_manifest: ConnectorManifest,
    ) -> Optional["SchemaResolver"]:
        """
        Create SchemaResolver for enhanced lineage extraction if enabled.

        Args:
            ctx: Pipeline context (contains graph connection)
            config: Kafka Connect source configuration
            connector_manifest: Connector manifest to determine platform

        Returns:
            SchemaResolver instance if feature is enabled and graph is available, None otherwise
        """
        if not config.use_schema_resolver or not ctx or not ctx.graph:
            return None

        try:
            from datahub.sql_parsing.schema_resolver import SchemaResolver

            # Determine platform from connector class
            connector_class = connector_manifest.config.get("connector.class", "")
            platform = ConnectorRegistry._infer_platform_from_connector(
                connector_class, connector_manifest
            )

            # Get platform instance if configured
            platform_instance = get_platform_instance(
                config, connector_manifest.name, platform
            )

            logger.info(
                f"Creating SchemaResolver for connector {connector_manifest.name} "
                f"with platform={platform}, instance={platform_instance}"
            )

            return SchemaResolver(
                platform=platform,
                platform_instance=platform_instance,
                env=config.env,
                graph=ctx.graph,
            )
        except Exception as e:
            logger.warning(
                f"Failed to create SchemaResolver for connector {connector_manifest.name}: {e}. "
                "Falling back to standard lineage extraction."
            )
            return None

    @staticmethod
    def _infer_platform_from_connector(
        connector_class: str, manifest: ConnectorManifest
    ) -> str:
        """Infer the source platform from connector class."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        # Try to get platform from Debezium source connector
        if (
            "debezium" in connector_class.lower()
            or connector_class in CLOUD_JDBC_SOURCE_CLASSES
        ):
            return DebeziumSourceConnector._get_platform_from_connector_class(
                connector_class
            )

        # Default platform inference
        connector_lower = connector_class.lower()
        if "postgres" in connector_lower:
            return "postgres"
        elif "mysql" in connector_lower:
            return "mysql"
        elif "sqlserver" in connector_lower or "mssql" in connector_lower:
            return "mssql"
        elif "oracle" in connector_lower:
            return "oracle"
        elif "mongodb" in connector_lower or "mongo" in connector_lower:
            return "mongodb"
        else:
            return "unknown"

    @staticmethod
    def get_connector_for_manifest(
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
        ctx: Optional["PipelineContext"] = None,
    ) -> Optional[BaseConnector]:
        """
        Get the appropriate connector instance for a manifest.

        Args:
            manifest: The connector manifest
            config: DataHub configuration
            report: Ingestion report
            ctx: Pipeline context (optional, for schema resolver)

        Returns:
            Connector instance or None if no handler found
        """
        connector_class_value = manifest.config.get("connector.class", "")

        # Create schema resolver if enabled
        schema_resolver = ConnectorRegistry.create_schema_resolver(
            ctx, config, manifest
        )

        # Determine connector type based on manifest type
        if manifest.type == SOURCE:
            connector = ConnectorRegistry._get_source_connector(
                connector_class_value, manifest, config, report
            )
        elif manifest.type == SINK:
            connector = ConnectorRegistry._get_sink_connector(
                connector_class_value, manifest, config, report
            )
        else:
            return None

        # Attach schema resolver to connector if created
        if connector and schema_resolver:
            connector.schema_resolver = schema_resolver

        return connector

    @staticmethod
    def _get_source_connector(
        connector_class_value: str,
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> Optional[BaseConnector]:
        """Get appropriate source connector implementation."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DEBEZIUM_SOURCE_CONNECTOR_PREFIX,
            JDBC_SOURCE_CONNECTOR_CLASS,
            MONGO_SOURCE_CONNECTOR_CLASS,
            ConfluentJDBCSourceConnector,
            DebeziumSourceConnector,
            MongoSourceConnector,
        )

        # Traditional JDBC source connector
        if connector_class_value == JDBC_SOURCE_CONNECTOR_CLASS:
            return ConfluentJDBCSourceConnector(manifest, config, report)
        # Cloud CDC connectors (use Debezium-style naming)
        elif (
            connector_class_value in CLOUD_JDBC_SOURCE_CLASSES
            or connector_class_value.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX)
        ):
            return DebeziumSourceConnector(manifest, config, report)
        elif connector_class_value == MONGO_SOURCE_CONNECTOR_CLASS:
            return MongoSourceConnector(manifest, config, report)

        # Handle generic connectors from config
        for generic_config in config.generic_connectors:
            if generic_config.connector_name == manifest.name:
                return _GenericConnector(manifest, config, report, generic_config)

        return None

    @staticmethod
    def _get_sink_connector(
        connector_class_value: str,
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> Optional[BaseConnector]:
        """Get appropriate sink connector implementation."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            BIGQUERY_SINK_CONNECTOR_CLASS,
            S3_SINK_CONNECTOR_CLASS,
            SNOWFLAKE_SINK_CONNECTOR_CLASS,
            BigQuerySinkConnector,
            ConfluentS3SinkConnector,
            JdbcSinkConnector,
            SnowflakeSinkConnector,
        )

        # BigQuery sink connectors
        if (
            connector_class_value == BIGQUERY_SINK_CONNECTOR_CLASS
            or connector_class_value
            == "io.confluent.connect.bigquery.BigQuerySinkConnector"
        ):
            return BigQuerySinkConnector(manifest, config, report)
        # S3 sink connectors
        elif connector_class_value == S3_SINK_CONNECTOR_CLASS:
            return ConfluentS3SinkConnector(manifest, config, report)
        # Snowflake sink connectors (both self-hosted and Cloud)
        elif connector_class_value in (
            SNOWFLAKE_SINK_CONNECTOR_CLASS,
            SNOWFLAKE_SINK_CLOUD,
        ):
            return SnowflakeSinkConnector(manifest, config, report)
        # Confluent Cloud JDBC sink connectors (Postgres, MySQL)
        elif connector_class_value == POSTGRES_SINK_CLOUD:
            return JdbcSinkConnector(manifest, config, report, platform="postgres")
        elif connector_class_value == MYSQL_SINK_CLOUD:
            return JdbcSinkConnector(manifest, config, report, platform="mysql")

        return None

    @staticmethod
    def extract_lineages(
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> List[KafkaConnectLineage]:
        """Extract lineages using the appropriate connector."""
        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )
        if connector:
            return connector.extract_lineages()
        return []

    @staticmethod
    def extract_flow_property_bag(
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> Optional[Dict[str, str]]:
        """Extract flow property bag using the appropriate connector."""
        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )
        if connector:
            return connector.extract_flow_property_bag()
        return None

    @staticmethod
    def get_topics_from_config(
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> List[str]:
        """Extract topics from config using the appropriate connector."""
        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )
        if connector:
            return connector.get_topics_from_config()
        return []


class _GenericConnector(BaseConnector):
    """Simple connector for handling generic configurations."""

    def __init__(
        self,
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
        generic_config: GenericConnectorConfig,
    ):
        super().__init__(manifest, config, report)
        self.generic_config = generic_config

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """Create basic lineage from generic configuration."""
        from datahub.ingestion.source.kafka_connect.common import KafkaConnectLineage

        return [
            KafkaConnectLineage(
                source_platform=self.generic_config.source_platform,
                source_dataset=self.generic_config.source_dataset,
                target_dataset="",  # Will be filled by kafka_connect.py
                target_platform="kafka",
            )
        ]

    def get_topics_from_config(self) -> List[str]:
        """Extract topics from manifest configuration."""
        config = self.connector_manifest.config

        # Try common topic configuration fields
        topics = config.get("topics", "")
        if topics:
            from datahub.ingestion.source.kafka_connect.common import (
                parse_comma_separated_list,
            )

            return parse_comma_separated_list(topics)

        # Single topic field
        topic = config.get("kafka.topic", "") or config.get("topic", "")
        if topic:
            return [topic.strip()]

        return []

    @staticmethod
    def supports_connector_class(connector_class: str) -> bool:
        """Generic connector supports any unknown class."""
        return True

    @staticmethod
    def get_platform(connector_class: str) -> str:
        """Generic connectors have configurable platforms."""
        return "unknown"

"""
Connector registry to resolve connector types without circular imports.

This module provides a clean separation between common utilities and connector implementations.
"""

import logging
from typing import TYPE_CHECKING, List, Optional

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    MYSQL_SINK_CLOUD,
    POSTGRES_SINK_CLOUD,
    SINK,
    SNOWFLAKE_SINK_CLOUD,
    SNOWFLAKE_SOURCE_CLOUD,
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
        connector: BaseConnector,
    ) -> Optional["SchemaResolver"]:
        """
        Create SchemaResolver for enhanced lineage extraction if enabled.

        Args:
            ctx: Pipeline context (contains graph connection)
            config: Kafka Connect source configuration
            connector: Connector instance to get platform from

        Returns:
            SchemaResolver instance if feature is enabled and graph is available, None otherwise
        """
        if not config.use_schema_resolver:
            return None

        if not ctx:
            logger.debug(
                f"SchemaResolver not available for connector {connector.connector_manifest.name}: "
                "PipelineContext is None"
            )
            return None

        if not ctx.graph:
            logger.warning(
                f"SchemaResolver not available for connector {connector.connector_manifest.name}: "
                "DataHub graph connection is not available. Make sure the ingestion is running with "
                "a valid DataHub connection (datahub_api or sink configuration)."
            )
            return None

        try:
            # Get platform from connector instance (single source of truth)
            platform = connector.get_platform()

            # Get platform instance if configured
            platform_instance = get_platform_instance(
                config, connector.connector_manifest.name, platform
            )

            logger.info(
                f"Initializing SchemaResolver for connector {connector.connector_manifest.name} "
                f"with platform={platform}, platform_instance={platform_instance}, env={config.env}"
            )

            # Use initialize_schema_resolver_from_datahub to create and populate the cache
            # This pre-fetches all schema metadata from DataHub for the given platform/env
            # Similar to BigQuery's approach for schema resolution
            return ctx.graph.initialize_schema_resolver_from_datahub(
                platform=platform,
                platform_instance=platform_instance,
                env=config.env,
            )
        except Exception as e:
            logger.warning(
                f"Failed to create SchemaResolver for connector {connector.connector_manifest.name}: {e}. "
                "Falling back to standard lineage extraction."
            )
            return None

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

        logger.info(
            f"Processing connector '{manifest.name}' - type={manifest.type}, class={connector_class_value}"
        )

        # Create connector instance first
        if manifest.type == SOURCE:
            connector = ConnectorRegistry._get_source_connector(
                connector_class_value, manifest, config, report
            )
        elif manifest.type == SINK:
            connector = ConnectorRegistry._get_sink_connector(
                connector_class_value, manifest, config, report
            )
        else:
            logger.warning(
                f"Unknown connector type '{manifest.type}' for connector '{manifest.name}'"
            )
            return None

        if connector:
            # Log which handler was selected
            handler_name = connector.__class__.__name__
            platform = connector.get_platform()
            logger.info(
                f"Connector '{manifest.name}' will be handled by {handler_name} (platform={platform})"
            )

            # Create and attach schema resolver using connector's platform
            schema_resolver = ConnectorRegistry.create_schema_resolver(
                ctx, config, connector
            )
            if schema_resolver:
                connector.schema_resolver = schema_resolver
        else:
            logger.debug(
                f"No handler found for connector '{manifest.name}' with class '{connector_class_value}'"
            )

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
            SnowflakeSourceConnector,
        )

        # Traditional JDBC source connector
        if connector_class_value == JDBC_SOURCE_CONNECTOR_CLASS:
            return ConfluentJDBCSourceConnector(manifest, config, report)
        # Snowflake Source connector (Confluent Cloud managed)
        elif connector_class_value == SNOWFLAKE_SOURCE_CLOUD:
            return SnowflakeSourceConnector(manifest, config, report)
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
    def get_topics_from_config(
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
        ctx: Optional["PipelineContext"] = None,
    ) -> List[str]:
        """Extract topics from config using the appropriate connector."""
        logger.debug(
            f"get_topics_from_config called for connector '{manifest.name}' "
            f"(type={manifest.type}, class={manifest.config.get('connector.class', 'unknown')})"
        )

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx
        )
        if connector:
            logger.debug(
                f"Calling get_topics_from_config on {connector.__class__.__name__} for '{manifest.name}'"
            )
            topics = connector.get_topics_from_config()
            logger.info(
                f"get_topics_from_config returned {len(topics)} topics for connector '{manifest.name}': "
                f"{topics[:10] if topics else '[]'}"
            )
            return topics
        else:
            logger.warning(
                f"No connector handler found for '{manifest.name}' - cannot derive topics from config"
            )
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

    def get_platform(self) -> str:
        """Generic connectors have configurable platforms."""
        return "unknown"

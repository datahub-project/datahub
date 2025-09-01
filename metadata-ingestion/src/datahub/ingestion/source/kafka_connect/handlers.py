import logging
from typing import List

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    ConnectorManifest,
    ConnectorTopicHandler,
    match_topics_by_prefix,
    parse_comma_separated_list,
    parse_comma_separated_with_quotes,
    parse_table_identifier,
    validate_jdbc_url,
    validate_topic_name,
)

logger = logging.getLogger(__name__)


class JDBCSourceTopicHandler(ConnectorTopicHandler):
    """Handler for standard JDBC source connectors."""

    SUPPORTED_CLASSES = ["io.confluent.connect.jdbc.JdbcSourceConnector"]

    def supports_connector_class(self, connector_class: str) -> bool:
        return connector_class in self.SUPPORTED_CLASSES

    def get_platform(self, connector_class: str) -> str:
        """Get the source platform for JDBC connectors.

        For standard JDBC connectors, the platform must be determined from
        configuration (connection.url) since the connector class is generic.
        This should be called with a ConnectorManifest to access the config.
        """
        # Since this handler only supports the generic JDBC connector,
        # we cannot determine platform from connector class alone
        return "unknown"

    def get_platform_from_config(self, connector_manifest: ConnectorManifest) -> str:
        """Get the actual platform by extracting protocol from JDBC connection URL."""
        config = connector_manifest.config
        connection_url = config.get("connection.url", "")

        if not validate_jdbc_url(connection_url):
            logger.warning(f"Invalid or missing JDBC URL in connector {connector_manifest.name}: '{connection_url}'")
            return "unknown"

        # Extract platform from JDBC URL protocol using robust parsing
        try:
            # Remove the jdbc: prefix first
            if not connection_url.startswith("jdbc:"):
                logger.warning(f"JDBC URL doesn't start with 'jdbc:': '{connection_url}'")
                return "unknown"

            # Extract the protocol part (jdbc:postgresql://... -> postgresql)
            remaining_url = connection_url[5:]  # Remove "jdbc:"
            protocol_end = remaining_url.find(":")
            if protocol_end == -1:
                logger.warning(f"No protocol separator found in JDBC URL: '{connection_url}'")
                return "unknown"

            protocol = remaining_url[:protocol_end].lower()

            # Handle postgresql -> postgres mapping for consistency
            return "postgres" if protocol == "postgresql" else protocol
        except Exception as e:
            logger.warning(f"Failed to parse JDBC URL protocol from '{connection_url}': {e}")
            return "unknown"

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics from JDBC source connector configuration."""
        try:
            config = connector_manifest.config
            topics = []

            # Direct topic specification
            if config.get("kafka.topic"):
                topic = config["kafka.topic"]
                if validate_topic_name(topic):
                    topics.append(topic)
                else:
                    logger.warning(f"Invalid kafka.topic specified: '{topic}' in connector {connector_manifest.name}")
                return topics

            # Table-based topic derivation
            topic_prefix = config.get("topic.prefix", "")
            table_config = config.get("table.include.list") or config.get("table.whitelist")

            if table_config:
                # Use enhanced parsing for quoted identifiers
                table_names = parse_comma_separated_with_quotes(table_config)
                for table_name in table_names:
                    # Clean quoted identifiers
                    clean_table = parse_table_identifier(table_name)
                    topic_name = f"{topic_prefix}{clean_table}" if topic_prefix else clean_table
                    if validate_topic_name(topic_name):
                        topics.append(topic_name)
                    else:
                        logger.warning(f"Invalid topic name generated: '{topic_name}' in connector {connector_manifest.name}")

            # Fallback: If we have actual topics and a prefix, try prefix-based matching
            # This is especially useful for Confluent Cloud where table config may be incomplete
            if not topics and hasattr(connector_manifest, 'actual_topics') and topic_prefix:
                prefix_matched_topics = match_topics_by_prefix(
                    connector_manifest.actual_topics,
                    topic_prefix,
                    []  # No table names for open matching
                )
                if prefix_matched_topics:
                    logger.info(f"Found {len(prefix_matched_topics)} topics using prefix matching for connector {connector_manifest.name}")
                    topics.extend(prefix_matched_topics)

            return topics
        except Exception as e:
            return self.handle_extraction_error(e, connector_manifest.name, "extract topics from JDBC config")

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return ["kafka.topic", "topic.prefix", "table.include.list", "table.whitelist"]


class CloudJDBCSourceTopicHandler(ConnectorTopicHandler):
    """Handler for Confluent Cloud JDBC source connectors."""

    def supports_connector_class(self, connector_class: str) -> bool:
        return connector_class in CLOUD_JDBC_SOURCE_CLASSES

    def get_platform(self, connector_class: str) -> str:
        """Get the source platform for Cloud JDBC connectors."""
        if connector_class in ["PostgresCdcSource", "PostgresCdcSourceV2", "PostgresSink"] or "postgres" in connector_class.lower():
            return "postgres"
        elif connector_class in ["MySqlSource", "MySqlCdcSource", "MySqlSink"] or "mysql" in connector_class.lower():
            return "mysql"
        else:
            return "unknown"

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics from Cloud JDBC source connector configuration."""
        config = connector_manifest.config
        topics = []

        # Direct topic specification
        if config.get("kafka.topic"):
            topics.append(config["kafka.topic"])
            return topics

        # Cloud JDBC naming: uses database.server.name as prefix
        topic_prefix = config.get("database.server.name", "")
        table_config = config.get("table.include.list")

        if table_config and topic_prefix:
            # Use enhanced parsing for quoted identifiers
            table_names = parse_comma_separated_with_quotes(table_config)
            for table_name in table_names:
                # Clean quoted identifiers and apply Cloud JDBC "prefix.table" format
                clean_table = parse_table_identifier(table_name)
                topic_name = f"{topic_prefix}.{clean_table}"
                if validate_topic_name(topic_name):
                    topics.append(topic_name)
                else:
                    logger.warning(f"Invalid topic name generated: '{topic_name}' in connector {connector_manifest.name}")

        # Fallback: Use prefix matching if no topics found but we have actual topics
        # This is critical for Confluent Cloud where table config may be missing/incomplete
        if not topics and hasattr(connector_manifest, 'actual_topics') and topic_prefix:
            # For Cloud JDBC, try both "prefix.table" and "prefix_table" patterns
            prefix_matched_topics = match_topics_by_prefix(
                connector_manifest.actual_topics,
                topic_prefix,
                []  # No table names for open matching
            )
            if prefix_matched_topics:
                logger.info(f"Cloud JDBC: Found {len(prefix_matched_topics)} topics using prefix matching for connector {connector_manifest.name}")
                topics.extend(prefix_matched_topics)

        return topics

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return ["kafka.topic", "database.server.name", "table.include.list"]


class DebeziumSourceTopicHandler(ConnectorTopicHandler):
    """Handler for Debezium CDC source connectors."""

    SUPPORTED_CLASSES = [
        "io.debezium.connector.mysql.MySqlConnector",
        "io.debezium.connector.postgresql.PostgresConnector",
        "io.debezium.connector.oracle.OracleConnector",
        "io.debezium.connector.sqlserver.SqlServerConnector",
        "io.debezium.connector.db2.Db2Connector",
        "io.debezium.connector.vitess.VitessConnector",
        "MySqlConnector",  # Cloud version
    ]

    def supports_connector_class(self, connector_class: str) -> bool:
        return connector_class in self.SUPPORTED_CLASSES or connector_class.startswith("io.debezium.connector.")

    def get_platform(self, connector_class: str) -> str:
        """Get the source platform for Debezium connectors."""
        connector_lower = connector_class.lower()

        # Special case: postgres uses "postgresql" in class name but "postgres" as platform
        if "postgres" in connector_lower:
            return "postgres"
        else:
            return connector_lower

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics from Debezium CDC connector configuration."""
        try:
            config = connector_manifest.config
            topics: List[str] = []

            # Debezium uses database.server.name as topic prefix
            server_name = config.get("database.server.name", "")
            if not server_name:
                return topics

            # Handle table inclusion lists
            table_config = (
                    config.get("table.include.list") or
                    config.get("table.whitelist") or
                    config.get("database.include.list")
            )

            if table_config:
                # Use enhanced parsing for quoted identifiers
                table_names = parse_comma_separated_with_quotes(table_config)
                for table_name in table_names:
                    # Clean quoted identifiers
                    clean_table = parse_table_identifier(table_name)

                    # Debezium CDC format: server_name.schema.table or server_name.table
                    if "." in clean_table:
                        # Already includes schema
                        topic_name = f"{server_name}.{clean_table}"
                    else:
                        # Just table name
                        topic_name = f"{server_name}.{clean_table}"

                    if validate_topic_name(topic_name):
                        topics.append(topic_name)
                    else:
                        logger.warning(f"Invalid topic name generated: '{topic_name}' in connector {connector_manifest.name}")

            # Fallback: Use prefix matching if no topics found but we have actual topics
            # This is especially valuable for Debezium when table config is missing
            if not topics and hasattr(connector_manifest, 'actual_topics') and server_name:
                prefix_matched_topics = match_topics_by_prefix(
                    connector_manifest.actual_topics,
                    server_name,
                    []  # No table names for open matching
                )
                if prefix_matched_topics:
                    logger.info(f"Debezium: Found {len(prefix_matched_topics)} topics using prefix matching for connector {connector_manifest.name}")
                    topics.extend(prefix_matched_topics)
            elif not topics:
                # Final fallback: use server name as topic prefix
                if validate_topic_name(server_name):
                    topics.append(server_name)
                else:
                    logger.warning(f"Invalid server name for topic: '{server_name}' in connector {connector_manifest.name}")

            return topics
        except Exception as e:
            return self.handle_extraction_error(e, connector_manifest.name, "extract topics from Debezium config")

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return [
            "database.server.name",
            "table.include.list",
            "table.whitelist",
            "database.include.list"
        ]


class MongoSourceTopicHandler(ConnectorTopicHandler):
    """Handler for MongoDB source connectors."""

    SUPPORTED_CLASSES = [
        "com.mongodb.kafka.connect.MongoSourceConnector",
        "io.debezium.connector.mongodb.MongoDbConnector"
    ]

    def supports_connector_class(self, connector_class: str) -> bool:
        return connector_class in self.SUPPORTED_CLASSES

    def get_platform(self, connector_class: str) -> str:
        """Get the source platform for MongoDB connectors."""
        return "mongodb"

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics from MongoDB connector configuration."""
        try:
            config = connector_manifest.config
            topics = []

            # Direct topic specification
            if config.get("kafka.topic"):
                topic = config["kafka.topic"]
                if validate_topic_name(topic):
                    topics.append(topic)
                else:
                    logger.warning(f"Invalid kafka.topic specified: '{topic}' in connector {connector_manifest.name}")
                return topics

            # MongoDB specific logic
            topic_prefix = config.get("topic.prefix", "")
            database_config = config.get("database.include.list")

            if database_config:
                databases = parse_comma_separated_list(database_config)
                for database in databases:
                    topic_name = f"{topic_prefix}{database}" if topic_prefix else database
                    if validate_topic_name(topic_name):
                        topics.append(topic_name)
                    else:
                        logger.warning(f"Invalid topic name generated: '{topic_name}' in connector {connector_manifest.name}")

            return topics
        except Exception as e:
            return self.handle_extraction_error(e, connector_manifest.name, "extract topics from MongoDB config")

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return ["kafka.topic", "topic.prefix", "database.include.list"]


class BaseSinkTopicHandler(ConnectorTopicHandler):
    """Base handler for standard sink connectors with common topic parsing logic."""

    def supports_connector_class(self, connector_class: str) -> bool:
        """Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement supports_connector_class")

    def get_platform(self, connector_class: str) -> str:
        """Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_platform")

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics using standard sink connector configuration."""
        config = connector_manifest.config
        topics: List[str] = []

        # Standard sink connector topics configuration
        if config.get("topics"):
            topic_list = parse_comma_separated_list(config["topics"])
            for topic in topic_list:
                if validate_topic_name(topic):
                    topics.append(topic)
                else:
                    logger.warning(f"Invalid topic name in connector {connector_manifest.name}: '{topic}'")

        if config.get("topics.regex"):
            platform = self.get_platform("")
            logger.info(f"{platform.title()} sink connector uses topics.regex: {config['topics.regex']}. Cannot enumerate specific topics from pattern.")

        return topics

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return ["topics", "topics.regex"]


class BigQuerySinkTopicHandler(BaseSinkTopicHandler):
    """Handler for BigQuery sink connectors."""

    SUPPORTED_CLASSES = ["com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"]

    def supports_connector_class(self, connector_class: str) -> bool:
        return (
                connector_class in self.SUPPORTED_CLASSES or
                "bigquery" in connector_class.lower()
        )

    def get_platform(self, connector_class: str) -> str:
        return "bigquery"


class S3SinkTopicHandler(BaseSinkTopicHandler):
    """Handler for S3 sink connectors."""

    SUPPORTED_CLASSES = ["io.confluent.connect.s3.S3SinkConnector"]

    def supports_connector_class(self, connector_class: str) -> bool:
        return (
                connector_class in self.SUPPORTED_CLASSES or
                "s3" in connector_class.lower()
        )

    def get_platform(self, connector_class: str) -> str:
        return "s3"


class SnowflakeSinkTopicHandler(BaseSinkTopicHandler):
    """Handler for Snowflake sink connectors."""

    SUPPORTED_CLASSES = ["com.snowflake.kafka.connector.SnowflakeSinkConnector"]

    def supports_connector_class(self, connector_class: str) -> bool:
        return (
                connector_class in self.SUPPORTED_CLASSES or
                "snowflake" in connector_class.lower()
        )

    def get_platform(self, connector_class: str) -> str:
        return "snowflake"



class GenericConnectorTopicHandler(ConnectorTopicHandler):
    """Fallback handler for unknown or generic connectors."""

    # Platform keywords for simple detection
    _PLATFORM_KEYWORDS = [
        "postgres", "mysql", "snowflake", "oracle",
        "sqlserver", "mongodb", "bigquery", "s3"
    ]

    def supports_connector_class(self, connector_class: str) -> bool:
        # This is the fallback handler, so it supports any connector
        return True

    def get_platform(self, connector_class: str) -> str:
        """Get the platform for generic/unknown connectors using simple keyword detection."""
        connector_lower = connector_class.lower()

        # Check for platform keywords in connector class name
        for platform in self._PLATFORM_KEYWORDS:
            if platform in connector_lower:
                return platform

        return "unknown"

    def get_topics_from_config(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Extract topics using generic logic for unknown connectors."""
        config = connector_manifest.config
        topics: List[str] = []

        # Try standard topic configuration fields
        topic_fields = ["topics", "kafka.topic", "topic.prefix"]

        for field in topic_fields:
            if config.get(field):
                if field == "topics":
                    # Handle comma-separated list
                    topic_list = parse_comma_separated_list(config[field])
                    topics.extend(topic_list)
                else:
                    # Single topic field
                    topics.append(config[field])

        # Log regex patterns but don't try to enumerate them
        if config.get("topics.regex"):
            logger.info(f"Generic connector uses topics.regex: {config['topics.regex']}. Cannot enumerate specific topics from pattern.")

        return topics

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        return ["topics", "topics.regex", "kafka.topic", "topic.prefix"]
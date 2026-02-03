import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Final, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER,
    KAFKA,
    KNOWN_TOPIC_ROUTING_TRANSFORMS,
    MYSQL_CDC_SOURCE_V2_CLOUD,
    POSTGRES_CDC_SOURCE_V2_CLOUD,
    REGEXROUTER_TRANSFORM,
    SNOWFLAKE_SOURCE_CLOUD,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    get_dataset_name,
    has_three_level_hierarchy,
    parse_comma_separated_list,
    remove_prefix,
    unquote,
    validate_jdbc_url,
)
from datahub.ingestion.source.kafka_connect.pattern_matchers import (
    JavaRegexMatcher,
    PatternMatcher,
)
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    get_transform_pipeline,
)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)

logger = logging.getLogger(__name__)

# Constants for JDBC URL parsing
JDBC_PREFIX_LENGTH: Final[int] = 5  # Length of "jdbc:" prefix


@dataclass(frozen=True)
class TableId:
    """
    Represents a table identifier with database, schema, and table components.

    Mirrors the Java TableId class from the original Kafka Connect JDBC Source:
    https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/util/TableId.java

    """

    database: Optional[str] = None
    schema: Optional[str] = None
    table: str = ""

    def __str__(self) -> str:
        """String representation for debugging."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)


@dataclass
class JdbcParser:
    """Data transfer object for JDBC connector configuration."""

    db_connection_url: str
    source_platform: str
    database_name: str
    topic_prefix: str
    query: str
    transforms: List[Dict[str, str]]


class JdbcParserFactory:
    """Factory for creating JDBC parsers based on configuration type."""

    def create_parser(self, connector_manifest: ConnectorManifest) -> JdbcParser:
        """Main factory method - delegates to URL or fields parser based on config."""
        if "connection.url" in connector_manifest.config:
            return self._create_url_parser(connector_manifest)
        else:
            return self._create_fields_parser(connector_manifest)

    def _create_url_parser(self, connector_manifest: ConnectorManifest) -> JdbcParser:
        """Create parser for traditional JDBC connector using connection.url."""
        # Reference: https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#connection-url
        url = remove_prefix(
            str(connector_manifest.config.get("connection.url")), "jdbc:"
        )
        url_instance = make_url(url)
        source_platform = get_platform_from_sqlalchemy_uri(str(url_instance))
        database_name = url_instance.database

        if not database_name:
            raise ValueError(
                f"Missing database name in JDBC URL: {url}. "
                f"JDBC URLs must include a database name, e.g., 'jdbc:postgresql://host:port/database_name'"
            )
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{database_name}"

        # Platform uses topic.prefix
        topic_prefix = connector_manifest.config.get("topic.prefix", "")

        return self._build_parser(
            connector_manifest,
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
        )

    def _create_fields_parser(
        self, connector_manifest: ConnectorManifest
    ) -> JdbcParser:
        """Create parser for Confluent Cloud JDBC connector using individual fields."""
        # References:
        # - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html#connection-details
        # - https://docs.confluent.io/cloud/current/connectors/cc-mysql-source.html#connection-details
        connector_class = connector_manifest.config.get(CONNECTOR_CLASS, "")

        # Use platform mapping from connector class
        # This is a simplified version for the factory - can be enhanced later
        if (
            "PostgresCdcSource" in connector_class
            or "postgres" in connector_class.lower()
        ):
            source_platform = "postgres"
        elif "MySqlCdcSource" in connector_class or "mysql" in connector_class.lower():
            source_platform = "mysql"
        else:
            source_platform = "unknown"

        hostname = connector_manifest.config.get("database.hostname")
        port = connector_manifest.config.get("database.port")
        database_name = connector_manifest.config.get("database.dbname")

        if not hostname or not port or not database_name:
            raise ValueError(
                f"Missing required Cloud connector config: "
                f"hostname={hostname}, port={port}, database_name={database_name}"
            )

        # Construct connection URL from individual fields
        if source_platform == "postgres":
            db_connection_url = f"postgresql://{hostname}:{port}/{database_name}"
        elif source_platform == "mysql":
            db_connection_url = f"mysql://{hostname}:{port}/{database_name}"
        else:
            db_connection_url = f"{source_platform}://{hostname}:{port}/{database_name}"

        # Cloud connector topic prefix logic:
        # - PostgresCdcSourceV2 and MySqlCdcSourceV2 use topic.prefix (Confluent Cloud V2 managed)
        # - PostgresCdcSource (V1) and MySqlCdcSource (V1) use database.server.name (Debezium-based)
        # References:
        #   - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
        #   - https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-v2-debezium/cc-mysql-source-cdc-v2-debezium.html
        if connector_class in (POSTGRES_CDC_SOURCE_V2_CLOUD, MYSQL_CDC_SOURCE_V2_CLOUD):
            # V2 uses topic.prefix (new Cloud-native implementation)
            topic_prefix = connector_manifest.config.get("topic.prefix", "")
        else:
            # V1 and other CDC connectors use database.server.name (Debezium standard)
            # Fallback to topic.prefix if database.server.name not present
            topic_prefix = connector_manifest.config.get(
                "database.server.name",
                connector_manifest.config.get("topic.prefix", ""),
            )

        return self._build_parser(
            connector_manifest,
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
        )

    def _build_parser(
        self,
        connector_manifest: ConnectorManifest,
        db_connection_url: str,
        source_platform: str,
        database_name: str,
        topic_prefix: str,
    ) -> JdbcParser:
        """Common parser building logic for both URL and fields parsers."""
        query = connector_manifest.config.get("query", "")

        # Parse transforms (common to both Platform and Cloud)
        transforms_config = connector_manifest.config.get("transforms", "")
        transform_names = (
            parse_comma_separated_list(transforms_config) if transforms_config else []
        )

        transforms = []
        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in connector_manifest.config:
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = (
                        connector_manifest.config[key]
                    )

        # Log transform configuration for debugging
        if transforms:
            logger.info(
                f"Connector '{connector_manifest.name}' has {len(transforms)} transform(s) configured"
            )
            for transform in transforms:
                transform_type = transform.get("type", "unknown")
                logger.info(
                    f"  Transform '{transform['name']}' (type={transform_type}): {transform}"
                )

        return JdbcParser(
            db_connection_url=db_connection_url,
            source_platform=source_platform,
            database_name=database_name,
            topic_prefix=topic_prefix,
            query=query,
            transforms=transforms,
        )


@dataclass
class ConfluentJDBCSourceConnector(BaseConnector):
    # Use imported constants from connector_constants module
    REGEXROUTER = REGEXROUTER_TRANSFORM
    KNOWN_TOPICROUTING_TRANSFORMS = KNOWN_TOPIC_ROUTING_TRANSFORMS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parser_factory = JdbcParserFactory()

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> JdbcParser:
        """Use the factory to create JDBC parser."""
        return self._parser_factory.create_parser(connector_manifest)

    def default_get_lineages(
        self,
        topic_prefix: str,
        database_name: str,
        source_platform: str,
        topic_names: Optional[Iterable[str]] = None,
        include_source_dataset: bool = True,
    ) -> List[KafkaConnectLineage]:
        """
        Default lineage extraction for simple topic-to-table mappings.

        This method handles straightforward cases where topic names directly correspond
        to source tables with optional prefix removal and schema resolution.
        """
        lineages: List[KafkaConnectLineage] = []

        # Use provided topics or fallback to manifest topics
        resolved_topic_names = topic_names or self.connector_manifest.topic_names
        if not resolved_topic_names:
            return lineages

        # Get table metadata for schema resolution
        table_ids: List[TableId] = self.get_table_names()

        # Process each topic to create lineage mappings
        for topic in resolved_topic_names:
            source_table = self._extract_source_table_from_topic(topic, topic_prefix)

            # Resolve schema information for hierarchical platforms
            resolved_source_table, dataset_included = self._resolve_source_table_schema(
                source_table, source_platform, table_ids, include_source_dataset
            )

            # Create final lineage mapping
            lineage = self._create_lineage_mapping(
                resolved_source_table,
                database_name,
                source_platform,
                topic,
                dataset_included,
            )
            lineages.append(lineage)

        return lineages

    def _extract_source_table_from_topic(self, topic: str, topic_prefix: str) -> str:
        """Extract source table name from topic by removing prefix and cleaning."""
        # Remove topic prefix if present
        source_table = remove_prefix(topic, topic_prefix) if topic_prefix else topic

        # Clean up leading dot from prefix removal
        if source_table.startswith("."):
            source_table = source_table[1:]

        return source_table

    def _resolve_source_table_schema(
        self,
        source_table: str,
        source_platform: str,
        table_ids: List[TableId],
        include_source_dataset: bool,
    ) -> Tuple[str, bool]:
        """
        Resolve schema information for source table in hierarchical naming platforms.

        Returns:
            Tuple of (resolved_source_table, include_dataset_flag)
        """
        if not has_three_level_hierarchy(source_platform):
            return source_table, include_source_dataset

        # Find matching table ID for schema resolution
        table_id = self._find_matching_table_id(source_table, table_ids)

        if table_id and table_id.schema:
            # Successfully resolved schema - use fully qualified name
            resolved_table = f"{table_id.schema}.{table_id.table}"
            return resolved_table, include_source_dataset
        else:
            # Failed to resolve schema - warn and exclude dataset
            self.report.warning(
                f"Could not find schema for table {self.connector_manifest.name} : {source_table}"
            )
            return source_table, False

    def _find_matching_table_id(
        self, source_table: str, table_ids: List[TableId]
    ) -> Optional[TableId]:
        """Find matching TableId for the given source table name."""
        if "." in source_table:
            # source_table is already in schema.table format
            schema_part, table_part = source_table.rsplit(".", 1)
            for table_id in table_ids:
                if (
                    table_id
                    and table_id.table == table_part
                    and table_id.schema == schema_part
                ):
                    return table_id
        else:
            # source_table is just table name - find by table only
            for table_id in table_ids:
                if table_id and table_id.table == source_table:
                    return table_id

        return None

    def _create_lineage_mapping(
        self,
        source_table: str,
        database_name: str,
        source_platform: str,
        topic: str,
        include_dataset: bool,
    ) -> KafkaConnectLineage:
        """Create a single lineage mapping from source table to topic."""
        dataset_name = get_dataset_name(database_name, source_table)

        # Extract column-level lineage if enabled (uses base class method)
        fine_grained = self._extract_fine_grained_lineage(
            source_dataset=dataset_name,
            source_platform=source_platform,
            target_dataset=topic,
            target_platform=KAFKA,
        )

        return KafkaConnectLineage(
            source_dataset=dataset_name if include_dataset else None,
            source_platform=source_platform,
            target_dataset=topic,
            target_platform=KAFKA,
            fine_grained_lineages=fine_grained,
        )

    def get_table_names(self) -> List[TableId]:
        """
        Extract table names from connector configuration and return as TableId objects.

        Returns:
            List of TableId objects representing the tables to be processed
        """
        sep: str = "."
        leading_quote_char: str = '"'
        trailing_quote_char: str = leading_quote_char

        table_ids: List[str] = []
        if self.connector_manifest.tasks:
            table_config_combined = ",".join(
                [
                    task["config"]["tables"]
                    for task in self.connector_manifest.tasks
                    if task and "config" in task and task["config"].get("tables")
                ]
            )
            table_ids = parse_comma_separated_list(table_config_combined)
            quote_method = self.connector_manifest.config.get(
                "quote.sql.identifiers", "always"
            )
            if (
                quote_method == "always"
                and table_ids
                and table_ids[0]
                and table_ids[-1]
            ):
                leading_quote_char = table_ids[0][0]
                trailing_quote_char = table_ids[-1][-1]
                # This will only work for single character quotes
        else:
            # Handle both field names for table discovery
            # Platform: https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#table-whitelist
            # Cloud: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html#table-include-list
            table_config = self.connector_manifest.config.get(
                "table.include.list"
            ) or self.connector_manifest.config.get("table.whitelist")

            if table_config:
                table_ids = parse_comma_separated_list(table_config)

        # Create TableId objects from parsed table identifiers
        tables: List[TableId] = []
        for table_id in table_ids:
            parts = table_id.split(sep)
            if len(parts) > 1:
                # Has schema: schema.table format
                schema = unquote(parts[-2], leading_quote_char, trailing_quote_char)
                table = unquote(parts[-1], leading_quote_char, trailing_quote_char)
            else:
                # No schema: just table
                schema = None
                table = unquote(parts[-1], leading_quote_char, trailing_quote_char)

            tables.append(TableId(schema=schema, table=table))

        return tables

    def get_topics_from_config(self) -> List[str]:
        """Extract expected topics from JDBC connector configuration."""
        try:
            # Try to get full parser for topic prefix
            try:
                parser = self.get_parser(self.connector_manifest)
                topic_prefix = parser.topic_prefix
            except Exception:
                # Fall back to direct config access if parser fails
                config = self.connector_manifest.config
                topic_prefix = config.get("topic.prefix", "")

            table_ids = self.get_table_names()

            # JDBC topics follow pattern: {topic_prefix}{table_name}
            topics = []
            for table_id in table_ids:
                # Use table name (not schema.table) for topic generation
                table_name = table_id.table
                if topic_prefix:
                    topic_name = f"{topic_prefix}{table_name}"
                else:
                    topic_name = table_name
                topics.append(topic_name)

            return topics
        except Exception as e:
            logger.debug(f"Failed to derive topics from JDBC connector config: {e}")
            return []

    def extract_flow_property_bag(self) -> Dict[str, str]:
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k not in ["connection.password", "connection.user"]
        }

        # Mask/Remove properties that may reveal credentials
        flow_property_bag["connection.url"] = self.get_parser(
            self.connector_manifest
        ).db_connection_url

        return flow_property_bag

    def _get_manifest_topics(self) -> List[str]:
        """Get all actual topics from connector manifest."""
        manifest_topics = list(self.connector_manifest.topic_names)
        if not manifest_topics:
            logger.debug("No topics found in manifest")
        return manifest_topics

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """Extract lineage mappings from connector configuration to Kafka topics."""
        parser = self.get_parser(self.connector_manifest)

        logging.debug(
            f"Extracting lineages for platform: {parser.source_platform}, "
            f"database: {parser.database_name}"
        )

        # Early return if no topics
        if not self.connector_manifest.topic_names:
            return []

        # Handle query-based connectors early (can't determine source tables from custom queries)
        if parser.query:
            return self._handle_query_based_connector(parser)

        # Determine environment and extraction approach
        connector_class = (
            self.connector_manifest.config.get(CONNECTOR_CLASS, "")
            if self.connector_manifest
            else ""
        )
        if connector_class:
            try:
                return self._extract_lineages_with_environment_awareness(parser)
            except Exception as e:
                logger.warning(
                    f"Environment-aware extraction failed: {e}, falling back to unified approach"
                )

        # Fallback: Unified lineage extraction approach
        return self._extract_lineages_unified(parser)

    def _extract_lineages_with_environment_awareness(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Extract lineages based on environment type and topic discovery approach."""
        connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")

        # Determine environment type
        is_cloud_environment = connector_class in CLOUD_JDBC_SOURCE_CLASSES

        if is_cloud_environment:
            return self._extract_lineages_cloud_environment(parser)
        else:
            return self._extract_lineages_platform_environment(parser)

    def _extract_lineages_platform_environment(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Extract lineages for non-cloud (platform) environment."""
        # For platform: Get topics from connector's topics endpoint (manifest.topic_names)
        # These are the actual topics reported by the connector
        topics = list(self.connector_manifest.topic_names)

        if parser.transforms:
            # Use transform pipeline to trace back from actual topics to source tables
            return self._extract_lineages_with_transforms(topics, parser)
        else:
            # Direct mapping from topics to source tables
            return self._create_direct_lineages_from_topics(topics, parser)

    def _extract_lineages_cloud_environment(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """
        Extract lineages for Confluent Cloud environment with transform pipeline support.

        This method implements a hybrid approach that properly handles transforms while
        working within Cloud environment constraints.
        """
        # Get all available topics from cluster (if available)
        # For Cloud, use all_cluster_topics (populated separately) if available,
        # otherwise fall back to connector_manifest.topic_names
        all_topics = (
            list(self.all_cluster_topics)
            if self.all_cluster_topics
            else list(self.connector_manifest.topic_names)
        )

        # If we have topics and transforms, try the transform-aware approach
        if all_topics and parser.transforms:
            logger.debug(
                f"Attempting transform-aware extraction for {len(all_topics)} topics"
            )
            result = self._extract_lineages_cloud_with_transforms(all_topics, parser)
            if result:
                return result

        # Fallback to existing config-based strategies
        logger.debug("Falling back to config-based extraction strategies")
        if self._has_one_to_one_topic_mapping(all_topics):
            return self._extract_lineages_from_config_mapping(parser, all_topics)
        elif parser.topic_prefix:
            return self._extract_lineages_from_prefix_inference(parser, all_topics)
        else:
            # Use available topics directly if no better strategy
            return self._create_direct_lineages_from_topics(all_topics, parser)

    def _has_one_to_one_topic_mapping(self, topics: Optional[List[str]] = None) -> bool:
        """Check if connector has clear 1:1 table to topic mapping from config."""
        config = self.connector_manifest.config

        # Check for single table configuration
        if config.get("kafka.topic") and config.get("table.name"):
            return True

        # Check for explicit table list with matching topic count
        table_config = config.get("table.include.list") or config.get("table.whitelist")
        if table_config:
            table_names = parse_comma_separated_list(table_config)
            # Use provided topics or fall back to connector manifest topics
            available_topics = (
                topics if topics is not None else self.connector_manifest.topic_names
            )
            return len(table_names) == len(available_topics)

        return False

    def _extract_lineages_cloud_with_transforms(
        self, all_topics: List[str], parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """
        Cloud-specific transform-aware lineage extraction.

        This method addresses the key limitation where Cloud environments get ALL cluster topics
        but need to filter to only topics relevant to this specific connector.

        Strategy:
        1. Get source tables from connector configuration
        2. Apply forward transforms to predict what topics this connector produces
        3. Filter all_topics to only include predicted topics that actually exist
        4. Create lineages from source tables to validated topics
        """
        try:
            # Step 1: Get configured source tables
            source_tables = self._get_source_tables_from_config()
            if not source_tables:
                logger.debug("No source tables found in connector config")
                return []

            # Step 2: Apply forward transforms to predict expected topics
            expected_topics = self._apply_forward_transforms(source_tables, parser)
            if not expected_topics:
                logger.debug("Forward transforms produced no expected topics")
                return []

            # Step 3: Filter all_topics to only include topics this connector actually produces
            connector_topics = [
                topic for topic in expected_topics if topic in all_topics
            ]
            if not connector_topics:
                logger.debug(
                    f"None of the expected topics {expected_topics} found in cluster topics"
                )
                return []

            logger.info(
                f"Cloud connector matched {len(connector_topics)} topics from {len(expected_topics)} predicted: {connector_topics}"
            )

            # Step 4: Create lineages from source tables to validated topics
            lineages = []
            for source_table, topic in zip(
                source_tables, connector_topics, strict=False
            ):
                # Handle schema resolution
                resolved_source_table, dataset_included = (
                    self._resolve_source_table_schema(
                        source_table,
                        parser.source_platform,
                        self.get_table_names(),
                        True,
                    )
                )

                lineage = self._create_lineage_mapping(
                    resolved_source_table,
                    parser.database_name,
                    parser.source_platform,
                    topic,
                    dataset_included,
                )
                lineages.append(lineage)

            return lineages

        except Exception as e:
            logger.warning(
                f"Cloud transform-aware extraction failed: {e}, falling back to config methods"
            )
            return []

    def _get_source_tables_from_config(self) -> List[str]:
        """Extract source table names from connector configuration."""
        config = self.connector_manifest.config

        # Single table mode
        if config.get("table.name"):
            return [config["table.name"]]

        # Multi-table mode
        table_config = config.get("table.include.list") or config.get("table.whitelist")
        if table_config:
            return parse_comma_separated_list(table_config)

        # Query mode - can't determine tables
        if config.get("query"):
            logger.debug("Query-based connector - cannot determine source tables")
            return []

        return []

    def _apply_forward_transforms(
        self, source_tables: List[str], parser: JdbcParser
    ) -> List[str]:
        """
        Apply transforms in forward direction: source tables -> expected topics.

        This is the opposite of the platform approach which works backwards from topics.
        """
        connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")

        # Step 1: Generate original topic names based on connector naming convention
        original_topics = []
        for table_name in source_tables:
            # Parse schema.table if present
            if "." in table_name.strip('"'):
                # Already has schema - use as-is for CDC connectors
                clean_table = table_name.strip('"')
                if "." in clean_table:
                    schema_part, table_part = clean_table.rsplit(".", 1)
                else:
                    schema_part, table_part = "", clean_table
            else:
                # Just table name
                schema_part, table_part = "", table_name.strip('"')

            # Generate original topic using connector-specific naming
            original_topic = self._generate_original_topic_name(
                schema_part, table_part, parser.topic_prefix, connector_class
            )
            original_topics.append(original_topic)

        # Step 2: Apply transforms if they exist
        if parser.transforms:
            final_topics = self._apply_transforms_forward(
                original_topics, parser.transforms
            )
            return final_topics
        else:
            return original_topics

    def _generate_original_topic_name(
        self, schema: str, table_name: str, topic_prefix: str, connector_class: str
    ) -> str:
        """
        Generate original topic name before transforms using connector-specific logic.

        Different connector types use different topic naming strategies:
        - Traditional JDBC: Simple concatenation (topicPrefix + tableName)
        - CDC/Debezium connectors: Hierarchical naming ({prefix}.{schema}.{table})
        - Cloud connectors: Hierarchical naming ({database.server.name}.{schema}.{table})
        """
        # Cloud connectors and Debezium connectors use hierarchical naming
        if connector_class in CLOUD_JDBC_SOURCE_CLASSES or connector_class.startswith(
            "io.debezium."
        ):
            # CDC topic name generation
            if schema:
                if topic_prefix:
                    return f"{topic_prefix}.{schema}.{table_name}"
                else:
                    return f"{schema}.{table_name}"
            else:
                if topic_prefix:
                    return f"{topic_prefix}.{table_name}"
                else:
                    return table_name

        # Traditional JDBC source connector uses simple concatenation
        elif connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector":
            return topic_prefix + table_name

        # Default behavior for unknown connectors
        else:
            return topic_prefix + table_name

    def _apply_transforms_forward(
        self, topics: List[str], transforms: List[Dict[str, str]]
    ) -> List[str]:
        """Apply transform configurations in forward direction."""
        result_topics = topics[:]

        for transform_config in transforms:
            transform_type = transform_config.get("type", "")

            # Handle RegexRouter transforms
            if transform_type in [
                "org.apache.kafka.connect.transforms.RegexRouter",
                "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            ]:
                regex_pattern = transform_config.get("regex", "")
                replacement = transform_config.get("replacement", "")

                if regex_pattern and replacement:
                    result_topics = self._apply_regex_to_topics(
                        result_topics, regex_pattern, replacement
                    )

            # Handle EventRouter - simplified forward prediction
            elif transform_type == "io.debezium.transforms.outbox.EventRouter":
                logger.debug(
                    "EventRouter forward transform - using simplified prediction"
                )
                # EventRouter is complex and context-dependent, use simplified approach
                # In forward direction, we can't predict exact event types without data
                result_topics = [f"outbox.event.{topic}" for topic in result_topics]

            # Skip other transforms
            else:
                logger.debug(
                    f"Skipping unsupported forward transform: {transform_type}"
                )

        return result_topics

    def _extract_lineages_from_config_mapping(
        self, parser: JdbcParser, topics: Optional[List[str]] = None
    ) -> List[KafkaConnectLineage]:
        """Extract lineages using direct config-to-topic mapping."""
        config = self.connector_manifest.config
        lineages = []

        # Single table mode
        if config.get("kafka.topic") and config.get("table.name"):
            topic_name = config["kafka.topic"]
            table_name = config["table.name"]

            lineage = self._create_lineage_mapping(
                table_name,
                parser.database_name,
                parser.source_platform,
                topic_name,
                True,
            )
            lineages.append(lineage)

        # Multi-table mode with clear mapping
        else:
            table_config = config.get("table.include.list") or config.get(
                "table.whitelist"
            )
            if table_config:
                table_names = parse_comma_separated_list(table_config)
                # Use provided topics or fall back to connector manifest topics
                available_topics = (
                    topics
                    if topics is not None
                    else list(self.connector_manifest.topic_names)
                )

                # Create 1:1 mapping (assuming same order)
                for table_name, topic in zip(
                    table_names, available_topics, strict=False
                ):
                    # Keep full table name including schema for proper lineage
                    clean_table = table_name.strip('"')
                    lineage = self._create_lineage_mapping(
                        clean_table,
                        parser.database_name,
                        parser.source_platform,
                        topic,
                        True,
                    )
                    lineages.append(lineage)

        return lineages

    def _extract_lineages_from_prefix_inference(
        self, parser: JdbcParser, topics: Optional[List[str]] = None
    ) -> List[KafkaConnectLineage]:
        """Extract lineages by inferring source tables from topic names using prefix."""
        lineages = []

        # Use provided topics or fall back to connector manifest topics
        available_topics = (
            topics if topics is not None else self.connector_manifest.topic_names
        )

        for topic in available_topics:
            # Remove prefix to get source table name
            source_table = self._extract_source_table_from_topic(
                topic, parser.topic_prefix
            )

            # Resolve schema information for hierarchical platforms
            resolved_source_table, dataset_included = self._resolve_source_table_schema(
                source_table, parser.source_platform, self.get_table_names(), True
            )

            # Create lineage mapping
            lineage = self._create_lineage_mapping(
                resolved_source_table,
                parser.database_name,
                parser.source_platform,
                topic,
                dataset_included,
            )
            lineages.append(lineage)

        return lineages

    def _extract_lineages_with_transforms(
        self, topics: List[str], parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Extract lineages by applying transform pipeline."""
        # Skip query-based connectors (can't determine source tables from custom queries)
        if parser.query:
            logger.warning(
                f"Query-based connector {self.connector_manifest.name}: "
                f"source tables cannot be determined from custom query configuration"
            )
            return []

        # Apply transforms if they exist
        if parser.transforms:
            return self._extract_lineages_with_transform_orchestrator(topics, parser)

        # Direct mapping (no transforms)
        return self._create_direct_lineages_from_topics(topics, parser)

    def _extract_lineages_with_transform_orchestrator(
        self, topics: List[str], parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """
        Extract lineages for self-hosted Kafka Connect with transforms.

        For self-hosted environments, the topics parameter contains connector-specific topics
        from the /connectors/{name}/topics API endpoint. These are the actual topics this
        connector produces, so we should use them directly.

        Strategy:
        1. Get source tables from config
        2. If single source table: All runtime topics come from that table
           (handles ExtractTopic and other 1-to-many transforms)
        3. If multiple source tables: Try to match each table to its topic(s) using transform prediction
        """
        config = self.connector_manifest.config

        # Get source tables from connector configuration
        table_ids = self.get_table_names()

        if not table_ids:
            logger.warning(
                f"No source tables found in config for connector {self.connector_manifest.name}"
            )
            return []

        # If no runtime topics available, connector may be failed/not started
        if not topics:
            logger.warning(
                f"No runtime topics found for connector {self.connector_manifest.name}. "
                f"Connector may be failed or not started."
            )
            return []

        # Single source table: all runtime topics come from this table
        # This handles cases like ExtractTopic where one table produces multiple topics
        if len(table_ids) == 1:
            table_id = table_ids[0]
            source_table = table_id.table

            resolved_source_table, dataset_included = self._resolve_source_table_schema(
                source_table, parser.source_platform, table_ids, True
            )

            lineages = []
            for runtime_topic in topics:
                lineage = self._create_lineage_mapping(
                    resolved_source_table,
                    parser.database_name,
                    parser.source_platform,
                    runtime_topic,
                    dataset_included,
                )
                lineages.append(lineage)

            logger.debug(
                f"Created {len(lineages)} lineages from single source table '{source_table}' "
                f"to runtime topics {topics}"
            )
            return lineages

        # Multiple source tables: match each table to its topic(s) using transform prediction
        # This is more complex because we need to figure out which table produced which topic
        lineages = []
        connector_class = config.get(CONNECTOR_CLASS, "")
        matched_topics = set()

        for table_id in table_ids:
            source_table = table_id.table

            # Generate original topic name (before transforms)
            original_topic = self._generate_original_topic_name(
                table_id.schema or "",
                table_id.table,
                parser.topic_prefix or "",
                connector_class,
            )

            # Apply transforms to predict final topic
            transform_result = get_transform_pipeline().apply_forward(
                [original_topic], config
            )

            # Log any warnings from transform processing
            for warning in transform_result.warnings:
                self.report.warning(
                    f"Transform warning for {self.connector_manifest.name}: {warning}"
                )

            predicted_topic = (
                transform_result.topics[0]
                if transform_result.topics
                else original_topic
            )

            # Resolve schema information for hierarchical platforms
            resolved_source_table, dataset_included = self._resolve_source_table_schema(
                source_table, parser.source_platform, table_ids, True
            )

            # Check if predicted topic exists in runtime topics
            if predicted_topic in topics:
                # Match found: create lineage
                lineage = self._create_lineage_mapping(
                    resolved_source_table,
                    parser.database_name,
                    parser.source_platform,
                    predicted_topic,
                    dataset_included,
                )
                lineages.append(lineage)
                matched_topics.add(predicted_topic)
                logger.debug(
                    f"Matched table '{source_table}' to runtime topic '{predicted_topic}'"
                )
            else:
                logger.warning(
                    f"Predicted topic '{predicted_topic}' for table '{source_table}' "
                    f"not found in runtime topics {topics}. Transform prediction may be inaccurate."
                )

        # Report any unmatched runtime topics
        unmatched_topics = set(topics) - matched_topics
        if unmatched_topics:
            logger.warning(
                f"Some runtime topics could not be matched to source tables: {unmatched_topics}. "
                f"This may indicate complex transforms that cannot be predicted."
            )

        if transform_result.fallback_used:
            self.report.info(
                f"Complex transforms detected in {self.connector_manifest.name}. "
                f"Consider using 'generic_connectors' config for explicit mappings."
            )

        return lineages

    def _create_direct_lineages_from_topics(
        self, topics: List[str], parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Create direct lineages from topics without transforms."""
        return self.default_get_lineages(
            topic_prefix=parser.topic_prefix,
            database_name=parser.database_name,
            source_platform=parser.source_platform,
            topic_names=topics,
            include_source_dataset=True,
        )

    def _extract_lineages_unified(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """
        Configuration-based lineage extraction with forward pipeline fallback.

        Primary approach: Derive topics from connector configuration (most reliable)
        Fallback: Forward pipeline for complex transform scenarios
        """
        try:
            # Primary: Configuration-based topic derivation
            config_derived_topics = self._derive_topics_from_config()
            if config_derived_topics:
                return self._create_lineages_from_config_topics(
                    config_derived_topics, parser.database_name, parser.source_platform
                )

            # Fallback: Forward pipeline for complex scenarios
            if parser.transforms and self._has_predictable_transforms(
                parser.transforms
            ):
                return self._extract_lineages_with_forward_pipeline(parser)

            # Final fallback: Basic lineage extraction
            return self.default_get_lineages(
                database_name=parser.database_name,
                source_platform=parser.source_platform,
                topic_prefix=parser.topic_prefix,
            )

        except Exception as e:
            logger.warning(
                f"Lineage extraction failed for connector {self.connector_manifest.name}: {e}"
            )
            return self.default_get_lineages(
                database_name=parser.database_name,
                source_platform=parser.source_platform,
                topic_prefix=parser.topic_prefix,
            )

    def _derive_topics_from_config(self) -> List[str]:
        """Extract topics directly from connector configuration - most reliable approach."""
        config = self.connector_manifest.config

        # Call own get_topics_from_config method directly to avoid creating new instance
        config_topics = self.get_topics_from_config()

        if config_topics:
            # Apply predictable transforms to get final topic names
            return self._apply_predictable_transforms(config_topics, config)

        return []

    def _apply_predictable_transforms(
        self, topics: List[str], config: Dict[str, str]
    ) -> List[str]:
        """Apply transforms we can predict reliably from configuration."""
        result_topics = topics[:]
        transforms = self._get_predictable_transforms(config)

        for transform_config in transforms:
            transform_type = transform_config.get("type", "")
            if transform_type == "org.apache.kafka.connect.transforms.RegexRouter":
                regex_pattern = transform_config.get("regex", "")
                replacement = transform_config.get("replacement", "")
                if regex_pattern and replacement:
                    result_topics = self._apply_regex_to_topics(
                        result_topics, regex_pattern, replacement
                    )
            # Skip EventRouter and other complex transforms - too unpredictable

        return result_topics

    def _get_predictable_transforms(
        self, config: Dict[str, str]
    ) -> List[Dict[str, str]]:
        """Get only transforms that can be reliably predicted from configuration."""
        predictable_types = {
            "org.apache.kafka.connect.transforms.RegexRouter",
            "io.confluent.connect.cloud.transforms.TopicRegexRouter",
        }

        all_transforms = self._parse_all_transforms(config)
        return [t for t in all_transforms if t.get("type") in predictable_types]

    def _has_predictable_transforms(self, transforms: List[Dict[str, str]]) -> bool:
        """Check if connector has transforms we can handle predictably."""
        predictable_types = {
            "org.apache.kafka.connect.transforms.RegexRouter",
            "io.confluent.connect.cloud.transforms.TopicRegexRouter",
        }
        return any(t.get("type") in predictable_types for t in transforms)

    def _apply_regex_to_topics(
        self, topics: List[str], regex_pattern: str, replacement: str
    ) -> List[str]:
        """Apply regex transform to list of topics."""
        result = []
        for topic in topics:
            try:
                # Use Java regex for exact Kafka Connect compatibility
                from java.util.regex import Pattern

                transform_regex = Pattern.compile(regex_pattern)
                matcher = transform_regex.matcher(topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(replacement))
                    logger.debug(f"RegexRouter: {topic} -> {transformed_topic}")
                    result.append(transformed_topic)
                else:
                    logger.debug(f"RegexRouter: {topic} (no match)")
                    result.append(topic)

            except ImportError:
                logger.warning(
                    f"Java regex library not available for RegexRouter transform. "
                    f"Cannot apply pattern '{regex_pattern}' to topic '{topic}'. "
                    f"Returning original topic name."
                )
                result.append(topic)

            except Exception as e:
                logger.warning(
                    f"RegexRouter failed for topic '{topic}' with pattern '{regex_pattern}' "
                    f"and replacement '{replacement}': {e}"
                )
                result.append(topic)

        return result

    def _parse_all_transforms(self, config: Dict[str, str]) -> List[Dict[str, str]]:
        """Parse all transform configurations from connector config."""
        transforms_config = config.get("transforms", "")
        if not transforms_config:
            return []

        transform_names = parse_comma_separated_list(transforms_config)
        transforms = []

        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in config:
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = config[key]

        return transforms

    def _create_lineages_from_config_topics(
        self, topics: List[str], database_name: str, source_platform: str
    ) -> List[KafkaConnectLineage]:
        """Create lineages from configuration-derived topics."""
        lineages = []

        for topic in topics:
            # For config-derived topics, we know the source table from the configuration
            # This is much more reliable than trying to reverse-engineer from topic names
            source_dataset = self._derive_source_dataset_from_topic(
                topic, database_name, source_platform
            )

            lineage = KafkaConnectLineage(
                source_dataset=source_dataset,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
            )
            lineages.append(lineage)

        return lineages

    def _derive_source_dataset_from_topic(
        self, topic: str, database_name: str, source_platform: str
    ) -> str:
        """Derive source dataset name from topic using connector-specific logic."""
        # This uses the same logic as the handlers but in reverse
        # Remove topic prefix and reconstruct source table name
        config = self.connector_manifest.config
        topic_prefix = config.get("topic.prefix", "") or config.get(
            "database.server.name", ""
        )

        if topic_prefix and topic.startswith(topic_prefix):
            table_part = topic[len(topic_prefix) :].lstrip(".")
            return get_dataset_name(database_name, table_part)
        else:
            return get_dataset_name(database_name, topic)

    def _extract_lineages_with_forward_pipeline(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Forward pipeline approach for complex transform scenarios."""
        if self._has_complex_transforms(parser.transforms):
            self.report.warning(
                f"Connector {self.connector_manifest.name} uses complex transforms "
                f"that cannot be reliably predicted. For accurate lineage, consider using "
                f"'generic_connectors' config to specify explicit source mappings."
            )

        # Use basic extraction as forward pipeline fallback
        return self.default_get_lineages(
            database_name=parser.database_name,
            source_platform=parser.source_platform,
            topic_prefix=parser.topic_prefix,
        )

    def _has_complex_transforms(self, transforms: List[Dict[str, str]]) -> bool:
        """Check if connector has complex transforms that can't be predicted."""
        complex_types = {
            "io.debezium.transforms.outbox.EventRouter",  # Context-dependent
        }
        return any(t.get("type") in complex_types for t in transforms)

    def _handle_query_based_connector(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Handle connectors with custom queries (can't determine source tables)."""
        lineages = []
        for topic in self.connector_manifest.topic_names:
            lineage = KafkaConnectLineage(
                source_dataset=None,
                source_platform=parser.source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
            )
            lineages.append(lineage)

        self.report.warning(
            "Could not find input dataset, the connector has query configuration set",
            self.connector_manifest.name,
        )
        return lineages

    def infer_mappings(
        self,
        all_topics: Optional[List[str]] = None,
        consumer_groups: Optional[Dict[str, List[str]]] = None,
    ) -> List[KafkaConnectLineage]:
        """
        Infer direct source mappings for JDBC connectors using enhanced topic matching.

        Provides improved lineage extraction by:
        - Direct configuration analysis
        - Advanced topic prefix pattern matching against actual Kafka topics
        - Multi-level naming hierarchy support (database.schema.table)

        Args:
            all_topics: Complete list of topics from Kafka cluster

        Returns:
            List of inferred lineage mappings with enhanced accuracy
        """
        config = self.connector_manifest.config
        lineages: List[KafkaConnectLineage] = []

        try:
            # Extract source platform from JDBC URL
            source_platform = self._extract_platform_from_jdbc_url(
                config.get("connection.url", "")
            )
            if source_platform == "unknown":
                logger.warning(
                    f"Could not determine source platform for JDBC connector {self.connector_manifest.name}"
                )
                return lineages

            # Handle single table mode
            if config.get("kafka.topic"):
                topic_name = config["kafka.topic"]
                table_name = config.get(
                    "table.name", config.get("query", "unknown_table")
                )

                lineage = KafkaConnectLineage(
                    source_platform=source_platform,
                    source_dataset=self._extract_dataset_name_from_config(
                        config, table_name
                    ),
                    target_platform=KAFKA,
                    target_dataset=topic_name,
                    job_property_bag={
                        "connector_type": "jdbc_source",
                        "mode": "single_table",
                        "inference_method": "direct",
                    },
                )
                lineages.append(lineage)
                return lineages

            # Handle multi-table mode with topic prefix
            topic_prefix = config.get("topic.prefix", "")
            table_config = config.get("table.include.list") or config.get(
                "table.whitelist"
            )

            if table_config:
                from datahub.ingestion.source.kafka_connect.common import (
                    parse_comma_separated_with_quotes,
                    parse_table_identifier,
                )

                table_names = parse_comma_separated_with_quotes(table_config)

                for table_name in table_names:
                    clean_table = parse_table_identifier(table_name)
                    predicted_topic = (
                        f"{topic_prefix}{clean_table}" if topic_prefix else clean_table
                    )

                    # Validate against actual topics if available
                    if all_topics and predicted_topic not in all_topics:
                        # Try enhanced pattern matching
                        matched_topics = self._match_topics_by_advanced_patterns(
                            all_topics, topic_prefix, [clean_table]
                        )
                        if matched_topics:
                            predicted_topic = matched_topics[0]  # Use first match
                        else:
                            logger.debug(
                                f"Topic {predicted_topic} not found in cluster topics for table {clean_table}"
                            )
                            continue

                    lineage = KafkaConnectLineage(
                        source_platform=source_platform,
                        source_dataset=self._extract_dataset_name_from_config(
                            config, clean_table
                        ),
                        target_platform=KAFKA,
                        target_dataset=predicted_topic,
                        job_property_bag={
                            "connector_type": "jdbc_source",
                            "mode": "multi_table",
                            "inference_method": "enhanced",
                        },
                    )
                    lineages.append(lineage)

            # Fallback: Advanced prefix pattern matching against actual topics
            elif topic_prefix and all_topics:
                matching_topics = self._find_topics_by_advanced_prefix_patterns(
                    all_topics, topic_prefix, config
                )
                for topic in matching_topics:
                    # Extract table name from topic with bounds checking
                    if topic_prefix and len(topic) > len(topic_prefix):
                        table_name = topic[len(topic_prefix) :]
                    elif not topic_prefix:
                        table_name = topic
                    else:
                        # Skip topics shorter than the prefix (shouldn't happen but be safe)
                        logger.debug(
                            f"Skipping topic '{topic}' shorter than prefix '{topic_prefix}'"
                        )
                        continue
                    lineage = KafkaConnectLineage(
                        source_platform=source_platform,
                        source_dataset=self._extract_dataset_name_from_config(
                            config, table_name
                        ),
                        target_platform=KAFKA,
                        target_dataset=topic,
                        job_property_bag={
                            "connector_type": "jdbc_source",
                            "mode": "inferred",
                            "inference_method": "pattern_matching",
                        },
                    )
                    lineages.append(lineage)

        except Exception as e:
            logger.warning(
                f"Failed to infer JDBC source mappings for {self.connector_manifest.name}: {e}"
            )

        return lineages

    def _extract_platform_from_jdbc_url(self, jdbc_url: str) -> str:
        """Extract platform from JDBC connection URL."""
        if not validate_jdbc_url(jdbc_url):
            return "unknown"

        try:
            # Remove jdbc: prefix and extract protocol
            remaining_url = jdbc_url[JDBC_PREFIX_LENGTH:]  # Remove "jdbc:" prefix
            protocol_end = remaining_url.find(":")
            if protocol_end == -1:
                return "unknown"

            protocol = remaining_url[:protocol_end].lower()
            return "postgres" if protocol == "postgresql" else protocol
        except Exception:
            return "unknown"

    def _extract_dataset_name_from_config(
        self, config: Dict[str, str], table_name: str
    ) -> str:
        """Construct dataset name considering database and schema hierarchy."""
        database = config.get("database.name")
        schema = config.get("schema.name") or config.get("database.default.schema")

        if database and schema:
            return f"{database}.{schema}.{table_name}"
        elif database:
            return f"{database}.{table_name}"
        else:
            return table_name

    def _match_topics_by_advanced_patterns(
        self, all_topics: List[str], topic_prefix: str, table_names: List[str]
    ) -> List[str]:
        """Enhanced topic matching with JDBC-specific pattern."""

        matched_topics = []

        for table_name in table_names:
            # JDBC Source uses simple concatenation: topic_prefix + table_name
            # Based on official implementation: topic = topicPrefix + name
            predicted_topic = (
                f"{topic_prefix}{table_name}" if topic_prefix else table_name
            )

            if predicted_topic in all_topics:
                matched_topics.append(predicted_topic)

        return matched_topics

    def _find_topics_by_advanced_prefix_patterns(
        self, all_topics: List[str], topic_prefix: str, config: Dict[str, str]
    ) -> List[str]:
        """
        Find topics using JDBC-specific prefix patterns and configuration hints.

        KAFKA CONNECT TOPIC NAMING PATTERNS BY CONNECTOR TYPE:

        **JDBC Source Connector (this method):**
        - Pattern: `{topic.prefix}{table}` (simple concatenation, NO separators)
        - Reference: https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/source/BulkTableQuerier.java
        - Java code: `topic = topicPrefix + tableId.tableName()`
        - Example: topic.prefix="db_" + table="users" → "db_users"

        Args:
            all_topics: Complete list of topics from Kafka cluster
            topic_prefix: Topic prefix from connector config (topic.prefix)
            config: Full connector configuration for additional hints

        Returns:
            List of topics matching JDBC-specific naming patterns
        """
        matching_topics = []

        # Basic JDBC prefix matching: topic_prefix + anything
        # This catches all topics that start with the configured prefix
        for topic in all_topics:
            if topic.startswith(topic_prefix):
                matching_topics.append(topic)

        # Enhanced JDBC database pattern matching
        # JDBC connectors can have database context in topic names
        # Pattern: topic_prefix + database + table (all concatenated, no separators)
        database = config.get("database.name", "")
        if database and topic_prefix:
            # Look for topics that follow JDBC database naming: topic_prefix + database + table
            # Example: prefix="db_" + database="prod" + table="users" → "db_produsers"
            database_pattern = f"{topic_prefix}{database}"

            for topic in all_topics:
                if topic.startswith(database_pattern) and topic not in matching_topics:
                    matching_topics.append(topic)

        return matching_topics

    def get_platform(self) -> str:
        """
        Get platform for JDBC connector.

        JDBC connectors can connect to multiple databases, so platform is inferred from
        the connection URL in the connector configuration.
        """
        try:
            parser = self.get_parser(self.connector_manifest)
            return parser.source_platform
        except Exception as e:
            logger.debug(f"Failed to get platform from parser: {e}")
            # If parser fails, try to infer from JDBC URL directly
            jdbc_url = self.connector_manifest.config.get("connection.url", "")
            if jdbc_url:
                return self._extract_platform_from_jdbc_url(jdbc_url)
            return "unknown"


@dataclass
class SnowflakeSourceConnector(BaseConnector):
    """
    Confluent Cloud Snowflake Source Connector.

    Reference: https://docs.confluent.io/cloud/current/connectors/cc-snowflake-source.html

    This connector uses JDBC-style polling (not CDC) to read from Snowflake tables.
    Topic naming: <topic.prefix><database.schema.tableName>
    """

    _cached_expanded_tables: Optional[List[str]] = field(default=None, init=False)

    @dataclass
    class SnowflakeSourceParser:
        source_platform: str
        database_name: Optional[str]
        topic_prefix: str
        table_names: List[str]
        transforms: List[Dict[str, str]]

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> SnowflakeSourceParser:
        """Parse Snowflake Source connector configuration."""
        config = connector_manifest.config

        # Extract table names from table.include.list
        table_config = config.get("table.include.list") or config.get(
            "table.whitelist", ""
        )
        table_names = parse_comma_separated_list(table_config) if table_config else []

        # Extract database name from connection.url or snowflake.database.name
        database_name = config.get("snowflake.database.name") or config.get(
            "database.name"
        )

        # Topic prefix (used in topic naming pattern)
        topic_prefix = config.get("topic.prefix", "")

        # Parse transforms
        transforms_config = config.get("transforms", "")
        transform_names = (
            parse_comma_separated_list(transforms_config) if transforms_config else []
        )

        transforms = []
        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in config:
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = config[key]

        parser = self.SnowflakeSourceParser(
            source_platform="snowflake",
            database_name=database_name,
            topic_prefix=topic_prefix,
            table_names=table_names,
            transforms=transforms,
        )

        return parser

    def get_topics_from_config(self) -> List[str]:
        """
        Extract expected topics from Snowflake Source connector configuration.

        This method performs pattern expansion early so that the manifest's topic_names
        contains the actual expanded topics rather than patterns. This allows the
        subsequent lineage extraction to correctly match topics.
        """
        try:
            parser = self.get_parser(self.connector_manifest)
            topic_prefix = parser.topic_prefix
            table_names = parser.table_names

            # Check if any table names contain patterns
            has_patterns = any(self._is_pattern(table) for table in table_names)

            # If patterns exist, expand them using DataHub schema resolver
            if has_patterns:
                if self.schema_resolver:
                    logger.info(
                        f"Expanding table patterns in get_topics_from_config for connector '{self.connector_manifest.name}'"
                    )
                    expanded_tables = self._expand_table_patterns(
                        table_names, parser.source_platform, parser.database_name
                    )
                    if expanded_tables:
                        # Cache expanded tables for reuse in extract_lineages
                        self._cached_expanded_tables = expanded_tables
                        table_names = expanded_tables
                        logger.info(
                            f"Expanded patterns to {len(expanded_tables)} tables for topic derivation"
                        )
                    else:
                        logger.warning(
                            f"No tables found matching patterns for connector '{self.connector_manifest.name}'"
                        )
                        # Cache empty list to signal that expansion was attempted but found nothing
                        self._cached_expanded_tables = []
                        return []
                else:
                    # Patterns detected but no schema resolver - cannot expand
                    logger.warning(
                        f"Table patterns detected for connector '{self.connector_manifest.name}' "
                        f"but schema resolver is not available. Cannot derive topics from patterns."
                    )
                    # Don't cache anything - let extract_lineages handle the warning
                    return []
            else:
                # No patterns - just lowercase explicit table names to match DataHub normalization
                table_names = [table.lower() for table in table_names]
                # Cache the lowercased explicit tables
                self._cached_expanded_tables = table_names

            # Snowflake Source topics follow pattern: {topic_prefix}{database.schema.table}
            topics = []
            for table_name in table_names:
                # Topic name is prefix + full table identifier
                if topic_prefix:
                    topic_name = f"{topic_prefix}{table_name}"
                else:
                    topic_name = table_name
                topics.append(topic_name)

            # Apply transforms if configured
            if parser.transforms:
                logger.debug(
                    f"Applying {len(parser.transforms)} transforms to {len(topics)} derived topics for connector '{self.connector_manifest.name}'"
                )
                result = get_transform_pipeline().apply_forward(
                    topics, self.connector_manifest.config
                )
                if result.warnings:
                    for warning in result.warnings:
                        logger.warning(
                            f"Transform warning for {self.connector_manifest.name}: {warning}"
                        )
                topics = result.topics
                logger.info(
                    f"Topics after transforms for '{self.connector_manifest.name}': {topics[:10] if len(topics) <= 10 else f'{topics[:10]}... ({len(topics)} total)'}"
                )

            return topics
        except Exception as e:
            logger.debug(
                f"Failed to derive topics from Snowflake Source connector config: {e}"
            )
            return []

    def _is_pattern(self, table_name: str) -> bool:
        """
        Check if table name contains wildcard pattern characters.

        IMPORTANT: Snowflake Source connector uses SIMPLE WILDCARD MATCHING, not Java regex.
        Supported wildcards:
        - "*" matches any sequence of characters (zero or more)
        - "?" matches any single character

        This is DIFFERENT from Debezium connectors which use full Java regex.

        Examples:
        - "ANALYTICS.PUBLIC.*" matches all tables in ANALYTICS.PUBLIC schema
        - "*.PUBLIC.TABLE1" matches TABLE1 in PUBLIC schema across all databases
        - "DB.SCHEMA.USER?" matches USER1, USERS, etc.

        Note: Without DataHub schema resolver, we cannot expand these patterns.
        """
        # Check for wildcard characters (simple patterns only, not full regex)
        # We check for all regex chars to detect if user accidentally used Java regex syntax
        pattern_chars = [
            "*",
            "+",
            "?",
            "[",
            "]",
            "(",
            ")",
            "|",
            "{",
            "}",
            "^",
            "$",
            "\\",
        ]
        return any(char in table_name for char in pattern_chars)

    def _expand_table_patterns(
        self,
        table_patterns: List[str],
        source_platform: str,
        database_name: Optional[str],
    ) -> List[str]:
        """
        Expand table patterns using DataHub schema metadata.

        Examples:
        - "ANALYTICS.PUBLIC.*" → ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS", ...]
        - "*.PUBLIC.TABLE1" → ["DB1.PUBLIC.TABLE1", "DB2.PUBLIC.TABLE1", ...]
        - "ANALYTICS.PUBLIC.USERS" → ["ANALYTICS.PUBLIC.USERS"] (no expansion)

        Args:
            table_patterns: List of table patterns from connector config
            source_platform: Source platform (should be 'snowflake')
            database_name: Database name for context (optional)

        Returns:
            List of fully expanded table names
        """
        if not self.schema_resolver:
            logger.warning(
                f"SchemaResolver not available for connector {self.connector_manifest.name} - cannot expand patterns"
            )
            return []

        expanded_tables = []

        for pattern in table_patterns:
            # Check if pattern needs expansion (contains regex special characters)
            if self._is_pattern(pattern):
                logger.info(
                    f"Expanding pattern '{pattern}' using DataHub schema metadata"
                )
                tables = self._query_tables_from_datahub(
                    pattern, source_platform, database_name
                )
                if tables:
                    logger.info(
                        f"Pattern expansion: '{pattern}' -> {len(tables)} tables found"
                    )
                    logger.debug(f"Expanded tables: {tables}")
                    expanded_tables.extend(tables)
                else:
                    logger.warning(
                        f"Pattern '{pattern}' did not match any tables in DataHub"
                    )
            else:
                # Already explicit table name - no expansion needed
                # Lowercase to match DataHub's normalization
                expanded_tables.append(pattern.lower())

        return expanded_tables

    def _query_tables_from_datahub(
        self,
        pattern: str,
        platform: str,
        database: Optional[str],
    ) -> List[str]:
        """
        Query DataHub for Snowflake tables matching the given pattern.

        Args:
            pattern: Pattern (e.g., "ANALYTICS.PUBLIC.*", "*.PUBLIC.USERS")
            platform: Source platform (should be "snowflake")
            database: Database name for context

        Returns:
            List of matching table names
        """
        if not self.schema_resolver or not self.schema_resolver.graph:
            return []

        try:
            # Query DataHub directly for tables matching the platform
            # SchemaResolver's cache may be empty, so we use its graph connection directly
            all_urns = list(
                self.schema_resolver.graph.get_urns_by_filter(
                    platform=platform,
                    env=self.schema_resolver.env,
                    entity_types=["dataset"],
                )
            )

            if not all_urns:
                logger.debug(
                    f"No {platform} datasets found in DataHub for pattern expansion"
                )
                return []

            matched_tables = []

            # Convert pattern to Python regex
            # Snowflake patterns are simpler than Debezium (just basic wildcard matching)
            # Convert SQL-style patterns to Python regex:
            # - "*" (any characters) → ".*"
            # - "?" (single character) → "."
            # Note: DataHub normalizes Snowflake table names to lowercase in URNs,
            # so we lowercase the pattern to match
            normalized_pattern = pattern.lower()
            regex_pattern = (
                normalized_pattern.replace(".", r"\.")
                .replace("*", ".*")
                .replace("?", ".")
            )
            regex = re.compile(regex_pattern)

            # TODO: Performance optimization - This loops through ALL datasets in DataHub
            # for the platform without filtering. For large DataHub instances with thousands
            # of tables, this could be very slow. Consider using graph.get_urns_by_filter()
            # with more specific filters or implementing pagination.
            for urn in all_urns:
                # URN format: urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)
                table_name = self._extract_table_name_from_urn(urn)
                if not table_name:
                    continue

                # Check if URN is for Snowflake platform
                if f"dataplatform:{platform.lower()}" not in urn.lower():
                    continue

                # Try pattern match (table_name from DataHub is already lowercase)
                if regex.fullmatch(table_name):
                    matched_tables.append(table_name)

            logger.debug(
                f"Pattern '{pattern}' matched {len(matched_tables)} tables from DataHub"
            )
            return matched_tables

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to connect to DataHub for pattern '{pattern}': {e}")
            if self.report:
                self.report.report_failure(
                    f"datahub_connection_{self.connector_manifest.name}", str(e)
                )
            return []
        except Exception as e:
            logger.warning(
                f"Failed to query tables from DataHub for pattern '{pattern}': {e}",
                exc_info=True,
            )
            return []

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """
        Extract lineage mappings from Snowflake tables to Kafka topics.

        This method always uses table.include.list from config as the source of truth.
        When manifest.topic_names is available (Kafka API accessible), it filters
        lineages to only topics that exist in Kafka. When unavailable (Confluent Cloud,
        air-gapped), it creates lineages for all configured tables without validating
        that derived topic names actually exist in Kafka.
        """
        parser = self.get_parser(self.connector_manifest)
        lineages: List[KafkaConnectLineage] = []

        logging.debug(
            f"Extracting lineages for Snowflake Source connector: "
            f"platform={parser.source_platform}, database={parser.database_name}"
        )

        # Get table names from config (cached from get_topics_from_config if available)
        if self._cached_expanded_tables is not None:
            table_names = self._cached_expanded_tables
            if not table_names:
                logger.debug(
                    f"Pattern expansion found no matching tables for connector '{self.connector_manifest.name}'"
                )
                return []
            logger.debug(
                f"Reusing {len(table_names)} cached expanded tables from get_topics_from_config()"
            )
        else:
            # Expand patterns if not already cached
            table_names = parser.table_names
            if not table_names:
                logger.debug(
                    "No table.include.list configuration found for Snowflake Source connector"
                )
                return []

            # Check if any table names contain patterns
            has_patterns = any(self._is_pattern(table) for table in table_names)

            # If patterns exist but schema resolver is not available, skip processing
            if has_patterns and not self.schema_resolver:
                self.report.warning(
                    f"Snowflake Source connector '{self.connector_manifest.name}' has table patterns "
                    f"in table.include.list but DataHub schema resolver is not available. "
                    f"Skipping lineage extraction to avoid generating invalid URNs. "
                    f"Enable 'use_schema_resolver' in config to support pattern expansion."
                )
                logger.warning(
                    f"Skipping lineage extraction for connector '{self.connector_manifest.name}' - "
                    f"patterns detected but schema resolver unavailable"
                )
                return []

            # If patterns exist and schema resolver is available, expand them
            if has_patterns and self.schema_resolver:
                logger.info(
                    f"Expanding table patterns for Snowflake Source connector '{self.connector_manifest.name}'"
                )
                table_names = self._expand_table_patterns(
                    table_names, parser.source_platform, parser.database_name
                )
                if not table_names:
                    logger.warning(
                        f"No tables found matching patterns for connector '{self.connector_manifest.name}'"
                    )
                    return []
            else:
                # No patterns - lowercase explicit table names
                table_names = [table.lower() for table in table_names]

        topic_prefix = parser.topic_prefix
        has_kafka_topics = bool(self.connector_manifest.topic_names)

        if not has_kafka_topics:
            logger.info(
                f"Kafka topics API not available for connector '{self.connector_manifest.name}' - "
                f"creating lineages for all {len(table_names)} configured tables without validating "
                f"that derived topic names actually exist in Kafka"
            )

        # Derive expected topics and apply transforms
        for table_name in table_names:
            # Build expected base topic name
            if topic_prefix:
                expected_topic = f"{topic_prefix}{table_name}"
            else:
                expected_topic = table_name

            # Apply transforms if configured
            if parser.transforms:
                result = get_transform_pipeline().apply_forward(
                    [expected_topic], self.connector_manifest.config
                )
                if result.warnings:
                    for warning in result.warnings:
                        logger.warning(
                            f"Transform warning for {self.connector_manifest.name}: {warning}"
                        )
                if result.topics and len(result.topics) > 0:
                    expected_topic = result.topics[0]

            # Filter by Kafka topics if available
            if (
                has_kafka_topics
                and expected_topic not in self.connector_manifest.topic_names
            ):
                logger.debug(
                    f"Expected topic '{expected_topic}' not found in Kafka - skipping lineage for table '{table_name}'"
                )
                continue

            # Extract column-level lineage if enabled
            fine_grained = self._extract_fine_grained_lineage(
                source_dataset=table_name,
                source_platform=parser.source_platform,
                target_dataset=expected_topic,
                target_platform=KAFKA,
            )

            # Create lineage mapping
            lineage = KafkaConnectLineage(
                source_dataset=table_name,
                source_platform=parser.source_platform,
                target_dataset=expected_topic,
                target_platform=KAFKA,
                fine_grained_lineages=fine_grained,
            )
            lineages.append(lineage)
            logger.debug(f"Created lineage: {table_name} -> {expected_topic}")

        logger.info(
            f"Created {len(lineages)} lineages for Snowflake connector '{self.connector_manifest.name}'"
        )
        return lineages

    def extract_flow_property_bag(self) -> Dict[str, str]:
        """Extract flow properties, masking sensitive information."""
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k
            not in [
                "connection.password",
                "connection.user",
                "snowflake.private.key",
                "snowflake.private.key.passphrase",
            ]
        }

        return flow_property_bag

    @staticmethod
    def supports_connector_class(connector_class: str) -> bool:
        """Check if this connector handles Snowflake Source."""
        return connector_class == SNOWFLAKE_SOURCE_CLOUD

    def get_platform(self) -> str:
        """Get the platform for Snowflake Source connector."""
        return "snowflake"


@dataclass
class MongoSourceConnector(BaseConnector):
    # https://www.mongodb.com/docs/kafka-connector/current/source-connector/

    @dataclass
    class MongoSourceParser:
        db_connection_url: Optional[str]
        source_platform: str
        database_name: Optional[str]
        topic_prefix: Optional[str]
        transforms: List[str]

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> MongoSourceParser:
        parser = self.MongoSourceParser(
            db_connection_url=connector_manifest.config.get("connection.uri"),
            source_platform="mongodb",
            database_name=connector_manifest.config.get("database"),
            topic_prefix=connector_manifest.config.get("topic.prefix"),
            transforms=(
                parse_comma_separated_list(connector_manifest.config["transforms"])
                if "transforms" in connector_manifest.config
                else []
            ),
        )

        return parser

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        topic_prefix = parser.topic_prefix or ""

        # Escape topic_prefix to handle cases where it contains dots
        # Some users configure topic.prefix like "my.mongodb" which breaks the regex

        # \w is equivalent to [a-zA-Z0-9_]
        # So [\w-]+ matches alphanumeric characters, underscores, and hyphens
        topic_naming_pattern = rf"{re.escape(topic_prefix)}\.([\w-]+)\.([\w-]+)"

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = get_dataset_name(found.group(1), found.group(2))

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform=KAFKA,
                )
                lineages.append(lineage)
        return lineages

    def get_platform(self) -> str:
        """Get the platform for Mongo Source connector."""
        return "mongodb"


@dataclass
class DebeziumSourceConnector(BaseConnector):
    # Debezium topic naming patterns by connector type
    # - MySQL: {topic.prefix}.{database}.{table}
    # - PostgreSQL: {topic.prefix}.{schema}.{table}
    # - SQL Server: {topic.prefix}.{database}.{schema}.{table}
    # - Oracle: {topic.prefix}.{schema}.{table}
    # - DB2: {topic.prefix}.{schema}.{table}
    # - MongoDB: {topic.prefix}.{database}.{collection}
    # - Vitess: {topic.prefix}.{keyspace}.{table}

    # Note SQL Server allows for "database.names" (multiple databases) config,
    # and so database is in the topic naming pattern.
    # However, others have "database.dbname" which is a single database name. For these connectors,
    # additional databases would require a different connector instance

    # Use imported constants - connectors with 2-level container in pattern (database + schema)
    DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER_IN_PATTERN = (
        DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER
    )

    @dataclass
    class DebeziumParser:
        source_platform: str
        server_name: Optional[str]
        database_name: Optional[str]
        transforms: List[Dict[str, str]]

    def get_pattern_matcher(self) -> PatternMatcher:
        """
        Get the pattern matcher for this connector type.

        Debezium connectors use Java regex for table.include.list and table.exclude.list,
        which provides full regex capabilities including character classes, alternation,
        quantifiers, etc.

        This differs from Snowflake Source connectors which use simple wildcard matching.
        """
        return JavaRegexMatcher()

    def get_server_name(self, connector_manifest: ConnectorManifest) -> str:
        """Get the server name (topic prefix) for Debezium connectors.

        V2 connectors (PostgresCdcSourceV2, MySqlCdcSourceV2) use topic.prefix.
        V1 connectors use database.server.name with fallback to topic.prefix.

        References:
            - PostgresCdcSourceV2: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
            - MySqlCdcSourceV2: https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-v2-debezium/cc-mysql-source-cdc-v2-debezium.html
        """
        connector_class = connector_manifest.config.get("connector.class", "")

        # V2 connectors use topic.prefix
        if connector_class in (POSTGRES_CDC_SOURCE_V2_CLOUD, MYSQL_CDC_SOURCE_V2_CLOUD):
            return connector_manifest.config.get("topic.prefix", "")

        # V1 connectors use topic.prefix with fallback to database.server.name
        # topic.prefix is the canonical Debezium configuration for topic naming
        return connector_manifest.config.get(
            "topic.prefix", connector_manifest.config.get("database.server.name", "")
        )

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> DebeziumParser:
        # Map connector class to platform
        platform = self.get_platform()

        # Map handler platform to parser platform (handler uses "sqlserver", parser expects "mssql")
        parser_platform = "mssql" if platform == "sqlserver" else platform

        # Get database name based on platform
        database_name = self._get_database_name_for_platform(
            platform, connector_manifest.config
        )

        # Parse transforms
        config = connector_manifest.config
        transforms_config = config.get("transforms", "")
        transform_names = (
            parse_comma_separated_list(transforms_config) if transforms_config else []
        )

        transforms = []
        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in config:
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = config[key]

        return self.DebeziumParser(
            source_platform=parser_platform,
            server_name=self.get_server_name(connector_manifest),
            database_name=database_name,
            transforms=transforms,
        )

    def get_topics_from_config(self) -> List[str]:
        """Extract expected topics from Debezium connector configuration.

        Returns base topic names derived from connector configuration (e.g., server.schema.table).
        These are the topic names BEFORE any transforms are applied.

        Note: If transforms are configured (e.g., RegexRouter), the actual Kafka topics
        will differ from these base names. Transform application happens separately during
        lineage extraction.

        Returns:
            List of base topic names derived from connector config.
        """
        logger.debug(
            f"DebeziumSourceConnector.get_topics_from_config called for '{self.connector_manifest.name}'"
        )

        config = self.connector_manifest.config

        try:
            logger.debug(
                f"Debezium connector '{self.connector_manifest.name}' config keys: {list(config.keys())}"
            )

            # Get parser to extract database info
            parser = self.get_parser(self.connector_manifest)
            database_name = parser.database_name
            source_platform = parser.source_platform
            server_name = parser.server_name

            logger.debug(
                f"Debezium connector '{self.connector_manifest.name}' - "
                f"database='{database_name}', platform='{source_platform}', server_name='{server_name}'"
            )

            # Step 1: Get initial set of tables
            table_names = self._get_table_names_from_config_or_discovery(
                config, database_name, source_platform
            )
            if not table_names:
                return []

            # Step 2: Apply schema filters (if tables were discovered from database)
            if (
                self.schema_resolver
                and self.config.use_schema_resolver
                and database_name
            ):
                table_names = self._apply_schema_filters(config, table_names)
                if not table_names:
                    return []

            # Step 3: Apply table filters
            table_names = self._apply_table_filters(config, table_names)
            if not table_names:
                return []

            # Step 4: Derive topics from filtered tables
            topics = self._derive_topics_from_tables(table_names, server_name)

            logger.info(
                f"Derived {len(topics)} topics from Debezium connector '{self.connector_manifest.name}' config: "
                f"{topics[:10] if len(topics) <= 10 else f'{topics[:10]}... ({len(topics)} total)'}"
            )
            return topics
        except Exception as e:
            logger.warning(
                f"Failed to derive topics from Debezium connector '{self.connector_manifest.name}' config: {e}",
                exc_info=True,
            )
            return []

    def _get_table_names_from_config_or_discovery(
        self, config: Dict[str, Any], database_name: Optional[str], source_platform: str
    ) -> List[str]:
        """Get table names either from config or by discovering from database.

        Args:
            config: Connector configuration
            database_name: Database name from connector config
            source_platform: Source platform (e.g., "postgres", "mysql")

        Returns:
            List of table names in "schema.table" format, or empty list if none found
        """
        if not self.schema_resolver or not self.config.use_schema_resolver:
            # SchemaResolver not available - fall back to table.include.list only
            table_config = config.get("table.include.list") or config.get(
                "table.whitelist"
            )
            if not table_config:
                logger.info(
                    f"No table.include.list found and SchemaResolver not available for connector '{self.connector_manifest.name}' - "
                    f"cannot derive topics from config"
                )
                return []

            table_names = parse_comma_separated_list(table_config)
            logger.debug(
                f"Using {len(table_names)} tables from config (SchemaResolver not available): {table_names[:5]}"
            )
            return table_names

        # SchemaResolver is available - use database.dbname to discover tables
        if not database_name:
            logger.warning(
                f"Cannot discover tables for connector '{self.connector_manifest.name}' - "
                f"database.dbname not configured"
            )
            # Fall back to table.include.list if no database name
            table_config = config.get("table.include.list") or config.get(
                "table.whitelist"
            )
            if not table_config:
                logger.info(
                    f"No database.dbname and no table.include.list for connector '{self.connector_manifest.name}'"
                )
                return []
            return parse_comma_separated_list(table_config)

        # Discover all tables from database
        logger.info(
            f"Discovering tables from database '{database_name}' using SchemaResolver for connector '{self.connector_manifest.name}'"
        )

        discovered_tables = self._discover_tables_from_database(
            database_name, source_platform
        )

        if not discovered_tables:
            logger.warning(
                f"No tables found in database '{database_name}' from SchemaResolver. "
                f"Make sure you've ingested {source_platform} datasets for database '{database_name}' "
                f"into DataHub before running Kafka Connect ingestion."
            )
            return []

        logger.info(
            f"Discovered {len(discovered_tables)} tables from database '{database_name}': "
            f"{discovered_tables[:10]}"
            + (
                f"... ({len(discovered_tables)} total)"
                if len(discovered_tables) > 10
                else ""
            )
        )

        return discovered_tables

    def _apply_schema_filters(
        self, config: Dict[str, Any], tables: List[str]
    ) -> List[str]:
        """Apply schema.include.list and schema.exclude.list filters to tables.

        Args:
            config: Connector configuration
            tables: List of table names in "schema.table" format

        Returns:
            Filtered list of table names
        """
        # Apply schema.include.list filter if it exists
        schema_include_config = config.get("schema.include.list")
        if schema_include_config:
            schema_include_patterns = parse_comma_separated_list(schema_include_config)
            logger.info(
                f"Applying schema.include.list filter with {len(schema_include_patterns)} patterns: {schema_include_patterns}"
            )

            # Filter by schema name (first part of "schema.table")
            filtered_tables = []
            for table in tables:
                # Extract schema name from "schema.table" format
                if "." in table:
                    schema_name = table.split(".")[0]
                    if self._matches_any_pattern(schema_name, schema_include_patterns):
                        filtered_tables.append(table)

            tables = filtered_tables
            logger.info(
                f"After schema.include.list filtering, {len(tables)} tables remain"
            )

            if not tables:
                logger.warning(
                    f"No tables matched schema.include.list patterns for connector '{self.connector_manifest.name}'"
                )
                return []

        # Apply schema.exclude.list filter if it exists
        schema_exclude_config = config.get("schema.exclude.list")
        if schema_exclude_config:
            schema_exclude_patterns = parse_comma_separated_list(schema_exclude_config)
            logger.info(
                f"Applying schema.exclude.list filter with {len(schema_exclude_patterns)} patterns: {schema_exclude_patterns}"
            )

            # Filter out tables whose schema matches exclude patterns
            before_count = len(tables)
            filtered_tables = []
            for table in tables:
                # Extract schema name from "schema.table" format
                if "." in table:
                    schema_name = table.split(".")[0]
                    if not self._matches_any_pattern(
                        schema_name, schema_exclude_patterns
                    ):
                        filtered_tables.append(table)
                else:
                    # Keep tables without schema separator
                    filtered_tables.append(table)

            tables = filtered_tables
            excluded_count = before_count - len(tables)
            logger.info(
                f"After schema.exclude.list filtering, excluded {excluded_count} tables, {len(tables)} tables remain"
            )

            if not tables:
                logger.warning(
                    f"All tables were excluded by schema.exclude.list for connector '{self.connector_manifest.name}'"
                )
                return []

        return tables

    def _apply_table_filters(
        self, config: Dict[str, Any], tables: List[str]
    ) -> List[str]:
        """Apply table.include.list and table.exclude.list filters to tables.

        Args:
            config: Connector configuration
            tables: List of table names in "schema.table" format

        Returns:
            Filtered list of table names
        """
        # Apply table.include.list filter if it exists
        table_config = config.get("table.include.list") or config.get("table.whitelist")

        if table_config:
            # Parse patterns from config
            table_patterns = parse_comma_separated_list(table_config)
            logger.info(
                f"Applying table.include.list filter with {len(table_patterns)} patterns: {table_patterns}"
            )

            # Filter tables using patterns
            filtered_tables = self._filter_tables_by_patterns(tables, table_patterns)

            logger.info(
                f"After include filtering, {len(filtered_tables)} tables match the patterns: "
                f"{filtered_tables[:10]}"
                + (
                    f"... ({len(filtered_tables)} total)"
                    if len(filtered_tables) > 10
                    else ""
                )
            )

            if not filtered_tables:
                logger.warning(
                    f"No tables matched the include patterns for connector '{self.connector_manifest.name}'"
                )
                return []

            tables = filtered_tables
        else:
            # No filter - use all tables
            logger.info(
                f"No table.include.list filter - using all {len(tables)} tables"
            )

        # Apply table.exclude.list filter if it exists
        exclude_config = config.get("table.exclude.list") or config.get(
            "table.blacklist"
        )

        if exclude_config:
            exclude_patterns = parse_comma_separated_list(exclude_config)
            logger.info(
                f"Applying table.exclude.list filter with {len(exclude_patterns)} patterns: {exclude_patterns}"
            )

            excluded_tables = self._filter_tables_by_patterns(tables, exclude_patterns)

            tables = [t for t in tables if t not in excluded_tables]

            logger.info(
                f"After exclude filtering, {len(tables)} tables remain: "
                f"{tables[:10]}"
                + (f"... ({len(tables)} total)" if len(tables) > 10 else "")
            )

            if not tables:
                logger.warning(
                    f"All tables were excluded by table.exclude.list for connector '{self.connector_manifest.name}'"
                )
                return []

        return tables

    def _derive_topics_from_tables(
        self, table_names: List[str], server_name: Optional[str]
    ) -> List[str]:
        """Derive Kafka topic names from table names.

        Debezium topics follow pattern: {server_name}.{schema.table}

        Args:
            table_names: List of table names in "schema.table" format
            server_name: Server name from connector config

        Returns:
            List of derived topic names
        """
        topics = []
        for table_name in table_names:
            # Table name already includes schema prefix (e.g., "public.users")
            topic_name = f"{server_name}.{table_name}" if server_name else table_name
            topics.append(topic_name)

        return topics

    def _filter_tables_by_patterns(
        self, tables: List[str], patterns: List[str]
    ) -> List[str]:
        """
        Filter tables by matching against patterns using the connector's pattern matcher.

        This method uses the connector-specific pattern matcher (Java regex for Debezium)
        to filter table names.

        Args:
            tables: List of table names to filter (e.g., ["public.users", "public.orders"])
            patterns: List of patterns from table.include.list or table.exclude.list

        Returns:
            List of tables that match at least one pattern
        """
        # Get the appropriate pattern matcher for this connector type
        matcher = self.get_pattern_matcher()

        # Use the matcher's filter_matches method
        return matcher.filter_matches(patterns, tables)

    def _matches_any_pattern(self, text: str, patterns: List[str]) -> bool:
        """
        Check if text matches any of the given patterns using the connector's pattern matcher.

        Args:
            text: Text to check (e.g., schema name like "public")
            patterns: List of patterns to match against (Java regex for Debezium)

        Returns:
            True if text matches at least one pattern, False otherwise
        """
        matcher = self.get_pattern_matcher()
        return any(matcher.matches(pattern, text) for pattern in patterns)

    def _get_database_name_for_platform(
        self, platform: str, config: Dict[str, str]
    ) -> Optional[str]:
        """Get database name based on platform-specific configuration."""
        if platform in ["mysql", "mongodb"]:
            return None
        elif platform in ["sqlserver", "mssql"]:
            database_name = config.get("database.names") or config.get(
                "database.dbname"
            )
            if database_name and "," in str(database_name):
                raise Exception(
                    f"Only one database is supported for Debezium's SQL Server connector. Found: {database_name}"
                )
            return database_name
        elif platform == "vitess":
            return config.get("vitess.keyspace")
        else:
            # postgres, oracle, db2 use database.dbname
            return config.get("database.dbname")

    def get_platform(self) -> str:
        """Map Debezium connector class to platform name."""
        connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")
        # Map based on well-known Debezium connector classes
        if "mysql" in connector_class.lower():
            return "mysql"
        elif "postgres" in connector_class.lower():
            return "postgres"
        elif "sqlserver" in connector_class.lower():
            return "sqlserver"
        elif "oracle" in connector_class.lower():
            return "oracle"
        elif "db2" in connector_class.lower():
            return "db2"
        elif "mongodb" in connector_class.lower():
            return "mongodb"
        elif "vitess" in connector_class.lower():
            return "vitess"
        else:
            return "unknown"

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """
        Extract lineage mappings from Debezium source tables to Kafka topics.

        Flow:
        1. Handle EventRouter (special case - data-dependent routing)
        2. Fallback: If no table.include.list and no SchemaResolver, parse topic names from runtime API
        3. Get table names (from config or discover from DataHub)
        4. For each table: generate topic name -> apply transforms -> create lineage
        5. Filter lineages by Kafka topics if available
        """
        try:
            parser = self.get_parser(self.connector_manifest)

            # EventRouter requires special handling (data-dependent routing)
            if self._has_event_router_transform():
                logger.debug(
                    f"Connector {self.connector_manifest.name} uses EventRouter - using table-based lineage extraction"
                )
                return self._extract_lineages_for_event_router(
                    parser.source_platform, parser.database_name
                )

            # Check for missing configuration: no table.include.list and no SchemaResolver
            table_config = self.connector_manifest.config.get(
                "table.include.list"
            ) or self.connector_manifest.config.get("table.whitelist")

            if not table_config and (
                not self.schema_resolver or not self.config.use_schema_resolver
            ):
                # Try fallback: parse topic names from OSS connector-specific topics
                # NOTE: topic_names is only populated for OSS (from /connectors/{name}/topics API)
                # For Confluent Cloud, topic_names is empty (we use all_cluster_topics separately)
                if self.connector_manifest.topic_names:
                    logger.info(
                        f"Debezium connector '{self.connector_manifest.name}' has no table.include.list configured "
                        f"and SchemaResolver is not available. Attempting fallback: parsing topic names from "
                        f"{len(self.connector_manifest.topic_names)} connector-specific topics."
                    )
                    lineages = self._extract_lineages_from_topics(
                        parser.source_platform, parser.server_name, parser.database_name
                    )
                    if lineages:
                        logger.info(
                            f"Fallback succeeded: extracted {len(lineages)} lineages for connector "
                            f"'{self.connector_manifest.name}' by parsing topic names"
                        )
                        return lineages
                    else:
                        logger.warning(
                            f"Fallback failed: could not extract lineages from topic names for connector "
                            f"'{self.connector_manifest.name}'"
                        )

                # Cannot extract lineages without table configuration or schema resolver
                logger.warning(
                    f"Debezium connector '{self.connector_manifest.name}' has no table.include.list configured "
                    f"and SchemaResolver is not available. Cannot extract lineages. "
                    f"Please either: "
                    f"1) Add 'table.include.list' to the connector configuration, OR "
                    f"2) Enable 'use_schema_resolver: true' in the DataHub ingestion config and ensure you've "
                    f"ingested {parser.source_platform} datasets into DataHub first."
                )
                return []

            # Step 1: Get table names to process
            table_names = self._get_table_names_to_process(parser)
            if not table_names:
                return []

            # Step 2: Generate lineages for each table
            lineages = self._generate_lineages_for_tables(table_names, parser)

            logger.info(
                f"Created {len(lineages)} lineages for Debezium connector '{self.connector_manifest.name}'"
            )
            return lineages

        except Exception as e:
            self.report.warning(
                "Error resolving lineage for connector",
                self.connector_manifest.name,
                exc=e,
            )
            return []

    def _get_table_names_to_process(self, parser: DebeziumParser) -> List[str]:
        """
        Get list of table names to process for lineage extraction.

        Returns table names in schema.table format (e.g., "public.users").
        For 3-tier platforms (e.g., SQL Server), tables may include database prefix
        from explicit config, but discovered tables never include database prefix.
        """
        # Check for explicit table configuration
        table_config = self.connector_manifest.config.get(
            "table.include.list"
        ) or self.connector_manifest.config.get("table.whitelist")

        # If no explicit config, discover tables from DataHub
        if not table_config:
            return self._discover_tables_from_datahub(
                parser.database_name, parser.source_platform
            )

        # Expand patterns if needed
        table_names = self._expand_table_patterns(
            table_config, parser.source_platform, parser.database_name
        )

        if not table_names:
            logger.warning(
                f"No tables found after expanding patterns for connector '{self.connector_manifest.name}'"
            )

        return table_names

    def _discover_tables_from_datahub(
        self, database_name: Optional[str], source_platform: str
    ) -> List[str]:
        """Discover tables from DataHub for the given database and platform."""
        if not self.schema_resolver or not database_name:
            logger.warning(
                f"Debezium connector {self.connector_manifest.name} has no table.include.list config "
                f"and SchemaResolver is not available (or database_name is missing). "
                f"Cannot extract lineages. Please either: "
                f"1) Configure table.include.list in the connector, OR "
                f"2) Enable 'use_schema_resolver' in DataHub ingestion config"
            )
            return []

        logger.info(
            f"Debezium connector {self.connector_manifest.name} has no table.include.list config - "
            f"discovering all tables from database '{database_name}' using SchemaResolver"
        )

        discovered_tables = self._discover_tables_from_database(
            database_name, source_platform
        )

        if not discovered_tables:
            logger.warning(
                f"No tables discovered from database '{database_name}' for connector '{self.connector_manifest.name}'. "
                f"Make sure you've ingested {source_platform} datasets for database '{database_name}' "
                f"into DataHub before running Kafka Connect ingestion."
            )
            return []

        logger.info(
            f"Discovered {len(discovered_tables)} tables from database '{database_name}' - "
            f"will generate expected topics and validate against Kafka"
        )
        return discovered_tables

    def _generate_lineages_for_tables(
        self, table_names: List[str], parser: DebeziumParser
    ) -> List[KafkaConnectLineage]:
        """Generate lineages for a list of table names."""
        lineages: List[KafkaConnectLineage] = []
        has_kafka_topics = bool(self.connector_manifest.topic_names)

        if not has_kafka_topics:
            logger.info(
                f"Kafka topics API not available for connector '{self.connector_manifest.name}' - "
                f"creating lineages for all {len(table_names)} configured tables without validating "
                f"that derived topic names actually exist in Kafka"
            )

        for table_name in table_names:
            lineage = self._create_lineage_for_table(table_name, parser)
            if lineage:
                # Validate against Kafka topics if available
                if (
                    has_kafka_topics
                    and lineage.target_dataset
                    not in self.connector_manifest.topic_names
                ):
                    logger.debug(
                        f"Expected topic '{lineage.target_dataset}' not found in Kafka - "
                        f"skipping lineage for table '{table_name}'"
                    )
                    continue
                lineages.append(lineage)

        return lineages

    def _create_lineage_for_table(
        self, table_name: str, parser: DebeziumParser
    ) -> Optional[KafkaConnectLineage]:
        """
        Create a single lineage mapping for a table.

        Args:
            table_name: Table name in schema.table format (e.g., "public.users")
            parser: Parsed connector configuration

        Returns:
            KafkaConnectLineage or None if lineage cannot be created
        """
        # Build source dataset URN (includes database if available)
        source_dataset = get_dataset_name(parser.database_name, table_name)

        # Generate Debezium topic name (before transforms)
        expected_topic = self._generate_debezium_topic_name(
            table_name, parser.server_name, parser.database_name
        )

        # Apply transforms if configured
        expected_topic = self._apply_transforms_to_topic(expected_topic, parser)

        # Extract fine-grained lineage if enabled
        fine_grained = self._extract_fine_grained_lineage(
            source_dataset, parser.source_platform, expected_topic, KAFKA
        )

        return KafkaConnectLineage(
            source_dataset=source_dataset,
            source_platform=parser.source_platform,
            target_dataset=expected_topic,
            target_platform=KAFKA,
            fine_grained_lineages=fine_grained,
        )

    def _generate_debezium_topic_name(
        self,
        schema_table: str,
        server_name: Optional[str],
        database_name: Optional[str],
    ) -> str:
        """
        Generate expected Debezium topic name (before transforms).

        Topic naming patterns:
        - Standard (PostgreSQL, MySQL): {server_name}.{schema}.{table}
        - SQL Server: {server_name}.{database}.{schema}.{table}

        Args:
            schema_table: Table name in schema.table format (e.g., "public.users")
            server_name: Server name / topic prefix
            database_name: Database name

        Returns:
            Expected topic name before transforms
        """
        connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")
        includes_database_in_topic = (
            connector_class
            in self.DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER_IN_PATTERN
        )

        if includes_database_in_topic and database_name:
            # SQL Server: server.database.schema.table
            if server_name:
                return f"{server_name}.{database_name}.{schema_table}"
            else:
                return f"{database_name}.{schema_table}"
        elif server_name:
            # Standard: server.schema.table
            return f"{server_name}.{schema_table}"
        else:
            # No server name (rare case)
            return schema_table

    def _apply_transforms_to_topic(
        self, topic_name: str, parser: DebeziumParser
    ) -> str:
        """Apply configured transforms to topic name."""
        if not parser.transforms:
            return topic_name

        # Log transform details for debugging (only once per connector)
        if not hasattr(self, "_logged_transforms"):
            for transform in parser.transforms:
                logger.info(
                    f"Transform '{transform.get('name', 'unknown')}' "
                    f"(type={transform.get('type', 'unknown')}): {transform}"
                )
            self._logged_transforms = True

        logger.info(
            f"Applying {len(parser.transforms)} transform(s) to topic '{topic_name}' "
            f"for connector '{self.connector_manifest.name}'"
        )

        result = get_transform_pipeline().apply_forward(
            [topic_name], self.connector_manifest.config
        )

        if result.warnings:
            for warning in result.warnings:
                logger.warning(
                    f"Transform warning for {self.connector_manifest.name}: {warning}"
                )

        if result.topics and len(result.topics) > 0:
            transformed_topic = result.topics[0]
            logger.debug(f"Topic transformed: '{topic_name}' -> '{transformed_topic}'")
            return transformed_topic

        return topic_name

    def _has_event_router_transform(self) -> bool:
        """Check if connector uses Debezium EventRouter transform."""
        transforms_config = self.connector_manifest.config.get("transforms", "")
        if not transforms_config:
            return False

        transform_names = parse_comma_separated_list(transforms_config)
        for name in transform_names:
            transform_type = self.connector_manifest.config.get(
                f"transforms.{name}.type", ""
            )
            if transform_type == "io.debezium.transforms.outbox.EventRouter":
                return True

        return False

    def _extract_lineages_for_event_router(
        self, source_platform: str, database_name: Optional[str]
    ) -> List[KafkaConnectLineage]:
        """
        Extract lineages for connectors using EventRouter transform.

        EventRouter is a data-dependent transform that reads fields from row data
        to determine output topics. We cannot predict output topics from configuration alone,
        so we extract source tables from table.include.list and try to match them to
        actual topics using RegexRouter patterns.

        Reference: https://debezium.io/documentation/reference/transformations/outbox-event-router.html
        """
        lineages: List[KafkaConnectLineage] = []

        # Extract source tables from configuration
        table_config = self.connector_manifest.config.get(
            "table.include.list"
        ) or self.connector_manifest.config.get("table.whitelist")

        if not table_config:
            logger.warning(
                f"EventRouter connector {self.connector_manifest.name} has no table.include.list config"
            )
            return lineages

        # Expand table patterns if schema resolver is enabled
        table_names = self._expand_table_patterns(
            table_config, source_platform, database_name
        )

        # Try to filter topics using RegexRouter replacement pattern (if available)
        filtered_topics = self._filter_topics_for_event_router()

        # For each source table, create lineages to filtered topics
        for table_name in table_names:
            # Clean quoted table names
            clean_table = table_name.strip('"')

            # Apply database name if present
            if database_name:
                source_dataset = get_dataset_name(database_name, clean_table)
            else:
                source_dataset = clean_table

            # Create lineages from this source table to filtered topics
            for topic in filtered_topics:
                lineage = KafkaConnectLineage(
                    source_dataset=source_dataset,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform=KAFKA,
                )
                lineages.append(lineage)

        logger.info(
            f"Created {len(lineages)} EventRouter lineages from {len(table_names)} source tables "
            f"to {len(filtered_topics)} topics for connector {self.connector_manifest.name}"
        )

        return lineages

    def _filter_topics_for_event_router(self) -> List[str]:
        """
        Filter topics for EventRouter connectors using RegexRouter replacement pattern.

        EventRouter often works with RegexRouter to rename output topics. We can use
        the RegexRouter replacement pattern to identify which topics belong to this connector.
        """
        # Use all_cluster_topics for Cloud (contains all topics), otherwise use topic_names (OSS)
        available_topics = (
            list(self.all_cluster_topics)
            if self.all_cluster_topics
            else list(self.connector_manifest.topic_names)
        )

        # Look for RegexRouter transform configuration
        transforms_config = self.connector_manifest.config.get("transforms", "")
        if not transforms_config:
            return available_topics

        transform_names = parse_comma_separated_list(transforms_config)

        # Find RegexRouter configuration
        regex_replacement = None
        for name in transform_names:
            transform_type = self.connector_manifest.config.get(
                f"transforms.{name}.type", ""
            )
            if transform_type in [
                "org.apache.kafka.connect.transforms.RegexRouter",
                "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            ]:
                # Extract the replacement pattern
                # Example: "dev.ern.cashout.$1" -> we want topics starting with "dev.ern.cashout."
                replacement = self.connector_manifest.config.get(
                    f"transforms.{name}.replacement", ""
                )
                if replacement:
                    # Extract prefix from replacement pattern (before first $)
                    # "dev.ern.cashout.$1" -> "dev.ern.cashout."
                    if "$" in replacement:
                        regex_replacement = replacement.split("$")[0]
                    else:
                        regex_replacement = replacement
                    break

        # Filter topics using the replacement prefix
        if regex_replacement:
            filtered_topics = [
                topic
                for topic in available_topics
                if topic.startswith(regex_replacement)
            ]
            logger.debug(
                f"Filtered EventRouter topics to {len(filtered_topics)} topics matching prefix '{regex_replacement}'"
            )
            return filtered_topics

        # No RegexRouter found - use all topics (risky but best effort)
        logger.warning(
            f"EventRouter connector {self.connector_manifest.name} has no RegexRouter - cannot filter topics accurately"
        )
        return available_topics

    def _extract_lineages_from_topic_names(
        self, parser: DebeziumParser
    ) -> List[KafkaConnectLineage]:
        """
        Extract lineages by reverse-engineering table names from runtime Kafka topic names.

        This is a fallback used when:
        1. No table.include.list configured
        2. SchemaResolver not available (or use_schema_resolver=False)
        3. But runtime topic names ARE available from Kafka Connect API

        This is the OLD behavior that allows lineage extraction without explicit
        table configuration by parsing the actual topics the connector is producing.
        """
        return self._extract_lineages_from_topics(
            parser.source_platform, parser.server_name or "", parser.database_name
        )

    def _extract_lineages_from_topics(
        self,
        source_platform: str,
        server_name: Optional[str],
        database_name: Optional[str],
    ) -> List[KafkaConnectLineage]:
        """
        Extract lineages by reverse-engineering table names from Kafka topic names.

        This is used when table.include.list is not configured, meaning Debezium captures
        ALL tables from the database. We parse the actual topic names from Kafka API to
        determine which tables are being captured.

        Debezium topic naming patterns:
        - MySQL: {server_name}.{database}.{table}
        - PostgreSQL: {server_name}.{schema}.{table}
        - SQL Server: {server_name}.{database}.{schema}.{table}
        - Oracle: {server_name}.{schema}.{table}
        """
        lineages: List[KafkaConnectLineage] = []

        if not self.connector_manifest.topic_names:
            return lineages

        connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")
        includes_database_in_topic = (
            connector_class
            in self.DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER_IN_PATTERN
        )

        for topic in self.connector_manifest.topic_names:
            # Skip internal Debezium topics (schema history, transaction metadata, etc.)
            if any(
                internal in topic
                for internal in [
                    ".schema-changes",
                    ".transaction",
                    "dbhistory",
                    "__debezium",
                ]
            ):
                logger.debug(f"Skipping internal Debezium topic: {topic}")
                continue

            # Parse topic name to extract table information
            # Expected format: {server_name}.{container}.{table} or {server_name}.{table}
            parts = topic.split(".")

            # Skip topics that don't match expected Debezium format
            if len(parts) < 2:
                logger.debug(
                    f"Skipping topic '{topic}' - does not match Debezium naming pattern"
                )
                continue

            # Try to match server_name prefix
            if server_name and topic.startswith(f"{server_name}."):
                # Remove server_name prefix
                remaining = topic[len(server_name) + 1 :]
                remaining_parts = remaining.split(".")

                # Extract table information based on connector type
                if includes_database_in_topic:
                    # SQL Server: {server}.{database}.{schema}.{table}
                    if len(remaining_parts) >= 3:
                        db_name = remaining_parts[0]
                        schema_table = ".".join(remaining_parts[1:])
                    elif len(remaining_parts) == 2:
                        # Fallback: {database}.{table}
                        db_name = remaining_parts[0]
                        schema_table = remaining_parts[1]
                    else:
                        logger.debug(
                            f"Skipping topic '{topic}' - unexpected format for 2-level container connector"
                        )
                        continue

                    # Use database from topic if available, otherwise use configured database
                    if database_name and db_name != database_name:
                        logger.debug(
                            f"Skipping topic '{topic}' - database '{db_name}' does not match configured '{database_name}'"
                        )
                        continue

                    dataset_name = get_dataset_name(db_name, schema_table)
                else:
                    # Standard: {server}.{schema}.{table} or {server}.{table}
                    schema_table = remaining

                    # Build dataset name
                    if database_name:
                        dataset_name = get_dataset_name(database_name, schema_table)
                    else:
                        dataset_name = schema_table
            else:
                # No server_name prefix or doesn't match - try best effort parsing
                logger.debug(
                    f"Topic '{topic}' does not start with expected server_name '{server_name}' - attempting best-effort parsing"
                )

                if includes_database_in_topic:
                    # Assume format: {database}.{schema}.{table}
                    if len(parts) >= 2:
                        db_name = parts[0]
                        schema_table = ".".join(parts[1:])
                        dataset_name = get_dataset_name(db_name, schema_table)
                    else:
                        continue
                else:
                    # Assume format: {schema}.{table} or just {table}
                    if database_name:
                        dataset_name = get_dataset_name(database_name, topic)
                    else:
                        dataset_name = topic

            # Extract fine-grained lineage if enabled
            fine_grained = self._extract_fine_grained_lineage(
                dataset_name, source_platform, topic, KAFKA
            )

            # Create lineage mapping
            lineage = KafkaConnectLineage(
                source_dataset=dataset_name,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
                fine_grained_lineages=fine_grained,
            )
            lineages.append(lineage)

        logger.info(
            f"Extracted {len(lineages)} lineages from {len(self.connector_manifest.topic_names)} topics "
            f"for connector '{self.connector_manifest.name}'"
        )
        return lineages

    def _expand_table_patterns(
        self,
        table_config: str,
        source_platform: str,
        database_name: Optional[str],
    ) -> List[str]:
        """
        Expand table patterns using DataHub schema metadata.

        Examples:
        - "mydb.*" → ["mydb.table1", "mydb.table2", ...]
        - "public.*" → ["public.table1", "public.table2", ...]
        - "schema1.table1" → ["schema1.table1"] (no expansion)

        Args:
            table_config: Comma-separated table patterns from connector config
            source_platform: Source platform (e.g., 'postgres', 'mysql')
            database_name: Database name for context (optional)

        Returns:
            List of fully expanded table names
        """
        # Check if feature is enabled
        if (
            not self.config.use_schema_resolver
            or not self.config.schema_resolver_expand_patterns
        ):
            # Fall back to original behavior - parse as-is
            return parse_comma_separated_list(table_config)

        if not self.schema_resolver:
            logger.debug(
                f"SchemaResolver not available for connector {self.connector_manifest.name} - skipping pattern expansion"
            )
            return parse_comma_separated_list(table_config)

        patterns = parse_comma_separated_list(table_config)
        expanded_tables = []

        logger.info(
            f"Processing table patterns for connector {self.connector_manifest.name}: "
            f"platform={source_platform}, database={database_name}, patterns={patterns}"
        )

        for pattern in patterns:
            # Check if pattern needs expansion (contains regex special characters)
            if self._is_regex_pattern(pattern):
                logger.info(
                    f"Pattern '{pattern}' contains wildcards - will query DataHub for matching tables "
                    f"(platform={source_platform}, database={database_name})"
                )
                tables = self._query_tables_from_datahub(
                    pattern, source_platform, database_name
                )
                if tables:
                    logger.info(
                        f"Expanded pattern '{pattern}' to {len(tables)} tables: {tables[:5]}..."
                    )
                    expanded_tables.extend(tables)
                else:
                    logger.warning(
                        f"Pattern '{pattern}' did not match any tables in DataHub - keeping as-is"
                    )
                    expanded_tables.append(pattern)
            else:
                # Already explicit table name - no expansion needed
                logger.debug(
                    f"Table '{pattern}' is explicit (no wildcards) - using as-is without querying DataHub"
                )
                expanded_tables.append(pattern)

        return expanded_tables

    def _is_regex_pattern(self, pattern: str) -> bool:
        """
        Check if pattern contains Java regex special characters.

        Debezium uses Java regex for table.include.list, which supports:
        - Wildcards: * (zero or more), + (one or more), ? (zero or one)
        - Character classes: [abc], [0-9], [a-z]
        - Grouping and alternation: (pattern1|pattern2)
        - Quantifiers: {n}, {n,}, {n,m}
        - Anchors and boundaries: ^, $, \\b
        - Escapes: \\ (backslash for escaping special chars like \\.)

        Returns:
            True if pattern contains regex special characters
        """
        # Common regex special characters that indicate a pattern needs expansion
        # Note: backslash (\) is included to detect escaped patterns like "public\\.users"
        regex_chars = ["*", "+", "?", "[", "]", "(", ")", "|", "{", "}", "^", "$", "\\"]
        return any(char in pattern for char in regex_chars)

    def _query_tables_from_datahub(
        self,
        pattern: str,
        platform: str,
        database: Optional[str],
    ) -> List[str]:
        """
        Query DataHub for tables matching the given pattern.

        Debezium uses Java regex for table.include.list patterns. Patterns are anchored,
        meaning they must match the entire fully-qualified table name.

        Args:
            pattern: Java regex pattern (e.g., "public.*", "mydb\\.(users|orders)")
            platform: Source platform
            database: Database name for context

        Returns:
            List of matching table names
        """
        if not self.schema_resolver:
            return []

        try:
            # Get all URNs from schema resolver cache
            all_urns = self.schema_resolver.get_urns()

            logger.info(
                f"SchemaResolver returned {len(all_urns)} cached URNs for platform={platform}, "
                f"database={database}, will match against pattern='{pattern}'"
            )

            if not all_urns:
                logger.warning(
                    f"No cached schemas available in SchemaResolver for platform={platform}. "
                    f"Make sure you've ingested {platform} datasets into DataHub before running Kafka Connect ingestion."
                )
                return []

            matched_tables = []

            # Try to use Java regex for exact compatibility with Debezium
            try:
                from java.util.regex import Pattern as JavaPattern

                regex_pattern = JavaPattern.compile(pattern)
                use_java_regex = True
            except (ImportError, RuntimeError):
                # Fallback to Python re module for testing/environments without JPype
                logger.debug(
                    "Java regex not available, falling back to Python re module"
                )
                # Convert Java regex to Python regex (mostly compatible)
                # Main difference: Java regex uses \. for literal dot, Python uses \.
                # For simple patterns like "public.*" they're identical
                python_pattern = pattern.replace(r"\\.", r"\.")
                regex_pattern = re.compile(python_pattern)
                use_java_regex = False

            for urn in all_urns:
                # URN format: urn:li:dataset:(urn:li:dataPlatform:postgres,database.schema.table,PROD)
                table_name = self._extract_table_name_from_urn(urn)
                if not table_name:
                    continue

                # Try direct match first (handles patterns like "mydb.schema.*")
                full_name_matches = (
                    regex_pattern.matcher(table_name).matches()
                    if use_java_regex
                    else regex_pattern.fullmatch(table_name) is not None
                )

                if full_name_matches:
                    matched_tables.append(table_name)
                    continue

                # For patterns without database prefix (e.g., "schema.*" or "public.*"),
                # also try matching against the table name without the first component.
                # This handles Debezium patterns that don't include the database name:
                # - PostgreSQL: "public.*" matches "testdb.public.users" (3-tier URN)
                # - MySQL: "mydb.*" matches "mydb.table1" (2-tier URN, already matched above)
                if "." in table_name:
                    table_without_database = table_name.split(".", 1)[1]
                    schema_name_matches = (
                        regex_pattern.matcher(table_without_database).matches()
                        if use_java_regex
                        else regex_pattern.fullmatch(table_without_database) is not None
                    )

                    if schema_name_matches:
                        matched_tables.append(table_name)

            logger.debug(
                f"Pattern '{pattern}' matched {len(matched_tables)} tables from DataHub"
            )
            return matched_tables

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to connect to DataHub for pattern '{pattern}': {e}")
            if self.report:
                self.report.report_failure(
                    f"datahub_connection_{self.connector_manifest.name}", str(e)
                )
            return []
        except Exception as e:
            logger.warning(
                f"Failed to query tables from DataHub for pattern '{pattern}': {e}",
                exc_info=True,
            )
            return []


@dataclass
class ConfigDrivenSourceConnector(BaseConnector):
    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = []
        target_connector = None

        # Find matching generic connector configuration
        for connector in self.config.generic_connectors:
            if connector.connector_name == self.connector_manifest.name:
                target_connector = connector
                break

        if target_connector is None:
            logger.error(
                f"No generic connector configuration found for '{self.connector_manifest.name}'. "
                "Please add this connector to the 'generic_connectors' config list."
            )
            return lineages

        # Create lineages for all topics
        for topic in self.connector_manifest.topic_names:
            lineage = KafkaConnectLineage(
                source_dataset=target_connector.source_dataset,
                source_platform=target_connector.source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
            )
            lineages.append(lineage)
        return lineages


JDBC_SOURCE_CONNECTOR_CLASS: Final[str] = (
    "io.confluent.connect.jdbc.JdbcSourceConnector"
)
DEBEZIUM_SOURCE_CONNECTOR_PREFIX: Final[str] = "io.debezium.connector"
MONGO_SOURCE_CONNECTOR_CLASS: Final[str] = (
    "com.mongodb.kafka.connect.MongoSourceConnector"
)

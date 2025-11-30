import logging
import re
from dataclasses import dataclass
from typing import Dict, Final, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER,
    KAFKA,
    KNOWN_TOPIC_ROUTING_TRANSFORMS,
    REGEXROUTER_TRANSFORM,
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

        # Cloud uses database.server.name as topic prefix
        topic_prefix = connector_manifest.config.get("database.server.name", "")

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

        return KafkaConnectLineage(
            source_dataset=dataset_name if include_dataset else None,
            source_platform=source_platform,
            target_dataset=topic,
            target_platform=KAFKA,
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
        all_topics = list(self.connector_manifest.topic_names)

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
        if self._has_one_to_one_topic_mapping():
            return self._extract_lineages_from_config_mapping(parser)
        elif parser.topic_prefix:
            return self._extract_lineages_from_prefix_inference(parser)
        else:
            # Use available topics directly if no better strategy
            return self._create_direct_lineages_from_topics(all_topics, parser)

    def _has_one_to_one_topic_mapping(self) -> bool:
        """Check if connector has clear 1:1 table to topic mapping from config."""
        config = self.connector_manifest.config

        # Check for single table configuration
        if config.get("kafka.topic") and config.get("table.name"):
            return True

        # Check for explicit table list with matching topic count
        table_config = config.get("table.include.list") or config.get("table.whitelist")
        if table_config:
            table_names = parse_comma_separated_list(table_config)
            return len(table_names) == len(self.connector_manifest.topic_names)

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
            for source_table, topic in zip(source_tables, connector_topics):
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
        self, parser: JdbcParser
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
                topics = list(self.connector_manifest.topic_names)

                # Create 1:1 mapping (assuming same order)
                for table_name, topic in zip(table_names, topics):
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
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Extract lineages by inferring source tables from topic names using prefix."""
        lineages = []

        for topic in self.connector_manifest.topic_names:
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

        # Use the connector registry for configuration-based topic derivation
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        config_topics = ConnectorRegistry.get_topics_from_config(
            self.connector_manifest, self.config, self.report
        )

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

    def get_server_name(self, connector_manifest: ConnectorManifest) -> str:
        if "topic.prefix" in connector_manifest.config:
            return connector_manifest.config["topic.prefix"]
        else:
            return connector_manifest.config.get("database.server.name", "")

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> DebeziumParser:
        connector_class = connector_manifest.config.get(CONNECTOR_CLASS, "")

        # Map connector class to platform
        platform = self._get_platform_from_connector_class(connector_class)

        # Map handler platform to parser platform (handler uses "sqlserver", parser expects "mssql")
        parser_platform = "mssql" if platform == "sqlserver" else platform

        # Get database name based on platform
        database_name = self._get_database_name_for_platform(
            platform, connector_manifest.config
        )

        return self.DebeziumParser(
            source_platform=parser_platform,
            server_name=self.get_server_name(connector_manifest),
            database_name=database_name,
        )

    def get_topics_from_config(self) -> List[str]:
        """Extract expected topics from Debezium connector configuration."""
        try:
            parser = self.get_parser(self.connector_manifest)
            config = self.connector_manifest.config
            server_name = parser.server_name or ""

            # Extract table names from configuration
            table_config = config.get("table.include.list") or config.get(
                "table.whitelist"
            )
            if not table_config:
                logger.debug("No table configuration found in Debezium connector")
                return []

            table_names = parse_comma_separated_list(table_config)

            # Debezium topics follow pattern: {server_name}.{table} where table includes schema
            topics = []
            for table_name in table_names:
                if server_name:
                    # Table name already includes schema prefix (e.g., "public.users")
                    topic_name = f"{server_name}.{table_name}"
                else:
                    topic_name = table_name
                topics.append(topic_name)

            return topics
        except Exception as e:
            logger.debug(f"Failed to derive topics from Debezium connector config: {e}")
            return []

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

    def _get_platform_from_connector_class(self, connector_class: str) -> str:
        """Map Debezium connector class to platform name."""
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
        lineages: List[KafkaConnectLineage] = list()

        try:
            parser = self.get_parser(self.connector_manifest)
            source_platform = parser.source_platform
            server_name = parser.server_name
            database_name = parser.database_name

            if not self.connector_manifest.topic_names:
                return lineages

            # Check for EventRouter transform - requires special handling
            if self._has_event_router_transform():
                logger.debug(
                    f"Connector {self.connector_manifest.name} uses EventRouter transform - using table-based lineage extraction"
                )
                return self._extract_lineages_for_event_router(
                    source_platform, database_name
                )

            # Standard Debezium topic processing
            # Escape server_name to handle cases where topic.prefix contains dots
            # Some users configure topic.prefix like "my.server" which breaks the regex
            server_name = server_name or ""
            # Regex pattern (\w+\.\w+(?:\.\w+)?) supports BOTH 2-part and 3-part table names
            topic_naming_pattern = rf"({re.escape(server_name)})\.(\w+\.\w+(?:\.\w+)?)"

            # Handle connectors with 2-level container (database + schema) in topic pattern
            connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")
            maybe_duplicated_database_name = (
                connector_class
                in self.DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER_IN_PATTERN
            )

            for topic in self.connector_manifest.topic_names:
                found = re.search(re.compile(topic_naming_pattern), topic)
                logger.debug(
                    f"Processing topic: '{topic}' with regex pattern '{topic_naming_pattern}', found: {found}"
                )

                if found:
                    # Extract the table part after server_name
                    table_part = found.group(2)

                    if (
                        maybe_duplicated_database_name
                        and database_name
                        and table_part.startswith(f"{database_name}.")
                    ):
                        table_part = table_part[len(database_name) + 1 :]

                    logger.debug(
                        f"Extracted table part: '{table_part}' from topic '{topic}'"
                    )
                    # Apply database name to create final dataset name
                    table_name = get_dataset_name(database_name, table_part)
                    logger.debug(f"Final table name: '{table_name}'")

                    lineage = KafkaConnectLineage(
                        source_dataset=table_name,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform=KAFKA,
                    )
                    lineages.append(lineage)
            return lineages
        except Exception as e:
            self.report.warning(
                "Error resolving lineage for connector",
                self.connector_manifest.name,
                exc=e,
            )

        return []

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

        table_names = parse_comma_separated_list(table_config)

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
        # Look for RegexRouter transform configuration
        transforms_config = self.connector_manifest.config.get("transforms", "")
        if not transforms_config:
            return list(self.connector_manifest.topic_names)

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
                for topic in self.connector_manifest.topic_names
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
        return list(self.connector_manifest.topic_names)


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

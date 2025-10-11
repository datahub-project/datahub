import logging
import re
from dataclasses import dataclass
from typing import Dict, Final, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    KAFKA,
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
from datahub.ingestion.source.kafka_connect.connector_constants import (
    CONFLUENT_NON_TOPIC_ROUTING_TRANSFORMS,
    DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER,
    KAFKA_NON_TOPIC_ROUTING_TRANSFORMS,
    KNOWN_NON_TOPIC_ROUTING_TRANSFORMS,
    KNOWN_TOPIC_ROUTING_TRANSFORMS,
    REGEXROUTER_TRANSFORM,
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
class TransformResult:
    """Result of applying a transform pipeline to a source table."""

    source_table: str
    schema: str
    final_topics: List[str]
    original_topic: str


class BaseTransform:
    """Base class for Kafka Connect transforms."""

    def __init__(self, config: Dict[str, str]):
        self.config = config

    def apply(self, current_topics: List[str], manifest_topics: List[str]) -> List[str]:
        """Apply the transform to the current topics."""
        raise NotImplementedError("Subclasses must implement apply method")


class EventRouterTransform(BaseTransform):
    """Debezium EventRouter transform (Outbox pattern) - Simplified safe implementation."""

    def apply(self, current_topics: List[str], manifest_topics: List[str]) -> List[str]:
        """
        Apply EventRouter transform based on documented behavior:
        https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html

        EventRouter transforms outbox table topics to outbox.event.{aggregatetype} format.
        This is an intermediate transformation step - the final topics may be further transformed.
        """
        # Look for outbox tables in current topics
        outbox_topics = [t for t in current_topics if "outbox" in t.lower()]

        if not outbox_topics:
            logger.warning(
                "EventRouter transform applied but no outbox table found in source topics. "
                "This may result in incomplete lineage."
            )
            return current_topics

        # EventRouter produces outbox.event.{aggregatetype} format as documented
        # This is a predictable transformation regardless of manifest topics
        # since it's based on the event types extracted from the outbox table

        # For the pipeline, we generate standard EventRouter output topics
        # The actual event types will be determined by downstream RegexRouter or manifest topics

        # Check if manifest contains EventRouter-style topics or further transformed topics
        event_topics = []

        # First, check for direct outbox.event.* pattern in manifest
        direct_event_topics = [
            t for t in manifest_topics if t.startswith("outbox.event.")
        ]

        if direct_event_topics:
            event_topics = direct_event_topics
        else:
            # EventRouter with further transforms - this is complex and error-prone to guess
            # The documented EventRouter behavior only guarantees outbox.event.* intermediate format
            # Any further transforms are connector-specific and not predictable
            logger.warning(
                "EventRouter detected but no 'outbox.event.*' topics found in manifest. "
                "This suggests EventRouter output is further transformed by other transforms. "
                "For accurate lineage mapping with complex transform chains, "
                "please use 'generic_connectors' config to specify explicit source-to-topic mappings."
            )
            # Return manifest topics as-is - don't attempt to guess transform patterns
            event_topics = list(manifest_topics)

        logger.info(
            f"EventRouter mapping: {len(outbox_topics)} outbox tables -> {len(event_topics)} event topics"
        )
        return event_topics


class RegexRouterTransform(BaseTransform):
    """Kafka Connect RegexRouter transform."""

    def apply(self, current_topics: List[str], manifest_topics: List[str]) -> List[str]:
        """Apply RegexRouter transform to rename topics."""
        regex_pattern = self.config.get("regex", "")
        replacement = self.config.get("replacement", "")

        if not regex_pattern or not replacement:
            logger.warning("RegexRouter missing regex or replacement pattern")
            return current_topics

        transformed_topics = []
        for topic in current_topics:
            try:
                # Use Java regex for exact Kafka Connect compatibility
                from java.util.regex import Pattern

                transform_regex = Pattern.compile(regex_pattern)
                matcher = transform_regex.matcher(topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(replacement))
                    logger.debug(f"RegexRouter: {topic} -> {transformed_topic}")
                    transformed_topics.append(transformed_topic)
                else:
                    logger.debug(f"RegexRouter: {topic} (no match)")
                    transformed_topics.append(topic)

            except ImportError:
                logger.warning(
                    f"Java regex library not available for RegexRouter transform. "
                    f"Cannot apply pattern '{regex_pattern}' to topic '{topic}'. "
                    f"Returning original topic name."
                )
                transformed_topics.append(topic)

            except Exception as e:
                logger.warning(
                    f"RegexRouter failed for topic '{topic}' with pattern '{regex_pattern}' "
                    f"and replacement '{replacement}': {e}"
                )
                transformed_topics.append(topic)

        return transformed_topics


class TransformPipeline:
    """
    Handles multiple Kafka Connect transforms in sequence.

    ARCHITECTURAL COMPLEXITY RATIONALE:

    The transform pipeline complexity exists because Kafka Connect supports multiple
    transformation strategies that must be handled consistently:

    1. **Forward Pipeline**: Apply transforms to source tables to predict final topics
       - Used when we have table configurations but limited topic information
       - Requires generating original topic names based on connector naming conventions

    2. **Reverse Pipeline**: Work backward from actual topics to find source mappings
       - Used when we have actual topic names from the cluster/API
       - More accurate for environments with complex transform chains

    3. **Connector-Specific Naming**: Different connectors use different topic naming:
       - JDBC Source: Simple concatenation (prefix + table)
       - Debezium CDC: Hierarchical (server.schema.table)
       - Cloud connectors: Uses database.server.name as prefix

    4. **Transform Types**: Each transform type has different behavior:
       - RegexRouter: Pattern-based topic renaming
       - EventRouter: Outbox pattern for CDC events
       - Others: Field manipulation, data transformation

    This complexity cannot be easily simplified without breaking compatibility with
    existing connector configurations in production environments.
    """

    # Known transform types
    TRANSFORM_CLASSES = {
        "io.debezium.transforms.outbox.EventRouter": EventRouterTransform,
        "org.apache.kafka.connect.transforms.RegexRouter": RegexRouterTransform,
        "io.confluent.connect.cloud.transforms.TopicRegexRouter": RegexRouterTransform,
    }

    def __init__(self, transform_configs: List[Dict[str, str]]):
        """Initialize pipeline with transform configurations."""
        self.transforms = []

        for config in transform_configs:
            transform_type = config.get("type", "")
            transform_class = self.TRANSFORM_CLASSES.get(transform_type)

            if transform_class:
                self.transforms.append(transform_class(config))
            else:
                logger.debug(f"Skipping unsupported transform type: {transform_type}")

    def apply_transforms(
        self,
        tables: List[TableId],
        topic_prefix: str,
        manifest_topics: List[str],
        connector_class: str,
    ) -> List[TransformResult]:
        """
        Apply transform pipeline to generate lineage mappings.

        Args:
            tables: List of TableId objects from source
            topic_prefix: Topic prefix from connector config
            manifest_topics: Actual topic names from connector manifest
            connector_class: The connector class name to determine topic naming strategy

        Returns:
            List of TransformResult objects mapping sources to final topics
        """
        results = []

        for table_id in tables:
            # Generate original topic name (before transforms)
            original_topic = self._generate_original_topic(
                table_id.schema or "", table_id.table, topic_prefix, connector_class
            )

            # Apply transform pipeline
            final_topics = self._apply_pipeline([original_topic], manifest_topics)

            # Create result if we have matching topics
            if final_topics:
                results.append(
                    TransformResult(
                        source_table=table_id.table,
                        schema=table_id.schema or "",
                        final_topics=final_topics,
                        original_topic=original_topic,
                    )
                )

        return results

    def _generate_jdbc_topic_name(self, table_name: str, topic_prefix: str) -> str:
        """
        Generate topic name for traditional JDBC Source connectors.

        Based on official Kafka Connect JDBC Source implementation:
        https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/source/BulkTableQuerier.java

        ```java
        // Topic generation logic:
        String name = tableId.tableName(); // Returns ONLY table name, not schema.table
        topic = topicPrefix + name;         // Simple concatenation
        ```

        JDBC Source behavior:
        - Without topic.prefix: topic = "member" (just table name)
        - With topic.prefix: topic = "prefix-member" (prefix + table name)

        Args:
            table_name: Table name (from tableId.tableName())
            topic_prefix: Topic prefix from connector config (topic.prefix)

        Returns:
            Topic name following official JDBC Source naming convention
        """
        return topic_prefix + table_name

    def _generate_cdc_topic_name(
        self, schema: str, table_name: str, topic_prefix: str
    ) -> str:
        """
        Generate topic name for CDC connectors (Debezium/Cloud).

        CDC connectors use hierarchical naming with a topic prefix:

        ACTUAL PATTERNS (from official documentation):
        - MySQL CDC: {topic.prefix}.{databaseName}.{tableName}
          Example: "fulfillment.inventory.orders"
        - PostgreSQL CDC: {topic.prefix}.{schemaName}.{tableName}
          Example: "fulfillment.public.users"

        When topic prefix is empty:
        - MySQL CDC: {databaseName}.{tableName}
        - PostgreSQL CDC: {schemaName}.{tableName}

        TOPIC PREFIX SOURCE:
        - Confluent Cloud: Uses "database.server.name" config as topic prefix
        - Platform/Debezium: Uses "topic.prefix" config as topic prefix

        References:
        - Debezium MySQL: https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-topic-names
        - Debezium PostgreSQL: https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-topic-names
        - Cloud MySQL CDC: https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source.html

        Args:
            schema: Database schema (PostgreSQL) or database name (MySQL)
            table_name: Table name
            topic_prefix: Topic prefix (from topic.prefix or database.server.name config)

        Returns:
            Topic name: {topic_prefix}.{schema}.{table_name} or {schema}.{table_name} if prefix empty
        """
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

    def _generate_original_topic(
        self, schema: str, table_name: str, topic_prefix: str, connector_class: str
    ) -> str:
        """
        Generate the original topic name before any transforms based on connector type.

        Different connector types use different topic naming strategies:
        - Traditional JDBC: Simple concatenation (topicPrefix + tableName)
        - CDC/Debezium connectors: Hierarchical naming ({prefix}.{schema}.{table})
        - Cloud connectors: Hierarchical naming ({database.server.name}.{schema}.{table})

        Args:
            schema: Database schema or database name
            table_name: Table name
            topic_prefix: Topic prefix (topic.prefix or database.server.name)
            connector_class: The connector class name to determine naming strategy

        Returns:
            Topic name using appropriate naming convention
        """
        # Cloud connectors always use hierarchical naming
        if connector_class in CLOUD_JDBC_SOURCE_CLASSES or connector_class.startswith(
            "io.debezium."
        ):
            return self._generate_cdc_topic_name(schema, table_name, topic_prefix)

        # Traditional JDBC source connector
        elif connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector":
            return self._generate_jdbc_topic_name(table_name, topic_prefix)

        else:
            # Default behavior: use topic_prefix presence as fallback (for unknown connectors)
            # This preserves backward compatibility for unknown connector types
            return self._generate_jdbc_topic_name(table_name, topic_prefix)

    def _apply_pipeline(
        self, current_topics: List[str], manifest_topics: List[str]
    ) -> List[str]:
        """Apply all transforms in sequence."""
        topics = current_topics[:]

        for transform in self.transforms:
            topics = transform.apply(topics, manifest_topics)

        return topics

    def discover_topic_transformations(
        self, manifest_topics: List[str]
    ) -> Dict[str, List[str]]:
        """
        Apply the complete transform pipeline to all topics and find which ones map to existing topics.

        This implements the improved strategy: for each topic, apply all transforms and check if
        the result exists in the topic list. This works regardless of connector type and handles
        complex transform chains automatically.

        Args:
            manifest_topics: All actual topic names from the connector manifest

        Returns:
            Dict mapping original_topic -> [transformed_topics] where transformed topics exist
        """
        topic_mappings = {}

        for original_topic in manifest_topics:
            # Apply the complete transform pipeline to this topic
            transformed_topics = self._apply_pipeline([original_topic], manifest_topics)

            # Check if any of the transformed results exist in the actual topic list
            existing_transformed_topics = [
                topic for topic in transformed_topics if topic in manifest_topics
            ]

            # Only store mappings where the transformation produces an existing topic
            if existing_transformed_topics and existing_transformed_topics != [
                original_topic
            ]:
                topic_mappings[original_topic] = existing_transformed_topics
                logger.debug(
                    f"Topic transformation discovered: {original_topic} -> {existing_transformed_topics}"
                )

        return topic_mappings


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
    KAFKA_NONTOPICROUTING_TRANSFORMS = KAFKA_NON_TOPIC_ROUTING_TRANSFORMS
    CONFLUENT_NONTOPICROUTING_TRANSFORMS = CONFLUENT_NON_TOPIC_ROUTING_TRANSFORMS
    KNOWN_NONTOPICROUTING_TRANSFORMS = KNOWN_NON_TOPIC_ROUTING_TRANSFORMS

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

    def _extract_lineages_with_pipeline(
        self,
        transforms: List[Dict[str, str]],
        database_name: str,
        source_platform: str,
        topic_prefix: str,
    ) -> List[KafkaConnectLineage]:
        """Extract lineages using the new transform pipeline for complex scenarios."""
        try:
            tables = self.get_table_names()
            connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")

            pipeline = TransformPipeline(transforms)
            results = pipeline.apply_transforms(
                tables,
                topic_prefix,
                list(self.connector_manifest.topic_names),
                connector_class,
            )

            return self._convert_transform_results_to_lineages(
                results, database_name, source_platform
            )

        except Exception as e:
            logger.warning(f"Transform pipeline failed: {e}")
            return self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
            )

    def _convert_transform_results_to_lineages(
        self, results: List[TransformResult], database_name: str, source_platform: str
    ) -> List[KafkaConnectLineage]:
        """Convert transform pipeline results to KafkaConnectLineage objects."""
        lineages: List[KafkaConnectLineage] = []

        for result in results:
            source_dataset = self._build_source_dataset_from_result(
                result, database_name, source_platform
            )

            # Create lineages for all final topics
            for final_topic in result.final_topics:
                lineage = KafkaConnectLineage(
                    source_dataset=source_dataset,
                    source_platform=source_platform,
                    target_dataset=final_topic,
                    target_platform=KAFKA,
                )
                lineages.append(lineage)

                logger.debug(
                    f"Pipeline lineage: {source_dataset} -> {final_topic} "
                    f"(via {result.original_topic})"
                )

        return lineages

    def _build_source_dataset_from_result(
        self, result: TransformResult, database_name: str, source_platform: str
    ) -> str:
        """Build source dataset name from transform pipeline result."""
        if result.schema and has_three_level_hierarchy(source_platform):
            source_table_name = f"{result.schema}.{result.source_table}"
        else:
            source_table_name = result.source_table

        return get_dataset_name(database_name, source_table_name)

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

        # Fallback: Handle query-based connectors (can't determine source tables)
        if parser.query:
            return self._handle_query_based_connector(parser)

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
        """Generate original topic name before transforms using connector-specific logic."""
        # Use the same logic as TransformPipeline._generate_original_topic
        pipeline = TransformPipeline([])  # Empty pipeline just to use the method
        return pipeline._generate_original_topic(
            schema, table_name, topic_prefix, connector_class
        )

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
        """Extract lineages using simple transform application."""

        # Apply transforms using the unified transform pipeline
        config = self.connector_manifest.config
        transform_result = get_transform_pipeline().apply_forward(topics, config)
        transformed_topics = transform_result.topics

        # Log any warnings from transform processing
        for warning in transform_result.warnings:
            self.report.warning(
                f"Transform warning for {self.connector_manifest.name}: {warning}"
            )

        if transform_result.fallback_used:
            self.report.info(
                f"Complex transforms detected in {self.connector_manifest.name}. "
                f"Consider using 'generic_connectors' config for explicit mappings."
            )

        # Create lineages based on transform results
        lineages = []

        # Get source tables for schema resolution
        table_ids = self.get_table_names()

        for original_topic, final_topic in zip(topics, transformed_topics):
            # Extract source table name from original topic
            source_table = self._extract_source_table_from_topic(
                original_topic, parser.topic_prefix
            )

            # Resolve schema information for hierarchical platforms
            resolved_source_table, dataset_included = self._resolve_source_table_schema(
                source_table, parser.source_platform, table_ids, True
            )

            # Create lineage mapping
            lineage = self._create_lineage_mapping(
                resolved_source_table,
                parser.database_name,
                parser.source_platform,
                final_topic,
                dataset_included,
            )
            lineages.append(lineage)

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
        topic_naming_pattern = rf"{re.escape(topic_prefix)}\.(\w+)\.(\w+)"

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
            # Escape server_name to handle cases where topic.prefix contains dots
            # Some users configure topic.prefix like "my.server" which breaks the regex
            server_name = server_name or ""
            # Regex pattern (\w+\.\w+(?:\.\w+)?) supports BOTH 2-part and 3-part table names
            topic_naming_pattern = rf"({re.escape(server_name)})\.(\w+\.\w+(?:\.\w+)?)"

            if not self.connector_manifest.topic_names:
                return lineages

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

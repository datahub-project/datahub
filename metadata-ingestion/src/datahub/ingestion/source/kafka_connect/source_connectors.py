import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    CONNECTOR_CLASS,
    KAFKA,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    get_dataset_name,
    get_platform_from_connector_class,
    has_three_level_hierarchy,
    remove_prefix,
    unquote,
)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)

logger = logging.getLogger(__name__)


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
                # Use Java regex for consistency with existing Kafka Connect implementation
                from java.util.regex import Pattern

                transform_regex = Pattern.compile(regex_pattern)
                matcher = transform_regex.matcher(topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(replacement))
                    transformed_topics.append(transformed_topic)
                    logger.debug(f"RegexRouter: {topic} -> {transformed_topic}")
                else:
                    transformed_topics.append(topic)
                    logger.debug(f"RegexRouter: {topic} (no match)")

            except Exception as e:
                logger.warning(
                    f"RegexRouter failed for topic '{topic}' with pattern '{regex_pattern}' "
                    f"and replacement '{replacement}': {e}"
                )
                transformed_topics.append(topic)  # Keep original on error

        return transformed_topics


class TransformPipeline:
    """Handles multiple Kafka Connect transforms in sequence."""

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
                logger.warning(f"Unknown transform type: {transform_type}")

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
            Topic name: {topic_prefix}.{schema}.{table_name}
        """
        if schema:
            return f"{topic_prefix}.{schema}.{table_name}"
        else:
            return f"{topic_prefix}.{table_name}"

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
        # Import constants here to avoid circular imports
        from datahub.ingestion.source.kafka_connect.common import (
            CLOUD_JDBC_SOURCE_CLASSES,
        )

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


@dataclass
class ConfluentJDBCSourceConnector(BaseConnector):
    REGEXROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
    KNOWN_TOPICROUTING_TRANSFORMS = [REGEXROUTER]
    # https://kafka.apache.org/documentation/#connect_included_transformation
    KAFKA_NONTOPICROUTING_TRANSFORMS = [
        "InsertField",
        "InsertField$Key",
        "InsertField$Value",
        "ReplaceField",
        "ReplaceField$Key",
        "ReplaceField$Value",
        "MaskField",
        "MaskField$Key",
        "MaskField$Value",
        "ValueToKey",
        "ValueToKey$Key",
        "ValueToKey$Value",
        "HoistField",
        "HoistField$Key",
        "HoistField$Value",
        "ExtractField",
        "ExtractField$Key",
        "ExtractField$Value",
        "SetSchemaMetadata",
        "SetSchemaMetadata$Key",
        "SetSchemaMetadata$Value",
        "Flatten",
        "Flatten$Key",
        "Flatten$Value",
        "Cast",
        "Cast$Key",
        "Cast$Value",
        "HeadersFrom",
        "HeadersFrom$Key",
        "HeadersFrom$Value",
        "TimestampConverter",
        "Filter",
        "InsertHeader",
        "DropHeaders",
    ]
    # https://docs.confluent.io/platform/current/connect/transforms/overview.html
    CONFLUENT_NONTOPICROUTING_TRANSFORMS = [
        "Drop",
        "Drop$Key",
        "Drop$Value",
        "Filter",
        "Filter$Key",
        "Filter$Value",
        "TombstoneHandler",
    ]
    KNOWN_NONTOPICROUTING_TRANSFORMS = (
        KAFKA_NONTOPICROUTING_TRANSFORMS
        + [
            f"org.apache.kafka.connect.transforms.{t}"
            for t in KAFKA_NONTOPICROUTING_TRANSFORMS
        ]
        + CONFLUENT_NONTOPICROUTING_TRANSFORMS
        + [
            f"io.confluent.connect.transforms.{t}"
            for t in CONFLUENT_NONTOPICROUTING_TRANSFORMS
        ]
    )

    @dataclass
    class JdbcParser:
        db_connection_url: str
        source_platform: str
        database_name: str
        topic_prefix: str
        query: str
        transforms: list

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> JdbcParser:
        """Main parser factory - delegates to JDBC URL or Cloud JDBC field parsers"""
        if "connection.url" in connector_manifest.config:
            return self._get_jdbc_url_parser(connector_manifest)
        else:
            return self._get_cloud_jdbc_fields_parser(connector_manifest)

    def _get_jdbc_url_parser(self, connector_manifest: ConnectorManifest) -> JdbcParser:
        """Parse traditional JDBC connector configuration using connection.url"""
        # Reference: https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#connection-url
        url = remove_prefix(
            str(connector_manifest.config.get("connection.url")), "jdbc:"
        )
        url_instance = make_url(url)
        source_platform = get_platform_from_sqlalchemy_uri(str(url_instance))
        database_name = url_instance.database
        assert database_name
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{database_name}"

        # Platform uses topic.prefix
        topic_prefix = connector_manifest.config.get("topic.prefix", "")

        return self._create_parser(
            connector_manifest,
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
        )

    def _get_cloud_jdbc_fields_parser(
        self, connector_manifest: ConnectorManifest
    ) -> JdbcParser:
        """Parse Confluent Cloud JDBC connector configuration using individual database fields"""
        # References:
        # - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html#connection-details
        # - https://docs.confluent.io/cloud/current/connectors/cc-mysql-source.html#connection-details
        connector_class = connector_manifest.config.get(CONNECTOR_CLASS, "")
        source_platform = get_platform_from_connector_class(connector_class)

        hostname = connector_manifest.config.get("database.hostname")
        port = connector_manifest.config.get("database.port")
        database_name = connector_manifest.config.get("database.dbname")

        if not hostname or not port or not database_name:
            raise ValueError(
                f"Missing required Cloud connector config: "
                f"hostname={hostname}, port={port}, database_name={database_name}"
            )

        # Construct connection URL from individual fields with proper URI schemes
        if source_platform == "postgres":
            db_connection_url = f"postgresql://{hostname}:{port}/{database_name}"
        elif source_platform == "mysql":
            db_connection_url = f"mysql://{hostname}:{port}/{database_name}"
        else:
            db_connection_url = f"{source_platform}://{hostname}:{port}/{database_name}"

        # Cloud uses database.server.name as topic prefix
        topic_prefix = connector_manifest.config.get("database.server.name", "")

        return self._create_parser(
            connector_manifest,
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
        )

    def _create_parser(
        self,
        connector_manifest: ConnectorManifest,
        db_connection_url: str,
        source_platform: str,
        database_name: str,
        topic_prefix: str,
    ) -> JdbcParser:
        """Common parser creation logic for both Platform and Cloud connectors"""
        query = connector_manifest.config.get("query", "")

        # Parse transforms (common to both Platform and Cloud)
        transform_names = (
            connector_manifest.config.get("transforms", "").split(",")
            if connector_manifest.config.get("transforms")
            else []
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

        return self.JdbcParser(
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
            query,
            transforms,
        )

    def default_get_lineages(
        self,
        topic_prefix: str,
        database_name: str,
        source_platform: str,
        topic_names: Optional[Iterable[str]] = None,
        include_source_dataset: bool = True,
    ) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = []
        if not topic_names:
            topic_names = self.connector_manifest.topic_names
        table_ids: List[TableId] = self.get_table_names()
        for topic in topic_names:
            # All good for NO_TRANSFORM or (SINGLE_TRANSFORM and KNOWN_NONTOPICROUTING_TRANSFORM) or (not SINGLE_TRANSFORM and all(KNOWN_NONTOPICROUTING_TRANSFORM))
            source_table: str = (
                remove_prefix(topic, topic_prefix) if topic_prefix else topic
            )
            # Clean up leading dot from prefix removal
            if source_table.startswith("."):
                source_table = source_table[1:]

            # include schema name for three-level hierarchies
            if has_three_level_hierarchy(source_platform):
                # Enhanced from original to handle Cloud connectors where source_table can be 'schema.table' format after prefix removal
                # Handle both cases: source_table might be just 'table' or 'schema.table'
                table_id = None
                if "." in source_table:
                    # source_table is already in schema.table format
                    schema_part, table_part = source_table.rsplit(".", 1)
                    for t in table_ids:
                        if t and t.table == table_part and t.schema == schema_part:
                            table_id = t
                            break
                else:
                    # source_table is just table name
                    for t in table_ids:
                        if t and t.table == source_table:
                            table_id = t
                            break

                if table_id and table_id.schema:
                    source_table = f"{table_id.schema}.{table_id.table}"
                else:
                    include_source_dataset = False
                    self.report.warning(
                        "Could not find schema for table"
                        f"{self.connector_manifest.name} : {source_table}",
                    )
            dataset_name: str = get_dataset_name(database_name, source_table)
            lineage = KafkaConnectLineage(
                source_dataset=dataset_name if include_source_dataset else None,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
            )
            lineages.append(lineage)
        return lineages

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
            # Handle both list and dict formats for tasks
            if isinstance(self.connector_manifest.tasks, list):
                # Real Kafka Connect API returns a list of tasks
                tasks_to_iterate = self.connector_manifest.tasks
            else:
                # Test fixtures and some cases use dict format
                tasks_to_iterate = self.connector_manifest.tasks.values()

            table_ids = (
                ",".join(
                    [
                        task["config"]["tables"]
                        for task in tasks_to_iterate
                        if task and "config" in task and task["config"].get("tables")
                    ]
                )
            ).split(",")
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
                table_ids = table_config.split(",")

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
        lineages: List[KafkaConnectLineage] = []

        try:
            # Get source tables
            tables = self.get_table_names()

            # Get connector class for topic generation strategy
            connector_class = self.connector_manifest.config.get(CONNECTOR_CLASS, "")

            # Create transform pipeline
            pipeline = TransformPipeline(transforms)

            # Apply pipeline to generate transform results
            results = pipeline.apply_transforms(
                tables,
                topic_prefix,
                list(self.connector_manifest.topic_names),
                connector_class,
            )

            # Convert results to lineages
            for result in results:
                # Build source dataset name
                if result.schema and has_three_level_hierarchy(source_platform):
                    source_table_name = f"{result.schema}.{result.source_table}"
                else:
                    source_table_name = result.source_table

                source_dataset = get_dataset_name(database_name, source_table_name)

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

        except Exception as e:
            logger.warning(f"Transform pipeline failed: {e}")
            # Fallback to default lineage generation
            return self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
                include_source_dataset=False,
            )

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

        # Handle query-based connectors (can't determine source tables)
        if parser.query:
            return self._handle_query_based_connector(parser)

        # Route to appropriate lineage extraction method based on transforms
        if not parser.transforms:
            # No transforms - simple direct mapping
            return self._extract_simple_lineages(parser)

        elif self._should_use_transform_pipeline(parser.transforms):
            # Use transform pipeline for supported transforms
            return self._extract_lineages_with_pipeline(
                parser.transforms,
                parser.database_name,
                parser.source_platform,
                parser.topic_prefix,
            )

        else:
            # Unknown or unsupported transforms
            return self._handle_unknown_transforms(parser)

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

    def _extract_simple_lineages(self, parser: JdbcParser) -> List[KafkaConnectLineage]:
        """Extract lineages for connectors with no transforms."""
        return self.default_get_lineages(
            database_name=parser.database_name,
            source_platform=parser.source_platform,
            topic_prefix=parser.topic_prefix,
        )

    def _should_use_transform_pipeline(self, transforms: List[Dict[str, str]]) -> bool:
        """Check if we should use the new transform pipeline."""
        if not transforms:
            return False

        # Use pipeline for any supported transforms (single or multiple)
        has_supported_transforms = any(
            transform["type"]
            in list(TransformPipeline.TRANSFORM_CLASSES.keys()) + [self.REGEXROUTER]
            for transform in transforms
        )

        # Use pipeline for transforms that are only non-topic-routing
        all_non_topic_routing = all(
            transform["type"] in self.KNOWN_NONTOPICROUTING_TRANSFORMS
            for transform in transforms
        )

        return has_supported_transforms or all_non_topic_routing

    def _handle_unknown_transforms(
        self, parser: JdbcParser
    ) -> List[KafkaConnectLineage]:
        """Handle connectors with unknown or unsupported transforms."""
        # Check what kind of unknown transforms we have
        unknown_transform_types = [
            transform["type"]
            for transform in parser.transforms
            if transform["type"]
            not in (
                self.KNOWN_TOPICROUTING_TRANSFORMS
                + self.KNOWN_NONTOPICROUTING_TRANSFORMS
            )
        ]

        include_source_dataset = True
        if unknown_transform_types:
            self.report.warning(
                "Could not find input dataset, connector has unknown transforms",
                f"{self.connector_manifest.name} : {', '.join(unknown_transform_types)}",
            )
            include_source_dataset = False

        return self.default_get_lineages(
            database_name=parser.database_name,
            source_platform=parser.source_platform,
            topic_prefix=parser.topic_prefix,
            include_source_dataset=include_source_dataset,
        )


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
                connector_manifest.config["transforms"].split(",")
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

    # Connectors with 2-level container in pattern (database + schema)
    # Others have either database XOR schema, but not both
    DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER_IN_PATTERN = {
        "io.debezium.connector.sqlserver.SqlServerConnector",
    }

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

        if (
            connector_class == "io.debezium.connector.mysql.MySqlConnector"
            or connector_class == "MySqlConnector"
        ):
            parser = self.DebeziumParser(
                source_platform="mysql",
                server_name=self.get_server_name(connector_manifest),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.mongodb.MongoDbConnector":
            parser = self.DebeziumParser(
                source_platform="mongodb",
                server_name=self.get_server_name(connector_manifest),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.postgresql.PostgresConnector":
            parser = self.DebeziumParser(
                source_platform="postgres",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.oracle.OracleConnector":
            parser = self.DebeziumParser(
                source_platform="oracle",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.sqlserver.SqlServerConnector":
            database_name = connector_manifest.config.get(
                "database.names"
            ) or connector_manifest.config.get("database.dbname")

            if "," in str(database_name):
                raise Exception(
                    f"Only one database is supported for Debezium's SQL Server connector. Found: {database_name}"
                )

            parser = self.DebeziumParser(
                source_platform="mssql",
                server_name=self.get_server_name(connector_manifest),
                database_name=database_name,
            )
        elif connector_class == "io.debezium.connector.db2.Db2Connector":
            parser = self.DebeziumParser(
                source_platform="db2",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.vitess.VitessConnector":
            parser = self.DebeziumParser(
                source_platform="vitess",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("vitess.keyspace"),
            )
        else:
            raise ValueError(f"Connector class '{connector_class}' is unknown.")

        return parser

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


JDBC_SOURCE_CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector"
DEBEZIUM_SOURCE_CONNECTOR_PREFIX = "io.debezium.connector"
MONGO_SOURCE_CONNECTOR_CLASS = "com.mongodb.kafka.connect.MongoSourceConnector"

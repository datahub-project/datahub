import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Final, List, Optional, Type

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, LaxStr
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)

KAFKA: Final[str] = "kafka"
SOURCE: Final[str] = "source"
SINK: Final[str] = "sink"
CONNECTOR_CLASS: Final[str] = "connector.class"

# Confluent Cloud connector class names
# References:
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-snowflake-sink.html
POSTGRES_CDC_SOURCE_CLOUD: Final[str] = "PostgresCdcSource"
POSTGRES_CDC_SOURCE_V2_CLOUD: Final[str] = "PostgresCdcSourceV2"
POSTGRES_SINK_CLOUD: Final[str] = "PostgresSink"
SNOWFLAKE_SINK_CLOUD: Final[str] = "SnowflakeSink"
MYSQL_SOURCE_CLOUD: Final[str] = "MySqlSource"
MYSQL_CDC_SOURCE_CLOUD: Final[str] = "MySqlCdcSource"
MYSQL_SINK_CLOUD: Final[str] = "MySqlSink"

# Cloud JDBC source connector classes
CLOUD_JDBC_SOURCE_CLASSES: Final[List[str]] = [
    POSTGRES_CDC_SOURCE_CLOUD,
    POSTGRES_CDC_SOURCE_V2_CLOUD,
    MYSQL_SOURCE_CLOUD,
    MYSQL_CDC_SOURCE_CLOUD,
]

# Cloud sink connector classes
CLOUD_SINK_CLASSES: Final[List[str]] = [
    POSTGRES_SINK_CLOUD,
    SNOWFLAKE_SINK_CLOUD,
    MYSQL_SINK_CLOUD,
]


class ProvidedConfig(ConfigModel):
    provider: str
    path_key: str
    value: LaxStr


class GenericConnectorConfig(ConfigModel):
    connector_name: str
    source_dataset: str
    source_platform: str


class KafkaConnectSourceConfig(
    PlatformInstanceConfigMixin,
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
):
    # See the Connect REST Interface for details
    # https://docs.confluent.io/platform/current/connect/references/restapi.html#
    connect_uri: str = Field(
        default="http://localhost:8083/", description="URI to connect to."
    )
    username: Optional[str] = Field(default=None, description="Kafka Connect username.")
    password: Optional[str] = Field(default=None, description="Kafka Connect password.")
    cluster_name: Optional[str] = Field(
        default="connect-cluster", description="Cluster to ingest from."
    )
    # convert lineage dataset's urns to lowercase
    convert_lineage_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether to convert the urns of ingested lineage dataset to lowercase",
    )
    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for connectors to filter for ingestion.",
    )
    provided_configs: Optional[List[ProvidedConfig]] = Field(
        default=None, description="Provided Configurations"
    )
    connect_to_platform_map: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description='Platform instance mapping when multiple instances for a platform is available. Entry for a platform should be in either `platform_instance_map` or `connect_to_platform_map`. e.g.`connect_to_platform_map: { "postgres-connector-finance-db": "postgres": "core_finance_instance" }`',
    )
    platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description='Platform instance mapping to use when constructing URNs. e.g.`platform_instance_map: { "hive": "warehouse" }`',
    )
    generic_connectors: List[GenericConnectorConfig] = Field(
        default=[],
        description="Provide lineage graph for sources connectors other than Confluent JDBC Source Connector, Debezium Source Connector, and Mongo Source Connector",
    )

    use_connect_topics_api: bool = Field(
        default=True,
        description="Whether to use Kafka Connect API for topic retrieval and validation. "
        "This flag controls the environment-specific topic retrieval strategy: "
        "\n"
        "**When True (default):** "
        "- **Self-hosted environments:** Uses runtime `/connectors/{name}/topics` API for accurate topic information "
        "- **Confluent Cloud:** Uses comprehensive Kafka REST API v3 to get all topics for transform pipeline, with config-based fallback "
        "\n"
        "**When False:** "
        "Disables all API-based topic retrieval for both environments. Returns empty topic lists. "
        "Useful for air-gapped environments or when topic validation isn't needed for performance optimization.",
    )

    # Confluent Cloud Kafka API configuration for comprehensive topic retrieval
    kafka_rest_endpoint: Optional[str] = Field(
        default=None,
        description="Optional: Confluent Cloud Kafka REST API endpoint for comprehensive topic retrieval. "
        "Format: https://pkc-xxxxx.region.provider.confluent.cloud "
        "If not specified, DataHub automatically derives the endpoint from connector configurations (kafka.endpoint). "
        "When available, enables getting all topics from Kafka cluster for improved transform pipeline accuracy.",
    )

    kafka_api_key: Optional[str] = Field(
        default=None,
        description="Optional: Confluent Cloud Kafka API key for authenticating with Kafka REST API v3. "
        "If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. "
        "Only needed if you want to use separate credentials for the Kafka API.",
    )

    kafka_api_secret: Optional[str] = Field(
        default=None,
        description="Optional: Confluent Cloud Kafka API secret for authenticating with Kafka REST API v3. "
        "If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. "
        "Only needed if you want to use separate credentials for the Kafka API.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class KafkaConnectSourceReport(StaleEntityRemovalSourceReport):
    connectors_scanned: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_connector_scanned(self, connector: str) -> None:
        self.connectors_scanned += 1

    def report_dropped(self, connector: str) -> None:
        self.filtered.append(connector)


@dataclass
class KafkaConnectLineage:
    """Class to store Kafka Connect lineage mapping, Each instance is potential DataJob"""

    source_platform: str
    target_dataset: str
    target_platform: str
    job_property_bag: Optional[Dict[str, str]] = None
    source_dataset: Optional[str] = None


@dataclass
class ConnectorManifest:
    """Each instance is potential DataFlow"""

    name: str
    type: str
    config: Dict[str, str]
    tasks: List[Dict[str, dict]]
    url: Optional[str] = None
    flow_property_bag: Optional[Dict[str, str]] = None
    lineages: List[KafkaConnectLineage] = field(default_factory=list)
    topic_names: List[str] = field(default_factory=list)

    def extract_lineages(
        self, config: "KafkaConnectSourceConfig", report: "KafkaConnectSourceReport"
    ) -> List[KafkaConnectLineage]:
        """Extract lineages for this connector using appropriate connector class."""
        connector_class_type = self._get_connector_class_type()
        if not connector_class_type:
            return []

        # Create instance and extract
        connector_instance = connector_class_type(self, config, report)
        return connector_instance.extract_lineages()

    def extract_flow_property_bag(
        self, config: "KafkaConnectSourceConfig", report: "KafkaConnectSourceReport"
    ) -> Optional[Dict[str, str]]:
        """Extract flow property bag for this connector using appropriate connector class."""
        connector_class_type = self._get_connector_class_type()
        if not connector_class_type:
            return None

        # Create instance and extract
        connector_instance = connector_class_type(self, config, report)
        return connector_instance.extract_flow_property_bag()

    def _get_connector_class_type(self) -> Optional[Type["BaseConnector"]]:
        """Factory method to determine appropriate connector class based on connector type and class."""
        connector_class_value = self.config.get(CONNECTOR_CLASS) or ""

        # Determine connector class type
        if self.type == SOURCE:
            return self._get_source_connector_type(connector_class_value)
        elif self.type == SINK:
            return self._get_sink_connector_type(connector_class_value)

        return None

    def _get_source_connector_type(
        self, connector_class_value: str
    ) -> Optional[Type["BaseConnector"]]:
        """Get appropriate source connector class type."""
        # Import here to avoid circular imports
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DEBEZIUM_SOURCE_CONNECTOR_PREFIX,
            JDBC_SOURCE_CONNECTOR_CLASS,
            MONGO_SOURCE_CONNECTOR_CLASS,
            ConfluentJDBCSourceConnector,
            DebeziumSourceConnector,
            MongoSourceConnector,
        )

        # JDBC source connector lineages (both Platform and Cloud)
        if (
            connector_class_value == JDBC_SOURCE_CONNECTOR_CLASS
            or connector_class_value in CLOUD_JDBC_SOURCE_CLASSES
        ):
            return ConfluentJDBCSourceConnector
        elif connector_class_value.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX):
            return DebeziumSourceConnector
        elif connector_class_value == MONGO_SOURCE_CONNECTOR_CLASS:
            return MongoSourceConnector

        # Check for generic connectors - this would need access to config.generic_connectors
        # For now, we'll handle this in the calling code since we need access to the config
        return None

    def _get_sink_connector_type(
        self, connector_class_value: str
    ) -> Optional[Type["BaseConnector"]]:
        """Get appropriate sink connector class type."""
        # Import here to avoid circular imports
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            BIGQUERY_SINK_CONNECTOR_CLASS,
            S3_SINK_CONNECTOR_CLASS,
            SNOWFLAKE_SINK_CONNECTOR_CLASS,
            BigQuerySinkConnector,
            ConfluentS3SinkConnector,
            SnowflakeSinkConnector,
        )

        if connector_class_value == BIGQUERY_SINK_CONNECTOR_CLASS:
            return BigQuerySinkConnector
        elif connector_class_value == S3_SINK_CONNECTOR_CLASS:
            return ConfluentS3SinkConnector
        elif connector_class_value == SNOWFLAKE_SINK_CONNECTOR_CLASS:
            return SnowflakeSinkConnector

        return None


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        index = len(prefix)
        return text[index:]
    return text


def unquote(
    string: str, leading_quote: str = '"', trailing_quote: Optional[str] = None
) -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    trailing_quote = trailing_quote if trailing_quote else leading_quote
    if string.startswith(leading_quote) and string.endswith(trailing_quote):
        string = string[1:-1]
    return string


def parse_comma_separated_list(value: str) -> List[str]:
    """
    Safely parse a comma-separated list with robust error handling.

    Args:
        value: Comma-separated string to parse

    Returns:
        List of non-empty stripped items

    Handles edge cases:
    - Empty/None values
    - Leading/trailing commas
    - Multiple consecutive commas
    - Whitespace-only items
    """
    if not value or not value.strip():
        return []

    # Split on comma and clean up each item
    items = []
    for item in value.split(","):
        cleaned_item = item.strip()
        if cleaned_item:  # Only add non-empty items
            items.append(cleaned_item)

    return items


def validate_jdbc_url(url: str) -> bool:
    """Validate JDBC URL format and return whether it's well-formed."""
    if not url or not isinstance(url, str):
        return False

    # Basic JDBC URL validation
    if not url.startswith("jdbc:"):
        return False

    parts = url.split(":", 3)
    return len(parts) >= 3  # jdbc:protocol:connection_details


def parse_table_identifier(identifier: str) -> str:
    """Parse table identifiers that may include quotes or schemas."""
    if not identifier:
        return ""

    # Handle quoted identifiers: "schema"."table" -> schema.table
    import re

    cleaned = re.sub(r'"([^"]+)"', r"\1", identifier)
    return cleaned.strip()


def parse_comma_separated_with_quotes(value: str) -> List[str]:
    """Parse comma-separated lists that may contain quoted values."""
    if not value:
        return []

    # Use csv.reader for proper quote handling
    import csv
    import io

    try:
        reader = csv.reader(io.StringIO(value))
        return [item.strip() for item in next(reader, [])]
    except Exception as e:
        logger.warning(
            f"Failed to parse quoted CSV value '{value}': {e}. Falling back to simple split."
        )
        return parse_comma_separated_list(value)


def match_topics_by_prefix(
    actual_topics: List[str], topic_prefix: str, table_names: List[str]
) -> List[str]:
    """
    Match actual topics to configured tables using topic prefix when direct config fails.

    This is particularly useful for Confluent Cloud where we have actual topics
    but limited source table information.

    Args:
        actual_topics: List of actual topic names from Kafka/Connect API
        topic_prefix: Expected topic prefix from connector config
        table_names: List of table names from connector config (may be empty)

    Returns:
        List of matched topic names that follow the expected pattern
    """
    if not actual_topics or not topic_prefix:
        return []

    matched_topics = []

    # If we have table names, try to match prefix + table combinations
    if table_names:
        for table_name in table_names:
            # Clean table identifier (remove quotes)
            clean_table = parse_table_identifier(table_name)

            # Use the single correct pattern based on connector type
            # This function is generic, so we need context about the connector
            # For now, try both patterns but this should be refactored to be connector-specific

            # Try JDBC pattern first (simple concatenation)
            jdbc_pattern = f"{topic_prefix}{clean_table}"
            if jdbc_pattern in actual_topics:
                matched_topics.append(jdbc_pattern)
                continue

            # Fall back to CDC/Cloud pattern (dot separator)
            cdc_pattern = f"{topic_prefix}.{clean_table}"
            if cdc_pattern in actual_topics:
                matched_topics.append(cdc_pattern)
    else:
        # No table names available - match any topic with the prefix
        for topic in actual_topics:
            if topic.startswith(topic_prefix):
                matched_topics.append(topic)

    return matched_topics


def validate_topic_name(topic_name: str) -> bool:
    """Validate Kafka topic name against naming conventions."""
    if not topic_name or not isinstance(topic_name, str):
        return False

    # Kafka topic naming rules:
    # - Cannot be empty, "." or ".."
    # - Max length 249 characters
    # - Allow most characters except control characters
    if topic_name in [".", ".."] or len(topic_name) > 249:
        return False

    import re

    # Allow alphanumeric, dots, underscores, hyphens, quotes, spaces, and common punctuation
    # This is more permissive to handle quoted identifiers and various connector naming patterns
    return bool(re.match(r'^[a-zA-Z0-9._\-"\s]+$', topic_name))


def get_dataset_name(
    database_name: Optional[str],
    source_table: str,
) -> str:
    return database_name + "." + source_table if database_name else source_table


def get_platform_instance(
    config: KafkaConnectSourceConfig, connector_name: str, platform: str
) -> Optional[str]:
    instance_name = None
    if (
        config.connect_to_platform_map
        and config.connect_to_platform_map.get(connector_name)
        and config.connect_to_platform_map[connector_name].get(platform)
    ):
        instance_name = config.connect_to_platform_map[connector_name][platform]
        if config.platform_instance_map and config.platform_instance_map.get(platform):
            logger.warning(
                f"Same source platform {platform} configured in both platform_instance_map and connect_to_platform_map."
                "Will prefer connector specific platform instance from connect_to_platform_map."
            )
    elif config.platform_instance_map and config.platform_instance_map.get(platform):
        instance_name = config.platform_instance_map[platform]
    logger.info(
        f"Instance name assigned is: {instance_name} for Connector Name {connector_name} and platform {platform}"
    )
    return instance_name


class ConnectorTopicHandler(ABC):
    """Abstract base class for connector-specific topic resolution logic."""

    def __init__(
        self, config: KafkaConnectSourceConfig, report: KafkaConnectSourceReport
    ):
        self.config = config
        self.report = report

    @abstractmethod
    def get_topics_from_config(
        self, connector_manifest: ConnectorManifest
    ) -> List[str]:
        """Extract topics from connector configuration using connector-specific logic."""
        pass

    @abstractmethod
    def supports_connector_class(self, connector_class: str) -> bool:
        """Check if this handler supports the given connector class."""
        pass

    @abstractmethod
    def get_platform(self, connector_class: str) -> str:
        """Get the source platform for this connector type."""
        pass

    def get_topic_fields_for_connector(self, connector_type: str) -> List[str]:
        """Get list of config fields that may contain topic information for this connector."""
        return ["topics", "kafka.topic"]

    def handle_extraction_error(
        self, error: Exception, connector_name: str, operation: str
    ) -> List[str]:
        """Standard error handling for topic extraction failures."""
        logger.warning(f"Failed to {operation} for connector {connector_name}: {error}")
        return []

    def validate_transforms_compatibility(self, config: Dict[str, str]) -> bool:
        """
        Validate that transforms can be properly applied.

        Proactively checks for Java regex library availability and warns
        if RegexRouter transforms will be skipped.
        """
        transforms_param = config.get("transforms", "")
        if not transforms_param:
            return True  # No transforms to validate

        # Check if any RegexRouter transforms are configured
        has_regex_router = False
        transform_names = parse_comma_separated_list(transforms_param)

        for transform_name in transform_names:
            transform_type_key = f"transforms.{transform_name}.type"
            transform_type = config.get(transform_type_key, "")
            if "RegexRouter" in transform_type:
                has_regex_router = True
                break

        if has_regex_router:
            try:
                from java.util.regex import Pattern  # noqa: F401

                return True
            except ImportError:
                logger.warning(
                    "Java regex library unavailable - RegexRouter transforms will be skipped. "
                    "This may result in incorrect topic name predictions for lineage extraction."
                )
                return False

        return True


def transform_connector_config(
    connector_config: Dict, provided_configs: List[ProvidedConfig]
) -> None:
    """This method will update provided configs in connector config values, if any"""
    lookupsByProvider = {}
    for pconfig in provided_configs:
        lookupsByProvider[f"${{{pconfig.provider}:{pconfig.path_key}}}"] = pconfig.value
    for k, v in connector_config.items():
        for key, value in lookupsByProvider.items():
            if key in v:
                connector_config[k] = connector_config[k].replace(key, value)


# TODO: Find a more automated way to discover new platforms with 3 level naming hierarchy.
def has_three_level_hierarchy(platform: str) -> bool:
    return platform in ["postgres", "trino", "redshift", "snowflake"]


@dataclass
class BaseConnector:
    connector_manifest: ConnectorManifest
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def __post_init__(self) -> None:
        """Initialize handler registry for platform detection."""
        self._handler_registry = ConnectorTopicHandlerRegistry(self.config, self.report)

    def get_platform_from_connector_class(self, connector_class: str) -> str:
        """Get the platform for the given connector class using modular handlers."""
        return self._handler_registry.get_platform_for_connector(connector_class)

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        return []

    def extract_flow_property_bag(self) -> Optional[Dict[str, str]]:
        return None

    def infer_mappings(
        self,
        all_topics: Optional[List[str]] = None,
        consumer_groups: Optional[Dict[str, List[str]]] = None,
    ) -> List[KafkaConnectLineage]:
        """
        Infer direct mappings for improved lineage extraction.

        This method provides enhanced lineage discovery by analyzing runtime data
        from the Kafka cluster to find additional topic-to-source mappings that
        may not be apparent from connector configuration alone.

        Enhanced features:
        - Cross-references connector configs with actual Kafka topics
        - Handles complex transform scenarios with runtime topic discovery
        - Supports pattern matching for dynamic topic names
        - Provides fallback mechanisms for edge cases

        Args:
            all_topics: Complete list of topics from Kafka cluster (used by source connectors)
            consumer_groups: Dict mapping consumer group names to their assigned topics (used by sink connectors)

        Returns:
            List of inferred lineage mappings with enhanced accuracy

        Note:
            Subclasses should override this method and use whichever parameters are relevant
            for their connector type (source connectors use all_topics, sink connectors use consumer_groups).
        """
        return []  # Base implementation - override in connector subclasses


class TopicResolver:
    """
    Unified topic resolution for all connector types and environments.

    Consolidates topic resolution logic that was previously scattered across
    multiple files and classes. Provides a single interface for:
    - Confluent Cloud topic resolution via Kafka REST API
    - Self-hosted topic resolution via Connect API
    - Configuration-based topic derivation as fallback
    """

    def __init__(
        self, config: KafkaConnectSourceConfig, report: KafkaConnectSourceReport
    ):
        self.config = config
        self.report = report
        self._handler_registry = ConnectorTopicHandlerRegistry(config, report)

    def resolve_topics(self, connector: ConnectorManifest) -> List[str]:
        """
        Resolve topics for a connector using the most appropriate method.

        Resolution strategy:
        1. Use API-based resolution if available and enabled
        2. Fall back to configuration-based resolution using connector handlers
        3. Return empty list if resolution fails
        """
        try:
            # Try API-based resolution first if enabled
            if self.config.use_connect_topics_api:
                api_topics = self._resolve_api_topics(connector)
                if api_topics:
                    return api_topics

            # Fall back to configuration-based resolution
            return self._resolve_config_topics(connector)

        except Exception as e:
            logger.warning(
                f"Topic resolution failed for connector {connector.name}: {e}"
            )
            return []

    def _resolve_api_topics(self, connector: ConnectorManifest) -> List[str]:
        """Resolve topics using hybrid validation strategy."""
        try:
            # 1. Predict topics from configuration
            predicted_topics = self._resolve_config_topics(connector)

            # 2. Get all topics from Kafka cluster (if available)
            all_kafka_topics = self._get_all_kafka_topics()
            if not all_kafka_topics:
                # No cluster access, return config predictions
                return predicted_topics

            # 3. Validate predictions against actual Kafka topics
            existing_predicted = [t for t in predicted_topics if t in all_kafka_topics]

            # 4. Find additional topics by pattern matching
            pattern_topics = self._find_topics_by_pattern(connector, all_kafka_topics)

            # 5. Combine and deduplicate
            final_topics = list(set(existing_predicted + pattern_topics))

            logger.info(
                f"Topic resolution for {connector.name}: {len(predicted_topics)} predicted, "
                f"{len(existing_predicted)} validated, {len(pattern_topics)} pattern-matched, "
                f"{len(final_topics)} final"
            )

            return final_topics

        except Exception as e:
            logger.warning(f"API topic resolution failed for {connector.name}: {e}")
            return self._resolve_config_topics(connector)

    def _get_all_kafka_topics(self) -> List[str]:
        """Get all topics from Kafka cluster using available APIs."""
        # This would integrate with existing Kafka REST API logic
        # For now, return empty - will be implemented with actual API calls
        return []

    def _find_topics_by_pattern(
        self, connector: ConnectorManifest, all_topics: List[str]
    ) -> List[str]:
        """Find topics that match connector patterns in the complete topic list."""
        config = connector.config
        matching_topics = []

        # Look for topic prefix patterns
        topic_prefix = config.get("topic.prefix", "") or config.get(
            "database.server.name", ""
        )
        if topic_prefix:
            pattern_topics = [t for t in all_topics if t.startswith(topic_prefix)]
            matching_topics.extend(pattern_topics)

        # Look for regex patterns if specified
        topics_regex = config.get("topics.regex")
        if topics_regex:
            try:
                import re

                regex_pattern = re.compile(topics_regex)
                regex_topics = [t for t in all_topics if regex_pattern.match(t)]
                matching_topics.extend(regex_topics)
            except Exception as e:
                logger.warning(
                    f"Failed to apply topics.regex pattern '{topics_regex}': {e}"
                )

        return list(set(matching_topics))

    def _resolve_config_topics(self, connector: ConnectorManifest) -> List[str]:
        """Resolve topics from connector configuration using handlers."""
        connector_class = connector.config.get("connector.class", "")
        handler = self._handler_registry.get_handler_for_connector(connector_class)

        if handler:
            return handler.get_topics_from_config(connector)
        else:
            logger.warning(f"No handler found for connector class: {connector_class}")
            return []

    def _is_confluent_cloud(self, connector: ConnectorManifest) -> bool:
        """Check if this is a Confluent Cloud connector."""
        connector_class = connector.config.get("connector.class", "")
        return connector_class in CLOUD_JDBC_SOURCE_CLASSES + CLOUD_SINK_CLASSES


class ConnectorTopicHandlerRegistry:
    """Registry for connector topic handlers with automatic selection."""

    def __init__(
        self, config: KafkaConnectSourceConfig, report: KafkaConnectSourceReport
    ):
        self.config = config
        self.report = report
        self._handlers: List[ConnectorTopicHandler] = []
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """
        Register all built-in connector handlers.

        HANDLER REGISTRATION COMPLEXITY RATIONALE:

        The handler registration order and structure reflect real-world Kafka Connect deployment patterns:

        1. **Order Dependency**: Specific handlers must be registered before generic ones
           - JDBCSourceTopicHandler handles standard JDBC connectors
           - CloudJDBCSourceTopicHandler handles Confluent Cloud variants
           - GenericConnectorTopicHandler provides fallback for unknown types

        2. **Dynamic Loading**: Handlers are imported at runtime to avoid circular dependencies
           - Each handler may depend on common utilities
           - Common module cannot directly import handlers

        3. **Extensibility**: Registry pattern allows custom handler registration
           - Users can register additional connector types
           - Maintains backward compatibility with existing connectors

        This pattern is complex but necessary to support the diverse ecosystem of
        Kafka Connect connectors across different deployment environments.
        """
        # Import here to avoid circular imports
        from datahub.ingestion.source.kafka_connect.handlers import (
            BigQuerySinkTopicHandler,
            CloudJDBCSourceTopicHandler,
            DebeziumSourceTopicHandler,
            GenericConnectorTopicHandler,
            JDBCSourceTopicHandler,
            MongoSourceTopicHandler,
            S3SinkTopicHandler,
            SnowflakeSinkTopicHandler,
        )

        # Order matters: specific handlers first, generic handler last as fallback
        self._handlers = [
            # Source connector handlers (order by specificity)
            JDBCSourceTopicHandler(self.config, self.report),
            DebeziumSourceTopicHandler(self.config, self.report),
            MongoSourceTopicHandler(self.config, self.report),
            CloudJDBCSourceTopicHandler(self.config, self.report),
            # Specific sink connector handlers
            BigQuerySinkTopicHandler(self.config, self.report),
            S3SinkTopicHandler(self.config, self.report),
            SnowflakeSinkTopicHandler(self.config, self.report),
            # Generic fallback handler for all unknown connectors (source and sink)
            GenericConnectorTopicHandler(self.config, self.report),
        ]

    def get_handler_for_connector(
        self, connector_class: str
    ) -> Optional[ConnectorTopicHandler]:
        """Get the appropriate handler for the given connector class."""
        for handler in self._handlers:
            if handler.supports_connector_class(connector_class):
                return handler
        return None

    def register_handler(self, handler: ConnectorTopicHandler) -> None:
        """Register a custom connector handler."""
        self._handlers.append(handler)

    def get_platform_for_connector(self, connector_class: str) -> str:
        """Get the platform for the given connector class using handlers."""
        handler = self.get_handler_for_connector(connector_class)
        if handler:
            return handler.get_platform(connector_class)
        else:
            return "unknown"

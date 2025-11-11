import logging
from dataclasses import dataclass, field
from typing import Dict, Final, List, Optional

from pydantic import model_validator
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

# Default connection settings
DEFAULT_CONNECT_URI: Final[str] = "http://localhost:8083/"

# ================================
# TRANSFORM TYPE CONSTANTS
# ================================

# Topic routing transforms (these affect topic names)
REGEXROUTER_TRANSFORM: Final[str] = "org.apache.kafka.connect.transforms.RegexRouter"
CONFLUENT_TOPIC_REGEX_ROUTER: Final[str] = (
    "io.confluent.connect.cloud.transforms.TopicRegexRouter"
)
DEBEZIUM_EVENT_ROUTER: Final[str] = "io.debezium.transforms.outbox.EventRouter"

KNOWN_TOPIC_ROUTING_TRANSFORMS: Final[List[str]] = [
    REGEXROUTER_TRANSFORM,
    CONFLUENT_TOPIC_REGEX_ROUTER,
    DEBEZIUM_EVENT_ROUTER,
]

# ================================
# DEBEZIUM SPECIFIC CONSTANTS
# ================================

# Debezium connectors that use 2-level container patterns (database + schema)
# Others use either database XOR schema, but not both
DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER: Final[set] = {
    "io.debezium.connector.sqlserver.SqlServerConnector",
}


class ConnectorConfigKeys:
    """Centralized configuration keys to avoid magic strings throughout the codebase."""

    # Core connector configuration
    CONNECTOR_CLASS: Final[str] = "connector.class"

    # Topic configuration
    TOPICS: Final[str] = "topics"
    TOPICS_REGEX: Final[str] = "topics.regex"
    KAFKA_TOPIC: Final[str] = "kafka.topic"
    TOPIC: Final[str] = "topic"
    TOPIC_PREFIX: Final[str] = "topic.prefix"

    # JDBC configuration
    CONNECTION_URL: Final[str] = "connection.url"
    TABLE_INCLUDE_LIST: Final[str] = "table.include.list"
    TABLE_WHITELIST: Final[str] = "table.whitelist"
    QUERY: Final[str] = "query"
    MODE: Final[str] = "mode"

    # Debezium/CDC configuration
    DATABASE_SERVER_NAME: Final[str] = "database.server.name"
    DATABASE_HOSTNAME: Final[str] = "database.hostname"
    DATABASE_PORT: Final[str] = "database.port"
    DATABASE_DBNAME: Final[str] = "database.dbname"
    DATABASE_INCLUDE_LIST: Final[str] = "database.include.list"

    # Kafka configuration
    KAFKA_ENDPOINT: Final[str] = "kafka.endpoint"
    BOOTSTRAP_SERVERS: Final[str] = "bootstrap.servers"
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = "kafka.bootstrap.servers"

    # BigQuery configuration
    PROJECT: Final[str] = "project"
    DEFAULT_DATASET: Final[str] = "defaultDataset"
    DATASETS: Final[str] = "datasets"
    TOPICS_TO_TABLES: Final[str] = "topicsToTables"
    SANITIZE_TOPICS: Final[str] = "sanitizeTopics"
    KEYFILE: Final[str] = "keyfile"

    # Snowflake configuration
    SNOWFLAKE_DATABASE_NAME: Final[str] = "snowflake.database.name"
    SNOWFLAKE_SCHEMA_NAME: Final[str] = "snowflake.schema.name"
    SNOWFLAKE_TOPIC2TABLE_MAP: Final[str] = "snowflake.topic2table.map"
    SNOWFLAKE_PRIVATE_KEY: Final[str] = "snowflake.private.key"
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: Final[str] = "snowflake.private.key.passphrase"

    # S3 configuration
    S3_BUCKET_NAME: Final[str] = "s3.bucket.name"
    TOPICS_DIR: Final[str] = "topics.dir"
    AWS_ACCESS_KEY_ID: Final[str] = "aws.access.key.id"
    AWS_SECRET_ACCESS_KEY: Final[str] = "aws.secret.access.key"
    S3_SSE_CUSTOMER_KEY: Final[str] = "s3.sse.customer.key"
    S3_PROXY_PASSWORD: Final[str] = "s3.proxy.password"

    # MongoDB configuration

    # Transform configuration
    TRANSFORMS: Final[str] = "transforms"

    # Authentication configuration
    VALUE_CONVERTER_BASIC_AUTH_USER_INFO: Final[str] = (
        "value.converter.basic.auth.user.info"
    )


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
        default=DEFAULT_CONNECT_URI, description="URI to connect to."
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

    # Confluent Cloud specific configuration
    confluent_cloud_environment_id: Optional[str] = Field(
        default=None,
        description="Confluent Cloud environment ID (e.g., 'env-xyz123'). "
        "When specified along with confluent_cloud_cluster_id, the connect_uri will be automatically constructed. "
        "This is the recommended approach for Confluent Cloud instead of manually constructing the full URI.",
    )

    confluent_cloud_cluster_id: Optional[str] = Field(
        default=None,
        description="Confluent Cloud Kafka Connect cluster ID (e.g., 'lkc-abc123'). "
        "When specified along with confluent_cloud_environment_id, the connect_uri will be automatically constructed. "
        "This is the recommended approach for Confluent Cloud instead of manually constructing the full URI.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @model_validator(mode="before")
    @classmethod
    def auto_construct_connect_uri(cls, values: Dict) -> Dict:
        """
        Auto-construct connect_uri from Confluent Cloud environment and cluster IDs.

        If both confluent_cloud_environment_id and confluent_cloud_cluster_id are provided,
        and connect_uri is not explicitly set or is the default value, automatically
        construct the Confluent Cloud Connect URI.
        """
        env_id = values.get("confluent_cloud_environment_id")
        cluster_id = values.get("confluent_cloud_cluster_id")
        connect_uri = values.get("connect_uri")

        # Auto-construct if both IDs provided and URI is default or not set
        if (
            env_id
            and cluster_id
            and (not connect_uri or connect_uri == DEFAULT_CONNECT_URI)
        ):
            values["connect_uri"] = (
                f"https://api.confluent.cloud/connect/v1/"
                f"environments/{env_id}/"
                f"clusters/{cluster_id}"
            )
            logger.info(
                f"Auto-constructed Confluent Cloud Connect URI from environment '{env_id}' "
                f"and cluster '{cluster_id}'"
            )

        return values

    def get_connect_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """Get the appropriate credentials for Connect API access."""
        return self.username, self.password

    def get_kafka_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """
        Get the appropriate credentials for Kafka REST API access.

        If dedicated Kafka API credentials are provided, use those.
        Otherwise, fall back to reusing Connect credentials.
        """
        if self.kafka_api_key and self.kafka_api_secret:
            return self.kafka_api_key, self.kafka_api_secret
        # Fall back to Connect credentials (username/password)
        return self.username, self.password

    @staticmethod
    def construct_confluent_cloud_uri(environment_id: str, cluster_id: str) -> str:
        """
        Construct Confluent Cloud Connect URI from environment and cluster IDs.

        Args:
            environment_id: Confluent Cloud environment ID (e.g., 'env-xyz123')
            cluster_id: Confluent Cloud cluster ID (e.g., 'lkc-abc456')

        Returns:
            Fully constructed Confluent Cloud Connect URI
        """
        return (
            f"https://api.confluent.cloud/connect/v1/"
            f"environments/{environment_id}/"
            f"clusters/{cluster_id}"
        )

    def get_effective_connect_uri(self) -> str:
        """
        Get the effective Connect URI.

        Note: With the auto_construct_connect_uri validator, this now simply returns
        connect_uri as it's already been auto-constructed if needed during validation.

        Returns:
            The URI to use for connecting to Kafka Connect
        """
        return self.connect_uri


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
        """Extract lineages for this connector using connector registry."""
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        return ConnectorRegistry.extract_lineages(self, config, report)

    def extract_flow_property_bag(
        self, config: "KafkaConnectSourceConfig", report: "KafkaConnectSourceReport"
    ) -> Optional[Dict[str, str]]:
        """Extract flow property bag for this connector using connector registry."""
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        return ConnectorRegistry.extract_flow_property_bag(self, config, report)

    def get_topics_from_config(
        self, config: "KafkaConnectSourceConfig", report: "KafkaConnectSourceReport"
    ) -> List[str]:
        """Extract topics from connector configuration using connector registry."""
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        return ConnectorRegistry.get_topics_from_config(self, config, report)


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


# Removed: ConnectorTopicHandler abstraction - logic moved directly to BaseConnector subclasses


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
    """
    Simplified base class for connector-specific lineage extraction.

    Each connector type (JDBC, Debezium, S3, etc.) has its own subclass that implements:
    - extract_lineages(): Creates lineage mappings from source to Kafka topics
    - get_topics_from_config(): Extracts topic names from connector configuration
    - supports_connector_class(): Checks if this connector handles the given class
    """

    connector_manifest: ConnectorManifest
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """Extract lineage mappings for this connector. Override in subclasses."""
        return []

    def extract_flow_property_bag(self) -> Optional[Dict[str, str]]:
        """Extract flow properties for this connector. Override in subclasses."""
        return None

    def get_topics_from_config(self) -> List[str]:
        """Extract topics from connector configuration. Override in subclasses."""
        return []

    @staticmethod
    def supports_connector_class(connector_class: str) -> bool:
        """Check if this connector handles the given class. Override in subclasses."""
        return False

    @staticmethod
    def get_platform(connector_class: str) -> str:
        """Get the platform for this connector type. Override in subclasses."""
        return "unknown"


# Removed: TopicResolver and ConnectorTopicHandlerRegistry - logic moved directly to BaseConnector subclasses

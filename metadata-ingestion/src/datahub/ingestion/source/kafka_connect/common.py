import csv
import io
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Dict, Final, List, Optional

from pydantic import model_validator
from pydantic.fields import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    LaxStr,
    TransparentSecretStr,
)
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.kafka_connect.config_constants import (
    parse_comma_separated_list,
)
from datahub.ingestion.source.kafka_connect.pattern_matchers import JavaRegexMatcher
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    get_transform_pipeline,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaMetadataClass,
)
from datahub.sql_parsing._models import _TableName
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

if TYPE_CHECKING:
    from datahub.sql_parsing.schema_resolver import SchemaResolver

logger = logging.getLogger(__name__)

KAFKA: Final[str] = "kafka"
SOURCE: Final[str] = "source"
SINK: Final[str] = "sink"
CONNECTOR_CLASS: Final[str] = "connector.class"
JDBC_PREFIX: Final[str] = "jdbc:"

# Default connection settings
DEFAULT_CONNECT_URI: Final[str] = "http://localhost:8083/"

_QUOTED_IDENTIFIER_RE: Final = re.compile(r'"([^"]+)"')
_VALID_TOPIC_NAME_RE: Final = re.compile(r'^[a-zA-Z0-9._\-"\s]+$')

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


# Confluent Cloud connector class names
# References:
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-snowflake-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-snowflake-source.html
POSTGRES_CDC_SOURCE_CLOUD: Final[str] = "PostgresCdcSource"
POSTGRES_CDC_SOURCE_V2_CLOUD: Final[str] = "PostgresCdcSourceV2"
POSTGRES_SINK_CLOUD: Final[str] = "PostgresSink"
SNOWFLAKE_SINK_CLOUD: Final[str] = "SnowflakeSink"
SNOWFLAKE_SOURCE_CLOUD: Final[str] = "SnowflakeSource"
MYSQL_SOURCE_CLOUD: Final[str] = "MySqlSource"
MYSQL_CDC_SOURCE_CLOUD: Final[str] = "MySqlCdcSource"
MYSQL_CDC_SOURCE_V2_CLOUD: Final[str] = "MySqlCdcSourceV2"
MYSQL_SINK_CLOUD: Final[str] = "MySqlSink"

# Cloud JDBC source connector classes
CLOUD_JDBC_SOURCE_CLASSES: Final[List[str]] = [
    POSTGRES_CDC_SOURCE_CLOUD,
    POSTGRES_CDC_SOURCE_V2_CLOUD,
    MYSQL_SOURCE_CLOUD,
    MYSQL_CDC_SOURCE_CLOUD,
    MYSQL_CDC_SOURCE_V2_CLOUD,
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
    password: Optional[TransparentSecretStr] = Field(
        default=None, description="Kafka Connect password."
    )
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

    kafka_api_secret: Optional[TransparentSecretStr] = Field(
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

    # Schema resolver configuration for enhanced lineage
    use_schema_resolver: bool = Field(
        default=False,
        description="Use DataHub's schema metadata to enhance Kafka Connect connector lineage. "
        "When enabled (requires DataHub graph connection): "
        "1) Expands table patterns (e.g., 'database.*') to actual tables using DataHub metadata "
        "2) Generates fine-grained column-level lineage for Kafka Connect sources/sinks. "
        "\n\n"
        "**Auto-enabled for Confluent Cloud:** This feature is automatically enabled for Confluent Cloud "
        "environments where DataHub graph connection is required. Set `use_schema_resolver: false` to disable. "
        "\n\n"
        "**Prerequisite:** Source database tables must be ingested into DataHub before Kafka Connect ingestion "
        "for this feature to work. Without prior database ingestion, schema resolver will not find table metadata.",
    )

    schema_resolver_expand_patterns: Optional[bool] = Field(
        default=None,
        description="Enable table pattern expansion using DataHub schema metadata. "
        "When use_schema_resolver=True, this controls whether to expand patterns like 'database.*' "
        "to actual table names by querying DataHub. Only applies when use_schema_resolver is enabled. "
        "Defaults to True when use_schema_resolver is enabled.",
    )

    schema_resolver_finegrained_lineage: Optional[bool] = Field(
        default=None,
        description="Enable fine-grained (column-level) lineage extraction using DataHub schema metadata. "
        "When use_schema_resolver=True, this controls whether to generate column-level lineage "
        "by matching schemas between source tables and Kafka topics. Only applies when use_schema_resolver is enabled. "
        "Defaults to True when use_schema_resolver is enabled.",
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

    def is_confluent_cloud(self) -> bool:
        """
        Detect if this configuration is for Confluent Cloud.

        Detection logic:
        1. If environment_id and cluster_id are explicitly configured, assume Confluent Cloud
        2. Otherwise, check if connect_uri follows Confluent Cloud pattern:
           https://api.confluent.cloud/connect/v1/environments/{env-id}/clusters/{cluster-id}
        """
        # Explicit Confluent Cloud configuration takes precedence
        if self.confluent_cloud_environment_id and self.confluent_cloud_cluster_id:
            return True

        # Fallback to URI-based detection
        uri = self.connect_uri.lower()
        return "api.confluent.cloud" in uri and "/connect/v1/" in uri

    @model_validator(mode="after")
    def validate_configuration_interdependencies(self) -> "KafkaConnectSourceConfig":
        """
        Validate configuration field interdependencies and provide clear error messages.

        Checks:
        1. Auto-enable schema_resolver for Confluent Cloud (if not explicitly disabled)
        2. Schema resolver dependent fields require use_schema_resolver=True
        3. Kafka API credentials are complete (key + secret)
        4. Confluent Cloud IDs are complete (environment + cluster)
        5. Warn if conflicting configurations are provided
        """
        # 0. Auto-enable schema_resolver for Confluent Cloud if not explicitly configured
        # This provides better defaults for Cloud users who likely have proper DataHub setup
        if self.is_confluent_cloud() and self.use_schema_resolver is False:
            # Only auto-enable if use_schema_resolver was not explicitly set (still has default False)
            # We check if model_fields_set contains 'use_schema_resolver' to detect explicit setting
            # If user explicitly set it to False, respect that choice
            if "use_schema_resolver" not in self.model_fields_set:
                self.use_schema_resolver = True
                logger.info(
                    "Auto-enabled 'use_schema_resolver' for Confluent Cloud. "
                    "This enables enhanced lineage extraction with column-level lineage and pattern expansion. "
                    "Note: Source database tables must be ingested into DataHub first for this feature to work. "
                    "Set 'use_schema_resolver: false' to disable this behavior."
                )

        # 1. Set schema resolver defaults if not explicitly configured
        if self.use_schema_resolver:
            # Schema resolver is enabled - set sensible defaults for sub-features
            if self.schema_resolver_expand_patterns is None:
                self.schema_resolver_expand_patterns = True
            if self.schema_resolver_finegrained_lineage is None:
                self.schema_resolver_finegrained_lineage = True
        else:
            # Schema resolver is disabled - set defaults to False
            if self.schema_resolver_expand_patterns is None:
                self.schema_resolver_expand_patterns = False
            if self.schema_resolver_finegrained_lineage is None:
                self.schema_resolver_finegrained_lineage = False

        # 2. Validate Kafka API credentials are complete
        kafka_api_key_provided = self.kafka_api_key is not None
        kafka_api_secret_provided = self.kafka_api_secret is not None

        if kafka_api_key_provided != kafka_api_secret_provided:
            raise ValueError(
                "Configuration error: Both 'kafka_api_key' and 'kafka_api_secret' must be provided together. "
                f"Currently kafka_api_key={'set' if kafka_api_key_provided else 'not set'}, "
                f"kafka_api_secret={'set' if kafka_api_secret_provided else 'not set'}."
            )

        # 3. Validate Confluent Cloud IDs are complete
        env_id_provided = self.confluent_cloud_environment_id is not None
        cluster_id_provided = self.confluent_cloud_cluster_id is not None

        if env_id_provided != cluster_id_provided:
            raise ValueError(
                "Configuration error: Both 'confluent_cloud_environment_id' and 'confluent_cloud_cluster_id' "
                "must be provided together for automatic URI construction. "
                f"Currently environment_id={'set' if env_id_provided else 'not set'}, "
                f"cluster_id={'set' if cluster_id_provided else 'not set'}."
            )

        # 4. Warn if conflicting configurations (informational, not error)
        if env_id_provided and cluster_id_provided:
            # Confluent Cloud IDs provided - check for potential conflicts
            if self.connect_uri and self.connect_uri != DEFAULT_CONNECT_URI:
                # User explicitly set connect_uri AND provided Cloud IDs
                constructed_uri = self.construct_confluent_cloud_uri(
                    self.confluent_cloud_environment_id,  # type: ignore[arg-type]
                    self.confluent_cloud_cluster_id,  # type: ignore[arg-type]
                )
                if self.connect_uri != constructed_uri:
                    logger.warning(
                        f"Configuration conflict: Both 'connect_uri' and Confluent Cloud IDs are set. "
                        f"Using connect_uri='{self.connect_uri}' (ignoring environment/cluster IDs). "
                        f"Expected URI from IDs would be: '{constructed_uri}'. "
                        f"Remove connect_uri to use automatic URI construction."
                    )

        # 5. Validate kafka_rest_endpoint format if provided
        if self.kafka_rest_endpoint:
            if not self.kafka_rest_endpoint.startswith(("http://", "https://")):
                raise ValueError(
                    f"Configuration error: 'kafka_rest_endpoint' must be a valid HTTP(S) URL. "
                    f"Got: '{self.kafka_rest_endpoint}'. "
                    f"Expected format: https://pkc-xxxxx.region.provider.confluent.cloud"
                )

        # 6. Warn if schema resolver enabled but all features explicitly disabled
        if self.use_schema_resolver:
            if (
                self.schema_resolver_expand_patterns is False
                and self.schema_resolver_finegrained_lineage is False
            ):
                logger.warning(
                    "Schema resolver is enabled but all features are disabled. "
                    "To fix: Either enable schema_resolver_expand_patterns=True or schema_resolver_finegrained_lineage=True, "
                    "or set use_schema_resolver=False to avoid unnecessary DataHub queries."
                )

        return self

    def get_connect_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """Get the appropriate credentials for Connect API access."""
        password = self.password.get_secret_value() if self.password else None
        return self.username, password

    def get_kafka_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """
        Get the appropriate credentials for Kafka REST API access.

        If dedicated Kafka API credentials are provided, use those.
        Otherwise, fall back to reusing Connect credentials.
        """
        if self.kafka_api_key and self.kafka_api_secret:
            return self.kafka_api_key, self.kafka_api_secret.get_secret_value()
        # Fall back to Connect credentials (username/password)
        password = self.password.get_secret_value() if self.password else None
        return self.username, password

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
    fine_grained_lineages: Optional[List[FineGrainedLineageClass]] = None


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


def validate_jdbc_url(url: str) -> bool:
    """Validate JDBC URL format and return whether it's well-formed."""
    if not url or not isinstance(url, str):
        return False

    if not url.startswith(JDBC_PREFIX):
        return False

    parts = url.split(":", 3)
    return len(parts) >= 3  # jdbc:protocol:connection_details


def parse_table_identifier(identifier: str) -> str:
    """Parse table identifiers that may include quotes or schemas."""
    if not identifier:
        return ""

    # Handle quoted identifiers: "schema"."table" -> schema.table
    cleaned = _QUOTED_IDENTIFIER_RE.sub(r"\1", identifier)
    return cleaned.strip()


def parse_comma_separated_with_quotes(value: str) -> List[str]:
    """Parse comma-separated lists that may contain quoted values."""
    if not value:
        return []

    # Use csv.reader for proper quote handling
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

    # Allow alphanumeric, dots, underscores, hyphens, quotes, spaces, and common punctuation
    # This is more permissive to handle quoted identifiers and various connector naming patterns
    return bool(_VALID_TOPIC_NAME_RE.match(topic_name))


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

    # Only log when a platform instance is actually assigned (non-None)
    if instance_name:
        logger.debug(
            f"Platform instance '{instance_name}' assigned for connector '{connector_name}' platform '{platform}'"
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
    schema_resolver: Optional["SchemaResolver"] = None
    all_cluster_topics: Optional[List[str]] = (
        None  # All topics from Kafka cluster (Confluent Cloud only, for validation)
    )

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

    def get_platform(self) -> str:
        """Get the platform for this connector instance. Override in subclasses."""
        return "unknown"

    def _discover_tables_from_database(
        self, database_name: str, platform: str
    ) -> List[str]:
        """
        Discover all tables in a database by querying DataHub.

        This method queries DataHub for all tables in the specified database and platform.
        It's used when connectors don't have table.include.list configured, meaning they
        capture ALL tables from the database.

        The SchemaResolver cache is pre-populated during initialization via
        initialize_schema_resolver_from_datahub(), which fetches all schema metadata
        from DataHub for the configured platform and environment.

        Args:
            database_name: The database name (e.g., "mydb", "testdb")
            platform: The platform name (e.g., "postgres", "mysql")

        Returns:
            List of table names in schema.table format (e.g., ["public.users", "public.orders"])
        """
        if not self.schema_resolver:
            logger.warning("SchemaResolver not available for table discovery")
            return []

        try:
            # Get URNs from pre-populated SchemaResolver cache
            all_urns = self.schema_resolver.get_urns()

            if not all_urns:
                logger.warning(
                    f"No datasets found in DataHub for platform={platform}, env={self.schema_resolver.env}. "
                    f"Make sure you've ingested {platform} datasets into DataHub before running Kafka Connect ingestion."
                )
                return []

            logger.debug(
                f"Processing {len(all_urns)} URNs from SchemaResolver cache for platform={platform}, database={database_name}"
            )

            discovered_tables = []

            for urn in all_urns:
                # URN format: urn:li:dataset:(urn:li:dataPlatform:postgres,database.schema.table,PROD)
                table_name = self._extract_table_name_from_urn(urn)
                if not table_name:
                    continue

                # Filter by database - check if table_name starts with database prefix
                if database_name:
                    if table_name.lower().startswith(f"{database_name.lower()}."):
                        # Remove database prefix to get "schema.table"
                        schema_table = table_name[len(database_name) + 1 :]
                        discovered_tables.append(schema_table)
                else:
                    # No database filtering - include all tables
                    discovered_tables.append(table_name)

            logger.info(
                f"Discovered {len(discovered_tables)} tables from database '{database_name}' for platform '{platform}'"
            )
            return discovered_tables

        except Exception as e:
            logger.warning(
                f"Failed to discover tables from database '{database_name}': {e}",
                exc_info=True,
            )
            return []

    def _apply_field_transforms(
        self, source_columns: List[str]
    ) -> Dict[str, Optional[str]]:
        """Apply field-level SMT transforms to a column mapping (source → target name).

        Handles ReplaceField (include/exclude/rename), ExtractField (unwrap a nested
        struct), and HoistField (wrap all fields under a new struct field).
        Returns a dict mapping each source column to its target name, or None if dropped.
        """
        transforms_config = self.connector_manifest.config.get("transforms", "")
        if not transforms_config:
            return {col: col for col in source_columns}

        transform_names = parse_comma_separated_list(transforms_config)
        column_mapping: Dict[str, Optional[str]] = {col: col for col in source_columns}

        for transform_name in transform_names:
            transform_type = self.connector_manifest.config.get(
                f"transforms.{transform_name}.type", ""
            )
            cfg_prefix = f"transforms.{transform_name}."
            transform_cfg = {
                k[len(cfg_prefix) :]: v
                for k, v in self.connector_manifest.config.items()
                if k.startswith(cfg_prefix)
            }

            if (
                transform_type
                == "org.apache.kafka.connect.transforms.ReplaceField$Value"
            ):
                column_mapping = self._apply_replace_field(
                    column_mapping, transform_cfg
                )
            elif transform_type in (
                "org.apache.kafka.connect.transforms.ExtractField$Value",
                "org.apache.kafka.connect.transforms.ExtractField$Key",
            ):
                column_mapping = self._apply_extract_field(
                    column_mapping, transform_cfg
                )
            elif transform_type in (
                "org.apache.kafka.connect.transforms.HoistField$Value",
                "org.apache.kafka.connect.transforms.HoistField$Key",
            ):
                column_mapping = self._apply_hoist_field(column_mapping, transform_cfg)
            elif transform_type in (
                "org.apache.kafka.connect.transforms.Flatten$Value",
                "org.apache.kafka.connect.transforms.Flatten$Key",
            ):
                column_mapping = self._apply_flatten(column_mapping, transform_cfg)

        return column_mapping

    @staticmethod
    def _apply_replace_field(
        column_mapping: Dict[str, Optional[str]], cfg: Dict[str, str]
    ) -> Dict[str, Optional[str]]:
        if include_config := cfg.get("include", ""):
            include_fields = set(parse_comma_separated_list(include_config))
            for col in list(column_mapping.keys()):
                if column_mapping[col] not in include_fields:
                    column_mapping[col] = None

        if exclude_config := cfg.get("exclude", ""):
            exclude_fields = set(parse_comma_separated_list(exclude_config))
            for col in list(column_mapping.keys()):
                if column_mapping[col] in exclude_fields:
                    column_mapping[col] = None

        if rename_config := cfg.get("renames", ""):
            rename_map = {}
            for pair in parse_comma_separated_list(rename_config):
                if ":" in pair:
                    from_field, to_field = pair.split(":", 1)
                    rename_map[from_field.strip()] = to_field.strip()
            for col in list(column_mapping.keys()):
                current = column_mapping[col]
                if current and current in rename_map:
                    column_mapping[col] = rename_map[current]

        return column_mapping

    @staticmethod
    def _apply_extract_field(
        column_mapping: Dict[str, Optional[str]], cfg: Dict[str, str]
    ) -> Dict[str, Optional[str]]:
        # Promotes sub-fields of one struct field as the top-level record.
        # e.g. ExtractField with field=after on a Debezium envelope exposes
        # {after.id, after.name} as {id, name}.
        extract_field = cfg.get("field", "")
        if not extract_field:
            return column_mapping
        prefix = f"{extract_field}."
        return {
            col: target[len(prefix) :]
            for col, target in column_mapping.items()
            if target is not None and target.startswith(prefix)
        }

    @staticmethod
    def _apply_hoist_field(
        column_mapping: Dict[str, Optional[str]], cfg: Dict[str, str]
    ) -> Dict[str, Optional[str]]:
        # Wraps all fields under a new named struct field.
        # e.g. HoistField with field=data maps {id, name} → {data.id, data.name}.
        hoist_field = cfg.get("field", "")
        if not hoist_field:
            return column_mapping
        return {
            col: f"{hoist_field}.{target}" if target is not None else None
            for col, target in column_mapping.items()
        }

    @staticmethod
    def _apply_flatten(
        column_mapping: Dict[str, Optional[str]], cfg: Dict[str, str]
    ) -> Dict[str, Optional[str]]:
        # Flatten converts nested struct paths to delimited names.
        # DataHub already normalises nested paths to dot notation (user.id), so
        # a "." delimiter (the default) is a no-op. Custom delimiters need a substitution.
        delimiter = cfg.get("delimiter", ".")
        if delimiter == ".":
            return column_mapping
        return {
            col: target.replace(".", delimiter) if target is not None else None
            for col, target in column_mapping.items()
        }

    def _extract_fine_grained_lineage(
        self,
        source_dataset: str,
        source_platform: str,
        target_dataset: str,
        target_platform: str = "kafka",
    ) -> Optional[List[FineGrainedLineageClass]]:
        """
        Extract column-level lineage using schema metadata from DataHub.

        This unified implementation works for all source connectors that preserve
        column names in a 1:1 mapping (e.g., Debezium connectors, JDBC polling connectors).

        Args:
            source_dataset: Source table name (e.g., "database.schema.table")
            source_platform: Source platform (e.g., "postgres", "snowflake", "mysql")
            target_dataset: Target Kafka topic name
            target_platform: Target platform (default: "kafka")

        Returns:
            List of fine-grained lineage dictionaries or None if not available
        """
        if not (
            self.config.use_schema_resolver
            and self.config.schema_resolver_finegrained_lineage
            and self.schema_resolver
        ):
            return None

        # Skip fine-grained lineage for Kafka source platform
        # SchemaResolver is designed for database platforms, not Kafka topics
        if source_platform.lower() == "kafka":
            logger.debug(
                f"Skipping fine-grained lineage extraction for Kafka topic {source_dataset} "
                "- schema resolver only supports database platforms"
            )
            return None

        try:
            # Build source table reference
            source_table = _TableName(
                database=None, db_schema=None, table=source_dataset
            )

            # Resolve source table schema from DataHub
            source_urn_str, source_schema = self.schema_resolver.resolve_table(
                source_table
            )

            if not source_schema:
                logger.debug(
                    f"No schema metadata found in DataHub for {source_platform} table {source_dataset}"
                )
                return None

            # Build target URN using DatasetUrn helper with correct target platform
            # Use platform_instance if configured in platform_instance_map for Kafka
            kafka_platform_instance = (
                self.config.platform_instance_map.get(target_platform)
                if self.config.platform_instance_map
                else None
            )
            target_urn = DatasetUrn.create_from_ids(
                platform_id=target_platform,
                table_name=target_dataset,
                env=self.config.env,
                platform_instance=kafka_platform_instance,
            )

            column_mapping = self._apply_field_transforms(list(source_schema.keys()))

            # Create fine-grained lineage for each source column
            fine_grained_lineages: List[FineGrainedLineageClass] = []

            for source_col in source_schema:
                target_col = column_mapping.get(source_col)

                # Skip if field was dropped by ReplaceField transform
                if target_col is None:
                    logger.debug(
                        f"Skipping column '{source_col}' - dropped by ReplaceField transform"
                    )
                    continue

                fine_grained_lineage = FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(source_urn_str, source_col)],
                    downstreams=[make_schema_field_urn(str(target_urn), target_col)],
                )
                fine_grained_lineages.append(fine_grained_lineage)

            if fine_grained_lineages:
                logger.debug(
                    f"Generated {len(fine_grained_lineages)} fine-grained lineages "
                    f"for {source_platform} table {source_dataset} -> {target_dataset}"
                )
                return fine_grained_lineages

        except Exception as e:
            logger.debug(
                f"Failed to extract fine-grained lineage for "
                f"{source_dataset} -> {target_dataset}: {e}"
            )

        return None

    def _extract_sink_fine_grained_lineage(
        self,
        source_topic: str,
        target_dataset: str,
        target_platform: str,
    ) -> Optional[List[FineGrainedLineageClass]]:
        """Extract column-level lineage for sink connectors (Kafka topic → DB table)."""
        if not (
            self.config.use_schema_resolver
            and self.config.schema_resolver_finegrained_lineage
            and self.schema_resolver
            and self.schema_resolver.graph
        ):
            return None

        try:
            kafka_platform_instance = (
                self.config.platform_instance_map.get(KAFKA)
                if self.config.platform_instance_map
                else None
            )
            source_urn = DatasetUrn.create_from_ids(
                platform_id=KAFKA,
                table_name=source_topic,
                env=self.config.env,
                platform_instance=kafka_platform_instance,
            )

            kafka_schema_aspect = self.schema_resolver.graph.get_aspect(
                str(source_urn), SchemaMetadataClass
            )
            if not kafka_schema_aspect:
                logger.debug(
                    f"No schema found in DataHub for Kafka topic '{source_topic}'. "
                    f"Ingest Kafka topic schemas before Kafka Connect to enable sink column lineage."
                )
                return None

            kafka_fields = [
                get_simple_field_path_from_v2_field_path(field.fieldPath)
                for field in kafka_schema_aspect.fields
            ]

            target_table = _TableName(
                database=None, db_schema=None, table=target_dataset
            )
            target_urn_str, target_schema = self.schema_resolver.resolve_table(
                target_table
            )
            if not target_schema:
                logger.debug(
                    f"No schema found in DataHub for {target_platform} table '{target_dataset}'. "
                    f"Ingest destination tables before Kafka Connect to enable sink column lineage."
                )
                return None

            column_mapping = self._apply_field_transforms(kafka_fields)

            # Some platforms (e.g. Snowflake) store columns in a different case than
            # Kafka field names — try exact match first, then case-insensitive fallback.
            target_schema_lower: Dict[str, str] = {k.lower(): k for k in target_schema}

            fine_grained_lineages: List[FineGrainedLineageClass] = []
            for source_field, target_field in column_mapping.items():
                if target_field is None:
                    continue
                if target_field in target_schema:
                    actual_target_field = target_field
                elif target_field.lower() in target_schema_lower:
                    actual_target_field = target_schema_lower[target_field.lower()]
                else:
                    logger.debug(
                        f"Kafka field '{source_field}' -> '{target_field}' not found "
                        f"in {target_platform} table '{target_dataset}' — skipping"
                    )
                    continue
                fine_grained_lineage = FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(str(source_urn), source_field)],
                    downstreams=[
                        make_schema_field_urn(target_urn_str, actual_target_field)
                    ],
                )
                fine_grained_lineages.append(fine_grained_lineage)

            if fine_grained_lineages:
                logger.debug(
                    f"Generated {len(fine_grained_lineages)} sink fine-grained lineages "
                    f"for Kafka topic '{source_topic}' -> {target_platform} table '{target_dataset}'"
                )
                return fine_grained_lineages

        except Exception as e:
            logger.debug(
                f"Failed to extract sink fine-grained lineage for "
                f"'{source_topic}' -> '{target_dataset}': {e}"
            )

        return None

    def _extract_table_name_from_urn(self, urn: str) -> Optional[str]:
        """
        Extract table name from DataHub URN using standard DatasetUrn parser.

        Args:
            urn: DataHub dataset URN
                Format: urn:li:dataset:(urn:li:dataPlatform:platform,table_name,ENV)
                Example: urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)

        Returns:
            Extracted table name (e.g., "database.schema.table") or None if parsing fails
        """
        try:
            return DatasetUrn.from_string(urn).name
        except Exception as e:
            logger.debug(f"Failed to extract table name from URN {urn}: {e}")
            return None

    def _extract_lineages_from_schema_resolver(
        self,
        source_platform: str,
        topic_namer: Callable[[str], str],
        transforms: List[Dict[str, str]],
        connector_type: str = "connector",
    ) -> List[KafkaConnectLineage]:
        """
        Common helper to extract lineages using SchemaResolver.

        This unified implementation eliminates code duplication between Snowflake, Debezium,
        and other source connectors that derive topic names from table names.

        Args:
            source_platform: Source database platform (postgres, snowflake, mysql, etc.)
            topic_namer: Callback function that converts table_name → base_topic_name
                        Example for Snowflake: lambda table: f"prefix{table}"
                        Example for Debezium: lambda table: f"server.{table}"
            transforms: List of transform configurations to apply to derived topics
            connector_type: Connector type name for logging (default: "connector")

        Returns:
            List of lineage mappings from database tables to expected Kafka topics
        """
        lineages: List[KafkaConnectLineage] = []

        if not self.schema_resolver:
            logger.debug(
                "SchemaResolver not available, cannot derive topics from DataHub"
            )
            return lineages

        try:
            # Get all URNs from pre-populated SchemaResolver cache
            # The cache was initialized with platform-specific datasets from DataHub
            all_urns = self.schema_resolver.get_urns()

            # Filter URNs by platform to ensure we only process the expected source platform
            # (defensive check, though cache should already be platform-specific)
            platform_urns = []
            for urn in all_urns:
                try:
                    dataset_urn = DatasetUrn.from_string(urn)
                    if dataset_urn.platform == source_platform:
                        platform_urns.append(urn)
                except Exception as e:
                    logger.debug(f"Failed to parse URN {urn}: {e}")
                    continue

            logger.info(
                f"SchemaResolver returned {len(platform_urns)} URNs for platform={source_platform}, "
                f"platform_instance={self.schema_resolver.platform_instance or 'None'}, "
                f"will derive {connector_type} topics for connector '{self.connector_manifest.name}'"
            )

            if not platform_urns:
                logger.warning(
                    f"No {source_platform} datasets found in DataHub SchemaResolver cache "
                    f"for platform_instance={self.schema_resolver.platform_instance or 'None'}. "
                    f"Make sure you've ingested {source_platform} datasets into DataHub before running Kafka Connect ingestion."
                )
                return lineages

            # Process each table and generate expected topic name
            for urn in platform_urns:
                table_name = self._extract_table_name_from_urn(urn)
                if not table_name:
                    continue

                # Generate base topic name using connector-specific naming logic
                expected_topic = topic_namer(table_name)

                # Apply transforms if configured
                if transforms:
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

                # Extract fine-grained lineage if enabled
                fine_grained = self._extract_fine_grained_lineage(
                    table_name, source_platform, expected_topic, KAFKA
                )

                # Create lineage mapping
                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=expected_topic,
                    target_platform=KAFKA,
                    fine_grained_lineages=fine_grained,
                )
                lineages.append(lineage)

            logger.info(
                f"Created {len(lineages)} lineages from DataHub schemas for {connector_type} '{self.connector_manifest.name}'"
            )
            return lineages

        except Exception as e:
            logger.warning(
                f"Failed to extract lineages from DataHub schemas for connector '{self.connector_manifest.name}': {e}",
                exc_info=True,
            )
            return []

    def _expand_topic_regex_patterns(
        self,
        topics_regex: str,
        available_topics: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Expand topics.regex pattern against available Kafka topics using JavaRegexMatcher.

        This helper method is used by sink connectors to resolve topics.regex patterns
        when the Kafka API is unavailable (e.g., Confluent Cloud).

        Priority order for topic sources:
        1. Use provided available_topics (from manifest.topic_names if Kafka API worked)
        2. Query DataHub for Kafka topics (if schema_resolver enabled)
        3. Return empty list and warn (can't expand without topic list)

        Args:
            topics_regex: Java regex pattern from topics.regex config
            available_topics: Optional list of available topics (from Kafka API)

        Returns:
            List of topics matching the regex pattern
        """
        matcher = JavaRegexMatcher()

        # Priority 1: Use provided available_topics (from Kafka API)
        if available_topics:
            matched_topics = matcher.filter_matches([topics_regex], available_topics)
            if matched_topics:
                logger.info(
                    f"Expanded topics.regex '{topics_regex}' to {len(matched_topics)} topics "
                    f"from {len(available_topics)} available Kafka topics"
                )
            elif not matched_topics:
                logger.warning(
                    f"Java regex pattern '{topics_regex}' did not match any of the {len(available_topics)} available topics"
                )
            return matched_topics

        # Priority 2: Query DataHub for Kafka topics
        if self.schema_resolver and self.schema_resolver.graph:
            logger.info(
                f"Kafka API unavailable for connector '{self.connector_manifest.name}' - "
                f"querying DataHub for Kafka topics to expand pattern '{topics_regex}'"
            )
            try:
                # Query DataHub for all Kafka topics
                kafka_topic_urns = list(
                    self.schema_resolver.graph.get_urns_by_filter(
                        platform="kafka",
                        env=self.schema_resolver.env,
                        entity_types=["dataset"],
                    )
                )

                datahub_topics = []
                for urn in kafka_topic_urns:
                    topic_name = self._extract_table_name_from_urn(urn)
                    if topic_name:
                        datahub_topics.append(topic_name)

                matched_topics = matcher.filter_matches([topics_regex], datahub_topics)

                logger.info(
                    f"Found {len(matched_topics)} Kafka topics in DataHub matching pattern '{topics_regex}' "
                    f"(out of {len(datahub_topics)} total Kafka topics)"
                )
                return matched_topics

            except Exception as e:
                logger.warning(
                    f"Failed to query DataHub for Kafka topics to expand pattern '{topics_regex}': {e}",
                    exc_info=True,
                )

        # Priority 3: No topic sources available - warn and return empty
        logger.warning(
            f"Cannot expand topics.regex '{topics_regex}' for connector '{self.connector_manifest.name}' - "
            f"Kafka API unavailable and DataHub query not available. "
            f"Enable 'use_schema_resolver' in config to query DataHub for Kafka topics."
        )
        return []

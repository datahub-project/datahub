import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

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

KAFKA = "kafka"
SOURCE = "source"
SINK = "sink"
CONNECTOR_CLASS = "connector.class"

# Common configuration field names
# Connection fields (Platform vs Cloud differences)
CONNECTION_URL = "connection.url"  # Platform JDBC URL
CONNECTION_HOST = "connection.host"  # Cloud individual field
CONNECTION_PORT = "connection.port"  # Cloud individual field
CONNECTION_USER = "connection.user"  # Both Platform and Cloud
CONNECTION_PASSWORD = "connection.password"  # Both Platform and Cloud
DATABASE_HOSTNAME = "database.hostname"  # Cloud alternative
DATABASE_PORT = "database.port"  # Cloud alternative
DATABASE_USER = "database.user"  # Cloud alternative
DATABASE_PASSWORD = "database.password"  # Cloud alternative
DATABASE_DBNAME = "database.dbname"  # Cloud database name
DB_NAME = "db.name"  # Cloud database name alternative

# Topic configuration fields
TOPICS = "topics"  # Sink connector topic list
TOPICS_REGEX = "topics.regex"  # Sink connector topic regex
TOPIC_PREFIX = "topic.prefix"  # Platform topic prefix
DATABASE_SERVER_NAME = "database.server.name"  # Cloud topic prefix

# Table configuration fields (Platform vs Cloud differences)
TABLE_WHITELIST = "table.whitelist"  # Platform table filter
TABLE_INCLUDE_LIST = "table.include.list"  # Cloud table filter
TABLE_NAME_FORMAT = "table.name.format"  # Table naming format

# Transform configuration
TRANSFORMS = "transforms"  # Transform list
TRANSFORM_TYPE_REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter"

# OSS Kafka Connect Platform connector class names
# References:
# - https://docs.confluent.io/platform/current/connect/
# - https://docs.confluent.io/kafka-connectors/jdbc/current/
# - https://debezium.io/documentation/reference/stable/connectors/
JDBC_SOURCE_CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector"
DEBEZIUM_SOURCE_CONNECTOR_PREFIX = "io.debezium"
MONGO_SOURCE_CONNECTOR_CLASS = "com.mongodb.kafka.connect.MongoSourceConnector"
S3_SINK_CONNECTOR_CLASS = "io.confluent.connect.s3.S3SinkConnector"
SNOWFLAKE_SINK_CONNECTOR_CLASS = "com.snowflake.kafka.connector.SnowflakeSinkConnector"
BIGQUERY_SINK_CONNECTOR_CLASS = "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
MYSQL_SINK_CONNECTOR_CLASS = (
    "io.confluent.connect.jdbc.JdbcSinkConnector"  # Platform MySQL via JDBC
)

# Confluent Cloud connector class names
# References:
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2.html
# - https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-mysql-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-snowflake-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html
# - https://docs.confluent.io/cloud/current/connectors/cc-s3-sink.html
# - https://docs.confluent.io/cloud/current/connectors/cc-bigquery-sink.html
POSTGRES_CDC_SOURCE_CLOUD = "PostgresCdcSource"
POSTGRES_CDC_SOURCE_V2_CLOUD = "PostgresCdcSourceV2"
MYSQL_CDC_SOURCE_CLOUD = "MySqlCdcSource"
POSTGRES_SINK_CLOUD = "PostgresSink"
MYSQL_SINK_CLOUD = "MySqlSink"
SNOWFLAKE_SINK_CLOUD = "SnowflakeSink"
DATAGEN_SOURCE_CLOUD = "DatagenSource"
S3_SINK_CLOUD = "S3Sink"
BIGQUERY_SINK_CLOUD = "BigQuerySink"

# OSS Kafka Connect Platform connector class lists
PLATFORM_SOURCE_CLASSES = [
    JDBC_SOURCE_CONNECTOR_CLASS,
    MONGO_SOURCE_CONNECTOR_CLASS,
    # Debezium connectors are handled by prefix matching
]

PLATFORM_SINK_CLASSES = [
    S3_SINK_CONNECTOR_CLASS,
    SNOWFLAKE_SINK_CONNECTOR_CLASS,
    BIGQUERY_SINK_CONNECTOR_CLASS,
    MYSQL_SINK_CONNECTOR_CLASS,
]

# Confluent Cloud connector class lists
CLOUD_JDBC_SOURCE_CLASSES = [
    POSTGRES_CDC_SOURCE_CLOUD,
    POSTGRES_CDC_SOURCE_V2_CLOUD,
    MYSQL_CDC_SOURCE_CLOUD,
]

CLOUD_OTHER_SOURCE_CLASSES = [DATAGEN_SOURCE_CLOUD]

CLOUD_SINK_CLASSES = [
    POSTGRES_SINK_CLOUD,
    MYSQL_SINK_CLOUD,
    SNOWFLAKE_SINK_CLOUD,
    S3_SINK_CLOUD,
    BIGQUERY_SINK_CLOUD,
]

# Combined lists for easy iteration
ALL_SOURCE_CLASSES = (
    PLATFORM_SOURCE_CLASSES + CLOUD_JDBC_SOURCE_CLASSES + CLOUD_OTHER_SOURCE_CLASSES
)
ALL_SINK_CLASSES = PLATFORM_SINK_CLASSES + CLOUD_SINK_CLASSES


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
    tasks: Dict
    url: Optional[str] = None
    flow_property_bag: Optional[Dict[str, str]] = None
    lineages: List[KafkaConnectLineage] = field(default_factory=list)
    topic_names: Iterable[str] = field(default_factory=list)


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


# Connector class to platform mapping for exact matches
CONNECTOR_CLASS_TO_PLATFORM_MAP = {
    # Confluent Cloud connectors
    POSTGRES_CDC_SOURCE_CLOUD: "postgres",
    POSTGRES_CDC_SOURCE_V2_CLOUD: "postgres",
    POSTGRES_SINK_CLOUD: "postgres",
    MYSQL_CDC_SOURCE_CLOUD: "mysql",
    MYSQL_SINK_CLOUD: "mysql",
    SNOWFLAKE_SINK_CLOUD: "snowflake",
    DATAGEN_SOURCE_CLOUD: "datagen",
    S3_SINK_CLOUD: "s3",
    BIGQUERY_SINK_CLOUD: "bigquery",
    # OSS Platform connectors
    JDBC_SOURCE_CONNECTOR_CLASS: "unknown",  # Needs config parsing
    MONGO_SOURCE_CONNECTOR_CLASS: "mongodb",
    S3_SINK_CONNECTOR_CLASS: "s3",
    SNOWFLAKE_SINK_CONNECTOR_CLASS: "snowflake",
    BIGQUERY_SINK_CONNECTOR_CLASS: "bigquery",
    MYSQL_SINK_CONNECTOR_CLASS: "unknown",  # Needs config parsing
}

# Debezium connector patterns for substring matching
DEBEZIUM_PLATFORM_PATTERNS = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "sqlserver": "mssql",
    "mssql": "mssql",
    "mongodb": "mongodb",
    "mongo": "mongodb",
}

# Generic platform patterns for unknown connectors (fallback)
GENERIC_PLATFORM_PATTERNS = {
    "mysql": "mysql",
    "postgresql": "postgres",
    "postgres": "postgres",
    "oracle": "oracle",
    "sqlserver": "mssql",
    "mssql": "mssql",
    "mongodb": "mongodb",
    "mongo": "mongodb",
    "s3": "s3",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}


def get_source_platform_from_connector_class(connector_class: str) -> str:
    """
    Determine source platform from connector class name.

    Supports both OSS Kafka Connect Platform and Confluent Cloud connector classes.
    Uses mapping dictionaries for clean, extensible platform detection.

    Args:
        connector_class: The connector class name (e.g., "PostgresCdcSource" or "io.confluent.connect.jdbc.JdbcSourceConnector")

    Returns:
        Platform name (e.g., "postgres", "mysql", "s3", etc.)

    References:
        - Cloud connectors: https://docs.confluent.io/cloud/current/connectors/
        - Platform connectors: https://docs.confluent.io/platform/current/connect/
    """
    # 1. Exact match lookup (most efficient)
    if connector_class in CONNECTOR_CLASS_TO_PLATFORM_MAP:
        return CONNECTOR_CLASS_TO_PLATFORM_MAP[connector_class]

    # 2. Debezium connector pattern matching
    if connector_class.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX):
        connector_lower = connector_class.lower()
        for pattern, platform in DEBEZIUM_PLATFORM_PATTERNS.items():
            if pattern in connector_lower:
                return platform
        return "unknown"

    # 3. Generic pattern matching for unknown connectors (fallback)
    connector_lower = connector_class.lower()
    for pattern, platform in GENERIC_PLATFORM_PATTERNS.items():
        if pattern in connector_lower:
            return platform

    # 4. Final fallback
    return "unknown"


def is_cloud_connector(connector_class: str) -> bool:
    """
    Determine if a connector class is a Confluent Cloud connector.

    Args:
        connector_class: The connector class name

    Returns:
        True if it's a Confluent Cloud connector, False if it's OSS Platform
    """
    return connector_class in (
        CLOUD_JDBC_SOURCE_CLASSES + CLOUD_OTHER_SOURCE_CLASSES + CLOUD_SINK_CLASSES
    )


def is_platform_connector(connector_class: str) -> bool:
    """
    Determine if a connector class is an OSS Kafka Connect Platform connector.

    Args:
        connector_class: The connector class name

    Returns:
        True if it's an OSS Platform connector, False if it's Confluent Cloud
    """
    return connector_class in (
        PLATFORM_SOURCE_CLASSES + PLATFORM_SINK_CLASSES
    ) or connector_class.startswith(DEBEZIUM_SOURCE_CONNECTOR_PREFIX)


def get_connector_platform_type(connector_class: str) -> str:
    """
    Get the platform type (OSS Platform vs Confluent Cloud) for a connector.

    Args:
        connector_class: The connector class name

    Returns:
        "cloud" for Confluent Cloud, "platform" for OSS, "unknown" for unrecognized
    """
    if is_cloud_connector(connector_class):
        return "cloud"
    elif is_platform_connector(connector_class):
        return "platform"
    else:
        return "unknown"


@dataclass
class BaseConnector:
    connector_manifest: ConnectorManifest
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        return []

    def extract_flow_property_bag(self) -> Optional[Dict[str, str]]:
        return None

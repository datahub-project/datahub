"""
Shared configuration constants and utilities for Kafka Connect.

This module contains constants and utility functions that are used across multiple
modules (common.py, transform_plugins.py, etc.) without creating circular dependencies.
"""

import logging
from typing import Dict, Final, FrozenSet, List, Optional

from pydantic import BaseModel

from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


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
    AWS_SESSION_TOKEN: Final[str] = "aws.session.token"
    S3_SSE_CUSTOMER_KEY: Final[str] = "s3.sse.customer.key"
    S3_SSE_CUSTOMER_KEY_MD5: Final[str] = "s3.sse.customer.key.md5"
    S3_PROXY_PASSWORD: Final[str] = "s3.proxy.password"
    # S3 source: https://docs.confluent.io/kafka-connectors/s3-source/current/configuration_options.html
    S3_SOURCE_MODE: Final[str] = "mode"
    TOPIC_REGEX_LIST: Final[str] = "topic.regex.list"

    # MongoDB configuration

    # ClickHouse configuration
    CLICKHOUSE_HOSTNAME: Final[str] = "hostname"
    CLICKHOUSE_PORT: Final[str] = "port"
    CLICKHOUSE_DATABASE: Final[str] = "database"
    CLICKHOUSE_PASSWORD: Final[str] = "password"
    CLICKHOUSE_TOPIC2TABLE_MAP: Final[str] = "topic2TableMap"

    # Iceberg configuration
    ICEBERG_TABLES: Final[str] = "iceberg.tables"
    ICEBERG_TABLES_DYNAMIC_ENABLED: Final[str] = "iceberg.tables.dynamic-enabled"

    # Transform configuration
    TRANSFORMS: Final[str] = "transforms"

    # Authentication configuration
    VALUE_CONVERTER_BASIC_AUTH_USER_INFO: Final[str] = (
        "value.converter.basic.auth.user.info"
    )


S3_SENSITIVE_CONFIG_KEYS: Final[FrozenSet[str]] = frozenset(
    {
        ConnectorConfigKeys.AWS_ACCESS_KEY_ID,
        ConnectorConfigKeys.AWS_SECRET_ACCESS_KEY,
        ConnectorConfigKeys.AWS_SESSION_TOKEN,
        ConnectorConfigKeys.S3_SSE_CUSTOMER_KEY,
        ConnectorConfigKeys.S3_SSE_CUSTOMER_KEY_MD5,
        ConnectorConfigKeys.S3_PROXY_PASSWORD,
    }
)


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


class S3SourceMode(StrEnum):
    GENERIC = "GENERIC"
    RESTORE_BACKUP = "RESTORE_BACKUP"


class TopicRegexMapping(BaseModel):
    topic: str
    regex: str


class TopicRegexList(BaseModel):
    mappings: List[TopicRegexMapping]
    malformed: List[str]


def parse_topic_regex_list(value: str) -> TopicRegexList:
    """Parse an S3 source `topic.regex.list` (`<topic>:<regex>,<topic>:<regex>`).

    Topic names cannot contain `:`, so the first `:` splits topic from regex.
    """
    mappings: List[TopicRegexMapping] = []
    malformed: List[str] = []
    for pair in parse_comma_separated_list(value):
        topic, sep, regex = pair.partition(":")
        if sep and topic.strip() and regex.strip():
            mappings.append(TopicRegexMapping(topic=topic.strip(), regex=regex.strip()))
        else:
            malformed.append(pair)
    return TopicRegexList(mappings=mappings, malformed=malformed)


def topics_from_regex_list(value: str) -> List[str]:
    return deduplicate_list(
        mapping.topic for mapping in parse_topic_regex_list(value).mappings
    )


def allowed_flow_properties(
    config: Dict[str, str], allowed_keys: FrozenSet[str]
) -> Dict[str, str]:
    """Keep only connector properties known to hold no credentials.

    Connector configs can carry secrets under arbitrary keys (`kafka.api.secret`,
    `aws.session.token`, ...), so new connectors list what they show instead of
    listing what they hide.
    """
    return {k: v for k, v in config.items() if k in allowed_keys}


_REGEX_METACHARACTERS: Final[FrozenSet[str]] = frozenset(".^$*+?{}[]|()")


def literal_directory_prefix(regex: str) -> Optional[str]:
    """Return the folder a path regex is pinned to, or None.

    Only a regex whose literal start ends in `/` (e.g. `topics/orders/.*`) names a
    folder; `.*\\.json` or `topics/orders.*` could match objects anywhere / in sibling
    folders, so they return None.
    """
    literal: List[str] = []
    i = 1 if regex.startswith("^") else 0
    while i < len(regex):
        char = regex[i]
        if char == "\\" and i + 1 < len(regex) and not regex[i + 1].isalnum():
            literal.append(regex[i + 1])
            i += 2
            continue
        if char in _REGEX_METACHARACTERS or char == "\\":
            break
        literal.append(char)
        i += 1
    prefix = "".join(literal)
    if i == len(regex):
        # A fully literal regex is a single object key, not a folder.
        return None
    if not prefix.endswith("/") or not prefix.strip("/"):
        return None
    return prefix.strip("/")


def parse_topic_to_table_map(mappings: List[str]) -> Dict[str, str]:
    """Parse a list of 'topic:table' mapping strings into a dict.

    Entries missing a ':' are skipped with a warning.
    Only the first ':' is used as the delimiter, so table names may contain colons.
    """
    result: Dict[str, str] = {}
    for mapping in mappings:
        if ":" not in mapping:
            logger.warning(
                f"Invalid topic:table mapping format: '{mapping}'. Expected 'topic:table'."
            )
            continue
        topic, table = mapping.split(":", 1)
        result[topic.strip()] = table.strip()
    return result

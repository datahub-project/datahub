"""
Shared configuration constants and utilities for Kafka Connect.

This module contains constants and utility functions that are used across multiple
modules (common.py, transform_plugins.py, etc.) without creating circular dependencies.
"""

import logging
from typing import Final, List

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
    S3_SSE_CUSTOMER_KEY: Final[str] = "s3.sse.customer.key"
    S3_PROXY_PASSWORD: Final[str] = "s3.proxy.password"

    # MongoDB configuration

    # Transform configuration
    TRANSFORMS: Final[str] = "transforms"

    # Authentication configuration
    VALUE_CONVERTER_BASIC_AUTH_USER_INFO: Final[str] = (
        "value.converter.basic.auth.user.info"
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

"""
Constants for Kafka Connect connector types and transforms.

This module centralizes all hardcoded constants used across connector implementations
to improve maintainability and reduce duplication.
"""

from typing import Final, List, Set

# ================================
# CONNECTOR CLASS CONSTANTS
# ================================

# Traditional Platform connectors
JDBC_SOURCE_CONNECTOR_CLASS: Final[str] = (
    "io.confluent.connect.jdbc.JdbcSourceConnector"
)
DEBEZIUM_SOURCE_CONNECTOR_PREFIX: Final[str] = "io.debezium.connector"
MONGO_SOURCE_CONNECTOR_CLASS: Final[str] = (
    "com.mongodb.kafka.connect.MongoSourceConnector"
)

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
# NON-TOPIC ROUTING TRANSFORMS
# ================================

# Standard Apache Kafka Connect transforms that don't affect topic names
# Reference: https://kafka.apache.org/documentation/#connect_included_transformation
KAFKA_NON_TOPIC_ROUTING_TRANSFORMS: Final[List[str]] = [
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

# Confluent-specific transforms that don't affect topic names
# Reference: https://docs.confluent.io/platform/current/connect/transforms/overview.html
CONFLUENT_NON_TOPIC_ROUTING_TRANSFORMS: Final[List[str]] = [
    "Drop",
    "Drop$Key",
    "Drop$Value",
    "Filter",
    "Filter$Key",
    "Filter$Value",
    "TombstoneHandler",
]

# Combined list with fully qualified class names
KNOWN_NON_TOPIC_ROUTING_TRANSFORMS: Final[List[str]] = (
    # Short names
    KAFKA_NON_TOPIC_ROUTING_TRANSFORMS
    + CONFLUENT_NON_TOPIC_ROUTING_TRANSFORMS
    +
    # Fully qualified Apache Kafka names
    [
        f"org.apache.kafka.connect.transforms.{transform}"
        for transform in KAFKA_NON_TOPIC_ROUTING_TRANSFORMS
    ]
    +
    # Fully qualified Confluent names
    [
        f"io.confluent.connect.transforms.{transform}"
        for transform in CONFLUENT_NON_TOPIC_ROUTING_TRANSFORMS
    ]
)

# ================================
# TRANSFORM CLASSIFICATION SETS
# ================================

# Transforms that can be reliably predicted from configuration alone
PREDICTABLE_TRANSFORM_TYPES: Final[Set[str]] = {
    REGEXROUTER_TRANSFORM,
    CONFLUENT_TOPIC_REGEX_ROUTER,
}

# Transforms that are context-dependent and cannot be reliably predicted
COMPLEX_TRANSFORM_TYPES: Final[Set[str]] = {
    DEBEZIUM_EVENT_ROUTER,  # Depends on runtime data from outbox table
}

# ================================
# DEBEZIUM SPECIFIC CONSTANTS
# ================================

# Debezium connectors that use 2-level container patterns (database + schema)
# Others use either database XOR schema, but not both
DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER: Final[Set[str]] = {
    "io.debezium.connector.sqlserver.SqlServerConnector",
}

# ================================
# UTILITY FUNCTIONS
# ================================


def is_topic_routing_transform(transform_type: str) -> bool:
    """Check if a transform type affects topic names."""
    return transform_type in KNOWN_TOPIC_ROUTING_TRANSFORMS


def is_non_topic_routing_transform(transform_type: str) -> bool:
    """Check if a transform type does NOT affect topic names."""
    return transform_type in KNOWN_NON_TOPIC_ROUTING_TRANSFORMS


def is_predictable_transform(transform_type: str) -> bool:
    """Check if a transform type can be predicted from configuration alone."""
    return transform_type in PREDICTABLE_TRANSFORM_TYPES


def is_complex_transform(transform_type: str) -> bool:
    """Check if a transform type requires runtime data and cannot be predicted."""
    return transform_type in COMPLEX_TRANSFORM_TYPES

"""
Constants for Kafka Connect connector types and transforms.

This module centralizes all hardcoded constants used across connector implementations
to improve maintainability and reduce duplication.
"""

from typing import Final, List, Set

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
DEBEZIUM_CONNECTORS_WITH_2_LEVEL_CONTAINER: Final[Set[str]] = {
    "io.debezium.connector.sqlserver.SqlServerConnector",
}

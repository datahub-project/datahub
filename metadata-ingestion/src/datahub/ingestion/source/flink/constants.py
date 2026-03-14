from typing import Dict, FrozenSet, List

# All valid Flink job states (from Flink REST API /v1/jobs/overview)
VALID_FLINK_JOB_STATES: FrozenSet[str] = frozenset(
    {
        "INITIALIZING",
        "CREATED",
        "RUNNING",
        "FAILING",
        "FAILED",
        "CANCELLING",
        "CANCELED",
        "FINISHED",
        "RESTARTING",
        "SUSPENDED",
        "RECONCILING",
    }
)

# Default states to include in ingestion
DEFAULT_INCLUDE_JOB_STATES: List[str] = ["RUNNING", "FINISHED", "FAILED", "CANCELED"]

# Flink SQL type -> DataHub type mapping
# Normalizes Flink SQL types (uppercase, precision stripped) to DataHub canonical types
FLINK_TYPE_MAP: Dict[str, str] = {
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT": "INT",
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL",
    "CHAR": "CHAR",
    "VARCHAR": "VARCHAR",
    "STRING": "STRING",
    "BINARY": "BINARY",
    "VARBINARY": "VARBINARY",
    "BYTES": "BYTES",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN",
    "ARRAY": "ARRAY",
    "MAP": "MAP",
    "ROW": "STRUCT",
    "MULTISET": "ARRAY",
}

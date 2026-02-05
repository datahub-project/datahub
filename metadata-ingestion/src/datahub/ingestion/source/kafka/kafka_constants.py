DEFAULT_CPU_COUNT_FALLBACK = 4
DEFAULT_MAX_WORKERS_MULTIPLIER = 5
DEFAULT_NESTED_FIELD_MAX_DEPTH = 10
DEFAULT_CONSUMER_TIMEOUT_SECONDS = 2.0
DEFAULT_SESSION_TIMEOUT_MS = 6000
DEFAULT_BATCH_SIZE = 100
DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_MAX_SAMPLE_TIME_SECONDS = 60
DEFAULT_MAX_MESSAGES_PER_TOPIC = 10

# Schema types
SCHEMA_TYPE_AVRO = "AVRO"
SCHEMA_TYPE_PROTOBUF = "PROTOBUF"
SCHEMA_TYPE_JSON = "JSON"

# Profiler type classifications
PROFILER_NUMERIC_TYPES = {"long", "int", "double", "float", "decimal"}
PROFILER_BOOLEAN_TYPES = {"boolean", "bool"}
PROFILER_DATETIME_TYPES = {
    "date",
    "time-micros",
    "time-millis",
    "timestamp-micros",
    "timestamp-millis",
}
PROFILER_STRING_TYPES = {"string", "enum"}
PROFILER_UNKNOWN_TYPES = {"bytes", "fixed", "array", "map", "record", "union", "null"}

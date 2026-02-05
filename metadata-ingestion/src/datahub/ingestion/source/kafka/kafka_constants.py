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

# Schema resolution methods
RESOLUTION_METHOD_SCHEMA_INFERENCE = "schema_inference"
RESOLUTION_METHOD_NONE = "none"
RESOLUTION_METHOD_REGISTRY_FAILED = "registry_failed"
RESOLUTION_METHOD_TOPIC_NAME_STRATEGY = "topic_name_strategy"
RESOLUTION_METHOD_TOPIC_NAME_FAILED = "topic_name_failed"
RESOLUTION_METHOD_TOPIC_SUBJECT_MAP = "topic_subject_map"
RESOLUTION_METHOD_SUBJECT_MAP_FAILED = "subject_map_failed"
RESOLUTION_METHOD_NO_INFERENCE_AVAILABLE = "no_inference_available"
RESOLUTION_METHOD_NO_RECORD_NAMES_FOUND = "no_record_names_found"
RESOLUTION_METHOD_RECORD_NAME_STRATEGY = "record_name_strategy"
RESOLUTION_METHOD_TOPIC_RECORD_NAME_STRATEGY = "topic_record_name_strategy"
RESOLUTION_METHOD_RECORD_NAME_STRATEGIES_FAILED = "record_name_strategies_failed"

# Schema resolution strategy names (for logging)
STRATEGY_NAME_RECORD_NAME = "RecordNameStrategy"
STRATEGY_NAME_TOPIC_RECORD_NAME = "TopicRecordNameStrategy"

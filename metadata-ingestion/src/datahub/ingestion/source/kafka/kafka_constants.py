from datahub.utilities.str_enum import StrEnum

DEFAULT_CPU_COUNT_FALLBACK = 4
DEFAULT_MAX_WORKERS_MULTIPLIER = 5
DEFAULT_NESTED_FIELD_MAX_DEPTH = 10
DEFAULT_CONSUMER_TIMEOUT_SECONDS = 2.0
DEFAULT_SESSION_TIMEOUT_MS = 6000
DEFAULT_BATCH_SIZE = 100
DEFAULT_SAMPLE_SIZE = 200
DEFAULT_MAX_SAMPLE_TIME_SECONDS = 60
DEFAULT_MAX_MESSAGES_PER_TOPIC = 10

# Confluent Schema Registry wire format: a single magic byte (0x00) followed by a
# 4-byte big-endian schema ID, i.e. a 5-byte header before the Avro payload.
CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_WIRE_HEADER_LENGTH = 5

# Profiling caps that keep a single message from bloating the profile or blowing up
# memory. MAX_SAMPLE_VALUE_STR_LENGTH bounds each rendered value string; the flatten
# caps bound how much of a nested payload we expand.
MAX_SAMPLE_VALUE_STR_LENGTH = 1000
MAX_FLATTEN_DICT_KEYS = 100
MAX_FLATTEN_LIST_ITEMS = 50
DEFAULT_HISTOGRAM_BUCKETS = 10
MAX_DISTINCT_VALUE_FREQUENCIES = 10
# Fallback cap on profiled fields when max_number_of_fields_to_profile is unset and
# expensive profiling is disabled.
DEFAULT_MAX_FIELDS_TO_PROFILE = 10

# Schema types
SCHEMA_TYPE_AVRO = "AVRO"
SCHEMA_TYPE_PROTOBUF = "PROTOBUF"
SCHEMA_TYPE_JSON = "JSON"


# Coarse field categories the profiler assigns to each field. STRING doubles as
# the "not yet determined" sentinel during type detection.
class ProfilerFieldType(StrEnum):
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"
    STRING = "STRING"
    UNKNOWN = "UNKNOWN"


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
class ResolutionMethod(StrEnum):
    SCHEMA_INFERENCE = "schema_inference"
    NONE = "none"
    REGISTRY_FAILED = "registry_failed"
    TOPIC_NAME_STRATEGY = "topic_name_strategy"
    TOPIC_NAME_FAILED = "topic_name_failed"
    TOPIC_SUBJECT_MAP = "topic_subject_map"
    SUBJECT_MAP_FAILED = "subject_map_failed"
    NO_INFERENCE_AVAILABLE = "no_inference_available"
    NO_RECORD_NAMES_FOUND = "no_record_names_found"
    RECORD_NAME_STRATEGY = "record_name_strategy"
    TOPIC_RECORD_NAME_STRATEGY = "topic_record_name_strategy"
    RECORD_NAME_STRATEGIES_FAILED = "record_name_strategies_failed"


# Schema resolution strategy names (for logging)
STRATEGY_NAME_RECORD_NAME = "RecordNameStrategy"
STRATEGY_NAME_TOPIC_RECORD_NAME = "TopicRecordNameStrategy"

import re
from typing import Dict, Set

from datahub.metadata.schema_classes import MLFeatureDataTypeClass

PLATFORM_NAME: str = "zipline"

# `compile.py` writes one thrift-JSON file per object, nested by team:
# group_bys/<team>/<name>.
DEFAULT_PRODUCTION_DIR: str = "production"
GROUP_BYS_DIR: str = "group_bys"
JOINS_DIR: str = "joins"
STAGING_QUERIES_DIR: str = "staging_queries"

# Keys embedded in the JSON string held by `MetaData.customJson`. The Chronon
# Python DSL stashes user-supplied tags there rather than in a first-class field.
CUSTOM_JSON_GROUPBY_TAGS_KEY: str = "groupby_tags"
CUSTOM_JSON_JOIN_TAGS_KEY: str = "join_tags"
CUSTOM_JSON_COLUMN_TAGS_KEY: str = "column_tags"

# Chronon serializes `Operation` as its thrift ordinal; this mirrors
# `ai.chronon.api.ttypes.Operation` so feature names match (SUM -> "sum").
OPERATION_NAMES: Dict[int, str] = {
    0: "min",
    1: "max",
    2: "first",
    3: "last",
    4: "unique_count",
    5: "approx_unique_count",
    6: "count",
    7: "sum",
    8: "average",
    9: "variance",
    10: "skew",
    11: "kurtosis",
    12: "approx_percentile",
    13: "last_k",
    14: "first_k",
    15: "top_k",
    16: "bottom_k",
    17: "histogram",
    18: "approx_histogram_k",
    19: "bounded_unique_count",
}

# Operations whose feature name is `<op_without_k><k_value>` (e.g. LAST_K with
# argMap {"k": "10"} -> "last10"), matching Chronon's `_get_op_suffix`.
K_OPERATIONS: Set[int] = {13, 14, 15, 16}
K_ARG_MAP_KEY: str = "k"

# `TimeUnit` thrift enum ordinal -> the single-character suffix Chronon appends
# to a window length (e.g. 7 DAYS -> "7d", 1 HOURS -> "1h").
TIME_UNIT_SUFFIX: Dict[int, str] = {
    0: "h",  # HOURS
    1: "d",  # DAYS
}

# Best-effort mapping from aggregation operation ordinal to an MLFeatureDataType.
# The compiled config carries no output schema, so the feature's statistical
# shape is inferred from the operation rather than a real column type.
_COUNT_OPERATIONS: Set[int] = {4, 5, 6, 19}
_CONTINUOUS_OPERATIONS: Set[int] = {0, 1, 7, 8, 9, 10, 11, 12}
_SEQUENCE_OPERATIONS: Set[int] = {13, 14, 15, 16, 17, 18}


def operation_to_feature_data_type(operation: int) -> str:
    if operation in _COUNT_OPERATIONS:
        return MLFeatureDataTypeClass.COUNT
    if operation in _CONTINUOUS_OPERATIONS:
        return MLFeatureDataTypeClass.CONTINUOUS
    if operation in _SEQUENCE_OPERATIONS:
        return MLFeatureDataTypeClass.SEQUENCE
    return MLFeatureDataTypeClass.UNKNOWN


# Mirrors `ai.chronon.utils.sanitize` (api.Extensions.scala): every character
# outside [a-zA-Z0-9_] becomes an underscore. Used to derive output table names
# from an object's dotted `MetaData.name`.
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize(name: str) -> str:
    return _SANITIZE_RE.sub("_", name)

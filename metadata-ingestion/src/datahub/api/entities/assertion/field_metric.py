from enum import Enum


class FieldMetric(Enum):
    UNIQUE_COUNT = "unique_count"
    UNIQUE_PERCENTAGE = "unique_percentage"
    NULL_COUNT = "null_count"
    NULL_PERCENTAGE = "null_percentage"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    STDDEV = "stddev"
    NEGATIVE_COUNT = "negative_count"
    NEGATIVE_PERCENTAGE = "negative_percentage"
    ZERO_COUNT = "zero_count"
    ZERO_PERCENTAGE = "zero_percentage"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    EMPTY_COUNT = "empty_count"
    EMPTY_PERCENTAGE = "empty_percentage"

"""
Constants specific to column metric assertions.

This module contains constants that are specific to column metric assertions.
Shared constants have been moved to column_assertion_constants.py.
"""

from enum import Enum
from typing import Union

# Re-export shared constants for backward compatibility
from acryl_datahub_cloud.sdk.assertion_input.column_assertion_constants import (  # noqa: F401
    ALLOWED_COLUMN_TYPES_FOR_COLUMN_ASSERTION,
    FIELD_VALUES_OPERATOR_CONFIG,
    OperatorInputType,
    OperatorType,
    RangeInputType,
    RangeTypeInputType,
    RangeTypeParsedType,
    ValueInputType,
    ValueType,
    ValueTypeInputType,
)
from datahub.metadata import schema_classes as models

# Backward compatibility alias
ALLOWED_COLUMN_TYPES_FOR_COLUMN_METRIC_ASSERTION = (
    ALLOWED_COLUMN_TYPES_FOR_COLUMN_ASSERTION
)

# Keep this in sync with FIELD_METRIC_TYPE_CONFIG in the frontend
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
FIELD_METRIC_TYPE_CONFIG = {
    "STRING": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
        models.FieldMetricTypeClass.MAX_LENGTH,
        models.FieldMetricTypeClass.MIN_LENGTH,
        models.FieldMetricTypeClass.EMPTY_COUNT,
        models.FieldMetricTypeClass.EMPTY_PERCENTAGE,
    ],
    "NUMBER": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
        models.FieldMetricTypeClass.MAX,
        models.FieldMetricTypeClass.MIN,
        models.FieldMetricTypeClass.MEAN,
        models.FieldMetricTypeClass.MEDIAN,
        models.FieldMetricTypeClass.STDDEV,
        models.FieldMetricTypeClass.NEGATIVE_COUNT,
        models.FieldMetricTypeClass.NEGATIVE_PERCENTAGE,
        models.FieldMetricTypeClass.ZERO_COUNT,
        models.FieldMetricTypeClass.ZERO_PERCENTAGE,
    ],
    "BOOLEAN": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "DATE": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "TIME": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "NULL": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
}


class MetricType(str, Enum):
    """Enum for field metric types used in column metric assertions."""

    NULL_COUNT = models.FieldMetricTypeClass.NULL_COUNT
    NULL_PERCENTAGE = models.FieldMetricTypeClass.NULL_PERCENTAGE
    UNIQUE_COUNT = models.FieldMetricTypeClass.UNIQUE_COUNT
    UNIQUE_PERCENTAGE = models.FieldMetricTypeClass.UNIQUE_PERCENTAGE
    MAX_LENGTH = models.FieldMetricTypeClass.MAX_LENGTH
    MIN_LENGTH = models.FieldMetricTypeClass.MIN_LENGTH
    EMPTY_COUNT = models.FieldMetricTypeClass.EMPTY_COUNT
    EMPTY_PERCENTAGE = models.FieldMetricTypeClass.EMPTY_PERCENTAGE
    MIN = models.FieldMetricTypeClass.MIN
    MAX = models.FieldMetricTypeClass.MAX
    MEAN = models.FieldMetricTypeClass.MEAN
    MEDIAN = models.FieldMetricTypeClass.MEDIAN
    STDDEV = models.FieldMetricTypeClass.STDDEV
    NEGATIVE_COUNT = models.FieldMetricTypeClass.NEGATIVE_COUNT
    NEGATIVE_PERCENTAGE = models.FieldMetricTypeClass.NEGATIVE_PERCENTAGE
    ZERO_COUNT = models.FieldMetricTypeClass.ZERO_COUNT
    ZERO_PERCENTAGE = models.FieldMetricTypeClass.ZERO_PERCENTAGE


# Type alias specific to column metric assertions
MetricInputType = Union[MetricType, models.FieldMetricTypeClass, str]

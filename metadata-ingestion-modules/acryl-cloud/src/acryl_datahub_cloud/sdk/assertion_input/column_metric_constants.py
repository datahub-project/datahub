"""
Shared constants for column metric assertions (both smart and non-smart).

This module contains constants that are used by both smart and non-smart column metric assertions
to ensure consistency and avoid duplication.
"""

from enum import Enum
from typing import Union

from datahub.metadata import schema_classes as models

# Keep this in sync with the frontend in getEligibleFieldColumns
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
ALLOWED_COLUMN_TYPES_FOR_COLUMN_METRIC_ASSERTION = [
    models.StringTypeClass(),
    models.NumberTypeClass(),
    models.BooleanTypeClass(),
    models.DateTypeClass(),
    models.TimeTypeClass(),
    models.NullTypeClass(),
]

# Keep this in sync with FIELD_VALUES_OPERATOR_CONFIG in the frontend
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
FIELD_VALUES_OPERATOR_CONFIG = {
    "STRING": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
        models.AssertionStdOperatorClass.EQUAL_TO,
        models.AssertionStdOperatorClass.IN,
        models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        models.AssertionStdOperatorClass.REGEX_MATCH,
        models.AssertionStdOperatorClass.GREATER_THAN,
        models.AssertionStdOperatorClass.LESS_THAN,
        models.AssertionStdOperatorClass.BETWEEN,
    ],
    "NUMBER": [
        models.AssertionStdOperatorClass.GREATER_THAN,
        models.AssertionStdOperatorClass.LESS_THAN,
        models.AssertionStdOperatorClass.BETWEEN,
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
        models.AssertionStdOperatorClass.EQUAL_TO,
        models.AssertionStdOperatorClass.IN,
        models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        models.AssertionStdOperatorClass.NOT_EQUAL_TO,
    ],
    "BOOLEAN": [
        models.AssertionStdOperatorClass.IS_TRUE,
        models.AssertionStdOperatorClass.IS_FALSE,
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "DATE": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "TIME": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "NULL": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
}

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


class OperatorType(str, Enum):
    """Enum for assertion operators used in column metric assertions."""

    EQUAL_TO = models.AssertionStdOperatorClass.EQUAL_TO
    NOT_EQUAL_TO = models.AssertionStdOperatorClass.NOT_EQUAL_TO
    GREATER_THAN = models.AssertionStdOperatorClass.GREATER_THAN
    GREATER_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    LESS_THAN = models.AssertionStdOperatorClass.LESS_THAN
    LESS_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    BETWEEN = models.AssertionStdOperatorClass.BETWEEN
    IN = models.AssertionStdOperatorClass.IN
    NOT_IN = models.AssertionStdOperatorClass.NOT_IN
    NULL = models.AssertionStdOperatorClass.NULL
    NOT_NULL = models.AssertionStdOperatorClass.NOT_NULL
    IS_TRUE = models.AssertionStdOperatorClass.IS_TRUE
    IS_FALSE = models.AssertionStdOperatorClass.IS_FALSE
    CONTAIN = models.AssertionStdOperatorClass.CONTAIN
    END_WITH = models.AssertionStdOperatorClass.END_WITH
    START_WITH = models.AssertionStdOperatorClass.START_WITH
    REGEX_MATCH = models.AssertionStdOperatorClass.REGEX_MATCH


class ValueType(str, Enum):
    """Enum for assertion parameter value types."""

    STRING = models.AssertionStdParameterTypeClass.STRING
    NUMBER = models.AssertionStdParameterTypeClass.NUMBER
    UNKNOWN = models.AssertionStdParameterTypeClass.UNKNOWN
    # Note: LIST and SET are intentionally excluded as they are not yet supported
    # LIST = models.AssertionStdParameterTypeClass.LIST
    # SET = models.AssertionStdParameterTypeClass.SET


# Type aliases
MetricInputType = Union[MetricType, models.FieldMetricTypeClass, str]
ValueInputType = Union[str, int, float]
ValueTypeInputType = Union[ValueType, models.AssertionStdParameterTypeClass, str]
RangeInputType = tuple[ValueInputType, ValueInputType]
RangeTypeInputType = Union[
    str,
    tuple[str, str],
    ValueTypeInputType,
    tuple[ValueTypeInputType, ValueTypeInputType],
]
RangeTypeParsedType = tuple[ValueTypeInputType, ValueTypeInputType]
OperatorInputType = Union[OperatorType, models.AssertionStdOperatorClass, str]

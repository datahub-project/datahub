"""
Shared utility functions for column assertions (column metric and column value).

This module contains utility functions that are used by both column metric and column value assertions
to ensure consistency and avoid duplication.
"""

import json
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    NO_PARAMETER_OPERATORS,
    RANGE_OPERATORS,
    SINGLE_VALUE_OPERATORS,
    _try_parse_and_validate_schema_classes_enum,
)
from acryl_datahub_cloud.sdk.assertion_input.column_assertion_constants import (
    RangeInputType,
    RangeTypeInputType,
    RangeTypeParsedType,
    ValueInputType,
    ValueTypeInputType,
)
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
)
from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata import schema_classes as models


def _try_parse_and_validate_value_type(
    value_type: Optional[ValueTypeInputType],
) -> models.AssertionStdParameterTypeClass:
    """Parse and validate a value type.

    Args:
        value_type: The value type to parse and validate.

    Returns:
        The parsed AssertionStdParameterTypeClass.

    Raises:
        SDKUsageError: If the value type is None or invalid.
    """
    if value_type is None:
        raise SDKUsageError("Value type is required")

    return _try_parse_and_validate_schema_classes_enum(
        value_type, models.AssertionStdParameterTypeClass
    )


def _deserialize_json_value(value: ValueInputType) -> ValueInputType:
    """
    Deserialize a value that might be a JSON string.

    Args:
        value: The value to deserialize, potentially a JSON string.

    Returns:
        The deserialized value or the original value if not JSON.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _convert_string_to_number(value: str) -> Union[int, float]:
    """
    Convert a string to a number (int or float).

    Args:
        value: The string value to convert.

    Returns:
        The converted number.

    Raises:
        ValueError: If the string cannot be converted to a number.
    """
    if "." in value:
        return float(value)
    return int(value)


def _validate_number_type(
    value: ValueInputType, original_value: ValueInputType
) -> ValueInputType:
    """
    Validate and convert a value to a number type.

    Args:
        value: The deserialized value to validate.
        original_value: The original input value for error messages.

    Returns:
        The validated number value.

    Raises:
        SDKUsageError: If the value cannot be converted to a number.
    """
    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        try:
            return _convert_string_to_number(value)
        except ValueError as e:
            raise SDKUsageError(
                f"Invalid value: {original_value}, must be a number"
            ) from e

    raise SDKUsageError(f"Invalid value: {original_value}, must be a number")


def _validate_string_type(
    value: ValueInputType, original_value: ValueInputType
) -> ValueInputType:
    """
    Validate that a value is a string type.

    Args:
        value: The deserialized value to validate.
        original_value: The original input value for error messages.

    Returns:
        The validated string value.

    Raises:
        SDKUsageError: If the value is not a string.
    """
    if not isinstance(value, str):
        raise SDKUsageError(f"Invalid value: {original_value}, must be a string")
    return value


def _validate_unsupported_types(value_type: ValueTypeInputType) -> None:
    """
    Check for unsupported value types and raise appropriate errors.

    Args:
        value_type: The value type to check.

    Raises:
        SDKNotYetSupportedError: If the value type is LIST or SET.
        SDKUsageError: If the value type is invalid.
    """
    if value_type in (
        models.AssertionStdParameterTypeClass.LIST,
        models.AssertionStdParameterTypeClass.SET,
    ):
        raise SDKNotYetSupportedError(
            "List and set value types are not supported for column assertions"
        )

    valid_types = {
        models.AssertionStdParameterTypeClass.NUMBER,
        models.AssertionStdParameterTypeClass.STRING,
        models.AssertionStdParameterTypeClass.UNKNOWN,
    }

    if value_type not in valid_types:
        raise SDKUsageError(
            f"Invalid value type: {value_type}, valid options are {get_enum_options(models.AssertionStdParameterTypeClass)}"
        )


def _try_parse_and_validate_value(
    value: Optional[ValueInputType],
    value_type: ValueTypeInputType,
) -> ValueInputType:
    """
    Parse and validate a value according to its expected type.

    Args:
        value: The value to parse and validate.
        value_type: The expected type of the value.

    Returns:
        The validated and potentially converted value.

    Raises:
        SDKUsageError: If the value is None, invalid, or cannot be converted.
        SDKNotYetSupportedError: If the value type is not supported.
    """
    if value is None:
        raise SDKUsageError("Value parameter is required for the chosen operator")

    # Deserialize JSON strings if applicable
    deserialized_value = _deserialize_json_value(value)

    # Validate based on expected type
    if value_type == models.AssertionStdParameterTypeClass.NUMBER:
        return _validate_number_type(deserialized_value, value)
    elif value_type == models.AssertionStdParameterTypeClass.STRING:
        return _validate_string_type(deserialized_value, value)
    elif value_type == models.AssertionStdParameterTypeClass.UNKNOWN:
        return deserialized_value  # Accept any type for unknown
    else:
        _validate_unsupported_types(value_type)
        return deserialized_value


def _is_range_required_for_operator(operator: models.AssertionStdOperatorClass) -> bool:
    """Check if the operator requires range parameters."""
    return operator in RANGE_OPERATORS


def _is_value_required_for_operator(operator: models.AssertionStdOperatorClass) -> bool:
    """Check if the operator requires a single value parameter."""
    return operator in SINGLE_VALUE_OPERATORS


def _is_no_parameter_operator(operator: models.AssertionStdOperatorClass) -> bool:
    """Check if the operator takes no parameters."""
    return operator in NO_PARAMETER_OPERATORS


def _try_parse_and_validate_range_type(
    range_type: Optional[RangeTypeInputType] = None,
) -> RangeTypeParsedType:
    """Parse and validate a range type.

    Args:
        range_type: The range type to parse and validate.

    Returns:
        A tuple of (min_type, max_type) as AssertionStdParameterTypeClass values.
    """
    if range_type is None:
        return (
            models.AssertionStdParameterTypeClass.UNKNOWN,
            models.AssertionStdParameterTypeClass.UNKNOWN,
        )
    if isinstance(range_type, tuple):
        return (
            _try_parse_and_validate_schema_classes_enum(
                range_type[0], models.AssertionStdParameterTypeClass
            ),
            _try_parse_and_validate_schema_classes_enum(
                range_type[1], models.AssertionStdParameterTypeClass
            ),
        )
    # Single value, we assume the same type for start and end:
    parsed_range_type = _try_parse_and_validate_schema_classes_enum(
        range_type, models.AssertionStdParameterTypeClass
    )
    return parsed_range_type, parsed_range_type


def _try_parse_and_validate_range(
    range: Optional[RangeInputType],
    range_type: RangeTypeParsedType,
    operator: models.AssertionStdOperatorClass,
) -> RangeInputType:
    """Parse and validate range parameters.

    Args:
        range: The range values (min, max).
        range_type: The types of the range values.
        operator: The operator being used.

    Returns:
        A tuple of validated (min_value, max_value).

    Raises:
        SDKUsageError: If the range is invalid for the operator.
    """
    if (range is None or range_type is None) and _is_range_required_for_operator(
        operator
    ):
        raise SDKUsageError(f"Range is required for operator {operator}")

    if range is None:
        raise SDKUsageError(f"Range is required for operator {operator}")

    range_start = _try_parse_and_validate_value(range[0], range_type[0])
    range_end = _try_parse_and_validate_value(range[1], range_type[1])

    return (range_start, range_end)

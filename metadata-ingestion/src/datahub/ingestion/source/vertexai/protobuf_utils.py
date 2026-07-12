import logging
from typing import Optional

from google.protobuf.struct_pb2 import Value

logger = logging.getLogger(__name__)


def extract_protobuf_value(value: Value) -> Optional[str]:
    """
    Extract a string value from a protobuf struct value.

    Handles string_value, number_value, and bool_value attributes commonly
    found in protobuf Struct/Value types.

    Args:
        value: A protobuf value object (typically from google.protobuf.struct_pb2.Value)

    Returns:
        String representation of the value, or None if extraction fails
    """
    try:
        if hasattr(value, "string_value") and value.string_value:
            return value.string_value
        elif hasattr(value, "number_value"):
            return str(value.number_value)
        elif hasattr(value, "bool_value"):
            return str(value.bool_value)
        else:
            return str(value)
    except (AttributeError, TypeError) as e:
        logger.debug(f"Failed to extract protobuf value: {e}")
        return None


def extract_numeric_value(value: Value) -> Optional[str]:
    """
    Extract a numeric value from a protobuf struct value and return as STRING.

    NOTE: Despite the name suggesting a numeric return type, this function returns
    a string representation. This is intentional as DataHub's MLMetricClass.value
    field requires string values. The function validates that the value IS numeric
    before returning its string representation.

    Only returns values that can be parsed as floats. This is useful for
    extracting metrics that must be numeric.

    Args:
        value: A protobuf value object (typically from google.protobuf.struct_pb2.Value)

    Returns:
        String representation of the numeric value, or None if not numeric
    """
    try:
        if hasattr(value, "number_value"):
            return str(value.number_value)
        elif hasattr(value, "string_value") and value.string_value:
            try:
                float(value.string_value)
                return value.string_value
            except ValueError:
                return None
        else:
            val_str = str(value)
            try:
                float(val_str)
                return val_str
            except ValueError:
                return None
    except (AttributeError, TypeError) as e:
        logger.debug(f"Failed to extract numeric value: {e}")
        return None

"""Utility functions for extracting values from protobuf structures."""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def extract_protobuf_value(value: Any) -> Optional[str]:
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
        if getattr(value, "string_value", None):
            return value.string_value
        elif getattr(value, "number_value", None) is not None:
            return str(value.number_value)
        elif getattr(value, "bool_value", None) is not None:
            return str(value.bool_value)
        else:
            return str(value)
    except Exception as e:
        logger.debug(f"Failed to extract protobuf value: {e}")
        return None


def extract_numeric_value(value: Any) -> Optional[str]:
    """
    Extract a numeric value from a protobuf struct value.

    Only returns values that can be parsed as floats. This is useful for
    extracting metrics that must be numeric.

    Args:
        value: A protobuf value object (typically from google.protobuf.struct_pb2.Value)

    Returns:
        String representation of the numeric value, or None if not numeric
    """
    try:
        if getattr(value, "number_value", None) is not None:
            return str(value.number_value)
        elif getattr(value, "string_value", None):
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
    except Exception as e:
        logger.debug(f"Failed to extract numeric value: {e}")
        return None

"""Utility functions for Kafka message processing."""

import base64
import json
import logging
from typing import Any, Callable, Dict, Optional, Union

logger = logging.getLogger(__name__)

MessageValue = Union[str, bytes, dict, list, int, float, bool, None]


def decode_kafka_message_value(
    value: bytes,
    topic: str,
    flatten_json_func: Optional[Callable[..., Any]] = None,
    max_depth: int = 10,
) -> Any:
    """
    Decode a Kafka message value handling various data types gracefully.

    Args:
        value: The raw message value as bytes
        topic: Topic name for logging
        flatten_json_func: Optional function to flatten JSON objects
        max_depth: Maximum depth for JSON flattening

    Returns:
        Decoded message value as appropriate Python type
    """
    if not isinstance(value, bytes):
        # Already decoded or not bytes
        return value

    try:
        # Try to decode as UTF-8 first
        try:
            text_data = value.decode("utf-8")
        except UnicodeDecodeError:
            # Binary data - encode as base64 for safe processing
            logger.debug(f"Binary data detected for topic {topic}, encoding as base64")
            return {"binary_data": base64.b64encode(value).decode("utf-8")}

        # Check if text_data is empty or whitespace only
        if not text_data.strip():
            logger.debug(f"Empty message detected for topic {topic}")
            return {"empty_message": True}

        # Try to parse as JSON
        try:
            decoded = json.loads(text_data)
            if isinstance(decoded, (dict, list)):
                if isinstance(decoded, list):
                    decoded = {"item": decoded}

                # Apply JSON flattening if function provided
                if flatten_json_func and isinstance(decoded, dict):
                    return flatten_json_func(decoded, "", None, max_depth)
                return decoded
            return decoded
        except json.JSONDecodeError:
            # Valid UTF-8 but not JSON - return as text
            return {"text_value": text_data}

    except Exception as e:
        # If all else fails, use base64 encoding
        logger.debug(f"Failed to process message data for topic {topic}: {e}")
        return {"binary_data": base64.b64encode(value).decode("utf-8")}


def process_kafka_message_for_sampling(value: Any) -> Dict[str, Any]:
    """
    Process a Kafka message value for schema inference sampling.

    Args:
        value: The message value (can be bytes, dict, etc.)

    Returns:
        Dictionary suitable for schema inference
    """
    if value is None:
        return {"null_value": True}

    if isinstance(value, dict):
        return value

    if isinstance(value, bytes):
        try:
            # Try to decode as UTF-8 first
            text_value = value.decode("utf-8")

            # Check if empty or whitespace only
            if not text_value.strip():
                return {"empty_message": True}

            # Try to parse as JSON
            try:
                decoded_value = json.loads(text_value)
                if isinstance(decoded_value, dict):
                    return decoded_value
                return {"json_value": decoded_value}
            except json.JSONDecodeError:
                # Valid UTF-8 but not JSON - treat as text
                return {"text_value": text_value}

        except UnicodeDecodeError:
            # Binary data - encode as base64 for schema inference
            return {"binary_data": base64.b64encode(value).decode("utf-8")}

    # For other types (str, int, float, bool, list)
    return {"value": str(value)}

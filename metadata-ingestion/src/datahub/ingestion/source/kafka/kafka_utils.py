import base64
import json
import logging
from typing import Any, Callable, Dict, Optional, Union

from datahub.ingestion.source.kafka.kafka_constants import (
    DEFAULT_NESTED_FIELD_MAX_DEPTH,
)

logger = logging.getLogger(__name__)

MessageValue = Union[str, bytes, dict, list, int, float, bool, None]

FlattenJsonFunc = Callable[..., Dict[str, str]]


def decode_kafka_message_value(
    value: bytes,
    topic: str,
    flatten_json_func: Optional[FlattenJsonFunc] = None,
    max_depth: int = DEFAULT_NESTED_FIELD_MAX_DEPTH,
) -> MessageValue:
    # Returns None to signal "skip this message" for a binary payload we can't
    # decode without a schema. Emitting a synthetic base64 field instead would
    # pollute the profile with a fake column. The caller counts the skip.
    if not isinstance(value, bytes):
        return value

    try:
        text_data = value.decode("utf-8")
    except UnicodeDecodeError:
        logger.debug(f"Skipping binary message for topic {topic}")
        return None

    if not text_data.strip():
        return {"empty_message": True}

    try:
        decoded = json.loads(text_data)
    except json.JSONDecodeError:
        return {"text_value": text_data}

    if isinstance(decoded, list):
        decoded = {"item": decoded}
    if isinstance(decoded, dict) and flatten_json_func:
        return flatten_json_func(decoded, "", None, max_depth)
    return decoded


def process_kafka_message_for_sampling(value: MessageValue) -> Dict[str, Any]:
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

import base64
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Union

from datahub.ingestion.source.kafka.kafka_constants import (
    DEFAULT_NESTED_FIELD_MAX_DEPTH,
    MAX_FLATTEN_DICT_KEYS,
    MAX_FLATTEN_LIST_ITEMS,
    MAX_SAMPLE_VALUE_STR_LENGTH,
)

logger = logging.getLogger(__name__)

MessageValue = Union[str, bytes, dict, list, int, float, bool, None]

FlattenJsonFunc = Callable[..., Dict[str, Any]]


def _render_leaf(value: Any, stringify: bool) -> Any:
    if not stringify:
        return value
    text = str(value)
    if len(text) > MAX_SAMPLE_VALUE_STR_LENGTH:
        text = text[:MAX_SAMPLE_VALUE_STR_LENGTH] + "..."
    return text


def flatten_json(
    nested_json: Union[Dict[str, Any], List[Any]],
    parent_key: str = "",
    flattened_dict: Optional[Dict[str, Any]] = None,
    max_depth: int = DEFAULT_NESTED_FIELD_MAX_DEPTH,
    current_depth: int = 0,
    seen_objects: Optional[Set[int]] = None,
    stringify_values: bool = True,
) -> Dict[str, Any]:
    # stringify_values=True (profiling) stringifies leaves and expands lists into
    # indexed keys; False (schema inference) keeps native types so fields can be typed.
    if flattened_dict is None:
        flattened_dict = {}
    if seen_objects is None:
        seen_objects = set()

    if current_depth >= max_depth:
        flattened_dict[parent_key or "truncated"] = (
            f"<truncated at depth {max_depth}>" if stringify_values else nested_json
        )
        return flattened_dict

    obj_id = id(nested_json)
    if obj_id in seen_objects:
        flattened_dict[parent_key or "circular"] = "<circular reference>"
        return flattened_dict
    if isinstance(nested_json, (dict, list)):
        seen_objects.add(obj_id)

    try:
        if isinstance(nested_json, dict):
            for key in list(nested_json.keys())[:MAX_FLATTEN_DICT_KEYS]:
                value = nested_json[key]
                new_key = f"{parent_key}.{key}" if parent_key else key
                if stringify_values and isinstance(value, (dict, list)):
                    flatten_json(
                        {"value": value} if isinstance(value, list) else value,
                        new_key,
                        flattened_dict,
                        max_depth,
                        current_depth + 1,
                        seen_objects,
                        stringify_values=True,
                    )
                elif (
                    not stringify_values
                    and isinstance(value, dict)
                    and current_depth < max_depth - 1
                ):
                    flatten_json(
                        value,
                        new_key,
                        flattened_dict,
                        max_depth,
                        current_depth + 1,
                        seen_objects,
                        stringify_values=False,
                    )
                else:
                    flattened_dict[new_key] = _render_leaf(value, stringify_values)
        elif isinstance(nested_json, list) and stringify_values:
            for i, item in enumerate(nested_json[:MAX_FLATTEN_LIST_ITEMS]):
                new_key = f"{parent_key}[{i}]"
                if isinstance(item, (dict, list)):
                    flatten_json(
                        {"item": item},
                        new_key,
                        flattened_dict,
                        max_depth,
                        current_depth + 1,
                        seen_objects,
                        stringify_values=True,
                    )
                else:
                    flattened_dict[new_key] = _render_leaf(item, True)
        elif isinstance(nested_json, list):
            flattened_dict[parent_key or "item"] = nested_json
        else:
            key = parent_key if stringify_values else (parent_key or "value")
            flattened_dict[key] = _render_leaf(nested_json, stringify_values)
    finally:
        if isinstance(nested_json, (dict, list)) and obj_id in seen_objects:
            seen_objects.discard(obj_id)

    return flattened_dict


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

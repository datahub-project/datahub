import json
from typing import Any

from datahub.api.entities.common.serialized_value import SerializedResourceValue
from datahub.metadata.schema_classes import (
    SerializedValueContentTypeClass,
)
from datahub.utilities.str_enum import StrEnum


class ResourceType(StrEnum):
    USER_INFO = "USER_INFO"
    CHANNEL_INFO = "CHANNEL_INFO"
    USER_ID_MAPPING = "USER_ID_MAPPING"


def from_serialized_value(serialized_value: SerializedResourceValue) -> Any:
    value_str = serialized_value.blob.decode("utf-8")
    if serialized_value.content_type == SerializedValueContentTypeClass.JSON:
        return json.loads(value_str)
    else:
        raise ValueError(f"unknown content type {serialized_value.content_type}")

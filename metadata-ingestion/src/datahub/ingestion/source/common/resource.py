import json
from typing import Any, Dict, Optional

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    SerializedValueClass,
    SerializedValueContentTypeClass,
)
from datahub.utilities.str_enum import StrEnum


class ResourceType(StrEnum):
    USER_INFO = "USER_INFO"
    CHANNEL_INFO = "CHANNEL_INFO"
    USER_ID_MAPPING = "USER_ID_MAPPING"


def generate_user_id_mapping_resource_urn(
    platform: str, platform_instance: Optional[str], env: str
) -> str:
    guid = builder.datahub_guid(
        {
            "platform": platform,
            "instance": platform_instance,
            "env": env,
            "type": ResourceType.USER_ID_MAPPING,
        }
    )
    resource_urn = f"urn:li:platformResource:{guid}"
    return resource_urn


def to_serialized_value(value: Dict[str, str]) -> SerializedValueClass:
    serialized_value = SerializedValueClass(
        blob=json.dumps(value).encode("utf-8"),
        contentType=SerializedValueContentTypeClass.JSON,
    )
    return serialized_value


def from_serialized_value(serialized_value: SerializedValueClass) -> Any:
    value_str = serialized_value.blob.decode("utf-8")
    if serialized_value.contentType == SerializedValueContentTypeClass.JSON:
        return json.loads(value_str)
    else:
        raise ValueError(f"unknown content type {serialized_value.contentType}")

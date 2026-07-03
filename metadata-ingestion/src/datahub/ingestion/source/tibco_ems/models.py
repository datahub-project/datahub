from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datahub.ingestion.source.tibco_ems.constants import (
    DEST_TYPE_QUEUE,
    DEST_TYPE_TOPIC,
)


class DestinationType(str, Enum):
    QUEUE = DEST_TYPE_QUEUE
    TOPIC = DEST_TYPE_TOPIC

    @classmethod
    def parse(cls, value: str) -> Optional["DestinationType"]:
        try:
            return cls(value.strip().lower())
        except ValueError:
            return None


class TibcoDestination(BaseModel):
    # Mirrors an EMS queue or topic as returned by the REST Proxy. Only the name
    # is required; the remaining attributes are surfaced as custom properties when
    # present. `destination_type` is set by the client from the source endpoint,
    # not parsed from the payload.
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    destination_type: DestinationType
    is_global: Optional[bool] = Field(default=None, alias="global")
    secure: Optional[bool] = None
    max_msgs: Optional[int] = Field(default=None, alias="maxMsgs")
    max_bytes: Optional[int] = Field(default=None, alias="maxBytes")
    prefetch: Optional[int] = None
    expiration: Optional[int] = None
    pending_message_count: Optional[int] = Field(
        default=None, alias="pendingMessageCount"
    )
    consumer_count: Optional[int] = Field(default=None, alias="consumerCount")


class BridgeTarget(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    destination_type: Optional[DestinationType] = Field(default=None, alias="type")
    selector: Optional[str] = None

    @field_validator("destination_type", mode="before")
    @classmethod
    def _normalise_type(cls, value: object) -> Optional[str]:
        if isinstance(value, str):
            parsed = DestinationType.parse(value)
            return parsed.value if parsed is not None else None
        return None


class TibcoBridge(BaseModel):
    # A bridge routes messages from a source destination to one or more targets.
    # It is the only cross-destination relationship the REST Proxy exposes, so it
    # is the basis for lineage edges.
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    source_name: str = Field(alias="name")
    source_type: Optional[DestinationType] = Field(default=None, alias="type")
    targets: List[BridgeTarget] = Field(default_factory=list)

    @field_validator("source_type", mode="before")
    @classmethod
    def _normalise_type(cls, value: object) -> Optional[str]:
        if isinstance(value, str):
            parsed = DestinationType.parse(value)
            return parsed.value if parsed is not None else None
        return None

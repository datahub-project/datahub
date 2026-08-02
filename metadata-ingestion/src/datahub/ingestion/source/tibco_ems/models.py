from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.tibco_ems.constants import (
    DEFAULT_SERVER_GROUP,
    DEST_TYPE_QUEUE,
    DEST_TYPE_TOPIC,
)
from datahub.utilities.str_enum import StrEnum


class DestinationType(StrEnum):
    QUEUE = DEST_TYPE_QUEUE
    TOPIC = DEST_TYPE_TOPIC

    @classmethod
    def parse(cls, value: str) -> Optional["DestinationType"]:
        try:
            return cls(value.strip().lower())
        except ValueError:
            return None


class ServerGroupMixin(BaseModel):
    # The proxy stamps every record with the server group and server instance it
    # came from. `server_group` is only absent on proxies predating server groups,
    # where the single implicit group is the whole estate.
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    server_group: str = Field(default=DEFAULT_SERVER_GROUP)
    server_role: Optional[str] = None

    @field_validator("server_group", mode="before")
    @classmethod
    def _default_server_group(cls, value: object) -> object:
        return value if isinstance(value, str) and value else DEFAULT_SERVER_GROUP


class TibcoDestination(ServerGroupMixin):
    # Mirrors an EMS queue or topic as returned by the REST Proxy. Only the name
    # is required; the remaining attributes are surfaced as custom properties when
    # present. `destination_type` is set by the client from the source endpoint,
    # not parsed from the payload.

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


class TibcoBridge(ServerGroupMixin):
    # A bridge routes messages from a source destination to one or more targets.
    # It is the only cross-destination relationship the REST Proxy exposes, so it
    # is the basis for lineage edges. A bridge never spans server groups, so both
    # of its endpoints live in the bridge's own group.

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


class TibcoEmsServerGroupKey(ContainerKey):
    # Identity of the container DataHub creates per EMS server group; the group
    # name is what makes each container urn unique within platform/instance/env.
    server_group: str


# Unbounded: the client parameterises this with the raw dicts of a page before
# they are validated into the record model.
RecordT = TypeVar("RecordT")


class TibcoEmsListing(BaseModel, Generic[RecordT]):
    # The records of a paginated list call, plus any per-server-group errors the
    # proxy reported alongside them. A partial response still carries HTTP 200, so
    # the errors are the only signal that a server group was unreachable.
    records: List[RecordT] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)

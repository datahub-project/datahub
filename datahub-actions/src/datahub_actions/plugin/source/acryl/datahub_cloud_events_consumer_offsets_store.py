import logging
import time
from typing import Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import datahub_guid, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    PlatformResourceInfoClass,
    SerializedValueClass,
    SerializedValueContentTypeClass,
    SerializedValueSchemaTypeClass,
)

logger = logging.getLogger(__name__)


class EventConsumerState(BaseModel):
    VERSION: int = (
        1  # Increment this version when the schema of EventConsumerState changes
    )
    offset_id: Optional[str] = None
    timestamp: Optional[int] = None

    def to_serialized_value(self) -> SerializedValueClass:
        return SerializedValueClass(
            blob=self.to_blob(),
            contentType=SerializedValueContentTypeClass.JSON,
            schemaType=SerializedValueSchemaTypeClass.JSON,
            schemaRef=f"EventConsumerState@{self.VERSION}",
        )

    @classmethod
    def from_serialized_value(cls, value: SerializedValueClass) -> "EventConsumerState":
        try:
            assert value.contentType == SerializedValueContentTypeClass.JSON
            assert value.schemaType == SerializedValueSchemaTypeClass.JSON
            if value.schemaRef is not None:
                assert value.schemaRef.split("@")[0] == "EventConsumerState"
            return cls.from_blob(value.blob)
        except Exception as e:
            logger.error(f"Failed to deserialize EventConsumerState: {e}")
            return cls()

    def to_blob(self) -> bytes:
        return self.json().encode()

    @staticmethod
    def from_blob(blob: bytes) -> "EventConsumerState":
        return EventConsumerState.parse_raw(blob.decode())


class DataHubEventsConsumerPlatformResourceOffsetsStore:
    """
    State store for the Event Source.
    This loads the offset id for a given consumer id string.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        consumer_id: str,
        state_resource_name: str = "state",
    ):
        self.graph = graph
        self.consumer_id = consumer_id
        self.state_resource_name = state_resource_name
        resource_guid = datahub_guid(
            {
                "platform": make_data_platform_urn("datahub"),
                "consumer_id": self.consumer_id,
                "state_resource_name": self.state_resource_name,
            }
        )
        self.state_resource_urn = f"urn:li:platformResource:{resource_guid}"

    def store_offset_id(self, offset_id: str) -> str:
        state = EventConsumerState(offset_id=offset_id, timestamp=int(time.time()))
        resource_info: PlatformResourceInfoClass = PlatformResourceInfoClass(
            resourceType="eventConsumerState",  # Resource type is eventConsumerState
            primaryKey=self.consumer_id,  # Primary key is the consumer id
            value=state.to_serialized_value(),  # Value is the serialized state
        )

        # Emit state to DataHub using MetadataChangeProposalWrapper
        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.state_resource_urn,
            aspect=resource_info,
        )

        # Write to graph
        self.graph.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
        logger.info(f"Stored offset id {offset_id} for consumer id {self.consumer_id}")
        return offset_id

    def load_offset_id(self) -> Optional[str]:
        # Read from graph
        existing_state = self.graph.get_aspect(
            self.state_resource_urn, PlatformResourceInfoClass
        )
        if existing_state is not None and existing_state.value is not None:
            # We have a state aspect
            state = EventConsumerState.from_serialized_value(existing_state.value)
            return state.offset_id

        return None

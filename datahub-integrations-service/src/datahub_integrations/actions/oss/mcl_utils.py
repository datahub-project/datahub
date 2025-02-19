from typing import Any, Callable

from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE


class MCLProcessor:
    """
    A utility class to register and process MetadataChangeLog events.
    """

    def __init__(self) -> None:
        self.entity_aspect_processors: dict[str, dict[str, Callable]] = {}
        pass

    def is_mcl(self, event: EventEnvelope) -> bool:
        return event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE

    def register_processor(
        self, entity_type: str, aspect: str, processor: Callable
    ) -> None:
        if entity_type not in self.entity_aspect_processors:
            self.entity_aspect_processors[entity_type] = {}
        self.entity_aspect_processors[entity_type][aspect] = processor

    def process(self, event: EventEnvelope) -> Any:
        if isinstance(event.event, MetadataChangeLogClass):
            entity_type = event.event.entityType
            aspect = event.event.aspectName
            if (
                entity_type in self.entity_aspect_processors
                and aspect in self.entity_aspect_processors[entity_type]
            ):
                return self.entity_aspect_processors[entity_type][aspect](
                    entity_urn=event.event.entityUrn,
                    aspect_name=event.event.aspectName,
                    aspect_value=event.event.aspect,
                    previous_aspect_value=event.event.previousAspectValue,
                )

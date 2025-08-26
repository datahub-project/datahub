import logging
from typing import Any, Callable, Optional

from datahub.configuration.common import ConfigModel
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent

logger = logging.getLogger(__name__)


class ECETrigger(ConfigModel):
    """
    A configuration class for EntityChangeEvent triggers.
    """

    enabled: bool = True


class ECEProcessor:
    """
    A utility class to register and process EntityChangeEvents.
    """

    EntityChangeEvent_v1: str = "EntityChangeEvent_v1"

    def __init__(self, trigger_config: Optional[ECETrigger] = None) -> None:
        self.entity_aspect_processors: dict[str, dict[str, Callable]] = {}
        self.trigger_config = trigger_config
        pass

    def is_ece(self, event: EventEnvelope) -> bool:
        return event.event_type == ECEProcessor.EntityChangeEvent_v1

    def check_trigger(self, event: EventEnvelope) -> bool:
        return self.is_ece(event)

    def register_processor(
        self, entity_type: str, category: str, processor: Callable
    ) -> None:
        if entity_type not in self.entity_aspect_processors:
            self.entity_aspect_processors[entity_type] = {}
        self.entity_aspect_processors[entity_type][category] = processor

    def process(self, event: EventEnvelope) -> Any:
        if isinstance(event.event, EntityChangeEvent):
            semantic_event = event.event
            entity_type = semantic_event.entityType
            category = semantic_event.category

            logger.debug(
                f"Checking ECE with entity type {entity_type} category {category} entity processors: {self.entity_aspect_processors}"
            )
            if (
                entity_type in self.entity_aspect_processors
                and category in self.entity_aspect_processors[entity_type]
            ):
                logger.info(
                    f"Processing ECE with entity type {entity_type} category {category}"
                )
                return self.entity_aspect_processors[entity_type][category](event=event)

        return None

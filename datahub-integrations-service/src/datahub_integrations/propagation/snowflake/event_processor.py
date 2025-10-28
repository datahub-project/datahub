import json
import logging
from typing import Any, Dict, Optional

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)

from datahub_integrations.propagation.snowflake.util import is_snowflake_urn

logger = logging.getLogger(__name__)


class EventData:
    """Container for processed event data."""

    def __init__(
        self,
        entity_urn: str,
        event_type: str,
        operation: str,
        category: Optional[str] = None,
        aspect_name: Optional[str] = None,
        change_type: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        aspect_data: Optional[Dict[str, Any]] = None,
    ):
        self.entity_urn = entity_urn
        self.event_type = event_type
        self.operation = operation
        self.category = category
        self.aspect_name = aspect_name
        self.change_type = change_type
        self.parameters = parameters or {}
        self.aspect_data = aspect_data or {}


class EventProcessor:
    """Processes different types of DataHub events and extracts relevant data."""

    def process_event(self, event: EventEnvelope) -> Optional[EventData]:
        """
        Process an event envelope and extract relevant data.

        Args:
            event: The event envelope to process

        Returns:
            EventData if the event is relevant, None otherwise
        """
        if event.event_type == "EntityChangeEvent_v1":
            return self._process_entity_change_event(event)
        elif event.event_type == "MetadataChangeLogEvent_v1":
            return self._process_metadata_change_log_event(event)
        else:
            logger.debug(f"Unsupported event type: {event.event_type}")
            return None

    def _process_entity_change_event(self, event: EventEnvelope) -> Optional[EventData]:
        """Process EntityChangeEvent_v1 events."""
        assert isinstance(event.event, EntityChangeEvent)
        semantic_event = event.event

        # Check if this is a Snowflake URN
        if not is_snowflake_urn(semantic_event.entityUrn):
            logger.debug(f"Skipping non-Snowflake URN: {semantic_event.entityUrn}")
            return None

        parameters = semantic_event._inner_dict.get("__parameters_json", {})

        return EventData(
            entity_urn=semantic_event.entityUrn,
            event_type="EntityChangeEvent_v1",
            operation=semantic_event.operation,
            category=semantic_event.category,
            parameters=parameters,
        )

    def _process_metadata_change_log_event(
        self, event: EventEnvelope
    ) -> Optional[EventData]:
        """Process MetadataChangeLogEvent_v1 events."""
        assert isinstance(event.event, MetadataChangeLogEvent)
        mcl_event = event.event

        # Check if this is a Snowflake URN
        if not is_snowflake_urn(mcl_event.entityUrn):
            logger.debug(f"Skipping non-Snowflake URN: {mcl_event.entityUrn}")
            return None

        # Extract aspect data if available
        aspect_data = {}
        if mcl_event.aspect and mcl_event.aspect.value:
            try:
                aspect_data = json.loads(mcl_event.aspect.value.decode("utf-8"))
            except Exception as e:
                logger.error(f"Failed to parse aspect data: {str(e)}")

        return EventData(
            entity_urn=mcl_event.entityUrn,
            event_type="MetadataChangeLogEvent_v1",
            operation=mcl_event.changeType,
            aspect_name=mcl_event.aspectName,
            change_type=mcl_event.changeType,
            aspect_data=aspect_data,
        )

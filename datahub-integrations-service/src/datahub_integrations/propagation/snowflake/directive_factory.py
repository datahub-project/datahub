import logging
from typing import Optional

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn

from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.event_processor import EventData

logger = logging.getLogger(__name__)


class DirectiveFactory:
    """Creates propagation directives based on extracted data and configuration."""

    def __init__(self, config: DescriptionPropagationConfig):
        self.config = config

    def create_description_directive(
        self,
        event_data: EventData,
        description: str,
        subtype: Optional[str] = None,
    ) -> Optional[DescriptionPropagationDirective]:
        """
        Create a description propagation directive based on event data and extracted description.

        Args:
            event_data: Processed event data
            description: Extracted description text
            subtype: Optional object subtype (TABLE, VIEW, etc.)

        Returns:
            DescriptionPropagationDirective if propagation should occur, None otherwise
        """
        if not self.config.enabled:
            logger.debug("Description propagation is disabled")
            return None

        if not description:
            logger.debug("No description provided")
            return None

        # Validate entity URN and check configuration
        try:
            entity_urn = Urn.create_from_string(event_data.entity_urn)
        except Exception as e:
            logger.error(
                f"Failed to parse entity URN {event_data.entity_urn}: {str(e)}"
            )
            return None

        # Check if propagation is enabled for this entity type
        if isinstance(entity_urn, SchemaFieldUrn):
            if not self.config.column_description_sync_enabled:
                logger.debug("Column description propagation is disabled")
                return None
        elif isinstance(entity_urn, DatasetUrn):
            if not self.config.table_description_sync_enabled:
                logger.debug("Table description propagation is disabled")
                return None
        else:
            logger.warning(
                f"Unsupported entity type for description propagation: {type(entity_urn)}"
            )
            return None

        # Determine operation based on event data
        operation = self._determine_operation(event_data)
        if not operation:
            logger.debug(f"Unsupported operation for event: {event_data.operation}")
            return None

        logger.info(
            f"Creating description directive for {event_data.entity_urn} with description: {description[:100]}..."
        )

        return DescriptionPropagationDirective(
            entity=event_data.entity_urn,
            description=description,
            operation=operation,
            propagate=True,
            subtype=subtype,
        )

    def _determine_operation(self, event_data: EventData) -> Optional[str]:
        """Determine the propagation operation based on event data."""
        if event_data.event_type == "EntityChangeEvent_v1":
            # EntityChangeEvent operations: ADD, MODIFY, REMOVE
            if event_data.operation in {"ADD", "MODIFY"}:
                return event_data.operation
            else:
                logger.debug(
                    f"Unsupported EntityChangeEvent operation: {event_data.operation}"
                )
                return None

        elif event_data.event_type == "MetadataChangeLogEvent_v1":
            # MCL events are typically modifications
            if event_data.change_type == "UPSERT":
                return "MODIFY"
            else:
                logger.debug(f"Unsupported MCL change type: {event_data.change_type}")
                return None

        else:
            logger.debug(f"Unsupported event type: {event_data.event_type}")
            return None

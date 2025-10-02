import logging
from typing import Any, Dict, Optional

from datahub_integrations.propagation.snowflake.event_processor import EventData

logger = logging.getLogger(__name__)


class DescriptionExtractor:
    """Extracts descriptions from different types of DataHub events and aspects."""

    def extract_description(self, event_data: EventData) -> Optional[str]:
        """
        Extract description from event data based on event type.

        Args:
            event_data: Processed event data

        Returns:
            Description string if found, None otherwise
        """
        if event_data.event_type == "EntityChangeEvent_v1":
            return self._extract_from_entity_change_event(event_data)
        elif event_data.event_type == "MetadataChangeLogEvent_v1":
            return self._extract_from_metadata_change_log_event(event_data)
        else:
            logger.debug(
                f"Unsupported event type for description extraction: {event_data.event_type}"
            )
            return None

    def _extract_from_entity_change_event(self, event_data: EventData) -> Optional[str]:
        """Extract description from EntityChangeEvent parameters."""
        if event_data.category != "DOCUMENTATION":
            logger.debug(
                f"Skipping non-documentation event category: {event_data.category}"
            )
            return None

        description = event_data.parameters.get("description")
        if description:
            logger.debug(
                f"Found description in EntityChangeEvent: {description[:100]}..."
            )

        return description

    def _extract_from_metadata_change_log_event(
        self, event_data: EventData
    ) -> Optional[str]:
        """Extract description from MetadataChangeLogEvent aspect data."""
        if not event_data.aspect_name:
            logger.debug("No aspect name in MetadataChangeLogEvent")
            return None

        # Only process description-related aspects
        if event_data.aspect_name not in [
            "editableDatasetProperties",
            "editableSchemaMetadata",
        ]:
            logger.debug(f"Skipping non-description aspect: {event_data.aspect_name}")
            return None

        # Only process UPSERT changes (not DELETE)
        if event_data.change_type != "UPSERT":
            logger.debug(f"Skipping non-UPSERT change: {event_data.change_type}")
            return None

        if event_data.aspect_name == "editableDatasetProperties":
            return self._extract_table_description(event_data.aspect_data)
        elif event_data.aspect_name == "editableSchemaMetadata":
            return self._extract_column_description(event_data.aspect_data)

        return None

    def _extract_table_description(self, aspect_data: Dict[str, Any]) -> Optional[str]:
        """Extract table/view description from editableDatasetProperties aspect."""
        description = aspect_data.get("description")
        if description:
            logger.debug(f"Found table description: {description[:100]}...")
            return description

        logger.debug("No table description found in editableDatasetProperties")
        return None

    def _extract_column_description(self, aspect_data: Dict[str, Any]) -> Optional[str]:
        """Extract column description from editableSchemaMetadata aspect."""
        editable_schema_field_info = aspect_data.get("editableSchemaFieldInfo", [])

        if not editable_schema_field_info:
            logger.debug("No editableSchemaFieldInfo found in aspect")
            return None

        logger.info(f"Found {len(editable_schema_field_info)} schema field updates")

        # Return the first description found
        # Note: In practice, MCL events typically contain updates for a single field
        for field_info in editable_schema_field_info:
            description = field_info.get("description")
            if description:
                field_path = field_info.get("fieldPath", "unknown")
                logger.info(
                    f"Found column description for field {field_path}: {description[:100]}..."
                )
                return description

        logger.debug("No column descriptions found in editableSchemaMetadata")
        return None

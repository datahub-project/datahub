import json
import logging
from typing import Iterable, Optional

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel, Field

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.propagation.snowflake.util import is_snowflake_urn

logger = logging.getLogger(__name__)


class DescriptionPropagationConfig(AutomationActionConfig):
    """Configuration for propagating DataHub descriptions to Snowflake as comments."""

    enabled: bool = Field(True, description="Enable description propagation")
    table_description_sync_enabled: bool = Field(
        True, description="Enable table description propagation"
    )
    column_description_sync_enabled: bool = Field(
        True, description="Enable column description propagation"
    )


class DescriptionPropagationDirective(BaseModel):
    """Directive for propagating descriptions to Snowflake."""

    entity: str
    docs: str
    operation: str
    propagate: bool
    subtype: Optional[str] = None


class DescriptionPropagationAction(ExtendedAction[str]):
    """
    Action to propagate DataHub descriptions to Snowflake as table and column comments.

    Handles both EntityChangeEvent and MetadataChangeLogEvent to detect description
    changes and create propagation directives for the main sync action.
    """

    def __init__(self, config: DescriptionPropagationConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: DescriptionPropagationConfig = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DescriptionPropagationAction":
        """Factory method to create an instance of DescriptionPropagationAction."""
        config = DescriptionPropagationConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    def name(self) -> str:
        return "DescriptionPropagation"

    def close(self) -> None:
        """Cleanup method called when the action is being shut down."""
        pass

    def _get_dataset_subtype(self, entity_urn: str) -> Optional[str]:
        """
        Query DataHub to get the dataset's subtype (TABLE, VIEW, etc.).

        Args:
            entity_urn: The URN of the dataset

        Returns:
            The subtype string (e.g., "TABLE", "VIEW") or None if not found
        """
        # Skip GraphQL query for now as it breaks event processing
        # Instead, return None to let the helper auto-detect the type
        logger.debug(f"Skipping subtype detection for {entity_urn} - will auto-detect")
        return None

    def _extract_description_from_aspect(
        self, mcl_event: MetadataChangeLogEvent
    ) -> Optional[str]:
        """
        Extract description from the aspect data in a MetadataChangeLogEvent.

        Args:
            mcl_event: The metadata change log event

        Returns:
            The description string or None if not found
        """
        try:
            if not mcl_event.aspect or not mcl_event.aspect.value:
                logger.debug("No aspect data in event")
                return None

            # Parse the aspect JSON data
            aspect_data = json.loads(mcl_event.aspect.value.decode("utf-8"))
            logger.debug(f"Aspect data keys: {list(aspect_data.keys())}")

            if mcl_event.aspectName == "editableDatasetProperties":
                # Table/view description
                description = aspect_data.get("description")
                if description:
                    logger.debug(f"Found table description: {description[:100]}...")
                    return description

            elif mcl_event.aspectName == "editableSchemaMetadata":
                # Column descriptions
                editable_schema_field_info = aspect_data.get(
                    "editableSchemaFieldInfo", []
                )
                if editable_schema_field_info:
                    logger.info(
                        f"Found {len(editable_schema_field_info)} schema field updates"
                    )
                    for field_info in editable_schema_field_info:
                        if field_info.get("description"):
                            logger.info(
                                f"Found column description for field {field_info.get('fieldPath', 'unknown')}: {field_info.get('description', '')[:100]}..."
                            )
                            return field_info.get("description")

            logger.debug(f"No description found in aspect {mcl_event.aspectName}")
            return None

        except Exception as e:
            logger.error(f"Failed to extract description from aspect: {str(e)}")
            logger.exception("Exception details:")
            return None

    def rollbackable_assets(self) -> Iterable[str]:
        """Return an iterable of assets to execute rollback on."""
        # Description sync action is event-driven and doesn't support rollback
        return []

    def rollback_asset(self, asset: str) -> None:
        """Rollback an individual asset."""
        # Description sync action is event-driven and doesn't support rollback
        pass

    def bootstrappable_assets(self) -> Iterable[str]:
        """Return an iterable of assets to execute bootstrap on."""
        # Description sync action is event-driven and doesn't support bootstrap
        return []

    def bootstrap_asset(self, asset: str) -> None:
        """Bootstrap an individual asset."""
        # Description sync action is event-driven and doesn't support bootstrap
        pass

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[DescriptionPropagationDirective]:
        """Determine if a description change event should be propagated to Snowflake."""
        if not self.config.enabled:
            return None

        if event.event_type == "EntityChangeEvent_v1":
            return self._handle_entity_change_event(event)
        elif event.event_type == "MetadataChangeLogEvent_v1":
            return self._handle_metadata_change_log_event(event)
        else:
            logger.debug(f"Unsupported event type: {event.event_type}")
            return None

    def _handle_entity_change_event(
        self, event: EventEnvelope
    ) -> Optional[DescriptionPropagationDirective]:
        """Handle EntityChangeEvent_v1 events."""
        assert isinstance(event.event, EntityChangeEvent)
        assert self.ctx.graph is not None
        logger.info(f"Processing EntityChangeEvent {event.event}")
        semantic_event = event.event

        # Check if this is a Snowflake URN first
        if not is_snowflake_urn(semantic_event.entityUrn):
            logger.debug(f"Skipping non-Snowflake URN: {semantic_event.entityUrn}")
            return None

        parameters = semantic_event._inner_dict.get("__parameters_json", {})

        docs: Optional[str] = None
        if semantic_event.category == "DOCUMENTATION":
            docs = parameters.get("description")
        else:
            logger.debug(
                f"Skipping non-documentation event category: {semantic_event.category}"
            )
            return None

        if not docs:
            logger.info("No description found. Skipping description propagation.")
            return None

        entity_urn = Urn.create_from_string(semantic_event.entityUrn)

        if not self.config.column_description_sync_enabled and isinstance(
            entity_urn, SchemaFieldUrn
        ):
            logger.info("Column description propagation is disabled. Skipping.")
            return None

        if not self.config.table_description_sync_enabled and isinstance(
            entity_urn, DatasetUrn
        ):
            logger.info("Table description propagation is disabled. Skipping.")
            return None

        if semantic_event.operation in {"ADD", "MODIFY"}:
            logger.info(
                f"Creating directive for {semantic_event.entityUrn} with description: {docs[:100]}..."
            )

            # Query DataHub for the dataset's subtype (TABLE, VIEW, etc.)
            subtype = None
            if isinstance(entity_urn, DatasetUrn):
                subtype = self._get_dataset_subtype(semantic_event.entityUrn)

            return DescriptionPropagationDirective(
                entity=semantic_event.entityUrn,
                docs=docs,
                operation=semantic_event.operation,
                propagate=True,
                subtype=subtype,
            )
        else:
            logger.debug(
                f"Skipping unknown documentation operation {semantic_event.operation} for {event.event.entityUrn}"
            )

        return None

    def _handle_metadata_change_log_event(
        self, event: EventEnvelope
    ) -> Optional[DescriptionPropagationDirective]:
        """Handle MetadataChangeLogEvent_v1 events for description changes."""
        assert isinstance(event.event, MetadataChangeLogEvent)
        mcl_event = event.event

        logger.info(f"Processing MetadataChangeLogEvent for {mcl_event.entityUrn}")
        logger.info(f"- aspectName: {mcl_event.aspectName}")
        logger.info(f"- changeType: {mcl_event.changeType}")

        # Check if this is a Snowflake URN first
        if not is_snowflake_urn(mcl_event.entityUrn):
            logger.debug(f"Skipping non-Snowflake URN: {mcl_event.entityUrn}")
            return None

        # Only process description-related aspects
        if mcl_event.aspectName not in [
            "editableDatasetProperties",
            "editableSchemaMetadata",
        ]:
            logger.debug(f"Skipping non-description aspect: {mcl_event.aspectName}")
            return None

        # Only process UPSERT changes (not DELETE)
        if mcl_event.changeType != "UPSERT":
            logger.debug(f"Skipping non-UPSERT change: {mcl_event.changeType}")
            return None

        # Extract description from the aspect data
        docs = self._extract_description_from_aspect(mcl_event)
        if not docs:
            logger.info(
                "No description found in aspect. Skipping description propagation."
            )
            return None

        entity_urn = Urn.create_from_string(mcl_event.entityUrn)

        # Check if propagation is enabled for this entity type
        if (
            mcl_event.aspectName == "editableSchemaMetadata"
            and not self.config.column_description_sync_enabled
        ):
            logger.info("Column description propagation is disabled. Skipping.")
            return None

        if (
            mcl_event.aspectName == "editableDatasetProperties"
            and not self.config.table_description_sync_enabled
        ):
            logger.info("Table description propagation is disabled. Skipping.")
            return None

        logger.info(
            f"Creating MCL directive for {mcl_event.entityUrn} with description: {docs[:100]}..."
        )

        # Query DataHub for the dataset's subtype (TABLE, VIEW, etc.)
        subtype = None
        if isinstance(entity_urn, DatasetUrn):
            subtype = self._get_dataset_subtype(mcl_event.entityUrn)

        return DescriptionPropagationDirective(
            entity=mcl_event.entityUrn,
            docs=docs,
            operation="MODIFY",
            propagate=True,
            subtype=subtype,
        )

    def act(self, event: EventEnvelope) -> None:
        """Process an event and propagate descriptions if applicable."""
        description_directive = self.should_propagate(event)
        if description_directive and description_directive.propagate:
            logger.info(
                f"Processing description propagation for {description_directive.entity}"
            )
            # Note: The actual processing is handled by SnowflakeMetadataSyncAction

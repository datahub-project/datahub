import logging
from typing import Iterable, Optional

from datahub.metadata.urns import DatasetUrn, Urn
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.action_extended import ExtendedAction
from datahub_integrations.propagation.snowflake.description_extractor import (
    DescriptionExtractor,
)
from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.directive_factory import (
    DirectiveFactory,
)
from datahub_integrations.propagation.snowflake.event_processor import EventProcessor

logger = logging.getLogger(__name__)


class DescriptionPropagationAction(ExtendedAction[str]):
    """
    Action to propagate DataHub descriptions to Snowflake as table and column comments.

    Uses focused components for event processing, description extraction, and directive creation
    to maintain single responsibility and improve testability.
    """

    def __init__(self, config: DescriptionPropagationConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: DescriptionPropagationConfig = config

        # Initialize focused components
        self.event_processor = EventProcessor()
        self.description_extractor = DescriptionExtractor()
        self.directive_factory = DirectiveFactory(config)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DescriptionPropagationAction":
        """Factory method to create an instance of DescriptionPropagationAction."""
        config = DescriptionPropagationConfig.model_validate(config_dict or {})
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

        # Process the event to extract relevant data
        event_data = self.event_processor.process_event(event)
        if not event_data:
            return None

        # Extract description from the event data
        description = self.description_extractor.extract_description(event_data)
        if not description:
            logger.debug("No description found in event")
            return None

        # Get subtype for dataset entities if needed
        subtype = None
        if event_data.event_type == "EntityChangeEvent_v1":
            try:
                entity_urn = Urn.create_from_string(event_data.entity_urn)
                if isinstance(entity_urn, DatasetUrn):
                    subtype = self._get_dataset_subtype(event_data.entity_urn)
            except Exception as e:
                logger.error(
                    f"Failed to parse entity URN {event_data.entity_urn}: {str(e)}"
                )

        # Create and return the directive
        return self.directive_factory.create_description_directive(
            event_data, description, subtype
        )

    def act(self, event: EventEnvelope) -> None:
        """Process an event and propagate descriptions if applicable."""
        description_directive = self.should_propagate(event)
        if description_directive and description_directive.propagate:
            logger.info(
                f"Processing description propagation for {description_directive.entity}"
            )
            # Note: The actual processing is handled by SnowflakeMetadataSyncAction

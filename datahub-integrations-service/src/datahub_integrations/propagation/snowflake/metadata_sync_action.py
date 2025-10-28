import logging
from typing import Iterable, Optional, Union

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.stats_util import EventProcessingStats
from datahub_integrations.propagation.propagation_utils import SelectedAsset
from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.propagation_dispatcher import (
    PropagationDispatcher,
)
from datahub_integrations.propagation.snowflake.util import (
    SnowflakeTagHelper,
    is_snowflake_urn,
)
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationConfig,
    TagPropagationDirective,
)
from datahub_integrations.propagation.term.term_propagation_action import (
    TermPropagationConfig,
    TermPropagationDirective,
)

logger = logging.getLogger(__name__)


class SnowflakeMetadataSyncConfig(AutomationActionConfig):
    """Configuration for Snowflake metadata synchronization action."""

    snowflake: SnowflakeConnectionConfigPermissive
    tag_propagation: Optional[TagPropagationConfig] = None
    term_propagation: Optional[TermPropagationConfig] = None
    description_sync: Optional[DescriptionPropagationConfig] = None


class SnowflakeMetadataSyncAction(ExtendedAction[SelectedAsset]):
    """
    Action to synchronize DataHub metadata (tags, terms, descriptions) to Snowflake.

    Uses a PropagationDispatcher to orchestrate different types of metadata propagation
    without tightly coupling to specific propagation implementations.
    """

    def __init__(self, config: SnowflakeMetadataSyncConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self._stats.event_processing_stats = EventProcessingStats()
        self.config = config
        self.ctx = ctx

        self.snowflake_helper = SnowflakeTagHelper(self.config.snowflake)

        # Initialize propagation dispatcher and register strategies
        self.propagation_dispatcher = PropagationDispatcher(ctx)

        if self.config.tag_propagation is not None:
            self.propagation_dispatcher.add_tag_propagation(self.config.tag_propagation)

        if self.config.term_propagation is not None:
            self.propagation_dispatcher.add_term_propagation(
                self.config.term_propagation
            )

        if self.config.description_sync is not None:
            self.propagation_dispatcher.add_description_propagation(
                self.config.description_sync
            )

    def _process_propagation_event(self, event: EventEnvelope) -> None:
        """Process tag, term, and description propagation for an event using the dispatcher."""
        directives = self.propagation_dispatcher.process_event(event)

        for directive in directives:
            if isinstance(directive, DescriptionPropagationDirective):
                logger.info(
                    f"Processing description propagation for {directive.entity}"
                )
            self.process_directive(directive)

    def close(self) -> None:
        self.snowflake_helper.close()
        return super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = SnowflakeMetadataSyncConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    def name(self) -> str:
        return "SnowflakeMetadataSync"

    def process_directive(
        self,
        directive: Union[
            TermPropagationDirective,
            TagPropagationDirective,
            DescriptionPropagationDirective,
        ],
    ) -> None:
        if isinstance(directive, DescriptionPropagationDirective):
            logger.debug(
                f"Will {directive.operation.lower()} description on Snowflake {directive.entity} (subtype: {directive.subtype})"
            )
            self.snowflake_helper.apply_description(
                directive.entity, directive.description, directive.subtype
            )
        else:
            # Handle tag/term propagation directives
            entity_to_apply = directive.entity
            tag_to_apply = (
                directive.tag
                if isinstance(directive, TagPropagationDirective)
                else directive.term
            )
            logger.debug(
                f"Will {directive.operation.lower()} {tag_to_apply} on Snowflake {entity_to_apply}"
            )

            if directive.operation == "ADD":
                self.snowflake_helper.apply_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )
            elif directive.operation == "REMOVE":
                self.snowflake_helper.remove_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )
            else:
                logger.warning(
                    f"Unknown operation '{directive.operation}' for tag/term propagation. Skipping."
                )

    def act(self, event: EventEnvelope) -> None:
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        self._stats.event_processing_stats.start(event)

        success = True
        try:
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                assert self.ctx.graph is not None
                semantic_event = event.event

                if is_snowflake_urn(semantic_event.entityUrn):
                    self._process_propagation_event(event)

            elif event.event_type == "MetadataChangeLogEvent_v1":
                assert isinstance(event.event, MetadataChangeLogEvent)
                mcl_event = event.event

                # Check if this is a Snowflake URN and a description-related aspect
                if is_snowflake_urn(mcl_event.entityUrn):
                    # Check if this is a description-related aspect change
                    if mcl_event.aspectName in [
                        "editableDatasetProperties",
                        "editableSchemaMetadata",
                    ]:
                        self._process_propagation_event(event)

        except Exception as e:
            logger.exception("Error processing event", e)
            success = False
        finally:
            self._stats.event_processing_stats.end(event, success=success)

    def rollbackable_assets(self) -> Iterable[SelectedAsset]:
        yield from self.bootstrappable_assets()

    def rollback_asset(self, asset: SelectedAsset) -> None:
        directives = self.propagation_dispatcher.process_bootstrap_asset(
            asset, "REMOVE"
        )

        for directive in directives:
            self.process_directive(directive)

    def bootstrappable_assets(self) -> Iterable[SelectedAsset]:
        asset_filters = self.propagation_dispatcher.get_bootstrappable_assets()

        for target_entity_type, index_filters in asset_filters.items():
            for index_name, filters in index_filters.items():
                logger.info(f"Index {index_name} has filters: {filters}")
                for urn in self.ctx.graph.graph.get_urns_by_filter(
                    entity_types=[index_name],
                    platform="urn:li:dataPlatform:snowflake",
                    extra_or_filters=filters,
                ):
                    yield SelectedAsset(urn=urn, target_entity_type=target_entity_type)

    def bootstrap_asset(self, asset: SelectedAsset) -> None:
        directives = self.propagation_dispatcher.process_bootstrap_asset(asset, "ADD")

        for directive in directives:
            self.process_directive(directive)

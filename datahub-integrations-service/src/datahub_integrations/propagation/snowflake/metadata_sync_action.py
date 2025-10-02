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
from datahub_integrations.propagation.snowflake.description_propagation_action import (
    DescriptionPropagationAction,
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.util import (
    SnowflakeTagHelper,
    is_snowflake_urn,
)
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationAction,
    TagPropagationConfig,
    TagPropagationDirective,
)
from datahub_integrations.propagation.term.term_propagation_action import (
    TermPropagationAction,
    TermPropagationConfig,
    TermPropagationDirective,
)

logger = logging.getLogger(__name__)


class SnowflakeMetadataSyncConfig(AutomationActionConfig):
    """Configuration for Snowflake metadata synchronization action."""

    snowflake: SnowflakeConnectionConfigPermissive
    tag_propagation: Optional[TagPropagationConfig] = None
    term_propagation: Optional[TermPropagationConfig] = None
    description_propagation: Optional[DescriptionPropagationConfig] = None


class SnowflakeMetadataSyncAction(ExtendedAction[SelectedAsset]):
    """
    Action to synchronize DataHub metadata (tags, terms, descriptions) to Snowflake.

    This action orchestrates multiple sub-actions to handle different types of metadata
    propagation based on the configured settings.
    """

    def __init__(self, config: SnowflakeMetadataSyncConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self._stats.event_processing_stats = EventProcessingStats()
        self.config = config
        self.ctx = ctx

        self.snowflake_helper = SnowflakeTagHelper(self.config.snowflake)

        # Initialize sub-actions based on configuration
        self.tag_propagator = None
        self.term_propagator = None
        self.description_propagator = None

        if (
            self.config.tag_propagation is not None
            and self.config.tag_propagation.enabled
        ):
            self.tag_propagator = TagPropagationAction(self.config.tag_propagation, ctx)

        if (
            self.config.term_propagation is not None
            and self.config.term_propagation.enabled
        ):
            self.term_propagator = TermPropagationAction(
                self.config.term_propagation, ctx
            )

        if (
            self.config.description_propagation is not None
            and self.config.description_propagation.enabled
        ):
            self.description_propagator = DescriptionPropagationAction(
                config=self.config.description_propagation,
                ctx=ctx,
            )

    def _process_propagation_event(self, event: EventEnvelope) -> None:
        """Process tag, term, and description propagation for an event."""
        # Process each type of propagation independently
        if self.tag_propagator is not None:
            tag_directive = self.tag_propagator.should_propagate(event=event)
            if tag_directive is not None and tag_directive.propagate:
                self.process_directive(tag_directive)

        if self.term_propagator is not None:
            term_directive = self.term_propagator.should_propagate(event=event)
            if term_directive is not None and term_directive.propagate:
                self.process_directive(term_directive)

        if self.description_propagator is not None:
            description_directive = self.description_propagator.should_propagate(
                event=event
            )
            if description_directive is not None and description_directive.propagate:
                logger.info(
                    f"Processing description propagation for {description_directive.entity}"
                )
                self.process_directive(description_directive)

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
                directive.entity, directive.docs, directive.subtype
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
            else:
                self.snowflake_helper.remove_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )

    def act(self, event: EventEnvelope) -> None:
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        self._stats.event_processing_stats.start(event)
        try:
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                assert self.ctx.graph is not None
                semantic_event = event.event

                if not is_snowflake_urn(semantic_event.entityUrn):
                    return

                self._process_propagation_event(event)

            elif event.event_type == "MetadataChangeLogEvent_v1":
                assert isinstance(event.event, MetadataChangeLogEvent)
                mcl_event = event.event

                # Check if this is a Snowflake URN and a description-related aspect
                if not is_snowflake_urn(mcl_event.entityUrn):
                    return

                # Check if this is a description-related aspect change
                if mcl_event.aspectName in [
                    "editableDatasetProperties",
                    "editableSchemaMetadata",
                ]:
                    self._process_propagation_event(event)

            self._stats.event_processing_stats.end(event, success=True)
        except Exception as e:
            logger.exception("Error processing event", e)
            self._stats.event_processing_stats.end(event, success=False)

    def rollbackable_assets(self) -> Iterable[SelectedAsset]:
        yield from self.bootstrappable_assets()

    def rollback_asset(self, asset: SelectedAsset) -> None:
        if self.term_propagator is not None:
            for term_propagation_directive in self.term_propagator.process_one_asset(
                asset, "REMOVE"
            ):
                self.process_directive(term_propagation_directive)

        # Tag propagation rollback not yet implemented
        return None

    def bootstrappable_assets(self) -> Iterable[SelectedAsset]:
        asset_filters = {}
        if self.term_propagator is not None:
            asset_filters = self.term_propagator.asset_filters()

        if self.tag_propagator is not None:
            raise NotImplementedError("Tag propagation bootstrap not yet supported")

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
        if self.term_propagator is not None:
            for term_propagation_directive in self.term_propagator.process_one_asset(
                asset, "ADD"
            ):
                self.process_directive(term_propagation_directive)

        # Tag propagation bootstrap not yet implemented

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import (
    DataHubActionInfoClass,
    DataHubActionStageStatusCodeClass,
    DataHubActionStateClass,
    DataHubActionStatusClass,
    GenericAspectClass,
)
from datahub.utilities.urns._urn_base import Urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.plugin.action.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)
from pydantic import Field

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.propagation.propagation.docs.docs_propagator import (
    DocsPropagator,
    DocsPropagatorConfig,
)
from datahub_integrations.propagation.propagation.propagation_rule_config import (
    LookupType,
    PropagatedMetadata,
    PropagationRule,
)
from datahub_integrations.propagation.propagation.propagation_strategy.aspect_lookup_strategy import (
    AspectBasedStrategy,
    AspectBasedStrategyConfig,
)
from datahub_integrations.propagation.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
)
from datahub_integrations.propagation.propagation.propagation_strategy.lineage_strategy import (
    LineageBasedStrategy,
    LineageBasedStrategyConfig,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationConfig,
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    PropagationOutput,
)
from datahub_integrations.propagation.propagation.tag.tag_propagator import (
    TagPropagator,
    TagPropagatorConfig,
)
from datahub_integrations.propagation.propagation.term.term_propagator import (
    TermPropagator,
    TermPropagatorConfig,
)
from datahub_integrations.propagation.propagation.utils.ece_utils import ECEProcessor

logger = logging.getLogger(__name__)


class PropagationSettings(ConfigModel):
    """Settings for different types of propagation."""

    description: DocsPropagatorConfig
    tags: TagPropagatorConfig
    # terms: TermPropagationSettings
    # structuredProperties: StructuredPropertyPropagationSettings


class PropertyPropagationConfig(AutomationActionConfig, PropagationConfig):
    """
    Configuration model for property propagation.

    Attributes:
        enabled (bool): Indicates whether property propagation is enabled.
        propagation_rule (PropagationRule): Rules for property propagation.
    """

    enabled: bool = Field(
        default=True, description="Indicates whether property propagation is enabled."
    )

    propagation_rule: PropagationRule = Field(
        description="Rule for property propagation.",
    )


@dataclass
class SourcedAsset:
    """An asset with its source information."""

    urn: Urn
    generated_by: Optional[str] = None


class GenericPropagationAction(ExtendedAction[SourcedAsset]):
    """
    A generic action for propagating properties (documentation, tags, terms) across related entities.
    """

    ACTION_STALE_THRESHOLD_MILLIS = 300000  # 5 minutes

    def __init__(self, config: PropertyPropagationConfig, ctx: PipelineContext):
        """Initialize the GenericPropagationAction."""

        super().__init__(config, ctx)
        self.config: PropertyPropagationConfig = config
        self.last_config_refresh = 0
        self.ctx = ctx
        self.is_live_action_running = False
        self.actor_urn = "urn:li:corpuser:__datahub_system"

        # Initialize processors and stats
        self.mcl_processor = MCLProcessor()
        self.ece_processor = ECEProcessor()
        self._stats = ActionStageReport()
        self._stats.start()

        # Initialize rate-limited emit function
        assert self.ctx.graph
        self._rate_limited_emit_mcp = self.config.get_rate_limited_emit_mcp(
            self.ctx.graph.graph
        )

        # Initialize propagators
        self.propagators = self._init_propagators()

        # Initialize propagation strategies
        self.propagation_strategies = self._init_propagation_strategies()

    def _init_propagators(self) -> List[EntityPropagator]:
        """Initialize propagators based on configuration."""

        propagators: List[EntityPropagator] = []
        metadata_map = self.config.propagation_rule.metadata_propagated

        # Add documentation propagator if configured
        if PropagatedMetadata.DOCUMENTATION in metadata_map:
            propagators.append(
                self._create_docs_propagator(
                    metadata_map.get(PropagatedMetadata.DOCUMENTATION, {})
                )
            )

        # Add tags propagator if configured
        if PropagatedMetadata.TAGS in metadata_map:
            propagators.append(
                self._create_tag_propagator(
                    metadata_map.get(PropagatedMetadata.TAGS, {})
                )
            )

        # Add terms propagator if configured
        if PropagatedMetadata.TERMS in metadata_map:
            propagators.append(
                self._create_term_propagator(
                    metadata_map.get(PropagatedMetadata.TERMS, {})
                )
            )

        return propagators

    def _create_docs_propagator(self, config_params: Dict) -> DocsPropagator:
        """Create a documentation propagator."""

        return DocsPropagator(
            self.action_urn,
            self.ctx.graph,
            DocsPropagatorConfig(
                propagation_rule=self.config.propagation_rule,
                **config_params,
            ),
        )

    def _create_tag_propagator(self, config_params: Dict) -> TagPropagator:
        """Create a tag propagator."""

        return TagPropagator(
            self.action_urn,
            self.ctx.graph,
            TagPropagatorConfig(
                propagation_rule=self.config.propagation_rule,
                **config_params,
            ),
        )

    def _create_term_propagator(self, config_params: Dict) -> TermPropagator:
        """Create a term propagator."""

        return TermPropagator(
            self.action_urn,
            self.ctx.graph,
            TermPropagatorConfig(
                propagation_rule=self.config.propagation_rule,
                **config_params,
            ),
        )

    def _init_propagation_strategies(self) -> Dict[str, BaseStrategy]:
        """Initialize propagation strategies."""

        strategies: Dict[str, BaseStrategy] = {}

        # Add lineage-based strategy
        lineage_strategy = LineageBasedStrategy(
            self.ctx.graph, LineageBasedStrategyConfig(), self._stats
        )
        strategies[LookupType.RELATIONSHIP.value] = lineage_strategy

        # Add sibling-based strategy
        # sibling_strategy = SiblingBasedStrategy(
        #     self.ctx.graph, SiblingBasedStrategyConfig(), self._stats
        # )

        # This should be used in the future
        # strategies[RelationshipType.SIBLING] = AspectBasedStrategy(
        #    self.ctx.graph, AspectBasedStrategyConfig(
        #        aspect_lookup=AspectLookup(type="schemaName", aspect_name="siblings", field="siblings")
        #    ), self._stats
        # )
        strategies[LookupType.ASPECT.value] = AspectBasedStrategy(
            AspectBasedStrategyConfig(
                entity_types=self.config.propagation_rule.entity_types
            ),
            self.ctx.graph,
            self._stats,
        )

        logger.info(f"Initialized propagation strategies: {strategies}")
        return strategies

    def name(self) -> str:
        """Return the name of the action."""

        return "GenericPropagationAction"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        """Create an instance of the action from a config dictionary."""

        action_config = PropertyPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Generic Propagation Action configured with {action_config}")
        return cls(action_config, ctx)

    def determine_live_action_running(self) -> bool:
        """
        Determine if the live action is running by checking the graph.
        Returns True if action is active and not stale.
        """
        assert self.ctx.graph

        # Check if action is active
        action_info = self.ctx.graph.graph.get_aspect(
            self.action_urn, DataHubActionInfoClass
        )
        if action_info and action_info.state == DataHubActionStateClass.INACTIVE:
            return False

        # Check if action is running and not stale
        action_stats = self.ctx.graph.graph.get_aspect(
            self.action_urn, DataHubActionStatusClass
        )
        if (
            action_stats
            and action_stats.live
            and action_stats.live.statusCode
            == DataHubActionStageStatusCodeClass.RUNNING
        ):
            # Check if reporting time is not stale
            now_ts_millis = int(time.time() * 1000)
            if (
                now_ts_millis - action_stats.live.reportedTime.time
                < self.ACTION_STALE_THRESHOLD_MILLIS
            ):
                return True

        return False

    def _extract_property_value(
        self, property_type: str, aspect_value: Optional[GenericAspectClass]
    ) -> Any:
        """Extract the property value from the aspect based on property type."""

        if not aspect_value:
            return None

        value_obj = json.loads(aspect_value.value)

        # Add property-specific extraction logic here
        if property_type == "documentation":
            return value_obj.get("documentation", "")
        elif property_type == "tags":
            return value_obj.get("tags", [])

        return None

    def act(self, event: EventEnvelope) -> None:
        """Process the event and emit change proposals."""

        for mcp in self.act_async(event):
            self._rate_limited_emit_mcp(mcp)

    def act_async(self, event: EventEnvelope) -> PropagationOutput:
        if not self.config.enabled:
            logger.warning("Property propagation is disabled. Skipping event")
            return

        # Initialize event processing stats if needed
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()

        stats = self._stats.event_processing_stats
        assert stats

        stats.start(event)

        try:
            yield from self._process_event_with_propagators(event)
            stats.end(event, success=True)
        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def _process_event_with_propagators(
        self, event: EventEnvelope
    ) -> PropagationOutput:
        logger.debug("Calling propagators:")
        for propagator in self.propagators:
            logger.debug(
                f"Calling propagator: {propagator.__class__.__name__} with event {event}"
            )
            directive = propagator.should_propagate(event)
            logger.debug(f"Propagation directive {directive}")

            if directive is not None and directive.propagate:
                logger.info(
                    f"Propagating {directive} based on {event} for {propagator.__class__.__name__}"
                )
                self._stats.increment_assets_processed(directive.entity)
                yield from self._propagate_directive(
                    propagator=propagator, directive=directive
                )

    def _propagate_directive(
        self, propagator: EntityPropagator, directive: PropagationDirective
    ) -> PropagationOutput:
        assert self.ctx.graph

        for resolution in self.config.propagation_rule.target_urn_resolution:
            if strategy := self.propagation_strategies.get(resolution.lookup_type):
                # Create fresh context for each resolution to prevent depth accumulation across iterations
                # But preserve propagation_direction to maintain directional consistency
                context = SourceDetails(
                    origin=directive.origin,
                    via=directive.via,
                    propagated=True,
                    actor=directive.actor,
                    propagation_started_at=directive.propagation_started_at,
                    propagation_depth=directive.propagation_depth,
                    propagation_direction=directive.propagation_direction,
                )

                yield from strategy.propagate(
                    resolution, propagator, directive, context
                )
            else:
                raise ValueError(
                    f"No propagation strategy found for resolution {resolution.__class__.__name__} : {resolution}"
                )

    def bootstrappable_assets(self) -> Iterable[SourcedAsset]:
        """Find assets that can be bootstrapped with propagated metadata."""

        for propagator in self.propagators:
            propagator_filters = propagator.asset_filters()
            logger.debug(
                f"Propagator {propagator.__class__.__name__} filters: {propagator_filters}"
            )

            # Process each entity type and its filters
            for entity_type, index_filters in propagator_filters.items():
                for index, filters in index_filters.items():
                    logger.debug(
                        f"Bootstrapping assets for {entity_type} with {index} filters: {self.config.propagation_rule.bootstrap} and extra filters: {filters}"
                    )

                    # Find assets matching the filters
                    # Convert SearchFilterRule objects to raw format for JSON serialization
                    raw_filters = [filter_rule.to_raw() for filter_rule in filters]

                    for asset_urn in self.ctx.graph.graph.get_urns_by_filter(
                        entity_types=[index],
                        extraFilters=self.config.propagation_rule.bootstrap,
                        extra_or_filters=raw_filters,
                    ):
                        yield SourcedAsset(urn=Urn.from_string(asset_urn))

    def bootstrap_asset(self, asset: SourcedAsset) -> None:
        """Bootstrap an asset with propagated metadata."""

        for propagator in self.propagators:
            urn = asset.urn
            logger.debug(f"Bootstrapping asset {urn}")

            # Check if entity type is supported by this propagator
            if not self._is_entity_supported(propagator, urn):
                continue

            logger.info(
                f"Bootstrapping asset {urn} with {propagator.__class__.__name__}"
            )

            # Process all directives for this asset
            for directive in propagator.boostrap_asset(urn):
                logger.debug(
                    f"Bootstrapping asset {urn} with {propagator.__class__.__name__} and directive {directive}"
                )
                if directive is not None and directive.propagate:
                    self._stats.increment_assets_processed(directive.entity)

                    # Apply directive and emit change proposals
                    for mcp in self._propagate_directive(
                        propagator=propagator, directive=directive
                    ):
                        self._rate_limited_emit_mcp(mcp)

    def _is_entity_supported(self, propagator: EntityPropagator, urn: Urn) -> bool:
        """Check if an entity type is supported by a propagator."""

        supported_entities = propagator.get_supported_entity_types()

        # Special case for schema fields: they're associated with datasets
        if "schemaField" in supported_entities:
            supported_entities.append("dataset")

        return urn.entity_type in supported_entities

    def rollbackable_assets(self) -> Iterable[SourcedAsset]:
        """Find assets that can be rolled back."""

        for propagator in self.propagators:
            logger.info(
                f"Generating rollbackable assets by {propagator.__class__.__name__}"
            )
            for asset in propagator.rollbackable_assets():
                self._stats.increment_assets_processed(asset.urn())
                yield SourcedAsset(
                    urn=asset, generated_by=propagator.__class__.__name__
                )

    def rollback_asset(self, asset: SourcedAsset) -> None:
        """Roll back metadata propagation for an asset."""

        logger.info(
            f"Rolling back asset {asset.urn} with propagator {asset.generated_by}"
        )

        # Find the appropriate propagator for this asset
        for propagator in self.propagators:
            if propagator.__class__.__name__ == asset.generated_by:
                propagator.rollback_asset(asset.urn)

    def close(self) -> None:
        """Close the action and clean up resources."""

        return super().close()

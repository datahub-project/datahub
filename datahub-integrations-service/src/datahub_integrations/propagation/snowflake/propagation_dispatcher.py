import logging
from typing import Any, Optional, Protocol, Union

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.propagation.snowflake.description_propagation_action import (
    DescriptionPropagationAction,
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
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


class PropagationStrategy(Protocol):
    """Protocol for propagation strategies."""

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[
        Union[
            TagPropagationDirective,
            TermPropagationDirective,
            DescriptionPropagationDirective,
        ]
    ]:
        """Determine if an event should trigger propagation."""
        ...


class PropagationDispatcher:
    """
    Orchestrates different types of metadata propagation using a strategy pattern.

    This class manages multiple propagation strategies and coordinates their execution
    without tightly coupling the main action to specific propagation implementations.
    """

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.strategies: list[PropagationStrategy] = []

    def add_tag_propagation(self, config: TagPropagationConfig) -> None:
        """Add tag propagation strategy if enabled."""
        if config.enabled:
            strategy = TagPropagationAction(config, self.ctx)
            self.strategies.append(strategy)
            logger.debug("Added tag propagation strategy")

    def add_term_propagation(self, config: TermPropagationConfig) -> None:
        """Add term propagation strategy if enabled."""
        if config.enabled:
            strategy = TermPropagationAction(config, self.ctx)
            self.strategies.append(strategy)
            logger.debug("Added term propagation strategy")

    def add_description_propagation(self, config: DescriptionPropagationConfig) -> None:
        """Add description propagation strategy if enabled."""
        if config.enabled:
            strategy = DescriptionPropagationAction(config, self.ctx)
            self.strategies.append(strategy)
            logger.debug("Added description propagation strategy")

    def process_event(
        self, event: EventEnvelope
    ) -> list[
        Union[
            TagPropagationDirective,
            TermPropagationDirective,
            DescriptionPropagationDirective,
        ]
    ]:
        """
        Process an event through all registered strategies.

        Args:
            event: The event to process

        Returns:
            List of propagation directives from all strategies
        """
        directives = []

        for strategy in self.strategies:
            try:
                directive = strategy.should_propagate(event)
                if directive and directive.propagate:
                    directives.append(directive)
                    logger.debug(
                        f"Strategy {type(strategy).__name__} created directive for {directive.entity}"
                    )
            except Exception as e:
                logger.error(f"Error in strategy {type(strategy).__name__}: {str(e)}")
                logger.exception("Strategy error details:")

        return directives

    def get_bootstrappable_assets(self) -> dict:
        """Get bootstrappable assets from all strategies that support it."""
        asset_filters = {}

        for strategy in self.strategies:
            # Only term propagation currently supports bootstrapping
            if isinstance(strategy, TermPropagationAction):
                strategy_filters = strategy.asset_filters()
                asset_filters.update(strategy_filters)
            elif isinstance(strategy, TagPropagationAction):
                # Tag propagation bootstrap not yet implemented
                logger.warning("Tag propagation bootstrap not yet supported")

        return asset_filters

    def process_bootstrap_asset(
        self, asset: Any, operation: str
    ) -> list[
        Union[
            TagPropagationDirective,
            TermPropagationDirective,
            DescriptionPropagationDirective,
        ]
    ]:
        """Process a single asset for bootstrap/rollback operations."""
        directives: list[
            Union[
                TagPropagationDirective,
                TermPropagationDirective,
                DescriptionPropagationDirective,
            ]
        ] = []

        for strategy in self.strategies:
            try:
                # Only term propagation currently supports asset processing
                if isinstance(strategy, TermPropagationAction):
                    strategy_directives = strategy.process_one_asset(asset, operation)
                    directives.extend(strategy_directives)
            except Exception as e:
                logger.error(
                    f"Error processing asset in strategy {type(strategy).__name__}: {str(e)}"
                )
                logger.exception("Asset processing error details:")

        return directives

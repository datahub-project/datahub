import json
import logging
from functools import wraps
from typing import Any, Dict, Iterable, List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropagationConfig,
)
from pydantic import BaseModel, Field, field_validator
from ratelimit import limits, sleep_and_retry

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)
from datahub_integrations.propagation.propagation_utils import (
    RelationshipType,
    SelectedAsset,
    filter_downstreams_by_entity_type,
    get_unique_siblings,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TagPropagationConfig(PropagationConfig, AutomationActionConfig):
    """
    Configuration model for tag propagation.

    Attributes:
        enabled (bool): Indicates whether tag propagation is enabled or not. Default is True.
        tag_prefixes (Optional[List[str]]): Optional list of tag prefixes to restrict tag propagation.
            If provided, only tags with prefixes in this list will be propagated. Default is None,
            meaning all tags will be propagated.
        include_downstreams (bool): Indicates whether tags are propagated to downstream assets. Default is True.
        include_siblings (bool): Indicates whether tags are propagated to sibling assets. Default is True.

    Note:
        Tag propagation allows tags to be automatically propagated to downstream entities.
        Enabling tag propagation can help maintain consistent metadata across connected entities.
        The `enabled` attribute controls whether tag propagation is enabled or disabled.
        The `tag_prefixes` attribute can be used to specify a list of tag prefixes that define which tags
        should be propagated. If no prefixes are specified (default), all tags will be propagated.

    Example:
        config = TagPropagationConfig(enabled=True, tag_prefixes=["urn:li:tag:"])
    """

    enabled: bool = Field(
        True,
        description="Indicates whether tag propagation is enabled or not.",
        examples=[True],
    )
    tag_prefixes: Optional[List[str]] = Field(
        None,
        description="Optional list of tag prefixes to restrict tag propagation.",
        examples=[["urn:li:tag:classification"]],
    )
    include_downstreams: bool = Field(
        True,
        description="Indicates whether tags will be propagated to downstream assets.",
        examples=[True],
    )
    include_siblings: bool = Field(
        True,
        description="Indicates whether tags will be propagated to sibling assets.",
        examples=[True],
    )

    @field_validator("tag_prefixes")
    @classmethod
    def tag_prefix_should_start_with_urn(
        cls, v: Optional[List[str]]
    ) -> Optional[List[str]]:
        if v is None:
            return v
        return [make_tag_urn(item) if item else item for item in v]


class TagPropagationDirective(BaseModel):
    propagate: bool
    tag: str
    operation: str
    entity: str
    origin: Optional[str] = None
    propagation_depth: Optional[int] = None


class TagPropagationAction(ExtendedAction[str]):
    def __init__(self, config: TagPropagationConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: TagPropagationConfig = config
        self._rate_limited_add_tag = self.get_rate_limited_add_tag(self.ctx.graph)
        self._event_processing_stats = EventProcessingStats()

    def get_rate_limited_add_tag(self, graph: AcrylDataHubGraph) -> Any:
        """
        Returns a rate limited graph that can be used to emit metadata for propagation
        """

        @sleep_and_retry
        @limits(
            calls=self.config.rate_limit_propagated_writes,
            period=self.config.rate_limit_propagated_writes_period,
        )
        @wraps(graph.add_tags_to_dataset)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return graph.add_tags_to_dataset(*args, **kwargs)

        return wrapper

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TagPropagationAction":
        config = TagPropagationConfig.model_validate(config_dict or {})
        logger.info(f"TagPropagationAction configured with {config}")
        return cls(config, ctx)

    def name(self) -> str:
        return "TagPropagator"

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[TagPropagationDirective]:
        """
        Return a tag urn to propagate or None if no propagation is desired
        """
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if semantic_event.category == "TAG" and (
                semantic_event.operation == "ADD"
                or semantic_event.operation == "REMOVE"
            ):
                assert semantic_event.modifier, "tag urn should be present"
                propagate = self.config.enabled
                if self.config.tag_prefixes:
                    propagate = any(
                        [
                            True
                            for prefix in self.config.tag_prefixes
                            if semantic_event.modifier.startswith(prefix)
                        ]
                    )
                    if not propagate:
                        logger.debug(f"Not propagating {semantic_event.modifier}")
                parameters = (
                    semantic_event.parameters
                    if semantic_event.parameters is not None
                    else semantic_event._inner_dict.get("__parameters_json", {})
                )
                context_str = (
                    parameters.get("context")
                    if parameters.get("context") is not None
                    else "{}"
                )
                context = self._get_context_object(context_str)

                if propagate:
                    logger.info(
                        f"Propagating tag {semantic_event.modifier} with operation {semantic_event.operation} on {semantic_event.entityUrn} and event: {semantic_event}"
                    )
                    return TagPropagationDirective(
                        propagate=True,
                        tag=semantic_event.modifier,
                        operation=semantic_event.operation,
                        entity=semantic_event.entityUrn,
                        origin=context.get("origin"),
                        propagation_depth=context.get("propagation_depth"),
                    )
                else:
                    return TagPropagationDirective(
                        propagate=False,
                        tag=semantic_event.modifier,
                        operation=semantic_event.modifier,
                        entity=semantic_event.entityUrn,
                        origin=context.get("origin"),
                        propagation_depth=context.get("propagation_depth"),
                    )
        return None

    def asset_filters(self) -> Dict[str, Dict[str, List[RawSearchFilterRule]]]:
        asset_filters: Dict[str, Dict[str, List[RawSearchFilterRule]]] = {}

        entity_index_field_map = {
            "schemaField": {
                "dataset": ["globalTags"],
            },
            "dataset": {
                "dataset": ["globalTags"],
            },
            "chart": {
                "chart": ["globalTags"],
            },
            "dashboard": {
                "dashboard": ["globalTags"],
            },
            "dataJob": {
                "dataJob": ["globalTags"],
            },
            "dataFlow": {
                "dataFlow": ["globalTags"],
            },
        }
        entity_type = "dataset"
        index_field_map = entity_index_field_map.get(entity_type, {})
        asset_filters[entity_type] = {}
        for index, _index_fields in index_field_map.items():
            asset_filters[entity_type] = {}
            or_filters: List[RawSearchFilterRule] = []
            asset_filters[entity_type][index] = or_filters
        return asset_filters

    def process_one_asset(
        self, asset: SelectedAsset, operation: str
    ) -> Iterable[TagPropagationDirective]:
        """
        Process one asset and return a list of tag propagation directives
        """
        target_entity_type = asset.target_entity_type
        logger.info(f"Processing asset {asset.urn} of type {asset}")
        if target_entity_type == "dataset":
            assert self.ctx.graph
            dataset_urn = asset.urn
            global_tags = self.ctx.graph.graph.get_aspect(
                dataset_urn, models.GlobalTagsClass
            )
            if not global_tags:
                return
            for tag in global_tags.tags:
                logger.info(f"Processing tag {tag.tag} on {dataset_urn}")
                tag_urn = make_tag_urn(tag.tag)
                yield TagPropagationDirective(
                    propagate=True,
                    tag=tag_urn,
                    operation=operation,
                    entity=dataset_urn,
                    origin=dataset_urn,
                    propagation_depth=0,
                )

    def _is_dataset_urn(self, urn: str) -> bool:
        """Check if the given URN is a dataset URN."""
        return urn.startswith("urn:li:dataset:")

    def act(self, event: EventEnvelope) -> None:
        success = False
        try:
            self._event_processing_stats.start(event)
            tag_propagation_directive = self.should_propagate(event)
            if tag_propagation_directive is not None:
                if tag_propagation_directive.propagate:
                    assert self.ctx.graph
                    entity_urn: str = tag_propagation_directive.entity

                    # Only support dataset-level tag propagation
                    if not self._is_dataset_urn(entity_urn):
                        logger.info(
                            f"TagPropagationAction only supports dataset-level tag propagation. "
                            f"Entity URN {entity_urn} is not supported and will be skipped."
                        )
                        success = (
                            True  # Mark as successful since we handled it gracefully
                        )
                        return

                    assets_to_propagate_to = set()
                    # find downstream lineage
                    downstreams_set = set()
                    if self.config.include_downstreams:
                        downstreams = self.ctx.graph.get_downstreams(entity_urn)
                        # ensure entity types are aligned for propagation.
                        filtered_downstreams = filter_downstreams_by_entity_type(
                            entity_urn, downstreams
                        )
                        # only propagate to a max of max_propagation_fanout
                        truncated_downstreams = filtered_downstreams[
                            : self.config.max_propagation_fanout
                        ]
                        downstreams_set.update(truncated_downstreams)
                        assets_to_propagate_to.update(truncated_downstreams)
                        logger.debug(
                            f"Detected {len(downstreams)} downstreams for {entity_urn}: {downstreams}"
                        )
                        if len(downstreams) > self.config.max_propagation_fanout:
                            logger.warning(
                                f"Reached max fan-out limit for downstream assets for {entity_urn} when propagating tag"
                            )

                    # find siblings
                    siblings_set = set()
                    if self.config.include_siblings:
                        siblings = get_unique_siblings(self.ctx.graph, entity_urn)
                        siblings_set.update(siblings)
                        assets_to_propagate_to.update(siblings)
                        logger.debug(
                            f"Detected {len(siblings)} siblings for {entity_urn}: {siblings}"
                        )

                    tag = tag_propagation_directive.tag
                    logger.info(
                        f"Detected {tag} {tag_propagation_directive.operation} on {tag_propagation_directive.entity}"
                    )
                    origin = (
                        tag_propagation_directive.origin
                        if tag_propagation_directive.origin is not None
                        else tag_propagation_directive.entity
                    )
                    propagation_depth = (
                        tag_propagation_directive.propagation_depth
                        if tag_propagation_directive.propagation_depth is not None
                        else 0  # if there's no propagation_depth, this is the first time we propagate
                    )
                    # apply tags to assets
                    logger.debug(
                        f"About to propagate to {len(assets_to_propagate_to)} assets: {assets_to_propagate_to}"
                    )
                    for asset in assets_to_propagate_to:
                        # prevent propagation cycle
                        if asset == origin or self._has_been_propagated_to(
                            self.ctx.graph, asset, origin, tag
                        ):
                            logger.info(
                                f"Cycle detected! Stopping propagation of tag {tag} to asset {asset}."
                            )
                            continue
                        if propagation_depth >= self.config.max_propagation_depth:
                            logger.info(
                                f"Max propagation depth met! Stopping propagation of tag {tag} to asset {asset}."
                            )
                            continue

                        # Build context for tag propagation
                        context = {
                            "propagated": "true",  # String value for JSON serialization
                            "origin": origin,
                            "propagation_relationship": (  # Use correct field name for test validation
                                RelationshipType.SIBLINGS.value
                                if asset in siblings_set
                                and asset not in downstreams_set
                                else RelationshipType.LINEAGE.value
                            ),
                            "propagation_depth": str(
                                propagation_depth + 1
                            ),  # String value for JSON serialization
                            "propagation_direction": "down",  # Always downstream for tag propagation
                        }

                        # Get action URN from pipeline context for attribution
                        action_urn = self._get_action_urn()
                        if action_urn:
                            self._rate_limited_add_tag(
                                asset,
                                [tag],
                                context=context,
                                action_urn=action_urn,
                            )
                        else:
                            self._rate_limited_add_tag(
                                asset,
                                [tag],
                                context=context,
                            )
                else:
                    logger.debug(f"Not propagating {tag_propagation_directive.tag}")
            success = True
        finally:
            self._event_processing_stats.end(event, success)

    def _has_been_propagated_to(
        self, graph: AcrylDataHubGraph, entity_urn: str, origin: str, tag: str
    ) -> bool:
        """
        Determine if a given asset has already been propagated to by this propagation cascade.
        Do this by seeing if there is already the same tag with the same origin in the tags aspect.
        """
        tags = self._get_tags_aspect(graph, entity_urn)
        if tags:
            for tag_association in tags.tags:
                context = self._get_context_object(tag_association.context)
                if tag_association.tag == tag and context.get("origin") == origin:
                    return True

        return False

    def _get_tags_aspect(
        self, graph: AcrylDataHubGraph, entity_urn: str
    ) -> Optional[models.GlobalTagsClass]:
        """
        Get the globalTags aspect for a given entity
        """
        return graph.graph.get_aspect(entity_urn, models.GlobalTagsClass)

    def _get_context_object(self, context_str: str | None) -> Any:
        """
        Get the globalTags aspect for a given entity
        """
        if context_str is None or context_str == "":
            context_str = "{}"
        return json.loads(context_str)

    def _get_action_urn(self) -> Optional[str]:
        """
        Get the action URN from the pipeline context.
        """
        return getattr(self.ctx, "pipeline_name", None)

    def close(self) -> None:
        return super().close()

    # Required abstract methods from ExtendedAction
    def rollbackable_assets(self) -> Iterable[str]:
        """Return an iterable of assets to execute rollback on."""
        # For tag propagation, we don't have a predefined list of assets to rollback
        # Rollback is typically handled by the framework based on propagated metadata
        return []

    def rollback_asset(self, asset: str) -> None:
        """Rollback an individual asset."""
        # Tag propagation rollback is handled by the framework
        # by removing propagated tags with specific attribution
        pass

    def bootstrappable_assets(self) -> Iterable[str]:
        """Return an iterable of assets to execute bootstrap on."""
        # Tag propagation action is event-driven and doesn't support bootstrap
        return []

    def bootstrap_asset(self, asset: str) -> None:
        """Bootstrap an individual asset."""
        # Tag propagation action is event-driven and doesn't support bootstrap
        pass

    def get_report(self) -> ActionStageReport:
        """Return the action stage report for stats endpoint."""
        # Include event processing stats in the base report
        base_report = self._stats
        base_report.event_processing_stats = self._event_processing_stats
        return base_report

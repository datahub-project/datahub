# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
from functools import wraps
from typing import Any, Dict, Iterable, List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropagationConfig,
)
from pydantic import BaseModel, Field, validator
from ratelimit import limits, sleep_and_retry

from datahub_integrations.propagation.propagation_utils import (
    RelationshipType,
    SelectedAsset,
    filter_downstreams_by_entity_type,
    get_unique_siblings,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TagPropagationConfig(PropagationConfig):
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

    @validator("tag_prefixes", each_item=True)
    def tag_prefix_should_start_with_urn(cls, v: str) -> str:
        if v:
            return make_tag_urn(v)
        return v


class TagPropagationDirective(BaseModel):
    propagate: bool
    tag: str
    operation: str
    entity: str
    origin: Optional[str] = None
    propagation_depth: Optional[int] = None


class TagPropagationAction(Action):
    def __init__(self, config: TagPropagationConfig, ctx: PipelineContext):
        self.config: TagPropagationConfig = config
        self.ctx = ctx
        self._rate_limited_add_tag = self.get_rate_limited_add_tag(self.ctx.graph)

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
        config = TagPropagationConfig.parse_obj(config_dict or {})
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
            global_tags = self.ctx.graph.graph.get_aspect(dataset_urn, GlobalTags)
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

    def act(self, event: EventEnvelope) -> None:
        tag_propagation_directive = self.should_propagate(event)
        if tag_propagation_directive is not None:
            if tag_propagation_directive.propagate:
                assert self.ctx.graph
                entity_urn: str = tag_propagation_directive.entity
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

                    self._rate_limited_add_tag(
                        asset,
                        [tag],
                        context={
                            "propagated": True,
                            "origin": origin,
                            "relationship": (
                                RelationshipType.SIBLINGS.value
                                if asset in siblings_set
                                and asset not in downstreams_set
                                else RelationshipType.LINEAGE.value
                            ),
                            "propagation_depth": propagation_depth + 1,
                        },
                    )
            else:
                logger.debug(f"Not propagating {tag_propagation_directive.tag}")

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

    def close(self) -> None:
        return super().close()

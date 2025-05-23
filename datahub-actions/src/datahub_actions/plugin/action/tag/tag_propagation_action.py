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

import logging
from typing import List, Optional

from pydantic import BaseModel, Field, validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_tag_urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TagPropagationConfig(ConfigModel):
    """
    Configuration model for tag propagation.

    Attributes:
        enabled (bool): Indicates whether tag propagation is enabled or not. Default is True.
        tag_prefixes (Optional[List[str]]): Optional list of tag prefixes to restrict tag propagation.
            If provided, only tags with prefixes in this list will be propagated. Default is None,
            meaning all tags will be propagated.

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
    )
    tag_prefixes: Optional[List[str]] = Field(
        None,
        description="Optional list of tag prefixes to restrict tag propagation.",
        examples=[
            "urn:li:tag:classification",
        ],
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


class TagPropagationAction(Action):
    def __init__(self, config: TagPropagationConfig, ctx: PipelineContext):
        self.config: TagPropagationConfig = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict, ctx):
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
                if propagate:
                    return TagPropagationDirective(
                        propagate=True,
                        tag=semantic_event.modifier,
                        operation=semantic_event.operation,
                        entity=semantic_event.entityUrn,
                    )
                else:
                    return TagPropagationDirective(
                        propagate=False,
                        tag=semantic_event.modifier,
                        operation=semantic_event.modifier,
                        entity=semantic_event.entityUrn,
                    )
        return None

    def act(self, event: EventEnvelope) -> None:
        tag_propagation_directive = self.should_propagate(event)
        if tag_propagation_directive is not None:
            if tag_propagation_directive.propagate:
                # find downstream lineage
                assert self.ctx.graph
                entity_urn: str = tag_propagation_directive.entity
                downstreams = self.ctx.graph.get_downstreams(entity_urn)
                logger.info(
                    f"Detected {len(downstreams)} downstreams for {entity_urn}: {downstreams}"
                )
                logger.info(
                    f"Detected {tag_propagation_directive.tag} {tag_propagation_directive.operation} on {tag_propagation_directive.entity}"
                )
                # apply tags to downstreams
                for d in downstreams:
                    self.ctx.graph.add_tags_to_dataset(
                        d,
                        [tag_propagation_directive.tag],
                        context={
                            "propagated": True,
                            "origin": tag_propagation_directive.entity,
                        },
                    )
            else:
                logger.debug(f"Not propagating {tag_propagation_directive.tag}")

    def close(self) -> None:
        return

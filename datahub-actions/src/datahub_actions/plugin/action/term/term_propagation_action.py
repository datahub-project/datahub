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

from pydantic import BaseModel, Field

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.utils.term_resolver import GlossaryTermsResolver

logger = logging.getLogger(__name__)


class TermPropagationDirective(BaseModel):
    propagate: bool
    term: str
    operation: str
    entity: str


class TermPropagationConfig(ConfigModel):
    """
    Configuration model for term propagation.

    Attributes:
        enabled (bool): Indicates whether term propagation is enabled or not. Default is True.
        target_term (Optional[str]): Optional target term to restrict term propagation.
            If provided, only this specific term and terms related to it via `isA` relationship will be propagated.
            Default is None, meaning all terms will be propagated.
        term_groups (Optional[List[str]]): Optional list of term groups to restrict term propagation.
            If provided, only terms within these groups will be propagated. Default is None, meaning all term groups will be propagated.

    Note:
        Term propagation allows terms to be automatically propagated to downstream entities.
        Enabling term propagation can help maintain consistent metadata across connected entities.
        The `enabled` attribute controls whether term propagation is enabled or disabled.
        The `target_terms` attribute can be used to specify a set of specific terms or all terms related to these specific terms that should be propagated.
        The `term_groups` attribute can be used to specify a list of term groups to restrict propagation to.

    Example:
        config = TermPropagationConfig(enabled=True, target_terms=["urn:li:glossaryTerm:Sensitive"])
    """

    enabled: bool = Field(
        True,
        description="Indicates whether term propagation is enabled or not.",
    )
    target_terms: Optional[List[str]] = Field(
        None,
        description="Optional target terms to restrict term propagation to this and all terms related to these terms.",
        examples=[
            "urn:li:glossaryTerm:Sensitive",
        ],
    )
    term_groups: Optional[List[str]] = Field(
        None,
        description="Optional list of term groups to restrict term propagation.",
        examples=[
            "Group1",
            "Group2",
        ],
    )


class TermPropagationAction(Action):
    def __init__(self, config: TermPropagationConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx
        self.term_resolver = GlossaryTermsResolver(graph=self.ctx.graph)
        if self.config.target_terms:
            logger.info(
                f"[Config] Will propagate terms that inherit from terms {self.config.target_terms}"
            )
            resolved_terms = []
            for t in self.config.target_terms:
                if t.startswith("urn:li:glossaryTerm"):
                    resolved_terms.append(t)
                else:
                    resolved_term = self.term_resolver.get_glossary_term_urn(t)
                    if not resolved_term:
                        raise Exception(f"Failed to resolve term by name {t}")
                    resolved_terms.append(resolved_term)
            self.config.target_terms = resolved_terms
            logger.info(
                f"[Config] Will propagate terms that inherit from terms {self.config.target_terms}"
            )

        if self.config.term_groups:
            resolved_nodes = []
            for node in self.config.term_groups:
                if node.startswith("urn:li:glossaryNode"):
                    resolved_nodes.append(node)
                else:
                    resolved_node = self.term_resolver.get_glossary_node_urn(node)
                    if not resolved_node:
                        raise Exception(f"Failed to resolve node by name {node}")
                    resolved_nodes.append(resolved_node)
            self.config.term_groups = resolved_nodes
            logger.info(
                f"[Config] Will propagate all terms in groups {self.config.term_groups}"
            )

    def name(self) -> str:
        return "TermPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TermPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Term Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[TermPropagationDirective]:
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "GLOSSARY_TERM"
                and self.config is not None
                and self.config.enabled
            ):
                assert semantic_event.modifier
                for target_term in self.config.target_terms or [
                    semantic_event.modifier
                ]:
                    # a cheap way to handle optionality and always propagate if config is not set
                    # Check which terms have connectivity to the target term
                    if (
                        semantic_event.modifier == target_term
                        or self.ctx.graph.check_relationship(  # term has been directly applied  # term is indirectly associated
                            target_term,
                            semantic_event.modifier,
                            "IsA",
                        )
                    ):
                        return TermPropagationDirective(
                            propagate=True,
                            term=semantic_event.modifier,
                            operation=semantic_event.operation,
                            entity=semantic_event.entityUrn,
                        )
        return None

    def act(self, event: EventEnvelope) -> None:
        """This method responds to changes to glossary terms and propagates them to downstream entities"""

        term_propagation_directive = self.should_propagate(event)

        if (
            term_propagation_directive is not None
            and term_propagation_directive.propagate
        ):
            assert self.ctx.graph
            # find downstream lineage
            downstreams = self.ctx.graph.get_downstreams(
                entity_urn=term_propagation_directive.entity
            )

            # apply terms to downstreams
            for dataset in downstreams:
                self.ctx.graph.add_terms_to_dataset(
                    dataset,
                    [term_propagation_directive.term],
                    context={
                        "propagated": True,
                        "origin": term_propagation_directive.entity,
                    },
                )
                logger.info(
                    f"Will add term {term_propagation_directive.term} to {dataset}"
                )

    def close(self) -> None:
        return

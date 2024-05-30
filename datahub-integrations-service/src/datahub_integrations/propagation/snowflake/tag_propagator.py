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
from typing import Optional, Union

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.tag.tag_propagation_action import (
    TagPropagationAction,
    TagPropagationConfig,
    TagPropagationDirective,
)

from datahub_integrations.actions.action_extended import ExtendedAction
from datahub_integrations.actions.stats_util import EventProcessingStats
from datahub_integrations.propagation.snowflake.snowflake_util import (
    SnowflakeTagHelper,
    is_snowflake_urn,
)
from datahub_integrations.propagation.term.term_propagation_action import (
    TermPropagationAction,
    TermPropagationConfig,
    TermPropagationDirective,
)

logger = logging.getLogger(__name__)


class SnowflakeTagPropagatorConfig(ConfigModel):
    snowflake: SnowflakeV2Config
    tag_propagation: Optional[TagPropagationConfig] = None
    term_propagation: Optional[TermPropagationConfig] = None


class SnowflakeTagPropagatorAction(ExtendedAction):
    def __init__(self, config: SnowflakeTagPropagatorConfig, ctx: PipelineContext):
        self.event_processing_stats = EventProcessingStats()
        self.config: SnowflakeTagPropagatorConfig = config
        self.ctx = ctx
        self.snowflake_tag_helper = SnowflakeTagHelper(self.config.snowflake)
        logger.info("[Config] Snowflake tag sync enabled")
        self.tag_propagator = None
        self.term_propagator = None
        if self.config.tag_propagation:
            logger.info("[Config] Will propagate DataHub Tags")
            if self.config.tag_propagation.tag_prefixes:
                logger.info(
                    f"[Config] Tag prefixes: {self.config.tag_propagation.tag_prefixes}"
                )
            self.tag_propagator = TagPropagationAction(self.config.tag_propagation, ctx)
        if self.config.term_propagation:
            logger.info("[Config] Will propagate Glossary Terms")
            self.term_propagator = TermPropagationAction(
                self.config.term_propagation, ctx
            )
        self.bootstrap()

    def close(self) -> None:
        self.snowflake_tag_helper.close()
        return super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = SnowflakeTagPropagatorConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    def name(self) -> str:
        return "SnowflakeTagPropagator"

    def bootstrap(self) -> None:
        # First process configuration to understand what are the terms that need
        # to be propagated
        self.process_all_assets(operation="ADD")

    def rollback(self) -> None:
        self.process_all_assets(operation="REMOVE")

    def process_all_assets(self, operation: str) -> None:
        extra_filters = [
            {
                "field": "platform",
                "condition": "EQUAL",
                "values": ["urn:li:dataPlatform:snowflake"],
            }
        ]
        if self.term_propagator is not None:
            for term_propagation_directive in self.term_propagator.process_all_assets(
                operation, extra_filters=extra_filters
            ):
                self.process_directive(term_propagation_directive)

        if self.tag_propagator is not None:
            logger.error("Tag Propagation doesn't support bootstrap or rollback yet")
            # for tag_propagation_directive in self.tag_propagator.process_all_assets(
            #     operation, extra_filters=extra_filters
            # ):
            #     self.process_directive(tag_propagation_directive)

    def process_directive(
        self, directive: Union[TermPropagationDirective, TagPropagationDirective]
    ) -> None:
        entity_to_apply = directive.entity
        tag_to_apply = (
            directive.tag
            if isinstance(directive, TagPropagationDirective)
            else directive.term
        )
        logger.info(
            f"Will {directive.operation.lower()} {tag_to_apply} on Snowflake {entity_to_apply}"
        )

        if isinstance(directive, TagPropagationDirective):
            if directive.operation == "ADD":
                self.snowflake_tag_helper.apply_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )
            else:
                self.snowflake_tag_helper.remove_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )
        else:
            if directive.operation == "ADD":
                self.snowflake_tag_helper.apply_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )
            else:
                self.snowflake_tag_helper.remove_tag_or_term(
                    entity_to_apply, tag_to_apply, self.ctx.graph
                )

    def act(self, event: EventEnvelope) -> None:
        self.event_processing_stats.start(event)
        try:
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                assert self.ctx.graph is not None
                semantic_event = event.event
                if not is_snowflake_urn(semantic_event.entityUrn):
                    return
                entity_to_apply = None
                tag_to_apply = None
                propagation_directive: Union[
                    TermPropagationDirective, TagPropagationDirective
                ] = None
                if self.tag_propagator is not None:
                    propagation_directive = self.tag_propagator.should_propagate(
                        event=event
                    )
                if self.term_propagator is not None:
                    propagation_directive = self.term_propagator.should_propagate(
                        event=event
                    )

                if entity_to_apply is not None and tag_to_apply is not None:
                    self.process_directive(propagation_directive)
            self.event_processing_stats.end(event, success=True)
        except Exception:
            self.event_processing_stats.end(event, success=False)

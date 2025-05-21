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
from typing import Optional

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.snowflake.snowflake_util import SnowflakeTagHelper
from datahub_actions.plugin.action.tag.tag_propagation_action import (
    TagPropagationAction,
    TagPropagationConfig,
)
from datahub_actions.plugin.action.term.term_propagation_action import (
    TermPropagationAction,
    TermPropagationConfig,
)

logger = logging.getLogger(__name__)


class SnowflakeTagPropagatorConfig(ConfigModel):
    snowflake: SnowflakeV2Config
    tag_propagation: Optional[TagPropagationConfig] = None
    term_propagation: Optional[TermPropagationConfig] = None


class SnowflakeTagPropagatorAction(Action):
    def __init__(self, config: SnowflakeTagPropagatorConfig, ctx: PipelineContext):
        self.config: SnowflakeTagPropagatorConfig = config
        self.ctx = ctx
        self.snowflake_tag_helper = SnowflakeTagHelper(self.config.snowflake)
        logger.info("[Config] Snowflake tag sync enabled")
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

    def close(self) -> None:
        self.snowflake_tag_helper.close()
        return

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = SnowflakeTagPropagatorConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    @staticmethod
    def is_snowflake_urn(urn: str) -> bool:
        return urn.startswith("urn:li:dataset:(urn:li:dataPlatform:snowflake")

    def name(self) -> str:
        return "SnowflakeTagPropagator"

    def act(self, event: EventEnvelope) -> None:
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if not self.is_snowflake_urn(semantic_event.entityUrn):
                return
            entity_to_apply = None
            tag_to_apply = None
            if self.tag_propagator is not None:
                tag_propagation_directive = self.tag_propagator.should_propagate(
                    event=event
                )
                if (
                    tag_propagation_directive is not None
                    and tag_propagation_directive.propagate
                ):
                    entity_to_apply = tag_propagation_directive.entity
                    tag_to_apply = tag_propagation_directive.tag

            if self.term_propagator is not None:
                term_propagation_directive = self.term_propagator.should_propagate(
                    event=event
                )
                if (
                    term_propagation_directive is not None
                    and term_propagation_directive.propagate
                ):
                    entity_to_apply = term_propagation_directive.entity
                    tag_to_apply = term_propagation_directive.term

            if entity_to_apply is not None:
                assert tag_to_apply
                logger.info(
                    f"Will {semantic_event.operation.lower()} {tag_to_apply} on Snowflake {entity_to_apply}"
                )
                if semantic_event.operation == "ADD":
                    self.snowflake_tag_helper.apply_tag_or_term(
                        entity_to_apply, tag_to_apply, self.ctx.graph
                    )
                else:
                    self.snowflake_tag_helper.remove_tag_or_term(
                        entity_to_apply, tag_to_apply, self.ctx.graph
                    )

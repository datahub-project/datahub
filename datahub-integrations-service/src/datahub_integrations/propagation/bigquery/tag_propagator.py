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
from enum import auto
from typing import Iterable, Optional, Union

from datahub.configuration._config_enum import ConfigEnum
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.stats_util import EventProcessingStats
from datahub_integrations.propagation.bigquery.config import (
    BigqueryConnectionConfigPermissive,
)
from datahub_integrations.propagation.bigquery.description_sync_action import (
    DescriptionSyncAction,
    DescriptionSyncConfig,
    DescriptionSyncDirective,
)
from datahub_integrations.propagation.bigquery.util import (
    BigqueryTagHelper,
    is_urn_allowed,
)
from datahub_integrations.propagation.propagation_utils import SelectedAsset
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


class BigqueryTagPropagationMode(ConfigEnum):
    POLICY_TAGS = auto()
    LABELS = auto()


class BigqueryTagPropagatorConfig(AutomationActionConfig):
    taxonomy: str = "DataHub"
    bigquery: BigqueryConnectionConfigPermissive
    tag_propagation: Optional[TagPropagationConfig] = None
    term_propagation: Optional[TermPropagationConfig] = None
    description_sync: Optional[DescriptionSyncConfig] = None


class BigqueryTagPropagatorAction(ExtendedAction[SelectedAsset]):
    def __init__(self, config: BigqueryTagPropagatorConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)

        self._stats.event_processing_stats = EventProcessingStats()

        resolved_config = super().resolve_secrets(
            config_str=config.json(), graph=ctx.graph.graph
        )

        self.config = BigqueryTagPropagatorConfig.parse_obj(resolved_config)

        # self.config: BigqueryTagPropagatorConfig = config
        self.ctx = ctx
        self.bigquery_tag_helper = BigqueryTagHelper(self.config.bigquery, ctx.graph)

        logger.info("[Config] Bigquery tag sync enabled")
        self.tag_propagator = None
        self.term_propagator = None
        self.description_sync = None

        if (
            self.config.tag_propagation is not None
            and self.config.tag_propagation.enabled
        ):
            logger.info("[Config] Will propagate DataHub Tags")
            if self.config.tag_propagation.tag_prefixes:
                logger.info(
                    f"[Config] Tag prefixes: {self.config.tag_propagation.tag_prefixes}"
                )
            self.tag_propagator = TagPropagationAction(self.config.tag_propagation, ctx)

        if (
            self.config.term_propagation is not None
            and self.config.term_propagation.enabled
        ):
            logger.info("[Config] Will propagate Glossary Terms")
            self.bigquery_tag_helper.create_taxonomy()
            self.term_propagator = TermPropagationAction(
                self.config.term_propagation, ctx
            )

        if (
            self.config.description_sync is not None
            and self.config.description_sync.enabled
        ):
            logger.info("[Config] Will sync Descriptions")
            if self.config.description_sync.table_description_sync_enabled:
                logger.info("[Config] Will sync Table Descriptions")
            if self.config.description_sync.column_description_sync_enabled:
                logger.info("[Config] Will sync Column Descriptions")

            self.description_sync = DescriptionSyncAction(
                self.config.description_sync, ctx
            )

        return

    def close(self) -> None:
        self.bigquery_tag_helper.close()
        return super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = BigqueryTagPropagatorConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    def name(self) -> str:
        return "BigqueryTagPropagator"

    def process_directive(
        self,
        directive: Union[
            TermPropagationDirective, TagPropagationDirective, DescriptionSyncDirective
        ],
    ) -> None:
        entity_to_apply = directive.entity

        if isinstance(directive, DescriptionSyncDirective):
            logger.debug(
                f"Will {directive.operation.lower()} documentation on Bigquery {entity_to_apply}"
            )
            self.bigquery_tag_helper.apply_description(entity_to_apply, directive.docs)
            return

        if isinstance(directive, TagPropagationDirective) or isinstance(
            directive, TermPropagationDirective
        ):
            tag_to_apply = (
                directive.tag
                if isinstance(directive, TagPropagationDirective)
                else directive.term
            )
            logger.debug(
                f"Will {directive.operation.lower()} {tag_to_apply} on Bigquery {entity_to_apply}"
            )

            if directive.operation == "ADD":
                self.bigquery_tag_helper.apply_tag_or_term(
                    entity_to_apply, tag_to_apply
                )
            else:
                self.bigquery_tag_helper.remove_tag_or_term(
                    entity_to_apply, tag_to_apply
                )

    def act(self, event: EventEnvelope) -> None:
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        self._stats.event_processing_stats.start(event)
        try:
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                assert self.ctx.graph is not None
                logger.debug(f"Processing event {event.event}")
                semantic_event = event.event

                assert isinstance(self.config, BigqueryTagPropagatorConfig)
                if not is_urn_allowed(semantic_event.entityUrn, self.config.bigquery):
                    return
                propagation_directive: Union[
                    TermPropagationDirective,
                    TagPropagationDirective,
                    DescriptionSyncDirective,
                    None,
                ] = None
                if self.tag_propagator is not None:
                    propagation_directive = self.tag_propagator.should_propagate(
                        event=event
                    )
                if self.term_propagator is not None and propagation_directive is None:
                    propagation_directive = self.term_propagator.should_propagate(
                        event=event
                    )

                if self.description_sync is not None and propagation_directive is None:
                    propagation_directive = self.description_sync.should_propagate(
                        event=event
                    )

                if (
                    propagation_directive is not None
                    and propagation_directive.propagate
                ):
                    self.process_directive(propagation_directive)

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

        if self.tag_propagator is not None:
            # for tag_propagation_directive in self.tag_propagator.process_one_asset(
            #     asset, "REMOVE"
            # ):
            #     self.process_directive(tag_propagation_directive)
            pass

        return None

    def bootstrappable_assets(self) -> Iterable[SelectedAsset]:
        asset_filters = {}
        if self.term_propagator is not None:
            asset_filters = self.term_propagator.asset_filters()

        if self.tag_propagator is not None:
            # raise NotImplementedError("Tag Propagation doesn't support bootstrap yet")
            asset_filters = self.tag_propagator.asset_filters()
            # if asset_filters:
            #     for target_entity_type, index_filters in tag_asset_filters:
            #         if target_entity_type in asset_filters:
            #             for index_name, filters in tag_asset_filters[
            #                 target_entity_type
            #             ].items():
            #                 if index_name in asset_filters[target_entity_type]:
            #                     asset_filters[target_entity_type][index_name].extend(
            #                         filters
            #                     )
            #                 else:
            #                     asset_filters[target_entity_type][index_name] = filters
            #         else:
            #             asset_filters[target_entity_type] = index_filters

        for target_entity_type, index_filters in asset_filters.items():
            for index_name, filters in index_filters.items():
                logger.info(f"Index {index_name} has filters: {filters}")
                for urn in self.ctx.graph.graph.get_urns_by_filter(
                    entity_types=[index_name],
                    platform="urn:li:dataPlatform:bigquery",
                    extra_or_filters=filters,
                ):
                    yield SelectedAsset(urn=urn, target_entity_type=target_entity_type)

    def bootstrap_asset(self, asset: SelectedAsset) -> None:
        if self.term_propagator is not None:
            for term_propagation_directive in self.term_propagator.process_one_asset(
                asset, "ADD"
            ):
                self.process_directive(term_propagation_directive)

        if self.tag_propagator is not None:
            for tag_propagation_directive in self.tag_propagator.process_one_asset(
                asset, "ADD"
            ):
                self.process_directive(tag_propagation_directive)
            # pass

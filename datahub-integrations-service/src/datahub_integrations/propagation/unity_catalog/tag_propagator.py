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
from typing import Iterable, Optional, Union

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.stats_util import EventProcessingStats
from datahub_integrations.propagation.propagation_utils import SelectedAsset
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationAction,
    TagPropagationConfig,
    TagPropagationDirective,
)
from datahub_integrations.propagation.unity_catalog.config import (
    UnityCatalogConnectionConfig,
)
from datahub_integrations.propagation.unity_catalog.description_sync_action import (
    DescriptionSyncAction,
    DescriptionSyncConfig,
    UnityCatalogDescriptionSyncDirective,
)
from datahub_integrations.propagation.unity_catalog.unity_resource_manager import (
    UnityResourceManager,
)
from datahub_integrations.propagation.unity_catalog.util import (
    UnityCatalogTagHelper,
    UrnValidator,
)

logger = logging.getLogger(__name__)


class UnityCatalogTagPropagatorConfig(AutomationActionConfig):
    databricks: UnityCatalogConnectionConfig
    tag_propagation: Optional[TagPropagationConfig] = None
    description_sync: Optional[DescriptionSyncConfig] = None
    platform_instance: Optional[str] = None


class UnityCatalogPropagatorAction(ExtendedAction[SelectedAsset]):
    def __init__(self, config: UnityCatalogTagPropagatorConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)

        self._stats.event_processing_stats = EventProcessingStats()

        resolved_config = super().resolve_secrets(
            config_str=config.json(), graph=ctx.graph.graph
        )

        self.config = UnityCatalogTagPropagatorConfig.parse_obj(resolved_config)

        # self.config: BigqueryTagPropagatorConfig = config
        logger.info("Initializing Unity Catalog Tag Propagator Action")

        urm = UnityResourceManager(
            connection_string=self.config.databricks.create_databricks_connection_string()
        )
        logger.info("Connecting to Unity Catalog with connection string")
        self.ctx = ctx
        self.unity_catalog_helper = UnityCatalogTagHelper(
            self.config.databricks, ctx.graph, urm, self.config.platform_instance
        )

        logger.info("[Config] Unity Catalog sync enabled")
        self.tag_propagator = None
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
            self.config.description_sync is not None
            and self.config.description_sync.enabled
        ):
            logger.info("[Config] Will sync Descriptions")
            if self.config.description_sync.table_description_sync_enabled:
                logger.info("[Config] Will sync Table Descriptions")
            if self.config.description_sync.column_description_sync_enabled:
                logger.info("[Config] Will sync Column Descriptions")

            if self.config.description_sync.container_description_sync_enabled:
                logger.info("[Config] Will sync Container Descriptions")

            self.description_sync = DescriptionSyncAction(
                self.config.description_sync, ctx
            )

        return

    def close(self) -> None:
        self.unity_catalog_helper.close()
        return super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = UnityCatalogTagPropagatorConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    def name(self) -> str:
        return "UnityCatalogPropagator"

    def process_directive(
        self,
        directive: Union[TagPropagationDirective, UnityCatalogDescriptionSyncDirective],
    ) -> None:
        entity_to_apply = directive.entity

        if isinstance(directive, UnityCatalogDescriptionSyncDirective):
            logger.info(
                f"Will {directive.operation.lower()} documentation on Unity Catalog {entity_to_apply}"
            )
            self.unity_catalog_helper.apply_description(entity_to_apply, directive.docs)
            return

        if isinstance(directive, TagPropagationDirective):
            tag_to_apply = (
                directive.tag
                if isinstance(directive, TagPropagationDirective)
                else directive.term
            )
            logger.debug(
                f"Will {directive.operation.lower()} {tag_to_apply} on Unity Catalog {entity_to_apply}"
            )

            if directive.operation == "ADD":
                self.unity_catalog_helper.apply_tag(entity_to_apply, tag_to_apply)
            else:
                self.unity_catalog_helper.remove_tag(entity_to_apply, tag_to_apply)

    def act(self, event: EventEnvelope) -> None:
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        self._stats.event_processing_stats.start(event)
        try:
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent), (
                    "Expected EntityChangeEvent_v1 type"
                )
                assert self.ctx.graph is not None, (
                    "Graph must be initialized in the context"
                )
                semantic_event = event.event

                assert isinstance(self.config, UnityCatalogTagPropagatorConfig)
                if not UrnValidator.is_urn_allowed(semantic_event.entityUrn):
                    return
                propagation_directive: Union[
                    TagPropagationDirective,
                    UnityCatalogDescriptionSyncDirective,
                    None,
                ] = None
                if self.tag_propagator is not None:
                    propagation_directive = self.tag_propagator.should_propagate(
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
            logger.exception(f"Error processing event: {e}", exc_info=True)
            self._stats.event_processing_stats.end(event, success=False)

    def rollbackable_assets(self) -> Iterable[SelectedAsset]:
        yield from self.bootstrappable_assets()

    def rollback_asset(self, asset: SelectedAsset) -> None:
        if self.tag_propagator is not None:
            # for tag_propagation_directive in self.tag_propagator.process_one_asset(
            #     asset, "REMOVE"
            # ):
            #     self.process_directive(tag_propagation_directive)
            pass

        return None

    def bootstrappable_assets(self) -> Iterable[SelectedAsset]:
        asset_filters = {}
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
                    platform="urn:li:dataPlatform:databricks",
                    extra_or_filters=filters,
                ):
                    yield SelectedAsset(urn=urn, target_entity_type=target_entity_type)

    def bootstrap_asset(self, asset: SelectedAsset) -> None:
        if self.tag_propagator is not None:
            for tag_propagation_directive in self.tag_propagator.process_one_asset(
                asset, "ADD"
            ):
                self.process_directive(tag_propagation_directive)
            # pass

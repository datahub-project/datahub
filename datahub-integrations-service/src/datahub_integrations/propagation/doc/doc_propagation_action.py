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
import time
from typing import Iterable, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationClass,
    EditableSchemaMetadataClass,
    EntityChangeEventClass as EntityChangeEvent,
    ParametersClass,
    SchemaMetadataClass,
)
from datahub.utilities.urns.urn import Urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.propagation.docs.propagation_action import (
    DocPropagationAction as BaseDocPropagationAction,
    DocPropagationConfig as BaseDocPropagationConfig,
)
from datahub_actions.plugin.action.stats_util import EventProcessingStats

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)

logger = logging.getLogger(__name__)


class DocPropagationConfig(AutomationActionConfig, BaseDocPropagationConfig):
    pass


ECE_EVENT_TYPE = "EntityChangeEvent_v1"


class DocPropagationAction(ExtendedAction[str]):  # type: ignore
    """
    This action is responsible for propagating documentation between DataHub entities.
    It extends the open source Docs Propagation Action.

    Known Limitations & Risks:

        - Asset-level Propagation is fully unsupported - Only Entity Type supported is Schema Fields
        - Deletion / Cleanup of Propagated Documentation is not yet supported.
        - [Important!] This action contains read-modify-write patterns on aspects. This MUST be migrated to PATCH soon, as this
          can cause conflicting overwrites on the same URN. This is relevant because we are writing the DOWNSTREAM aspect
          so Kafka partitioning does not help us here.
        - Bootstrap & rollback can both be very expensive operations (with conflicting write) and can potentially overload the system. There is no max number of entities limited for either operation.

    Opportunities to Extend:

        - Support configuration of propagation mechanisms: Lineage, Siblings, Children (Containers)
        - Support for more entity types - Datasets, Containers, Dashboards, Charts, Data Jobs, ML Models
        - Support filtering the "source" entities for propagation
            - By Platform: Only propagate from Snowflake
            - By Asset Type or Sub Type: Only propagate from Views, etc.
            - By Container: Only propagate in the "prod" DB
            - By Domain: Only propagate for the Marketing Domain
            - By Tag: Only propagate for a specific tag

            This will require richer hydration support, but can lead to a vastly more usable experience.
    """

    ACTION_STALE_THRESHOLD_MILLIS = 300000  # 5 minutes

    def __init__(self, config: DocPropagationConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self._base_action = BaseDocPropagationAction(config, ctx)
        self._base_action._stats = self._stats
        self.is_live_action_running = (
            False  # We assume by default that the live action is not running
        )

    def name(self) -> str:
        return "DocPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DocPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Doc Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def determine_live_action_running(self) -> bool:
        """
        Determine if the live action is running by checking if the graph is
        available
        """
        assert self.ctx.graph
        import datahub.metadata.schema_classes as models

        action_info = self.ctx.graph.graph.get_aspect(
            self.action_urn, models.DataHubActionInfoClass
        )
        if action_info and action_info.state == models.DataHubActionStateClass.INACTIVE:
            return False

        # if the action is active, we still check the live stats
        action_stats = self.ctx.graph.graph.get_aspect(
            self.action_urn, models.DataHubActionStatusClass
        )
        if (
            action_stats
            and action_stats.live
            and action_stats.live.statusCode
            == models.DataHubActionStageStatusCodeClass.RUNNING
        ):
            # return True if reporting time is not stale
            now_ts_millis = int(time.time() * 1000)
            if (
                now_ts_millis - action_stats.live.reportedTime.time
                < self.ACTION_STALE_THRESHOLD_MILLIS
            ):
                return True
        return False

    def bootstrappable_assets(self) -> Iterable[str]:
        assert self.ctx.graph
        # Use this opportunity to detect if the live action is running, since
        # that determines how we should bootstrap an individual asset
        self.is_live_action_running = self.determine_live_action_running()
        if self.is_live_action_running:
            logger.info("Live action is running, will perform 1-hop bootstrapping only")
        else:
            logger.info(
                "Live action is not running, will perform full multi-hop bootstrapping"
            )

        return self.ctx.graph.graph.get_urns_by_filter(
            entity_types=["dataset"],
            extra_or_filters=[
                {
                    "field": "fieldDescriptions",
                    "condition": "EXISTS",
                    "negated": "false",
                },
                {
                    "field": "editedFieldDescriptions",
                    "condition": "EXISTS",
                    "negated": "false",
                },
            ],
        )

    def create_fake_ece(
        self, mcp: MetadataChangeProposalWrapper
    ) -> Optional[EntityChangeEvent]:
        """
        Create a fake ECE event based on the MCP
        """
        if isinstance(mcp.aspect, DocumentationClass):
            my_edit = [
                x
                for x in mcp.aspect.documentations
                if x.attribution and x.attribution.source == self.action_urn
            ]
            if my_edit:
                assert my_edit[0].attribution
                params = ParametersClass()
                params._inner_dict = {
                    "description": my_edit[0].documentation,
                    "source": self.action_urn,
                    "origin": (
                        my_edit[0].attribution.sourceDetail.get("origin")
                        if my_edit[0].attribution.sourceDetail
                        else None
                    ),
                }
                assert mcp.entityUrn
                entity_type = Urn.create_from_string(mcp.entityUrn).entity_type
                event = EntityChangeEvent(
                    entityType=entity_type,
                    entityUrn=mcp.entityUrn,
                    category="DOCUMENTATION",
                    operation="ADD",
                    auditStamp=AuditStampClass(
                        time=my_edit[0].attribution.time,
                        actor=my_edit[0].attribution.actor,
                    ),
                    version=1,
                    parameters=params,
                )
                logger.info(f"Input event: {event}")
                logger.info(f"Output mcp: {mcp}")
                return event

        return None

    def bootstrap_asset(self, asset: str) -> None:
        """
        Bootstrap the action by processing all the field descriptions on the
        dataset
        """
        assert self.ctx.graph
        # if "table_foo_0" in asset:
        #     breakpoint()

        logger.info(f"Bootstrapping asset {asset}")
        bootstrap_report = self._stats

        if bootstrap_report.event_processing_stats is None:
            bootstrap_report.event_processing_stats = EventProcessingStats()

        dataset_urn = asset
        edited_field_docs = self.ctx.graph.graph.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )
        base_field_docs = self.ctx.graph.graph.get_aspect(
            dataset_urn, SchemaMetadataClass
        )
        field_doc_map = {}
        if base_field_docs is not None:
            for field_info in base_field_docs.fields:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": field_info.lastModified,
                    }
        if edited_field_docs is not None:
            for field_info in edited_field_docs.editableSchemaFieldInfo:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": edited_field_docs.lastModified,
                    }

        ece_events = []  # we will process these later
        all_assets = set()
        for field_path, field_dict in field_doc_map.items():
            if field_dict.get("description"):
                # this field has a description, let's emit an ECE event
                schema_field_urn = make_schema_field_urn(dataset_urn, field_path)
                # make a fake ECE event
                params = ParametersClass()
                # this is the way you squirrel away parameters inside the event
                params._inner_dict = {"description": field_dict["description"]}
                event = EntityChangeEvent(
                    entityType="schemaField",
                    entityUrn=schema_field_urn,
                    category="DOCUMENTATION",
                    operation="ADD",  # ADD RESTATE later
                    auditStamp=field_dict["auditStamp"],
                    parameters=params,
                    version=1,
                )
                ece_events.append(event)
                all_assets.add(schema_field_urn)

        max_depth = 1 if self.is_live_action_running else 100
        # if "table_foo_0" in asset:
        #     breakpoint()

        while max_depth > 0 and ece_events:
            max_depth -= 1
            new_ece_events = []
            for event in ece_events:
                mcps = self._base_action.act_async(
                    EventEnvelope(event_type=ECE_EVENT_TYPE, event=event, meta={}),
                )
                for mcp in mcps:
                    self.ctx.graph.graph.emit(mcp)
                    if not self.is_live_action_running:
                        # we need to process the derived events
                        ece = self.create_fake_ece(mcp)
                        if ece and ece.entityUrn not in all_assets:
                            new_ece_events.append(ece)
            ece_events = new_ece_events

        if ece_events:
            logger.warning(
                f"Could not process all events since we hit processing limits, remaining {len(ece_events)} events"
            )
        bootstrap_report.increment_assets_processed(asset)

    def rollbackable_assets(self) -> Iterable[str]:
        return self.ctx.graph.graph.get_urns_by_filter(
            entity_types=["schemaField"],
            extra_or_filters=[
                {
                    "field": "documentationAttributionSources.keyword",
                    "condition": "IN",
                    "values": [self.action_urn],
                    "negated": "false",
                }
            ],
        )

    def rollback_asset(self, asset: str) -> None:
        try:
            assert self.ctx.graph
            logger.info(f"Rolling back documentation for asset {asset}")

            rollback_report = self._stats
            documentation = self.ctx.graph.graph.get_aspect(asset, DocumentationClass)
            if documentation is not None:
                exists = [
                    doc
                    for doc in documentation.documentations
                    if doc.attribution.source == self.action_urn
                ]
                if exists:
                    documentation.documentations = [
                        doc
                        for doc in documentation.documentations
                        if doc.attribution.source != self.action_urn
                    ]
                    self.ctx.graph.graph.emit(
                        MetadataChangeProposalWrapper(
                            entityUrn=asset, aspect=documentation
                        )
                    )
            else:
                logger.warning(f"Documentation aspect not found for asset {asset}")
            rollback_report.increment_assets_processed(asset)
        except Exception as e:
            logger.error(f"Error rolling back documentation: {e}")

    def act(self, event: EventEnvelope) -> None:
        return self._base_action.act(event)

    def close(self) -> None:
        return super().close()

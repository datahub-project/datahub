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
from typing import Iterable

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DocumentationClass,
    EditableSchemaMetadataClass,
)
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub.metadata.schema_classes import ParametersClass, SchemaMetadataClass
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.propagation.docs.propagation_action import (
    DocPropagationAction as BaseDocPropagationAction,
)
from datahub_integrations.actions.oss.propagation.docs.propagation_action import (
    DocPropagationConfig as BaseDocPropagationConfig,
)
from datahub_integrations.actions.oss.stats_util import EventProcessingStats

logger = logging.getLogger(__name__)


class DocPropagationConfig(AutomationActionConfig, BaseDocPropagationConfig):
    pass


ECE_EVENT_TYPE = "EntityChangeEvent_v1"


class DocPropagationAction(ExtendedAction[str]):  # type: ignore

    def __init__(self, config: DocPropagationConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self._base_action = BaseDocPropagationAction(config, ctx)
        self._base_action._stats = self._stats

    def name(self) -> str:
        return "DocPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DocPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Doc Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def bootstrappable_assets(self) -> Iterable[str]:
        assert self.ctx.graph
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

    def bootstrap_asset(self, asset: str) -> None:
        """
        Bootstrap the action by processing all the field descriptions on the
        dataset
        """
        assert self.ctx.graph

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

        for field_path, field_dict in field_doc_map.items():
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
            self.act(
                EventEnvelope(event_type=ECE_EVENT_TYPE, event=event, meta={}),
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
            rollback_report.increment_assets_processed(asset)
        except Exception as e:
            logger.error(f"Error rolling back documentation: {e}")

    def act(self, event: EventEnvelope) -> None:
        return self._base_action.act(event)

    def close(self) -> None:
        return super().close()

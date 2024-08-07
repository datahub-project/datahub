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
import time
from typing import Any, Iterable, List, Optional

import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaMetadataClass,
)
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub.metadata.schema_classes import (
    GenericAspectClass,
    MetadataAttributionClass,
    MetadataChangeLogClass,
)
from datahub.utilities.urns.urn import Urn
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel, Field, validator

from datahub_integrations.actions.oss.mcl_utils import MCLProcessor
from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)

logger = logging.getLogger(__name__)


class DocPropagationDirective(BaseModel):
    propagate: bool = Field(
        description="Indicates whether the documentation should be propagated."
    )
    doc_string: Optional[str] = Field(
        default=None, description="Documentation string to be propagated."
    )
    operation: str = Field(
        description="Operation to be performed on the documentation. Can be ADD, MODIFY or REMOVE."
    )
    entity: str = Field(
        description="Entity URN from which the documentation is propagated. This will either be the same as the origin or the via entity, depending on the propagation path."
    )
    origin: str = Field(
        description="Origin entity for the documentation. This is the entity that triggered the documentation propagation.",
    )
    via: Optional[str] = Field(
        None,
        description="Via entity for the documentation. This is the direct entity that the documentation was propagated through.",
    )
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the documentation propagation.",
    )


class SourceDetails(BaseModel):
    origin: Optional[str] = Field(
        None,
        description="Origin entity for the documentation. This is the entity that triggered the documentation propagation.",
    )
    via: Optional[str] = Field(
        None,
        description="Via entity for the documentation. This is the direct entity that the documentation was propagated through.",
    )
    propagated: Optional[str] = Field(
        None,
        description="Indicates whether the documentation was propagated.",
    )
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the documentation propagation.",
    )

    @validator("propagated", pre=True)
    def convert_boolean_to_lowercase_string(cls, v: Any) -> Optional[str]:
        if isinstance(v, bool):
            return str(v).lower()
        return v


class DocPropagationConfig(ConfigModel):
    """
    Configuration model for documentation propagation.

    Attributes:
        enabled (bool): Indicates whether documentation propagation is enabled or not. Default is True.
        columns_enabled (bool): Indicates whether column documentation propagation is enabled or not. Default is True.
        datasets_enabled (bool): Indicates whether dataset level documentation propagation is enabled or not. Default is False.

    Example:
        config = DocPropagationConfig(enabled=True)
    """

    enabled: bool = Field(
        True,
        description="Indicates whether documentation propagation is enabled or not.",
        example=True,
    )
    columns_enabled: bool = Field(
        True,
        description="Indicates whether column documentation propagation is enabled or not.",
        example=True,
    )
    datasets_enabled: bool = Field(
        False,
        description="Indicates whether dataset level documentation propagation is enabled or not.",
        example=False,
    )


def get_field_path(schema_field_urn: str) -> str:
    urn = Urn.create_from_string(schema_field_urn)
    return urn.get_entity_id()[1]


def get_field_doc_from_dataset(
    graph: AcrylDataHubGraph, dataset_urn: str, schema_field_urn: str
) -> Optional[str]:
    editableSchemaMetadata = graph.graph.get_aspect(
        dataset_urn, EditableSchemaMetadataClass
    )
    if editableSchemaMetadata is not None:
        if editableSchemaMetadata.editableSchemaFieldInfo is not None:
            field_info = [
                x
                for x in editableSchemaMetadata.editableSchemaFieldInfo
                if x.fieldPath == get_field_path(schema_field_urn)
            ]
            if field_info:
                return field_info[0].description
    return None


ECE_EVENT_TYPE = "EntityChangeEvent_v1"


class DocPropagationAction(Action):
    def __init__(self, config: DocPropagationConfig, ctx: PipelineContext):
        super().__init__()
        self.action_urn: str
        if not ctx.pipeline_name.startswith("urn:li:dataHubAction"):
            self.action_urn = f"urn:li:dataHubAction:{ctx.pipeline_name}"
        else:
            self.action_urn = ctx.pipeline_name

        self.config: DocPropagationConfig = config
        self.last_config_refresh: float = 0
        self.ctx = ctx
        self.mcl_processor = MCLProcessor()
        self.actor_urn = "urn:li:corpuser:__datahub_system"

        self.mcl_processor.register_processor(
            "schemaField",
            "documentation",
            self.process_schema_field_documentation,
        )
        self.refresh_config()
        self._stats = ActionStageReport()
        self._stats.start()

    def name(self) -> str:
        return "DocPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DocPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Doc Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def process_schema_field_documentation(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: GenericAspectClass,
        previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[DocPropagationDirective]:
        if aspect_name == "documentation":
            if self.config.columns_enabled:
                current_docs = DocumentationClass.from_obj(
                    json.loads(aspect_value.value)
                )
                old_docs = (
                    None
                    if previous_aspect_value is None
                    else DocumentationClass.from_obj(
                        json.loads(previous_aspect_value.value)
                    )
                )
                if current_docs.documentations:
                    # we assume that the first documentation is the primary one
                    # we can change this later
                    current_documentation_instance = current_docs.documentations[0]
                    source_details = (
                        (current_documentation_instance.attribution.sourceDetail)
                        if current_documentation_instance.attribution
                        else {}
                    )
                    origin_entity = source_details.get("origin")
                    if old_docs is None or not old_docs.documentations:
                        return DocPropagationDirective(
                            propagate=True,
                            doc_string=current_documentation_instance.documentation,
                            operation="ADD",
                            entity=entity_urn,
                            origin=origin_entity,
                            via=entity_urn,
                            actor=self.actor_urn,
                        )
                    else:
                        if (
                            current_docs.documentations[0].documentation
                            != old_docs.documentations[0].documentation
                        ):
                            return DocPropagationDirective(
                                propagate=True,
                                doc_string=current_documentation_instance.documentation,
                                operation="MODIFY",
                                entity=entity_urn,
                                origin=origin_entity,
                                via=entity_urn,
                                actor=self.actor_urn,
                            )
        return None

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[DocPropagationDirective]:

        if self.mcl_processor.is_mcl(event):
            return self.mcl_processor.process(event)
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            # logger.info(f"Received event {event}")
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "DOCUMENTATION"
                and self.config is not None
                and self.config.enabled
            ):
                if self.config.columns_enabled and (
                    semantic_event.entityType == "schemaField"
                ):
                    if semantic_event.parameters:
                        parameters = semantic_event.parameters
                    else:
                        parameters = semantic_event._inner_dict.get(
                            "__parameters_json", {}
                        )
                    doc_string = parameters.get("description")
                    origin = parameters.get("origin")
                    origin = origin or semantic_event.entityUrn
                    via = (
                        semantic_event.entityUrn
                        if origin != semantic_event.entityUrn
                        else None
                    )
                    logger.debug(f"Origin: {origin}")
                    logger.debug(f"Via: {via}")
                    logger.debug(f"Doc string: {doc_string}")
                    logger.debug(f"Semantic event {semantic_event}")
                    if doc_string:
                        return DocPropagationDirective(
                            propagate=True,
                            doc_string=doc_string,
                            operation=semantic_event.operation,
                            entity=semantic_event.entityUrn,
                            origin=origin,
                            via=via,  # if origin is set, then via is the entity itself
                            actor=(
                                semantic_event.auditStamp.actor
                                if semantic_event.auditStamp
                                else self.actor_urn
                            ),
                        )
        return None

    def modify_docs_on_columns(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        schema_field_urn: str,
        dataset_urn: str,
        field_doc: Optional[str],
        context: SourceDetails,
    ) -> Optional[MetadataChangeProposalWrapper]:
        if context.origin == schema_field_urn:
            # No need to propagate to self
            return None

        if not dataset_urn.startswith("urn:li:dataset"):
            logger.error(
                f"Invalid dataset urn {dataset_urn}. Must start with urn:li:dataset"
            )
            return None

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor=self.actor_urn
        )

        source_details = context.dict(exclude_none=True)
        attribution: MetadataAttributionClass = MetadataAttributionClass(
            source=self.action_urn,
            time=auditStamp.time,
            actor=self.actor_urn,
            sourceDetail=source_details,
        )
        documentations = graph.graph.get_aspect(schema_field_urn, DocumentationClass)
        if documentations:
            mutation_needed = False
            action_sourced = False
            # we check if there are any existing documentations generated by
            # this action, if so, we update them
            # otherwise, we add a new documentation entry sourced by this action
            for doc_association in documentations.documentations:
                if doc_association.attribution and doc_association.attribution.source:
                    if doc_association.attribution.source == self.action_urn:
                        action_sourced = True
                        if doc_association.documentation != field_doc:
                            mutation_needed = True
                            if operation == "ADD" or operation == "MODIFY":
                                doc_association.documentation = field_doc or ""
                                doc_association.attribution = attribution
                            elif operation == "REMOVE":
                                # TODO : should we remove the documentation or just set it to empty string?
                                doc_association.documentation = ""
                                doc_association.attribution = attribution
            if not action_sourced:
                documentations.documentations.append(
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                )
                mutation_needed = True
        else:
            # no docs found, create a new one
            # we don't check editableSchemaMetadata because our goal is to
            # propagate documentation to downstream entities
            # UI will handle resolving priorities and conflicts
            documentations = DocumentationClass(
                documentations=[
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                ]
            )
            mutation_needed = True

        if mutation_needed:
            logger.debug(
                f"Will emit documentation change proposal for {schema_field_urn} with {field_doc}"
            )
            return MetadataChangeProposalWrapper(
                entityUrn=schema_field_urn,
                aspect=documentations,
            )
        return None
        # graph.graph.emit(
        #     MetadataChangeProposalWrapper(
        #         entityUrn=schema_field_urn,
        #         aspect=documentations,
        #     )
        # )

    def refresh_config(self, event: Optional[EventEnvelope] = None) -> None:
        """
        Fetches important configuration flags from the global settings entity to
        override client-side settings.
        If not found, it will use the client-side values.
        """
        now = time.time()
        try:
            if now - self.last_config_refresh > 60 or self._is_settings_change(event):
                assert self.ctx.graph
                entity_dict = self.ctx.graph.graph.get_entity_raw(
                    "urn:li:globalSettings:0", ["globalSettingsInfo"]
                )
                if entity_dict:
                    global_settings = entity_dict.get("aspects", {}).get(
                        "globalSettingsInfo"
                    )
                    if global_settings:
                        doc_propagation_config = global_settings.get("value", {}).get(
                            "docPropagation"
                        )
                        if doc_propagation_config:
                            if doc_propagation_config.get("enabled") is not None:
                                self.config.enabled = doc_propagation_config.get(
                                    "enabled"
                                )
                            if (
                                doc_propagation_config.get("columnPropagationEnabled")
                                is not None
                            ):
                                self.config.columns_enabled = (
                                    doc_propagation_config.get(
                                        "columnPropagationEnabled"
                                    )
                                )
        except Exception:
            # We don't want to fail the pipeline if we can't fetch the config
            logger.warning(
                "Error fetching global settings for doc propagation. Will try again in 1 minute.",
                exc_info=True,
            )
        self.last_config_refresh = now

    def _is_settings_change(self, event: Optional[EventEnvelope]) -> bool:
        if event and isinstance(event.event, MetadataChangeLogClass):
            entity_type = event.event.entityType
            if entity_type == "globalSettings":
                return True
        return False

    def _get_unique_siblings(
        self, graph: AcrylDataHubGraph, entity_urn: str
    ) -> list[str]:
        """
        Get unique siblings for the entity urn
        """

        if entity_urn.startswith("urn:li:schemaField"):
            parent_urn = Urn.create_from_string(entity_urn).get_entity_id()[0]
            entity_field_path = Urn.create_from_string(entity_urn).get_entity_id()[1]
            # Does my parent have siblings?
            siblings: Optional[models.SiblingsClass] = graph.graph.get_aspect(
                parent_urn,
                models.SiblingsClass,
            )
            if siblings and siblings.siblings:
                other_siblings = [x for x in siblings.siblings if x != parent_urn]
                if len(other_siblings) == 1:
                    target_sibling = other_siblings[0]
                    # now we need to find the schema field in this sibling that
                    # matches us
                    if target_sibling.startswith("urn:li:dataset"):
                        schema_fields = graph.graph.get_aspect(
                            target_sibling, models.SchemaMetadataClass
                        )
                        if schema_fields:
                            for schema_field in schema_fields.fields:
                                if schema_field.fieldPath == entity_field_path:
                                    # we found the sibling field
                                    schema_field_urn = make_schema_field_urn(
                                        target_sibling, schema_field.fieldPath
                                    )
                                    return [schema_field_urn]
        return []

    def get_upstreams(self, graph: AcrylDataHubGraph, entity_urn: str) -> List[str]:
        """
        Will remove this once OSS PR is merged in
        """
        import urllib.parse

        url_frag = f"/relationships?direction=OUTGOING&types=List(DownstreamOf)&urn={urllib.parse.quote(entity_urn)}"
        url = f"{graph.graph._gms_server}{url_frag}"
        response = graph.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return entities
        return []

    def _only_one_upstream_field(
        self,
        graph: AcrylDataHubGraph,
        downstream_field: str,
        upstream_field: Optional[str] = None,
    ) -> bool:
        """
        Check if there is only one upstream field for the downstream field. If upstream_field is provided,
        it will also check if the upstream field is the only upstream
        """
        upstreams = (
            graph.get_upstreams(entity_urn=downstream_field)
            if hasattr(graph, "get_upstreams")
            else self.get_upstreams(graph, downstream_field)
        )
        upstream_fields = [x for x in upstreams if x.startswith("urn:li:schemaField")]
        if not upstream_fields:
            logger.debug("No upstream fields found")
            return False
        if not upstream_field:
            return len(upstream_fields) == 1
        result = len(upstream_fields) == 1 and upstream_fields[0] == upstream_field
        if not result:
            logger.debug(
                f"Failed check on solo upstream: Upstream fields: {upstream_fields} and upstream field: {upstream_field}"
            )
        return result

    def act(self, event: EventEnvelope) -> None:
        for mcp in self.act_async(event):
            self.ctx.graph.graph.emit(mcp)

    def act_async(
        self, event: EventEnvelope
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Process the event asynchronously and return the change proposals
        """
        self.refresh_config(event)
        if not self.config.enabled or not self.config.columns_enabled:
            logger.warning("Doc propagation is disabled. skipping event")
            return
        else:
            logger.debug(f"Processing event {event}")
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        stats = self._stats.event_processing_stats
        stats.start(event)
        try:
            doc_propagation_directive = self.should_propagate(event)
            logger.debug(
                f"Doc propagation directive for {event}: {doc_propagation_directive}"
            )
            if (
                doc_propagation_directive is not None
                and doc_propagation_directive.propagate is True
            ):
                self._stats.increment_assets_processed(doc_propagation_directive.entity)
                context = SourceDetails(
                    origin=doc_propagation_directive.origin,
                    via=doc_propagation_directive.via,
                    propagated=True,
                    actor=doc_propagation_directive.actor,
                )
                assert self.ctx.graph
                # find downstream lineage
                downstreams = self.ctx.graph.get_downstreams(
                    entity_urn=doc_propagation_directive.entity
                )
                logger.debug(
                    f"Downstreams: {downstreams} for {doc_propagation_directive.entity}"
                )
                entity_urn = doc_propagation_directive.entity
                if entity_urn.startswith("urn:li:schemaField"):
                    downstream_fields = [
                        x for x in downstreams if x.startswith("urn:li:schemaField")
                    ]
                    for field in downstream_fields:
                        schema_field_urn = Urn.create_from_string(field)
                        parent_urn = schema_field_urn.get_entity_id()[0]
                        field_path = schema_field_urn.get_entity_id()[1]
                        logger.debug(
                            f"Will {doc_propagation_directive.operation} documentation {doc_propagation_directive.doc_string} for {field_path} on {parent_urn}"
                        )
                        if parent_urn.startswith("urn:li:dataset"):
                            if self._only_one_upstream_field(
                                self.ctx.graph,
                                downstream_field=str(schema_field_urn),
                                upstream_field=entity_urn,
                            ):
                                # we only propagate if there is only one
                                # upstream field and that's us
                                maybe_mcp = self.modify_docs_on_columns(
                                    self.ctx.graph,
                                    doc_propagation_directive.operation,
                                    field,
                                    parent_urn,
                                    field_doc=doc_propagation_directive.doc_string,
                                    context=context,
                                )
                                if maybe_mcp:
                                    yield maybe_mcp
                        elif parent_urn.startswith("urn:li:chart"):
                            logger.warning(
                                "Charts are expected to have fields that are dataset schema fields. Skipping for now..."
                            )
                        self._stats.increment_assets_impacted(field)
                elif entity_urn.startswith("urn:li:dataset"):
                    logger.debug(
                        "Dataset level documentation propagation is not implemented yet."
                    )

                # Also handle siblings
                siblings = self._get_unique_siblings(self.ctx.graph, entity_urn)
                if siblings:
                    for sibling in siblings:
                        if entity_urn.startswith(
                            "urn:li:schemaField"
                        ) and sibling.startswith("urn:li:schemaField"):
                            parent_urn = Urn.create_from_string(
                                sibling
                            ).get_entity_id()[0]
                            self._stats.increment_assets_impacted(sibling)
                            maybe_mcp = self.modify_docs_on_columns(
                                self.ctx.graph,
                                doc_propagation_directive.operation,
                                schema_field_urn=sibling,
                                dataset_urn=parent_urn,
                                field_doc=doc_propagation_directive.doc_string,
                                context=context,
                            )
                            if maybe_mcp:
                                yield maybe_mcp
            else:
                logger.debug("No doc propagation directive")
            stats.end(event, success=True)
        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def close(self) -> None:
        return super().close()

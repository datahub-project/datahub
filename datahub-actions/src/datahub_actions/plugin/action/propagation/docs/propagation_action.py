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
from typing import Iterable, List, Optional, Tuple

from pydantic import Field

from datahub.configuration.common import ConfigEnum
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaMetadataClass,
    EntityChangeEventClass as EntityChangeEvent,
    GenericAspectClass,
    MetadataAttributionClass,
    MetadataChangeLogClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationConfig,
    PropagationDirective,
    RelationshipType,
    SourceDetails,
    get_unique_siblings,
)
from datahub_actions.plugin.action.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)

logger = logging.getLogger(__name__)


class DocPropagationDirective(PropagationDirective):
    doc_string: Optional[str] = Field(
        default=None, description="Documentation string to be propagated."
    )


class ColumnPropagationRelationships(ConfigEnum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    SIBLING = "sibling"


class DocPropagationConfig(PropagationConfig):
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
    )
    columns_enabled: bool = Field(
        True,
        description="Indicates whether column documentation propagation is enabled or not.",
    )
    # TODO: Currently this flag does nothing. Datasets are NOT supported for docs propagation.
    datasets_enabled: bool = Field(
        False,
        description="Indicates whether dataset level documentation propagation is enabled or not.",
    )
    column_propagation_relationships: List[ColumnPropagationRelationships] = Field(
        [
            ColumnPropagationRelationships.SIBLING,
            ColumnPropagationRelationships.DOWNSTREAM,
            ColumnPropagationRelationships.UPSTREAM,
        ],
        description="Relationships for column documentation propagation.",
    )


def get_field_path(schema_field_urn: str) -> str:
    urn = Urn.from_string(schema_field_urn)
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
        assert self.ctx.graph
        self._rate_limited_emit_mcp = self.config.get_rate_limited_emit_mcp(
            self.ctx.graph.graph
        )

    def name(self) -> str:
        return "DocPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DocPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Doc Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def should_stop_propagation(
        self, source_details: SourceDetails
    ) -> Tuple[bool, str]:
        """
        Check if the propagation should be stopped based on the source details.
        Return result and reason.
        """
        if source_details.propagation_started_at and (
            int(time.time() * 1000.0) - source_details.propagation_started_at
            >= self.config.max_propagation_time_millis
        ):
            return (True, "Propagation time exceeded.")
        if (
            source_details.propagation_depth
            and source_details.propagation_depth >= self.config.max_propagation_depth
        ):
            return (True, "Propagation depth exceeded.")
        return False, ""

    def get_propagation_relationships(
        self, entity_type: str, source_details: Optional[SourceDetails]
    ) -> List[Tuple[RelationshipType, DirectionType]]:
        possible_relationships = []
        if entity_type == "schemaField":
            if (source_details is not None) and (
                source_details.propagation_relationship
                and source_details.propagation_direction
            ):
                restricted_relationship = source_details.propagation_relationship
                restricted_direction = source_details.propagation_direction
            else:
                restricted_relationship = None
                restricted_direction = None

            for relationship in self.config.column_propagation_relationships:
                if relationship == ColumnPropagationRelationships.UPSTREAM:
                    if (
                        restricted_relationship == RelationshipType.LINEAGE
                        and restricted_direction == DirectionType.DOWN
                    ):  # Skip upstream if the propagation has been restricted to downstream
                        continue
                    possible_relationships.append(
                        (RelationshipType.LINEAGE, DirectionType.UP)
                    )
                elif relationship == ColumnPropagationRelationships.DOWNSTREAM:
                    if (
                        restricted_relationship == RelationshipType.LINEAGE
                        and restricted_direction == DirectionType.UP
                    ):  # Skip upstream if the propagation has been restricted to downstream
                        continue
                    possible_relationships.append(
                        (RelationshipType.LINEAGE, DirectionType.DOWN)
                    )
                elif relationship == ColumnPropagationRelationships.SIBLING:
                    possible_relationships.append(
                        (RelationshipType.SIBLING, DirectionType.ALL)
                    )
        logger.debug(f"Possible relationships: {possible_relationships}")
        return possible_relationships

    def process_schema_field_documentation(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: GenericAspectClass,
        previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[DocPropagationDirective]:
        """
        Process changes in the documentation aspect of schemaField entities.
        Produce a directive to propagate the documentation.
        Business Logic checks:
            - If the documentation is sourced by this action, then we propagate
              it.
            - If the documentation is not sourced by this action, then we log a
                warning and propagate it.
            - If we have exceeded the maximum depth of propagation or maximum
              time for propagation, then we stop propagation and don't return a directive.
        """
        if (
            aspect_name != "documentation"
            or guess_entity_type(entity_urn) != "schemaField"
        ):
            # not a documentation aspect or not a schemaField entity
            return None

        logger.debug("Processing 'documentation' MCL")
        if self.config.columns_enabled:
            current_docs = DocumentationClass.from_obj(json.loads(aspect_value.value))
            old_docs = (
                None
                if previous_aspect_value is None
                else DocumentationClass.from_obj(
                    json.loads(previous_aspect_value.value)
                )
            )
            if current_docs.documentations:
                # get the most recently updated documentation with attribution
                current_documentation_instance = sorted(
                    [doc for doc in current_docs.documentations if doc.attribution],
                    key=lambda x: x.attribution.time if x.attribution else 0,
                )[-1]
                assert current_documentation_instance.attribution
                if (
                    current_documentation_instance.attribution.source is None
                    or current_documentation_instance.attribution.source
                    != self.action_urn
                ):
                    logger.warning(
                        f"Documentation is not sourced by this action which is unexpected. Will be propagating for {entity_urn}"
                    )
                source_details = (
                    (current_documentation_instance.attribution.sourceDetail)
                    if current_documentation_instance.attribution
                    else {}
                )
                source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                    source_details
                )
                should_stop_propagation, reason = self.should_stop_propagation(
                    source_details_parsed
                )
                if should_stop_propagation:
                    logger.warning(f"Stopping propagation for {entity_urn}. {reason}")
                    return None
                else:
                    logger.debug(f"Propagating documentation for {entity_urn}")
                propagation_relationships = self.get_propagation_relationships(
                    entity_type="schemaField", source_details=source_details_parsed
                )
                origin_entity = (
                    source_details_parsed.origin
                    if source_details_parsed.origin
                    else entity_urn
                )
                if old_docs is None or not old_docs.documentations:
                    return DocPropagationDirective(
                        propagate=True,
                        doc_string=current_documentation_instance.documentation,
                        operation="ADD",
                        entity=entity_urn,
                        origin=origin_entity,
                        via=entity_urn,
                        actor=self.actor_urn,
                        propagation_started_at=source_details_parsed.propagation_started_at,
                        propagation_depth=(
                            source_details_parsed.propagation_depth + 1
                            if source_details_parsed.propagation_depth
                            else 1
                        ),
                        relationships=propagation_relationships,
                    )
                else:
                    old_docs_instance = sorted(
                        old_docs.documentations,
                        key=lambda x: x.attribution.time if x.attribution else 0,
                    )[-1]
                    if (
                        current_documentation_instance.documentation
                        != old_docs_instance.documentation
                    ):
                        return DocPropagationDirective(
                            propagate=True,
                            doc_string=current_documentation_instance.documentation,
                            operation="MODIFY",
                            entity=entity_urn,
                            origin=origin_entity,
                            via=entity_urn,
                            actor=self.actor_urn,
                            propagation_started_at=source_details_parsed.propagation_started_at,
                            propagation_depth=(
                                source_details_parsed.propagation_depth + 1
                                if source_details_parsed.propagation_depth
                                else 1
                            ),
                            relationships=propagation_relationships,
                        )
        return None

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[DocPropagationDirective]:
        if self.mcl_processor.is_mcl(event):
            return self.mcl_processor.process(event)
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "DOCUMENTATION"
                and self.config is not None
                and self.config.enabled
            ):
                logger.debug("Processing EntityChangeEvent Documentation Change")
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
                            propagation_started_at=int(time.time() * 1000.0),
                            propagation_depth=1,  # we start at 1 because this is the first propagation
                            relationships=self.get_propagation_relationships(
                                entity_type="schemaField",
                                source_details=None,
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

        try:
            DatasetUrn.from_string(dataset_urn)
        except Exception as e:
            logger.error(
                f"Invalid dataset urn {dataset_urn}. {e}. Skipping documentation propagation."
            )
            return None

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor=self.actor_urn
        )

        source_details = context.for_metadata_attribution()
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
            # this action and sourced from the same origin, if so, we update them
            # otherwise, we add a new documentation entry sourced by this action
            for doc_association in documentations.documentations[:]:
                if doc_association.attribution and doc_association.attribution.source:
                    source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                        doc_association.attribution.sourceDetail
                    )
                    if doc_association.attribution.source == self.action_urn and (
                        source_details_parsed.origin == context.origin
                    ):
                        action_sourced = True
                        if doc_association.documentation != field_doc:
                            mutation_needed = True
                            if operation == "ADD" or operation == "MODIFY":
                                doc_association.documentation = field_doc or ""
                                doc_association.attribution = attribution
                            elif operation == "REMOVE":
                                documentations.documentations.remove(doc_association)
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
                                logger.info(
                                    "Overwriting the asset-level config using globalSettings"
                                )
                                self.config.enabled = doc_propagation_config.get(
                                    "enabled"
                                )
                            if (
                                doc_propagation_config.get("columnPropagationEnabled")
                                is not None
                            ):
                                logger.info(
                                    "Overwriting the column-level config using globalSettings"
                                )
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

    def _only_one_upstream_field(
        self,
        graph: AcrylDataHubGraph,
        downstream_field: str,
        upstream_field: str,
    ) -> bool:
        """
        Check if there is only one upstream field for the downstream field. If upstream_field is provided,
        it will also check if the upstream field is the only upstream

        TODO: We should cache upstreams because we make this fetch upstreams call FOR EVERY downstream that must be propagated to.
        """
        upstreams = graph.get_upstreams(entity_urn=downstream_field)
        # Use a set here in case there are duplicated upstream edges
        upstream_fields = list(
            {x for x in upstreams if guess_entity_type(x) == "schemaField"}
        )

        # If we found no upstreams for the downstream field, simply skip.
        if not upstream_fields:
            logger.debug(
                f"No upstream fields found. Skipping propagation to downstream {downstream_field}"
            )
            return False

        # Convert the set to a list to access by index
        result = len(upstream_fields) == 1 and upstream_fields[0] == upstream_field
        if not result:
            logger.warning(
                f"Failed check for single upstream: Found upstream fields {upstream_fields} for downstream {downstream_field}. Expecting only one upstream field: {upstream_field}"
            )
        return result

    def act(self, event: EventEnvelope) -> None:
        assert self.ctx.graph
        for mcp in self.act_async(event):
            self._rate_limited_emit_mcp(mcp)

    def act_async(
        self, event: EventEnvelope
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Process the event asynchronously and return the change proposals
        """
        self.refresh_config(event)
        if not self.config.enabled or not self.config.columns_enabled:
            logger.warning("Doc propagation is disabled. Skipping event")
            return
        else:
            logger.debug(f"Processing event {event}")

        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()

        stats = self._stats.event_processing_stats
        stats.start(event)

        try:
            doc_propagation_directive = self.should_propagate(event)
            # breakpoint()
            logger.debug(
                f"Doc propagation directive for {event}: {doc_propagation_directive}"
            )

            if (
                doc_propagation_directive is not None
                and doc_propagation_directive.propagate
            ):
                self._stats.increment_assets_processed(doc_propagation_directive.entity)
                context = SourceDetails(
                    origin=doc_propagation_directive.origin,
                    via=doc_propagation_directive.via,
                    propagated=True,
                    actor=doc_propagation_directive.actor,
                    propagation_started_at=doc_propagation_directive.propagation_started_at,
                    propagation_depth=doc_propagation_directive.propagation_depth,
                )
                assert self.ctx.graph
                logger.debug(f"Doc Propagation Directive: {doc_propagation_directive}")
                # TODO: Put each mechanism behind a config flag to be controlled
                # externally.
                lineage_downstream = (
                    RelationshipType.LINEAGE,
                    DirectionType.DOWN,
                ) in doc_propagation_directive.relationships
                lineage_upstream = (
                    RelationshipType.LINEAGE,
                    DirectionType.UP,
                ) in doc_propagation_directive.relationships
                lineage_any = (
                    RelationshipType.LINEAGE,
                    DirectionType.ALL,
                ) in doc_propagation_directive.relationships
                logger.debug(
                    f"Lineage Downstream: {lineage_downstream}, Lineage Upstream: {lineage_upstream}, Lineage Any: {lineage_any}"
                )
                if lineage_downstream or lineage_any:
                    # Step 1: Propagate to downstream entities
                    yield from self._propagate_to_downstreams(
                        doc_propagation_directive, context
                    )

                if lineage_upstream or lineage_any:
                    # Step 2: Propagate to upstream entities
                    yield from self._propagate_to_upstreams(
                        doc_propagation_directive, context
                    )
                if (
                    RelationshipType.SIBLING,
                    DirectionType.ALL,
                ) in doc_propagation_directive.relationships:
                    # Step 3: Propagate to sibling entities
                    yield from self._propagate_to_siblings(
                        doc_propagation_directive, context
                    )
            stats.end(event, success=True)

        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def _propagate_to_downstreams(
        self, doc_propagation_directive: DocPropagationDirective, context: SourceDetails
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the documentation to downstream entities.
        """
        assert self.ctx.graph
        downstreams = self.ctx.graph.get_downstreams(
            entity_urn=doc_propagation_directive.entity
        )
        logger.debug(
            f"Downstreams: {downstreams} for {doc_propagation_directive.entity}"
        )
        entity_urn = doc_propagation_directive.entity
        propagated_context = SourceDetails.parse_obj(context.dict())
        propagated_context.propagation_relationship = RelationshipType.LINEAGE
        propagated_context.propagation_direction = DirectionType.DOWN
        propagated_entities_this_hop_count = 0
        # breakpoint()
        if guess_entity_type(entity_urn) == "schemaField":
            downstream_fields = {
                x for x in downstreams if guess_entity_type(x) == "schemaField"
            }
            for field in downstream_fields:
                schema_field_urn = Urn.from_string(field)
                parent_urn = schema_field_urn.get_entity_id()[0]
                field_path = schema_field_urn.get_entity_id()[1]

                logger.debug(
                    f"Will {doc_propagation_directive.operation} documentation {doc_propagation_directive.doc_string} for {field_path} on {parent_urn}"
                )

                parent_entity_type = guess_entity_type(parent_urn)

                if parent_entity_type == "dataset":
                    if self._only_one_upstream_field(
                        self.ctx.graph,
                        downstream_field=str(schema_field_urn),
                        upstream_field=entity_urn,
                    ):
                        if (
                            propagated_entities_this_hop_count
                            >= self.config.max_propagation_fanout
                        ):
                            # breakpoint()
                            logger.warning(
                                f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. Skipping propagation to downstream {field}"
                            )
                            # No need to propagate to more downstreams
                            return

                        maybe_mcp = self.modify_docs_on_columns(
                            self.ctx.graph,
                            doc_propagation_directive.operation,
                            field,
                            parent_urn,
                            field_doc=doc_propagation_directive.doc_string,
                            context=propagated_context,
                        )
                        if maybe_mcp:
                            propagated_entities_this_hop_count += 1
                            yield maybe_mcp

                elif parent_entity_type == "chart":
                    logger.warning(
                        "Charts are expected to have fields that are dataset schema fields. Skipping for now..."
                    )

                self._stats.increment_assets_impacted(field)

        elif guess_entity_type(entity_urn) == "dataset":
            logger.debug(
                "Dataset level documentation propagation is not yet supported!"
            )

    def _propagate_to_upstreams(
        self, doc_propagation_directive: DocPropagationDirective, context: SourceDetails
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the documentation to upstream entities.
        """
        assert self.ctx.graph
        upstreams = self.ctx.graph.get_upstreams(
            entity_urn=doc_propagation_directive.entity
        )
        logger.debug(f"Upstreams: {upstreams} for {doc_propagation_directive.entity}")
        entity_urn = doc_propagation_directive.entity
        propagated_context = SourceDetails.parse_obj(context.dict())
        propagated_context.propagation_relationship = RelationshipType.LINEAGE
        propagated_context.propagation_direction = DirectionType.UP
        propagated_entities_this_hop_count = 0

        if guess_entity_type(entity_urn) == "schemaField":
            upstream_fields = {
                x for x in upstreams if guess_entity_type(x) == "schemaField"
            }
            # We only propagate to the upstream field if there is only one
            # upstream field
            if len(upstream_fields) == 1:
                for field in upstream_fields:
                    schema_field_urn = Urn.from_string(field)
                    parent_urn = schema_field_urn.get_entity_id()[0]
                    field_path = schema_field_urn.get_entity_id()[1]

                    logger.debug(
                        f"Will {doc_propagation_directive.operation} documentation {doc_propagation_directive.doc_string} for {field_path} on {parent_urn}"
                    )

                    parent_entity_type = guess_entity_type(parent_urn)

                    if parent_entity_type == "dataset":
                        if (
                            propagated_entities_this_hop_count
                            >= self.config.max_propagation_fanout
                        ):
                            logger.warning(
                                f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. Skipping propagation to upstream {field}"
                            )
                            # No need to propagate to more upstreams
                            return
                        maybe_mcp = self.modify_docs_on_columns(
                            self.ctx.graph,
                            doc_propagation_directive.operation,
                            field,
                            parent_urn,
                            field_doc=doc_propagation_directive.doc_string,
                            context=propagated_context,
                        )
                        if maybe_mcp:
                            propagated_entities_this_hop_count += 1
                            yield maybe_mcp

                    elif parent_entity_type == "chart":
                        logger.warning(
                            "Charts are expected to have fields that are dataset schema fields. Skipping for now..."
                        )

                    self._stats.increment_assets_impacted(field)

        elif guess_entity_type(entity_urn) == "dataset":
            logger.debug(
                "Dataset level documentation propagation is not yet supported!"
            )

    def _propagate_to_siblings(
        self, doc_propagation_directive: DocPropagationDirective, context: SourceDetails
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the documentation to sibling entities.
        """
        assert self.ctx.graph
        entity_urn = doc_propagation_directive.entity
        siblings = get_unique_siblings(self.ctx.graph, entity_urn)
        propagated_context = SourceDetails.parse_obj(context.dict())
        propagated_context.propagation_relationship = RelationshipType.SIBLING
        propagated_context.propagation_direction = DirectionType.ALL

        logger.debug(f"Siblings: {siblings} for {doc_propagation_directive.entity}")

        for sibling in siblings:
            if (
                guess_entity_type(entity_urn) == "schemaField"
                and guess_entity_type(sibling) == "schemaField"
            ):
                parent_urn = Urn.from_string(sibling).get_entity_id()[0]
                self._stats.increment_assets_impacted(sibling)
                maybe_mcp = self.modify_docs_on_columns(
                    self.ctx.graph,
                    doc_propagation_directive.operation,
                    schema_field_urn=sibling,
                    dataset_urn=parent_urn,
                    field_doc=doc_propagation_directive.doc_string,
                    context=propagated_context,
                )
                if maybe_mcp:
                    yield maybe_mcp

    def close(self) -> None:
        return

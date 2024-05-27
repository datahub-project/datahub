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
from typing import Any, Dict, Iterable, List, Optional, Set

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
)
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    InputFieldsClass,
    MetadataAttributionClass,
    SchemaMetadataClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import Urn
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.utils.term_resolver import GlossaryTermsResolver
from pydantic import Field

from datahub_integrations.actions.action_extended import ExtendedAction
from datahub_integrations.propagation.propagation_utils import (
    PropagationDirective,
    get_attribution_and_context_from_directive,
)

logger = logging.getLogger(__name__)


class TermPropagationDirective(PropagationDirective):
    term: str
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the term propagation through the original term association.",
    )


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
        example=True,
    )
    target_terms: Optional[List[str]] = Field(
        None,
        description="Optional target terms to restrict term propagation to this and all terms related to these terms.",
        example="[urn:li:glossaryTerm:Sensitive]",
    )
    term_groups: Optional[List[str]] = Field(
        None,
        description="Optional list of term groups to restrict term propagation.",
        example=["Group1", "Group2"],
    )
    target_entities: List[str] = Field(
        ["schemaField"],
        description="List of target entities to propagate terms from and to.",
        example=["schemaField"],
    )


class TermPropagationAction(ExtendedAction):
    def __init__(self, config: TermPropagationConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx
        self.action_urn = (
            ctx.pipeline_name
            if ctx.pipeline_name.startswith("urn:li:dataHubAction:")
            else f"urn:li:dataHubAction:{ctx.pipeline_name}"
        )
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
        self.bootstrap()

    def name(self) -> str:
        return "TermPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TermPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Term Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def _get_target_terms_expanded(self) -> Iterable[str]:
        expanded_terms = set()
        if self.config.target_terms:
            for term in self.config.target_terms:
                expanded_terms.add(term)
                from datahub.ingestion.graph.client import DataHubGraph

                inherited_terms = self.ctx.graph.graph.get_related_entities(
                    term, ["IsA"], DataHubGraph.RelationshipDirection.INCOMING
                )
                if inherited_terms:
                    for inherited_term in inherited_terms:
                        expanded_terms.add(inherited_term.urn)
        return expanded_terms

    def _get_term_groups_expanded_to_terms(self) -> Iterable[str]:
        """
        Get all terms in the term groups
        """

        def __get_terms_in_group(group: str) -> Set[str]:
            terms = self.ctx.graph.get_relationships(group, "OUTGOING", "IsPartOf")
            expanded_terms = set()
            if terms:
                for term in terms:
                    if term.startswith("urn:li:glossaryTerm"):
                        expanded_terms.add(term)
                    elif term.startswith("urn:li:glossaryNode"):
                        expanded_terms.update(__get_terms_in_group(term))
            return expanded_terms

        expanded_terms = set()
        if self.config.term_groups:
            for term_group in self.config.term_groups:
                expanded_terms.update(__get_terms_in_group(term_group))
            return expanded_terms
        return expanded_terms

    def _process_asset(
        self, asset_urn: str, terms: List[str], target_entity_type: str, operation: str
    ) -> None:
        if target_entity_type == "schemaField" and asset_urn.startswith(
            "urn:li:dataset"
        ):
            # Schema Fields are special, we need to extract terms from dataset
            # aspects
            editable_schema_metadata = self.ctx.graph.graph.get_aspect(
                asset_urn, EditableSchemaMetadataClass
            )

            schema_metadata = self.ctx.graph.graph.get_aspect(
                asset_urn, SchemaMetadataClass
            )

            if schema_metadata and schema_metadata.fields:
                field_terms_map: dict[str, List[GlossaryTermAssociationClass]] = {
                    field.fieldPath: [
                        term
                        for term in field.glossaryTerms.terms
                        if term.urn in terms or not terms
                    ]
                    for field in schema_metadata.fields
                    if field.glossaryTerms
                }

            if (
                editable_schema_metadata
                and editable_schema_metadata.editableSchemaFieldInfo
            ):
                field_terms_map.update(
                    {
                        field.fieldPath: [
                            term
                            for term in field.glossaryTerms.terms
                            if term.urn in terms or not terms
                        ]
                        for field in editable_schema_metadata.editableSchemaFieldInfo
                        if field.glossaryTerms
                    }
                )

            breakpoint()
            for field_path, field_terms in field_terms_map.items():
                field_urn = make_schema_field_urn(asset_urn, field_path)
                for term in field_terms:
                    term_propagation_directive = TermPropagationDirective(
                        propagate=True,
                        term=term.urn,
                        operation=operation,
                        entity=field_urn,
                        actor=term.actor,
                        origin=asset_urn,
                    )
                    self.process_directive(term_propagation_directive)
        elif target_entity_type in [
            "dataset",
            "chart",
            "dashboard",
            "dataJob",
            "dataFlow",
        ]:
            glossaryTerms = self.ctx.graph.graph.get_aspect(
                asset_urn, GlossaryTermsClass
            )
            if glossaryTerms:
                asset_terms = [
                    term
                    for term in glossaryTerms.terms
                    if term.urn in terms or not terms
                ]
                for term in asset_terms:
                    term_propagation_directive = TermPropagationDirective(
                        propagate=True,
                        term=term.urn,
                        operation=operation,
                        entity=asset_urn,
                        actor=term.actor,
                        origin=asset_urn,
                    )
                    self.process_directive(term_propagation_directive)
        else:
            logger.error(f"Unsupported target entity type {target_entity_type}")

    def bootstrap(self) -> None:
        # First process configuration to understand what are the terms that need
        # to be propagated
        self.process_all_assets(operation="ADD")

    def rollback(self) -> None:
        self.process_all_assets(operation="REMOVE")

    def process_all_assets(self, operation: str) -> None:
        all_terms: List[str] = []
        if self.config.target_terms:
            all_terms.extend(self._get_target_terms_expanded())

        if self.config.term_groups:
            all_terms.extend(self._get_term_groups_expanded_to_terms())

        if all_terms:
            logger.info(f"Will propagate terms {all_terms}")
        else:
            logger.info("Will propagate all terms")

        # Then query the search index
        # to find all the entities that have these terms
        or_filters: List[Dict[str, Any]] = []

        entity_index_field_map = {
            "schemaField": {
                "dataset": ["fieldGlossaryTerms", "editedFieldGlossaryTerms"],
            },
            "dataset": {
                "dataset": ["glossaryTerms"],
            },
            "chart": {
                "chart": ["glossaryTerms"],
            },
            "dashboard": {
                "dashboard": ["glossaryTerms"],
            },
            "dataJob": {
                "dataJob": ["glossaryTerms"],
            },
            "dataFlow": {
                "dataFlow": ["glossaryTerms"],
            },
        }
        for entity_type in self.config.target_entities:
            index_field_map = entity_index_field_map.get(entity_type, {})
            for index, index_fields in index_field_map.items():
                or_filters = []
                for index_field in index_fields:
                    or_filters.extend(
                        [
                            {
                                "field": index_field,
                                "values": [term],
                                "condition": "IN",
                            }
                            for term in all_terms
                        ]
                    )
                assets = self.ctx.graph.graph.get_urns_by_filter(
                    entity_types=[index],
                    extra_or_filters=or_filters,
                )
                breakpoint()
                for asset in assets:
                    self._process_asset(asset, all_terms, entity_type, operation)

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[TermPropagationDirective]:
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            logger.info(f"Received event {event}")
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
                        parameters = (
                            semantic_event.parameters._inner_dict
                            if semantic_event.parameters
                            else {}
                        )
                        origin = (
                            parameters.get("attribution.origin")
                            or parameters.get("origin")
                            or semantic_event.entityUrn
                        )
                        return TermPropagationDirective(
                            propagate=True,
                            term=semantic_event.modifier,
                            operation=semantic_event.operation,
                            entity=semantic_event.entityUrn,
                            actor=semantic_event.auditStamp.actor,
                            origin=origin,
                        )
        return None

    def add_terms_to_dataset_fields(
        self,
        graph: AcrylDataHubGraph,
        dataset_urn: str,
        field_terms: dict[str, List[str]],
        context: dict,
    ) -> None:
        # Create a MetadataPatchProposal object
        dataset = DatasetPatchBuilder(urn=dataset_urn)
        for field_path, terms in field_terms.items():
            field_builder = dataset.for_field(field_path=field_path)
            for term in terms:
                field_builder.add_term(
                    GlossaryTermAssociationClass(
                        term, context=json.dumps(context) if context else None
                    )
                )
        for mcp in dataset.build():
            logger.info(f"{mcp}")
            graph.emit(mcp)

    def modify_terms_on_chart_fields_slow(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        chart_urn: str,
        field_terms: dict[str, List[str]],
        attribution: MetadataAttributionClass,
        context: str,
    ) -> None:
        if not chart_urn.startswith("urn:li:chart"):
            logger.error(f"Invalid chart urn {chart_urn}. Must start with urn:li:chart")
            return

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor="urn:li:corpuser:action-bot"
        )

        input_schema_fields = graph.graph.get_aspect(chart_urn, InputFieldsClass)
        mutation_needed = False
        if input_schema_fields is None or not input_schema_fields.fields:
            logger.error(f"Chart {chart_urn} does not have input schema fields")
            return
        else:
            # we have input schema fields
            for field_path, terms in field_terms.items():
                if field_path not in [
                    x.schemaField.fieldPath for x in input_schema_fields.fields
                ]:
                    logger.warning(f"Field {field_path} not found in chart {chart_urn}")
                else:
                    for fieldInfo in input_schema_fields.fields:
                        if fieldInfo.schemaField.fieldPath == field_path:
                            for term in terms:
                                if not fieldInfo.schemaField.glossaryTerms:
                                    if operation == "ADD":
                                        fieldInfo.schemaField.glossaryTerms = (
                                            GlossaryTermsClass(
                                                terms=[
                                                    GlossaryTermAssociationClass(
                                                        term,
                                                        context=context,
                                                        attribution=attribution,
                                                    )
                                                ],
                                                auditStamp=auditStamp,
                                            )
                                        )
                                        mutation_needed = True
                                else:
                                    if term not in [
                                        x.urn
                                        for x in fieldInfo.schemaField.glossaryTerms.terms
                                    ]:
                                        if operation == "ADD":
                                            fieldInfo.schemaField.glossaryTerms.terms.append(
                                                GlossaryTermAssociationClass(
                                                    term,
                                                    context=context,
                                                    attribution=attribution,
                                                )
                                            )
                                            fieldInfo.schemaField.glossaryTerms.auditStamp = (
                                                auditStamp
                                            )
                                            mutation_needed = True
                                    else:
                                        if operation == "REMOVE":
                                            fieldInfo.schemaField.glossaryTerms.terms = [
                                                x
                                                for x in fieldInfo.schemaField.glossaryTerms.terms
                                                if x.urn != term
                                            ]
                                            fieldInfo.schemaField.glossaryTerms.auditStamp = (
                                                auditStamp
                                            )
                                            mutation_needed = True
        if mutation_needed:
            assert input_schema_fields.validate()
            graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=input_schema_fields,
                )
            )

    def modify_terms_on_dataset_fields_slow(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        dataset_urn: str,
        field_terms: dict[str, List[str]],
        attribution: MetadataAttributionClass,
        context: str,
    ) -> None:
        breakpoint()
        if not dataset_urn.startswith("urn:li:dataset"):
            logger.error(
                f"Invalid dataset urn {dataset_urn}. Must start with urn:li:dataset"
            )
            return

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor="urn:li:corpuser:action-bot"
        )

        editableSchemaMetadata = graph.graph.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )
        mutation_needed = False
        if editableSchemaMetadata is None:
            if operation == "ADD":
                mutation_needed = True
                editableSchemaMetadata = EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath=field_path,
                            glossaryTerms=GlossaryTermsClass(
                                terms=[
                                    GlossaryTermAssociationClass(
                                        term, context=context, attribution=attribution
                                    )
                                    for term in terms
                                ],
                                auditStamp=auditStamp,
                            ),
                        )
                        for field_path, terms in field_terms.items()
                    ]
                )
        else:
            if editableSchemaMetadata.editableSchemaFieldInfo is None:
                editableSchemaMetadata.editableSchemaFieldInfo = []
            field_paths = [
                x.fieldPath for x in editableSchemaMetadata.editableSchemaFieldInfo
            ]
            field_paths_deduped = set(field_paths)
            if len(field_paths) != len(field_paths_deduped):
                logger.error(
                    f"Duplicate field paths found in EditableSchemaMetadata for {dataset_urn}"
                )
                # trim the fields
                editable_schema_field_info_deduped = []
                for field in editableSchemaMetadata.editableSchemaFieldInfo:
                    if field.fieldPath in field_paths_deduped:
                        editable_schema_field_info_deduped.append(field)
                        field_paths_deduped.remove(field.fieldPath)
                editableSchemaMetadata.editableSchemaFieldInfo = (
                    editable_schema_field_info_deduped
                )
                breakpoint()

            for field_path, terms in field_terms.items():
                if field_path not in [
                    x.fieldPath for x in editableSchemaMetadata.editableSchemaFieldInfo
                ]:
                    if operation == "ADD":
                        editableSchemaMetadata.editableSchemaFieldInfo.append(
                            EditableSchemaFieldInfoClass(
                                fieldPath=field_path,
                                glossaryTerms=GlossaryTermsClass(
                                    terms=[
                                        GlossaryTermAssociationClass(
                                            term,
                                            context=context,
                                            attribution=attribution,
                                        )
                                        for term in terms
                                    ],
                                    auditStamp=auditStamp,
                                ),
                            )
                        )
                        mutation_needed = True
                else:
                    for fieldInfo in editableSchemaMetadata.editableSchemaFieldInfo:
                        if (
                            fieldInfo.fieldPath.lower() == field_path.lower()
                        ):  # case insensitive
                            for term in terms:
                                if fieldInfo.glossaryTerms is None:
                                    if operation == "ADD":
                                        fieldInfo.glossaryTerms = GlossaryTermsClass(
                                            terms=[
                                                GlossaryTermAssociationClass(
                                                    term,
                                                    context=context,
                                                    attribution=attribution,
                                                )
                                            ],
                                            auditStamp=auditStamp,
                                        )
                                        mutation_needed = True
                                else:
                                    if term not in [
                                        x.urn for x in fieldInfo.glossaryTerms.terms
                                    ]:
                                        if operation == "ADD":
                                            fieldInfo.glossaryTerms.terms.append(
                                                GlossaryTermAssociationClass(
                                                    term,
                                                    context=context,
                                                    attribution=attribution,
                                                )
                                            )
                                            fieldInfo.glossaryTerms.auditStamp = (
                                                auditStamp
                                            )
                                            mutation_needed = True
                                    else:
                                        if operation == "REMOVE":
                                            fieldInfo.glossaryTerms.terms = [
                                                x
                                                for x in fieldInfo.glossaryTerms.terms
                                                if x.urn != term
                                            ]
                                            fieldInfo.glossaryTerms.auditStamp = (
                                                auditStamp
                                            )
                                            mutation_needed = True
        if mutation_needed:
            try:
                assert editableSchemaMetadata.validate()
                graph.graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=editableSchemaMetadata,
                    )
                )
            except Exception:
                breakpoint()

    def act(self, event: EventEnvelope) -> None:
        """This method responds to changes to glossary terms and propagates them to downstream entities"""

        term_propagation_directive = self.should_propagate(event)
        logger.info(f"Term propagation directive: {term_propagation_directive}")
        if term_propagation_directive:
            self.process_directive(term_propagation_directive)

    def process_directive(
        self, term_propagation_directive: TermPropagationDirective
    ) -> None:
        if (
            term_propagation_directive is not None
            and term_propagation_directive.propagate is True
        ):
            assert self.ctx.graph
            (attribution, context_string) = get_attribution_and_context_from_directive(
                self.action_urn,
                term_propagation_directive,
            )
            # find downstream lineage
            downstreams = self.ctx.graph.get_downstreams(
                entity_urn=term_propagation_directive.entity
            )
            logger.info(
                f"Downstreams: {downstreams} for {term_propagation_directive.entity}"
            )
            entity_urn = term_propagation_directive.entity
            if entity_urn.startswith("urn:li:schemaField"):
                downstream_fields = [
                    x for x in downstreams if x.startswith("urn:li:schemaField")
                ]
                for field in downstream_fields:

                    schema_field_urn = Urn.create_from_string(field)
                    dataset_urn = schema_field_urn.get_entity_id()[0]
                    field_path = schema_field_urn.get_entity_id()[1]
                    logger.info(
                        f"Will {term_propagation_directive.operation} term {term_propagation_directive.term} for {field_path} on {dataset_urn}"
                    )
                    if dataset_urn.startswith("urn:li:dataset"):
                        self.modify_terms_on_dataset_fields_slow(
                            self.ctx.graph,
                            term_propagation_directive.operation,
                            dataset_urn,
                            field_terms={field_path: [term_propagation_directive.term]},
                            attribution=attribution,
                            context=context_string,
                        )
                    elif dataset_urn.startswith("urn:li:chart"):
                        self.modify_terms_on_chart_fields_slow(
                            self.ctx.graph,
                            term_propagation_directive.operation,
                            dataset_urn,
                            field_terms={field_path: [term_propagation_directive.term]},
                            attribution=attribution,
                            context=context_string,
                        )
            elif entity_urn.startswith("urn:li:dataset"):
                downstream_datasets = [
                    x for x in downstreams if x.startswith("urn:li:dataset")
                ]
                for dataset in downstream_datasets:
                    self.ctx.graph.add_terms_to_dataset(
                        dataset,
                        [term_propagation_directive.term],
                        context=term_propagation_directive,
                    )
                    logger.info(
                        f"Will {term_propagation_directive.operation} term {term_propagation_directive.term} to {dataset}"
                    )
        else:
            print("No term propagation directive")

    def close(self) -> None:
        return super().close()

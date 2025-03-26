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
from typing import Dict, Iterable, List, Optional, Set, TypeVar

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    EntityChangeEventClass as EntityChangeEvent,
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

from datahub_integrations.actions.action_extended import (
    AutomationActionConfig,
    ExtendedAction,
)
from datahub_integrations.actions.oss.stats_util import EventProcessingStats
from datahub_integrations.propagation.propagation_utils import (
    ComposablePropagator,
    PropagationDirective,
    RelationshipType,
    SelectedAsset,
    get_attribution_and_context_from_directive,
    get_unique_siblings,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


class TermPropagationDirective(PropagationDirective):
    term: str
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the term propagation through the original term association.",
    )


class TermPropagationConfig(AutomationActionConfig):
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
    term_resolution_caching_ttl_sec: int = Field(
        default=60,  # 1 minute
        description="Term groups -> terms are resolved and cached for this duration in milliseconds.",
        example=60,
    )


class TermPropagationAction(ExtendedAction[SelectedAsset], ComposablePropagator):
    """
    Known Limitations & Risks:

        - Asset-level propagation is ONLY supported for Dataset Entities. This causes known issues across Data Job nodes.
        - Deletion / Cleanup of Propagated Glossary Terms when one is removed on upstream is not yet supported.
        - Glossary Term Relationship Lookup (IsA inheritance checks) happens on the fly and is not very performant.
        - [Important!] This action contains read-modify-write patterns on aspects. This MUST be migrated to PATCH soon, as this
          can cause conflicting overwrites on the same URN. This is relevant because we are writing the DOWNSTREAM aspect
          so Kafka partitioning does not help us here.
        - Bootstrap & rollback can both be very expensive operations (with conflicting write) and can potentially overload the system. There is no max number of entities limited for either operation.
        - By default, we fetch ALL glossary terms every 1 minute. This means glossary changes may take up to 1 minute to reflect. Control this via
          "term_resolution_caching_ttl_sec"

    Opportunities to Extend:

        - Support configuration of propagation mechanisms: Lineage, Siblings, Children (Containers)
        - Support for more entity types - Containers, Dashboards, Charts, Data Jobs, ML Models
        - Support filtering the "source" entities for propagation
            - By Platform: Only propagate from Snowflake
            - By Asset Type or Sub Type: Only propagate from Views, etc.
            - By Container: Only propagate in the "prod" DB
            - By Domain: Only propagate for the Marketing Domain
            - By Tag: Only propagate for a specific tag

            This will require richer hydration support, but can lead to a vastly more usable experience.
    """

    def __init__(self, config: TermPropagationConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self.config: TermPropagationConfig = config
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
                        logger.error(
                            f"Failed to resolve term by name {t}. Skipping propagation of this entity!"
                        )
                    resolved_terms.append(resolved_term)
            self.config.target_terms = resolved_terms

        if self.config.term_groups:
            logger.info(self.config)
            logger.info(
                f"[Config] Will propagate all terms in groups {self.config.term_groups}"
            )
            resolved_nodes = []
            for node in self.config.term_groups:
                if node.startswith("urn:li:glossaryNode"):
                    resolved_nodes.append(node)
                else:
                    resolved_node = self.term_resolver.get_glossary_node_urn(node)
                    if not resolved_node:
                        logger.error(
                            f"Failed to resolve node by name {node}. Skipping propagation of this entity!"
                        )
                    resolved_nodes.append(resolved_node)
            self.config.term_groups = resolved_nodes

        # Set Up Cached Terms
        self.cached_terms = self._get_all_terms()
        self.cached_terms_ts_sec = int(time.time())

        logger.info(f"Will propagate all resolved terms: {self.all_terms}")
        self.event_processing_stats = EventProcessingStats()

    def name(self) -> str:
        return "TermPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TermPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Term Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def process_one_asset(
        self, asset: SelectedAsset, operation: str
    ) -> Iterable[TermPropagationDirective]:
        terms = self.all_terms
        asset_urn = asset.urn
        target_entity_type = asset.target_entity_type
        if target_entity_type == "schemaField" and asset_urn.startswith(
            "urn:li:dataset"
        ):
            # Schema Fields are special, we need to extract terms from dataset
            # aspects

            field_terms_map: dict[str, List[GlossaryTermAssociationClass]] = {}

            editable_schema_metadata = self.ctx.graph.graph.get_aspect(
                asset_urn, EditableSchemaMetadataClass
            )

            schema_metadata = self.ctx.graph.graph.get_aspect(
                asset_urn, SchemaMetadataClass
            )

            if schema_metadata and schema_metadata.fields:
                field_terms_map = {
                    field.fieldPath: [
                        term
                        for term in field.glossaryTerms.terms
                        if term.urn in terms or not terms
                    ]
                    for field in schema_metadata.fields
                    if field.glossaryTerms
                }
                logger.debug(f"Field terms map: {field_terms_map}")

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
                logger.debug(f"Post update: Field terms map: {field_terms_map}")

            for field_path, field_terms in field_terms_map.items():
                field_urn = make_schema_field_urn(asset_urn, field_path)
                for term in field_terms:
                    term_propagation_directive = TermPropagationDirective(
                        propagate=True,
                        term=term.urn,
                        operation=operation,
                        entity=field_urn,
                        actor=term.actor,
                        origin=field_urn,
                    )
                    yield term_propagation_directive
        # Currently only support Datasets and Schema Fields
        elif target_entity_type in [
            "dataset",
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
                    yield term_propagation_directive
        else:
            logger.error(f"Unsupported target entity type {target_entity_type}")

    @property
    def all_terms(self) -> List[str]:
        if (
            self.cached_terms_ts_sec
            < int(time.time()) - self.config.term_resolution_caching_ttl_sec
        ):
            self.cached_terms = self._get_all_terms()
            self.cached_terms_ts_sec = int(time.time())

        return self.cached_terms

    def identify_assets(self) -> Iterable[SelectedAsset]:
        asset_filters = self.asset_filters()
        for entity_type, index_filters in asset_filters.items():
            for index, filters in index_filters.items():
                for asset_urn in self.ctx.graph.graph.get_urns_by_filter(
                    entity_types=[index], extra_or_filters=filters
                ):
                    yield SelectedAsset(urn=asset_urn, target_entity_type=entity_type)

    def bootstrappable_assets(self) -> Iterable[SelectedAsset]:
        return self.identify_assets()

    def bootstrap_asset(self, asset: SelectedAsset) -> None:
        logger.info(f"Bootstrapping asset {asset}")
        for directive in self.process_one_asset(
            asset=asset,
            operation="ADD",
        ):
            self.process_directive(directive)

    def rollbackable_assets(self) -> Iterable[SelectedAsset]:
        return self.identify_assets()

    def rollback_asset(self, asset: SelectedAsset) -> None:
        for directive in self.process_one_asset(
            asset=asset,
            operation="REMOVE",
        ):
            self.process_directive(directive)

    def asset_filters(self) -> Dict[str, Dict[str, List[RawSearchFilterRule]]]:
        asset_filters: Dict[str, Dict[str, List[RawSearchFilterRule]]] = {}

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
            asset_filters[entity_type] = {}
            for index, index_fields in index_field_map.items():
                or_filters: List[RawSearchFilterRule] = []
                for index_field in index_fields:
                    or_filters.extend(
                        [
                            {
                                "field": index_field,
                                "values": [term],
                                "condition": "IN",
                            }
                            for term in self.all_terms
                        ]
                    )
                asset_filters[entity_type][index] = or_filters
        return asset_filters

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[TermPropagationDirective]:
        logger.debug(f"Term Propagator: Received event {event}")
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            logger.debug(f"Received event {event}")
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "GLOSSARY_TERM"
                and (
                    semantic_event.operation == "ADD"
                    or semantic_event.operation == "REMOVE"
                )
                and self.config is not None
                and self.config.enabled
            ):
                assert semantic_event.modifier
                logger.debug(
                    f"Checking for {semantic_event.modifier} in {self.all_terms}"
                )
                for target_term in self.all_terms or [semantic_event.modifier]:
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

    def act(self, event: EventEnvelope) -> None:
        """This method responds to changes to glossary terms and propagates them
        to downstream entities"""
        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()
        self._stats.event_processing_stats.start(event)
        success = False
        try:
            term_propagation_directive = self.should_propagate(event)
            if term_propagation_directive:
                logger.info(f"Term propagation directive: {term_propagation_directive}")
                self.process_directive(term_propagation_directive)
            success = True
        finally:
            self._stats.event_processing_stats.end(event, success=success)

    def process_directive(
        self, term_propagation_directive: TermPropagationDirective
    ) -> None:
        if not term_propagation_directive or not term_propagation_directive.propagate:
            logger.debug("Skipping propagation. No term propagation directive provided")
            return

        assert self.ctx.graph
        attribution, context_dict = get_attribution_and_context_from_directive(
            self.action_urn, term_propagation_directive
        )

        downstreams = self.ctx.graph.get_downstreams(
            entity_urn=term_propagation_directive.entity
        )

        siblings = get_unique_siblings(
            self.ctx.graph, term_propagation_directive.entity
        )

        logger.debug(
            f"Downstreams: {downstreams}, Siblings: {siblings} for {term_propagation_directive.entity}"
        )

        entity_urn = term_propagation_directive.entity
        downstreams_set = set(downstreams)

        # Case 1: Propagate for Schema Fields
        if entity_urn.startswith("urn:li:schemaField"):
            downstream_fields = [
                x for x in downstreams if x.startswith("urn:li:schemaField")
            ]
            sibling_fields = [
                x
                for x in siblings
                if x.startswith("urn:li:schemaField") and x not in downstreams_set
            ]
            self._handle_schema_field_propagation(
                term_propagation_directive,
                downstream_fields,
                sibling_fields,
                attribution,
                context_dict,
            )
        # Case 2: Propagate for Datasets
        elif entity_urn.startswith("urn:li:dataset"):
            downstream_datasets = [
                x
                for x in downstreams
                if x.startswith("urn:li:dataset") and x != entity_urn
            ]
            sibling_datasets = [
                x
                for x in siblings
                if x.startswith("urn:li:dataset")
                and x != entity_urn
                and x not in downstreams_set
            ]
            self._handle_dataset_propagation(
                term_propagation_directive,
                downstream_datasets,
                sibling_datasets,
                attribution,
                context_dict,
            )
        else:
            logger.warning(
                f"Skipping term propagationg for unsupported entity urn: {entity_urn}"
            )

    def _handle_schema_field_propagation(
        self,
        term_propagation_directive: TermPropagationDirective,
        downstream_fields: List[str],
        sibling_fields: List[str],
        attribution: MetadataAttributionClass,
        context_dict: dict[str, str],
    ) -> None:
        # Case 1: Apply to Downstreams
        for field in downstream_fields:
            context_dict["relationship"] = RelationshipType.LINEAGE.value
            self._add_term_to_field(
                field, term_propagation_directive, attribution, json.dumps(context_dict)
            )

        # Case 2: Apply to Siblings
        for sibling in sibling_fields:
            context_dict["relationship"] = RelationshipType.SIBLINGS.value
            self._add_term_to_field(
                sibling,
                term_propagation_directive,
                attribution,
                json.dumps(context_dict),
            )

    def _handle_dataset_propagation(
        self,
        term_propagation_directive: TermPropagationDirective,
        downstream_datasets: List[str],
        sibling_datasets: List[str],
        attribution: MetadataAttributionClass,
        context_dict: dict[str, str],
    ) -> None:
        for dataset in downstream_datasets:
            context_dict["relationship"] = RelationshipType.LINEAGE.value
            self._add_term_to_dataset(
                dataset,
                term_propagation_directive,
                attribution,
                json.dumps(context_dict),
            )

        for sibling in sibling_datasets:
            context_dict["relationship"] = RelationshipType.SIBLINGS.value
            self._add_term_to_dataset(
                sibling,
                term_propagation_directive,
                attribution,
                json.dumps(context_dict),
            )

    def _add_term_to_field(
        self,
        field: str,
        term_propagation_directive: TermPropagationDirective,
        attribution: MetadataAttributionClass,
        context_string: str,
    ) -> None:
        schema_field_urn = Urn.create_from_string(field)
        dataset_urn = schema_field_urn.get_entity_id()[0]
        field_path = schema_field_urn.get_entity_id()[1]

        logger.debug(
            f"Will {term_propagation_directive.operation} term {term_propagation_directive.term} for {field_path} on {dataset_urn}"
        )

        # Case 1: Modify Dataset Fields
        if dataset_urn.startswith("urn:li:dataset"):
            self._add_term_to_dataset_field_slow(
                self.ctx.graph,
                term_propagation_directive.operation,
                dataset_urn,
                field_terms={field_path: [term_propagation_directive.term]},
                attribution=attribution,
                context=context_string,
            )
        # Case 2: Modify Chart Fields
        elif dataset_urn.startswith("urn:li:chart"):
            self._add_term_to_chart_field_slow(
                self.ctx.graph,
                term_propagation_directive.operation,
                dataset_urn,
                field_terms={field_path: [term_propagation_directive.term]},
                attribution=attribution,
                context=context_string,
            )
        else:
            logger.warning(
                f"Skipping propagating to unsupported URN type for field propagation: {dataset_urn}"
            )

    def _add_term_to_dataset(
        self,
        dataset: str,
        term_propagation_directive: TermPropagationDirective,
        attribution: MetadataAttributionClass,
        context: str,
    ) -> None:
        logger.info(
            f"Will {term_propagation_directive.operation} term {term_propagation_directive.term} to {dataset}"
        )

        patch_builder = DatasetPatchBuilder(urn=dataset)

        patch_builder.add_term(
            GlossaryTermAssociationClass(
                term_propagation_directive.term,
                attribution=attribution,
                context=context if context else None,
            )
        )

        for mcp in patch_builder.build():
            self.ctx.graph.graph.emit(mcp)

    def _add_term_to_chart_field_slow(
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
                                            fieldInfo.schemaField.glossaryTerms.auditStamp = auditStamp
                                            mutation_needed = True
                                    else:
                                        if operation == "REMOVE":
                                            fieldInfo.schemaField.glossaryTerms.terms = [
                                                x
                                                for x in fieldInfo.schemaField.glossaryTerms.terms
                                                if x.urn != term
                                            ]
                                            fieldInfo.schemaField.glossaryTerms.auditStamp = auditStamp
                                            mutation_needed = True
        if mutation_needed:
            assert input_schema_fields.validate()
            graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=input_schema_fields,
                )
            )

    def _add_term_to_dataset_field_slow(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        dataset_urn: str,
        field_terms: dict[str, List[str]],
        attribution: MetadataAttributionClass,
        context: str,
    ) -> None:
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
            assert editableSchemaMetadata.validate()
            graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=editableSchemaMetadata,
                )
            )

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
        Get all terms in the term groups.
        Will recurse down term group hierarchy to get all terms.
        """

        def __get_terms_in_group(group: str) -> Set[str]:
            terms = self.ctx.graph.get_relationships(
                entity_urn=group, direction="INCOMING", relationship_types=["IsPartOf"]
            )
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

    def _get_all_terms(self) -> List[str]:
        all_terms: List[str] = []
        if self.config.target_terms:
            all_terms.extend(self._get_target_terms_expanded())

        if self.config.term_groups:
            all_terms.extend(self._get_term_groups_expanded_to_terms())

        return list(set(all_terms))

    def close(self) -> None:
        return super().close()

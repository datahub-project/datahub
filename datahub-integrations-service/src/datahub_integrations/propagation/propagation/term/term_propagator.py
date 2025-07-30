import json
import logging
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Type

import cachetools
from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    InputFieldsClass,
    MetadataAttributionClass,
    SchemaMetadataClass,
    TagAssociationClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.plugin.action.utils.term_resolver import GlossaryTermsResolver
from pydantic import Field

from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    EntityPropagatorConfig,
    PropagationOutput,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TermPropagatorConfig(EntityPropagatorConfig):
    """Configuration for the TermPropagator."""

    target_terms: Optional[List[str]] = Field(
        default=None,
        description="Optional target terms to restrict term propagation to this and all terms related to these terms.",
        examples=["urn:li:glossaryTerm:Sensitive"],
    )

    term_groups: Optional[List[str]] = Field(
        default=None,
        description="Optional list of term groups to restrict term propagation.",
        examples=["Group1", "Group2"],
    )

    term_resolution_caching_ttl_sec: int = Field(
        default=60,  # 1 minute
        description="Term groups -> terms are resolved and cached for this duration in seconds.",
        examples=[60],
    )


class TermPropagationDirective(PropagationDirective):
    """Directive for term propagation."""

    terms: List[str]


class TermPropagator(EntityPropagator):
    """
    A propagator that propagates glossary terms across entities based on relationships.
    """

    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: TermPropagatorConfig
    ) -> None:
        """Initialize the TermPropagator."""

        super().__init__(action_urn, graph, config)
        self.config: TermPropagatorConfig = config
        self.graph = graph
        self.action_urn = action_urn

        self.term_resolver = GlossaryTermsResolver(graph=self.graph)
        self.cache: cachetools.TTLCache = cachetools.TTLCache(
            ttl=self.config.term_resolution_caching_ttl_sec, maxsize=sys.maxsize
        )

        self._init_term_groups()
        self._register_processors()

    def _init_term_groups(self) -> None:
        """Initialize term groups configuration."""

        if not self.config.term_groups:
            return

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
                    continue
                resolved_nodes.append(resolved_node)

        self.config.term_groups = resolved_nodes

    def _register_processors(self) -> None:
        """Register processors for different entity types."""

        entity_categories = [
            ("dataset", "GLOSSARY_TERM"),
            ("schemaField", "GLOSSARY_TERM"),
            ("chart", "GLOSSARY_TERM"),
        ]

        for entity_type, category in entity_categories:
            self.ece_processor.register_processor(
                entity_type=entity_type,
                category=category,
                processor=self.process_ece,
            )

    def aspects(self) -> List[Type[_Aspect]]:
        """Return the aspects this propagator handles."""

        return [SchemaMetadataClass, EditableSchemaMetadataClass]

    def get_metadata_from_aspect(self, aspect: Aspect) -> List[TagAssociationClass]:
        """Extract metadata from aspect."""

        assert isinstance(aspect, GlobalTagsClass)
        return aspect.tags

    def generate_directive(
        self,
        entity_urn: str,
        terms: List[str],
        source_details: Optional[SourceDetails] = None,
    ) -> Optional[TermPropagationDirective]:
        """Generate a propagation directive for terms."""

        if source_details:
            origin = source_details.origin or entity_urn
            via = (
                entity_urn
                if source_details.origin and source_details.origin != entity_urn
                else None
            )
            propagation_depth = (
                source_details.propagation_depth + 1
                if source_details.propagation_depth
                else 1
            )
            actor = source_details.actor
            propagation_started_at = source_details.propagation_started_at

        else:
            origin = entity_urn
            via = None
            propagation_depth = 1
            actor = self.actor_urn
            propagation_started_at = int(time.time() * 1000.0)

        return TermPropagationDirective(
            propagate=True,
            terms=terms,
            operation="ADD",
            entity=entity_urn,
            via=via,
            origin=origin,
            propagation_depth=propagation_depth,
            actor=actor,
            propagation_started_at=propagation_started_at,
        )

    def process_ece(self, event: EventEnvelope) -> Optional[TermPropagationDirective]:
        """
        Process metadata change events.
        Return a term propagation directive or None if no propagation is desired.
        """
        logger.info("Processing TERM MCE")

        if not (event.event_type == "EntityChangeEvent_v1" and self.config.enabled):
            return None

        assert isinstance(event.event, EntityChangeEvent)
        assert self.graph is not None
        assert isinstance(self.config, TermPropagatorConfig)

        semantic_event = event.event
        if not (
            semantic_event.category == "GLOSSARY_TERM"
            and semantic_event.operation in ["ADD", "REMOVE"]
        ):
            return None

        assert semantic_event.modifier

        # Call all_terms once and store it to prevent non-deterministic behavior.
        terms_to_check = self.all_terms

        for target_term in terms_to_check or [semantic_event.modifier]:
            # Check if term is directly applied or indirectly associated
            if semantic_event.modifier == target_term or self.graph.check_relationship(
                target_term,
                semantic_event.modifier,
                "IsA",
            ):
                parameters = (
                    semantic_event.parameters
                    if semantic_event.parameters
                    else semantic_event._inner_dict.get("__parameters_json", {})
                )
                context_str = parameters.get("context", "{}")
                context = json.loads(context_str) if context_str else {}

                # Determine origin and via - correct propagation logic
                context_origin = context.get("origin")
                logger.info(f"[MCL PROCESSOR] Origin from context: {context_origin}")

                if context_origin:
                    # This is a propagated event, keep original origin and set via to current entity
                    origin = context_origin
                    via = semantic_event.entityUrn
                else:
                    # This is the starting point of propagation
                    origin = semantic_event.entityUrn
                    via = None

                logger.info(f"[MCL PROCESSOR] Using origin: {origin}")
                logger.info(f"[MCL PROCESSOR] Entity Urn: {semantic_event.entityUrn}")
                logger.info(f"[MCL PROCESSOR] VIA: {via}")

                logger.info(f"[MCL PROCESSOR] VIA: {via}")

                propagation_depth = int(context.get("propagation_depth", 0)) + 1
                propagation_started_at = context.get(
                    "propagation_started_at",
                    int(time.time() * 1000.0),
                )

                logger.info(
                    f"[MCL PROCESSOR] Creating directive for term {semantic_event.modifier} and parameter {parameters} with origin {origin} and via {via}, propagation depth {propagation_depth} and started at {propagation_started_at}"
                )

                # Create directive - match docs propagator logic exactly
                return TermPropagationDirective(
                    propagate=True,
                    terms=[semantic_event.modifier],
                    operation=semantic_event.operation,
                    entity=semantic_event.entityUrn,
                    origin=origin,
                    via=via,
                    actor=(
                        semantic_event.auditStamp.actor
                        if semantic_event.auditStamp
                        else self.actor_urn
                    ),
                    propagation_started_at=propagation_started_at,
                    propagation_depth=propagation_depth,
                )

        return None

    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> PropagationOutput:
        """Create property change proposals based on the directive."""

        assert isinstance(propagation_directive, TermPropagationDirective)
        logger.info(
            f"Creating term propagation proposal for {propagation_directive} for entity {entity_urn} with context {context}"
        )

        if entity_urn.entity_type == "schemaField":
            schema_field_urn: SchemaFieldUrn = SchemaFieldUrn.from_string(
                entity_urn.urn()
            )
            yield from self._add_term_to_dataset_field_patch(
                Urn.from_string(schema_field_urn.parent),
                {schema_field_urn.field_path: propagation_directive.terms},
                propagation_directive,
                context,
            )
        elif entity_urn.entity_type == "dataset":
            yield from self._add_term_to_dataset(
                entity_urn,
                propagation_directive,
                context,
            )
        elif entity_urn.entity_type == "chart":
            if entity_urn.entity_type == "schemaField":
                schema_field_urn = SchemaFieldUrn.from_string(entity_urn.urn())
                yield from self._add_term_to_chart_field_slow(
                    Urn.from_string(schema_field_urn.parent),
                    {schema_field_urn.field_path: propagation_directive.terms},
                    propagation_directive,
                    context,
                )

    def _get_target_terms_expanded(self) -> Set[str]:
        """Expand target terms to include related terms."""

        if not self.config.target_terms:
            return set()

        expanded_terms = set()
        for term in self.config.target_terms:
            expanded_terms.add(term)

            inherited_terms = self.graph.graph.get_related_entities(
                term, ["IsA"], DataHubGraph.RelationshipDirection.INCOMING
            )

            if inherited_terms:
                for inherited_term in inherited_terms:
                    expanded_terms.add(inherited_term.urn)

        return expanded_terms

    def _get_term_groups_expanded_to_terms(self) -> Set[str]:
        """
        Get all terms in the term groups.
        Will recurse down term group hierarchy to get all terms.
        """

        def __get_terms_in_group(group: str) -> Set[str]:
            terms = self.graph.get_relationships(
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

        if not self.config.term_groups:
            return set()

        expanded_terms = set()
        for term_group in self.config.term_groups:
            group_terms = __get_terms_in_group(term_group)
            expanded_terms.update(group_terms)

        return expanded_terms

    @property
    def all_terms(self) -> List[str]:
        return self._get_all_terms()

    @cachetools.cachedmethod(lambda self: self.cache)
    def _get_all_terms(self) -> List[str]:
        """Get all terms based on configuration."""

        all_terms: Set[str] = set()

        if self.config.target_terms:
            target_terms_expanded = self._get_target_terms_expanded()
            all_terms.update(target_terms_expanded)

        if self.config.term_groups:
            term_groups_expanded = self._get_term_groups_expanded_to_terms()
            all_terms.update(term_groups_expanded)

        return list(all_terms)

    def _create_attribution(
        self, source_details: SourceDetails
    ) -> MetadataAttributionClass:
        """Create a metadata attribution object."""

        source_details_dict = source_details.for_metadata_attribution()
        return MetadataAttributionClass(
            source=self.action_urn,
            time=int(time.time() * 1000.0),
            actor=self.actor_urn
            if source_details.actor is None
            else source_details.actor,
            sourceDetail=source_details_dict,
        )

    def _create_audit_stamp(self, source_details: SourceDetails) -> AuditStampClass:
        """Create an audit stamp object."""

        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor=source_details.actor if source_details.actor else self.actor_urn,
        )

    def _add_term_to_dataset(
        self,
        dataset: Urn,
        term_propagation_directive: TermPropagationDirective,
        source_details: SourceDetails,
    ) -> PropagationOutput:
        """Add terms to a dataset."""

        logger.info(
            f"Will {term_propagation_directive.operation} term {term_propagation_directive.terms} to {dataset}"
        )
        attribution = self._create_attribution(source_details)
        patch_builder = DatasetPatchBuilder(urn=dataset.urn())

        for term in term_propagation_directive.terms:
            patch_builder.add_term(
                GlossaryTermAssociationClass(
                    term,
                    attribution=attribution,
                    context=json.dumps(source_details.for_metadata_attribution()),
                )
            )

        for mcp in patch_builder.build():
            yield mcp

    def _add_term_to_dataset_field_patch(
        self,
        dataset_urn: Urn,
        field_terms: Dict[str, List[str]],
        term_propagation_directive: TermPropagationDirective,
        source_details: SourceDetails,
    ) -> PropagationOutput:
        """Add terms to dataset fields using patch-based approach to avoid race conditions."""

        if not dataset_urn.entity_type == "dataset":
            logger.error(
                f"Invalid dataset urn {dataset_urn}. Must start with urn:li:dataset"
            )
            return

        attribution = self._create_attribution(source_details)
        source_details_dict = source_details.for_metadata_attribution()

        # Use DatasetPatchBuilder for atomic field-level updates
        patch_builder = DatasetPatchBuilder(urn=dataset_urn.urn())

        for field_path, terms in field_terms.items():
            field_builder = patch_builder.for_field(field_path=field_path)
            # Explicitly add the fieldPath property for each field
            patch_builder._add_patch(
                field_builder.aspect_name,
                "add",
                path=(field_builder.aspect_field, field_path, "fieldPath"),
                value=field_path,
            )
            for term in terms:
                if term_propagation_directive.operation == "ADD":
                    field_builder.add_term(
                        GlossaryTermAssociationClass(
                            term,
                            context=json.dumps(source_details_dict),
                            attribution=attribution,
                        )
                    )
                elif term_propagation_directive.operation == "REMOVE":
                    field_builder.remove_term(term)

        yield from patch_builder.build()

    def _add_term_to_chart_field_slow(
        self,
        chart_urn: Urn,
        field_terms: Dict[str, List[str]],
        term_propagation_directive: TermPropagationDirective,
        source_details: SourceDetails,
    ) -> PropagationOutput:
        """Add terms to chart fields."""

        if not chart_urn.entity_type == "chart":
            logger.error(f"Invalid chart urn {chart_urn}. Must start with urn:li:chart")
            return

        audit_stamp = self._create_audit_stamp(source_details)
        attribution = self._create_attribution(source_details)
        source_details_dict = source_details.for_metadata_attribution()

        input_schema_fields = self.graph.graph.get_aspect(
            chart_urn.urn(), InputFieldsClass
        )

        if input_schema_fields is None or not input_schema_fields.fields:
            logger.error(f"Chart {chart_urn} does not have input schema fields")
            return

        mutation_needed = False
        operation = term_propagation_directive.operation

        # Process each field and its terms
        for field_path, terms in field_terms.items():
            if field_path not in [
                x.schemaField.fieldPath for x in input_schema_fields.fields
            ]:
                logger.warning(f"Field {field_path} not found in chart {chart_urn}")
                continue

            for field_info in input_schema_fields.fields:
                if field_info.schemaField.fieldPath == field_path:
                    # Handle case when no glossary terms exist
                    if not field_info.schemaField.glossaryTerms:
                        if operation == "ADD":
                            field_info.schemaField.glossaryTerms = GlossaryTermsClass(
                                terms=[
                                    GlossaryTermAssociationClass(
                                        term,
                                        context=json.dumps(source_details_dict),
                                        attribution=attribution,
                                    )
                                    for term in terms
                                ],
                                auditStamp=audit_stamp,
                            )
                            mutation_needed = True
                    else:
                        # Process existing terms
                        existing_terms = [
                            x.urn for x in field_info.schemaField.glossaryTerms.terms
                        ]

                        for term in terms:
                            if term not in existing_terms:
                                if operation == "ADD":
                                    field_info.schemaField.glossaryTerms.terms.append(
                                        GlossaryTermAssociationClass(
                                            term,
                                            context=json.dumps(source_details_dict),
                                            attribution=attribution,
                                        )
                                    )
                                    field_info.schemaField.glossaryTerms.auditStamp = (
                                        audit_stamp
                                    )
                                    mutation_needed = True
                            else:
                                if operation == "REMOVE":
                                    field_info.schemaField.glossaryTerms.terms = [
                                        x
                                        for x in field_info.schemaField.glossaryTerms.terms
                                        if x.urn != term
                                    ]
                                    field_info.schemaField.glossaryTerms.auditStamp = (
                                        audit_stamp
                                    )
                                    mutation_needed = True

        if mutation_needed:
            assert input_schema_fields.validate()
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn.urn(),
                aspect=input_schema_fields,
            )

    def boostrap_asset(self, asset_urn: Urn) -> Iterable[TermPropagationDirective]:
        """Bootstrap terms for an asset."""

        terms = self.all_terms
        logger.info(
            f"Bootstrapping terms for {asset_urn} with config {self.config} and entity types {self.config.propagation_rule.entity_types}"
        )

        if (
            asset_urn.entity_type == "dataset"
            and "schemaField" in self.config.propagation_rule.entity_types
        ):
            yield from self._bootstrap_dataset_fields(asset_urn, terms)

        if (
            asset_urn.entity_type == "dataset"
            and "dataset" in self.config.propagation_rule.entity_types
        ):
            yield from self._bootstrap_dataset(asset_urn, terms)
        else:
            logger.error(f"Unsupported target entity type {asset_urn.entity_type}")

    def _bootstrap_dataset_fields(
        self, asset_urn: Urn, terms: List[str]
    ) -> Iterable[TermPropagationDirective]:
        """Bootstrap terms for dataset fields."""

        field_terms_map: Dict[str, List[GlossaryTermAssociationClass]] = {}

        # Get terms from schema metadata
        schema_metadata = self.graph.graph.get_aspect(
            asset_urn.urn(), SchemaMetadataClass
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

        # Get terms from editable schema metadata
        editable_schema_metadata = self.graph.graph.get_aspect(
            asset_urn.urn(), EditableSchemaMetadataClass
        )
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

        # Create directives for each field and term
        for field_path, field_terms in field_terms_map.items():
            field_urn = SchemaFieldUrn(asset_urn, field_path)
            for term in field_terms:
                logger.info(
                    f"Bootstrapping terms for schema field {term} in {asset_urn}"
                )
                source_details = SourceDetails(
                    actor=term.actor,
                    origin=field_urn.urn(),
                    propagation_depth=1,
                    propagation_started_at=int(time.time() * 1000.0),
                )

                directive = self.generate_directive(
                    field_urn.urn(), [term.urn], source_details
                )
                if directive:
                    yield directive

    def _bootstrap_dataset(
        self, asset_urn: Urn, terms: List[str]
    ) -> Iterable[TermPropagationDirective]:
        """Bootstrap terms for dataset."""

        glossary_terms = self.graph.graph.get_aspect(
            asset_urn.urn(), GlossaryTermsClass
        )
        if not glossary_terms:
            return

        asset_terms = [
            term.urn for term in glossary_terms.terms if term.urn in terms or not terms
        ]

        source_details = SourceDetails(
            actor=self.actor_urn,
            propagation_started_at=int(time.time() * 1000.0),
        )
        directive = self.generate_directive(
            asset_urn.urn(), asset_terms, source_details
        )
        if directive:
            yield directive

    def asset_filters(self) -> Dict[str, Dict[str, List[SearchFilterRule]]]:
        """Get filters for asset types."""

        asset_filters: Dict[str, Dict[str, List[SearchFilterRule]]] = {}

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

        for entity_type in self.config.propagation_rule.entity_types:
            index_field_map = entity_index_field_map.get(entity_type, {})
            asset_filters[entity_type] = {}

            for index, index_fields in index_field_map.items():
                or_filters: List[SearchFilterRule] = []

                for index_field in index_fields:
                    or_filters.extend(
                        [
                            SearchFilterRule(
                                field=index_field,
                                values=[term],
                                condition="IN",
                            )
                            for term in self.all_terms
                        ]
                    )

                asset_filters[entity_type][index] = or_filters

        return asset_filters

    def identify_assets(self) -> Iterable[Urn]:
        """Identify assets for processing."""

        urns: Iterable[str] = self.graph.graph.get_urns_by_filter(
            entity_types=["dataset"],
            query="*",
            extra_or_filters=[
                {
                    "field": "termAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
                {
                    "field": "fieldTermAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
                {
                    "field": "editedFieldTermAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
            ],
        )

        for urn in urns:
            yield Urn.from_string(urn)

    def rollbackable_assets(self) -> Iterable[Urn]:
        """Get assets that can be rolled back."""

        return self.identify_assets()

    def rollback_asset(self, asset: Urn) -> None:
        """Rollback term associations from an asset."""

        # Rollback dataset-level term associations
        self._rollback_dataset_terms(asset)

        # Rollback field-level term associations in editable schema metadata
        self._rollback_editable_schema_terms(asset)

        # Rollback field-level term associations in schema metadata
        self._rollback_schema_terms(asset)

    def _rollback_dataset_terms(self, asset: Urn) -> None:
        """Rollback term associations from a dataset."""

        glossary_terms = self.graph.graph.get_aspect(asset.urn(), GlossaryTermsClass)
        aspect_updated = False
        if glossary_terms:
            terms = []
            for glossary_term in glossary_terms.terms:
                assert isinstance(glossary_term, GlossaryTermAssociationClass)
                if (
                    glossary_term.attribution
                    and glossary_term.attribution.source == self.action_urn
                ):
                    logger.info(f"Deleting term association {glossary_term.urn}")
                    aspect_updated = True
                else:
                    terms.append(glossary_term)

            if aspect_updated:
                self.graph.graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=asset.urn(),
                        aspect=glossary_terms,
                    )
                )

    def _rollback_editable_schema_terms(self, asset: Urn) -> None:
        """Rollback term associations from editable schema metadata."""

        logger.info("Checking if there is any field term to rollback")
        editable_schema_metadata = self.graph.graph.get_aspect(
            asset.urn(), EditableSchemaMetadataClass
        )
        logger.info(f"metadata: {editable_schema_metadata}")

        if not editable_schema_metadata:
            return

        editableSchemaFiledInfo = []
        aspect_updated = False

        for field in editable_schema_metadata.editableSchemaFieldInfo:
            if field.glossaryTerms:
                terms = []
                for term in field.glossaryTerms.terms:
                    if term.attribution and term.attribution.source == self.action_urn:
                        logger.info(f"Deleting term association {term.urn}")
                        aspect_updated = True
                    else:
                        terms.append(term)

                field.glossaryTerms.terms = terms

            editableSchemaFiledInfo.append(field)

        editable_schema_metadata.editableSchemaFieldInfo = editableSchemaFiledInfo

        if aspect_updated:
            self.graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=asset.urn(),
                    aspect=editable_schema_metadata,
                )
            )

    def _rollback_schema_terms(self, asset: Urn) -> None:
        """Rollback term associations from schema metadata."""

        logger.info("Checking if there is any schema field term to rollback")
        schema_metadata = self.graph.graph.get_aspect(asset.urn(), SchemaMetadataClass)
        logger.info(f"Schema metadata info: {schema_metadata}")

        if not schema_metadata:
            return

        schemaFiledInfo = []
        aspect_updated = False

        for field in schema_metadata.fields:
            if field.glossaryTerms:
                terms = []
                for term in field.glossaryTerms.terms:
                    if term.attribution and term.attribution.source == self.action_urn:
                        logger.info(f"Deleting term association {term.urn}")
                        aspect_updated = True
                    else:
                        terms.append(term)

                field.glossaryTerms.terms = terms

            schemaFiledInfo.append(field)

        schema_metadata.fields = schemaFiledInfo

        if aspect_updated:
            self.graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=asset.urn(),
                    aspect=schema_metadata,
                )
            )

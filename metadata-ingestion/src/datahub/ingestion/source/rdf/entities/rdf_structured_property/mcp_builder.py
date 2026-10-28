"""MCP builder for RDF extension structured properties."""

import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubStructuredPropertyAssignment,
    DataHubStructuredPropertyDefinition,
)
from datahub.ingestion.source.rdf.ontology.predicate_utils import predicate_display_name
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
)

logger = logging.getLogger(__name__)

GLOSSARY_TERM_ENTITY_TYPE = "urn:li:entityType:datahub.glossaryTerm"
URN_VALUE_TYPE = "urn:li:dataType:datahub.urn"


def collect_structured_property_definitions(
    datahub_graph: DataHubGraph,
) -> List[DataHubStructuredPropertyDefinition]:
    """Return SP definitions, synthesizing any missing from assignments."""
    definitions_by_qn: Dict[str, DataHubStructuredPropertyDefinition] = {
        defn.qualified_name: defn
        for defn in getattr(datahub_graph, "structured_property_definitions", [])
    }
    for assignment in getattr(datahub_graph, "structured_property_assignments", []):
        if assignment.qualified_name in definitions_by_qn:
            continue
        definitions_by_qn[assignment.qualified_name] = (
            DataHubStructuredPropertyDefinition(
                qualified_name=assignment.qualified_name,
                predicate_iri=assignment.predicate_iri,
                display_name=predicate_display_name(assignment.predicate_iri),
            )
        )
    return list(definitions_by_qn.values())


class RdfStructuredPropertyMCPBuilder(
    EntityMCPBuilder[DataHubStructuredPropertyDefinition]
):
    """Creates structured property definitions and value assignments for RDF extensions."""

    @property
    def entity_type(self) -> str:
        return "rdf_structured_property"

    def build_mcps(
        self,
        entity: DataHubStructuredPropertyDefinition,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        return [self._definition_mcp(entity)]

    def build_all_mcps(
        self,
        entities: List[DataHubStructuredPropertyDefinition],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        return [self._definition_mcp(defn) for defn in entities]

    def build_post_processing_mcps(
        self,
        datahub_graph: DataHubGraph,
        context: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        assignments = getattr(datahub_graph, "structured_property_assignments", [])
        if not assignments:
            return []

        grouped: Dict[str, List[DataHubStructuredPropertyAssignment]] = defaultdict(
            list
        )
        for assignment in assignments:
            grouped[assignment.subject_urn].append(assignment)

        mcps: List[MetadataChangeProposalWrapper] = []
        for subject_urn, subject_assignments in grouped.items():
            by_property: Dict[str, List[DataHubStructuredPropertyAssignment]] = (
                defaultdict(list)
            )
            for assignment in subject_assignments:
                by_property[assignment.qualified_name].append(assignment)

            properties = []
            for qualified_name, prop_assignments in by_property.items():
                property_urn = f"urn:li:structuredProperty:{qualified_name}"
                values = sorted({a.object_urn for a in prop_assignments})
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=property_urn,
                        values=values,
                    )
                )

            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=subject_urn,
                    aspect=StructuredPropertiesClass(properties=properties),
                )
            )

            provenance = context.get("relationship_provenance") if context else None
            if provenance is not None:
                for assignment in subject_assignments:
                    provenance[
                        (assignment.subject_urn, assignment.object_urn, "extension")
                    ] = {
                        "originalPredicateIri": assignment.predicate_iri,
                        "mappingClass": assignment.mapping_class.value,
                        "mappingTarget": assignment.qualified_name,
                    }

        logger.info("Built %d structured property assignment MCPs", len(mcps))
        return mcps

    @classmethod
    def register_definitions_sync(
        cls,
        graph: Any,
        definitions: List[DataHubStructuredPropertyDefinition],
    ) -> int:
        """
        Register structured property definitions synchronously via the graph client.

        GMS validates that property definitions exist before accepting value
        assignments. With ASYNC_BATCH sink mode, definition work units emitted
        early in the pipeline can still be in-flight when assignments arrive.
        """
        from datahub.emitter.rest_emitter import EmitMode

        created = 0
        for definition in definitions:
            mcp = cls._definition_mcp(definition)
            try:
                existing = graph.get_aspect(
                    mcp.entityUrn, StructuredPropertyDefinitionClass
                )
                if existing is not None:
                    continue
            except Exception as exc:
                logger.debug(
                    "Could not check existing structured property %s: %s",
                    mcp.entityUrn,
                    exc,
                )
            graph.emit_mcp(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
            created += 1
        if created:
            logger.info(
                "Synchronously registered %d RDF extension structured property definitions",
                created,
            )
        return created

    @staticmethod
    def _definition_mcp(
        definition: DataHubStructuredPropertyDefinition,
    ) -> MetadataChangeProposalWrapper:
        property_urn = f"urn:li:structuredProperty:{definition.qualified_name}"
        return MetadataChangeProposalWrapper(
            entityUrn=property_urn,
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName=definition.qualified_name,
                displayName=definition.display_name,
                valueType=URN_VALUE_TYPE,
                entityTypes=[GLOSSARY_TERM_ENTITY_TYPE],
                typeQualifier={
                    "allowedTypes": [GLOSSARY_TERM_ENTITY_TYPE],
                },
                cardinality="MULTIPLE",
                description=f"RDF predicate {definition.predicate_iri}",
            ),
        )

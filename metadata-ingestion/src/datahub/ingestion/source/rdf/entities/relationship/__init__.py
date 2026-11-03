"""
Relationship Entity Module

Ontology-gated relationship routing for RDF ingestion:
- Harvest all URI-object property triples
- Route via DataHub TBox alignments to native @Relationship fields or extension SPs
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.glossary_term import (
    ENTITY_TYPE as GLOSSARY_TERM_ENTITY_TYPE,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubNativeRelationship,
    DataHubRelationship,
    DataHubStructuredPropertyAssignment,
    DataHubStructuredPropertyDefinition,
    MappingClass,
    RDFRelationship,
    RDFStatement,
    RelationshipType,
)
from datahub.ingestion.source.rdf.entities.relationship.extractor import (
    RelationshipExtractor,
)
from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
    RelationshipMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.relationship.router import RelationshipRouter
from datahub.ingestion.source.rdf.entities.relationship.triple_harvester import (
    TripleHarvester,
)

ENTITY_TYPE = "relationship"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["relationship", "relationships"],
    rdf_ast_class=RDFStatement,
    datahub_ast_class=DataHubNativeRelationship,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[GLOSSARY_TERM_ENTITY_TYPE],
    processing_order=110,
)

__all__ = [
    "ENTITY_TYPE",
    "RelationshipExtractor",
    "RelationshipMCPBuilder",
    "RelationshipRouter",
    "TripleHarvester",
    "RDFRelationship",
    "RDFStatement",
    "DataHubRelationship",
    "DataHubNativeRelationship",
    "DataHubStructuredPropertyAssignment",
    "DataHubStructuredPropertyDefinition",
    "MappingClass",
    "RelationshipType",
    "ENTITY_METADATA",
]

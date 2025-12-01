"""
Relationship Entity Module

Self-contained processing for glossary term relationships:
- Extraction from RDF graphs (skos:broader, skos:narrower only)
- Conversion to DataHub AST
- MCP creation for DataHub ingestion (isRelatedTerms only)

Note: Only broader/narrower relationships are supported.
skos:related, skos:exactMatch, skos:closeMatch are NOT extracted.
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RDFRelationship,
    RelationshipType,
)
from datahub.ingestion.source.rdf.entities.relationship.converter import (
    RelationshipConverter,
)
from datahub.ingestion.source.rdf.entities.relationship.extractor import (
    RelationshipExtractor,
)
from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
    RelationshipMCPBuilder,
)

ENTITY_METADATA = EntityMetadata(
    entity_type="relationship",
    cli_names=["relationship", "relationships"],
    rdf_ast_class=RDFRelationship,
    datahub_ast_class=DataHubRelationship,
    export_targets=["pretty_print", "file", "datahub"],
    processing_order=3,  # After glossary terms (relationships reference terms)
)

__all__ = [
    "RelationshipExtractor",
    "RelationshipConverter",
    "RelationshipMCPBuilder",
    "RDFRelationship",
    "DataHubRelationship",
    "RelationshipType",
    "ENTITY_METADATA",
]

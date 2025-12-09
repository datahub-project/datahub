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
from datahub.ingestion.source.rdf.entities.glossary_term import (
    ENTITY_TYPE as GLOSSARY_TERM_ENTITY_TYPE,
)
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

# Entity type constant - part of the module contract
ENTITY_TYPE = "relationship"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["relationship", "relationships"],
    rdf_ast_class=RDFRelationship,
    datahub_ast_class=DataHubRelationship,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[
        GLOSSARY_TERM_ENTITY_TYPE
    ],  # Depends on glossary terms (relationships reference terms)
)

__all__ = [
    "ENTITY_TYPE",
    "RelationshipExtractor",
    "RelationshipConverter",
    "RelationshipMCPBuilder",
    "RDFRelationship",
    "DataHubRelationship",
    "RelationshipType",
    "ENTITY_METADATA",
]

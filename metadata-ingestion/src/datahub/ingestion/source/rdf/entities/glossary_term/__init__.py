"""
Glossary Term Entity Module

Self-contained processing for glossary terms:
- Extraction from RDF graphs (SKOS Concepts, OWL Classes, etc.) directly to DataHub AST
- MCP creation for DataHub ingestion

Supports:
- skos:Concept, owl:Class, owl:NamedIndividual
- Custom properties including FIBO-specific metadata

Note: Relationships (skos:broader/narrower) are extracted independently by the relationship entity.
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.domain import (
    ENTITY_TYPE as DOMAIN_ENTITY_TYPE,
)
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.extractor import (
    GlossaryTermExtractor,
)
from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
    GlossaryTermMCPBuilder,
)

# Entity type constant - part of the module contract
ENTITY_TYPE = "glossary_term"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["glossary", "glossary_terms"],
    rdf_ast_class=None,  # No RDF AST - extractor returns DataHub AST directly
    datahub_ast_class=DataHubGlossaryTerm,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[
        DOMAIN_ENTITY_TYPE,
    ],  # Depends on domain - ensures domains are processed before glossary terms
)

__all__ = [
    "ENTITY_TYPE",
    "GlossaryTermExtractor",
    "GlossaryTermMCPBuilder",
    "DataHubGlossaryTerm",
    "ENTITY_METADATA",
]

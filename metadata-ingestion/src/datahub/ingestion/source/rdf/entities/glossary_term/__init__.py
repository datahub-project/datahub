"""
Glossary Term Entity Module

Self-contained processing for glossary terms:
- Extraction from RDF graphs (SKOS Concepts, OWL Classes, etc.)
- Conversion to DataHub AST
- MCP creation for DataHub ingestion

Supports:
- skos:Concept, owl:Class, owl:NamedIndividual
- skos:broader/narrower relationships (only these are supported)
- Custom properties including FIBO-specific metadata
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
    RDFGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.converter import (
    GlossaryTermConverter,
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
    rdf_ast_class=RDFGlossaryTerm,
    datahub_ast_class=DataHubGlossaryTerm,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[],  # No dependencies - glossary terms are independent entities
)

__all__ = [
    "ENTITY_TYPE",
    "GlossaryTermExtractor",
    "GlossaryTermConverter",
    "GlossaryTermMCPBuilder",
    "RDFGlossaryTerm",
    "DataHubGlossaryTerm",
    "ENTITY_METADATA",
]

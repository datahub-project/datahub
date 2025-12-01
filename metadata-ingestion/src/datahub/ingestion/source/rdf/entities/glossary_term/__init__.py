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

ENTITY_METADATA = EntityMetadata(
    entity_type="glossary_term",
    cli_names=["glossary", "glossary_terms"],
    rdf_ast_class=RDFGlossaryTerm,
    datahub_ast_class=DataHubGlossaryTerm,
    export_targets=["pretty_print", "file", "datahub"],
    processing_order=2,  # After structured properties, before relationships
)

__all__ = [
    "GlossaryTermExtractor",
    "GlossaryTermConverter",
    "GlossaryTermMCPBuilder",
    "RDFGlossaryTerm",
    "DataHubGlossaryTerm",
    "ENTITY_METADATA",
]

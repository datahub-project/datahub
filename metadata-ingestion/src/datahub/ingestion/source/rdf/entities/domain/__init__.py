"""
Domain Entity Module

Handles DataHub domain hierarchy derived from IRI paths.
Domains are not extracted from RDF graphs - they are constructed
from the path segments of glossary terms.

Creates domains that have glossary terms in their hierarchy.
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.domain.mcp_builder import DomainMCPBuilder

# Entity type constant - part of the module contract
ENTITY_TYPE = "domain"

# Register domain as an entity type
# Domains are built from glossary terms in facade.py before MCP creation
# They don't have extractor/converter, but they do have an MCP builder
ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["domain", "domains"],
    rdf_ast_class=None,  # Domains are not extracted from RDF
    datahub_ast_class=DataHubDomain,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[],  # No dependencies - domains are created dynamically by glossary terms
)

__all__ = [
    "ENTITY_TYPE",
    "DomainBuilder",
    "DomainMCPBuilder",
    "DataHubDomain",
    "ENTITY_METADATA",
]

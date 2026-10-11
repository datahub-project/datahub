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

# Entity type constant - part of the module contract
ENTITY_TYPE = "domain"

# Register domain as an entity type
# Domains are built from glossary terms in facade.py before MCP creation
# They are used ONLY as a data structure to organize glossary terms into hierarchy
# Domains are NOT ingested as DataHub domain entities - the glossary module
# uses them to create glossary nodes (term groups) and terms
ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=[],  # Not exposed as CLI option - domains are data structure only, not ingested
    rdf_ast_class=None,  # Domains are not extracted from RDF
    datahub_ast_class=DataHubDomain,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[],  # No dependencies - domains are created dynamically by glossary terms
)

__all__ = [
    "ENTITY_TYPE",
    "DomainBuilder",
    "DataHubDomain",
    "ENTITY_METADATA",
]

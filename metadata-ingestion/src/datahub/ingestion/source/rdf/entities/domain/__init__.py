"""
Domain Entity Module

Handles DataHub domain hierarchy derived from IRI paths.
Domains are not extracted from RDF graphs - they are constructed
from the path segments of glossary terms and datasets.

Only creates domains that have datasets in their hierarchy.
Domains with only glossary terms are NOT created.
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.dataset import (
    ENTITY_TYPE as DATASET_ENTITY_TYPE,
)
from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.domain.mcp_builder import DomainMCPBuilder
from datahub.ingestion.source.rdf.entities.glossary_term import (
    ENTITY_TYPE as GLOSSARY_TERM_ENTITY_TYPE,
)

# Entity type constant - part of the module contract
ENTITY_TYPE = "domain"

# Register domain as an entity type with processing_order=2
# Domains are built (not extracted), so they don't have extractor/converter
# but they do have an MCP builder and should be processed after structured properties
ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["domain", "domains"],
    rdf_ast_class=None,  # Domains are not extracted from RDF
    datahub_ast_class=DataHubDomain,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[
        DATASET_ENTITY_TYPE,
        GLOSSARY_TERM_ENTITY_TYPE,
    ],  # Domains are built from datasets and glossary terms
)

__all__ = [
    "ENTITY_TYPE",
    "DomainBuilder",
    "DomainMCPBuilder",
    "DataHubDomain",
    "ENTITY_METADATA",
]

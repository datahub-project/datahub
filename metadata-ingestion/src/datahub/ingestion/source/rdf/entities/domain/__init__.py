"""
Domain Entity Module

Handles DataHub domain hierarchy derived from IRI paths.
Domains are not extracted from RDF graphs - they are constructed
from the path segments of glossary terms and datasets.

Only creates domains that have datasets in their hierarchy.
Domains with only glossary terms are NOT created.
"""

from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.domain.mcp_builder import DomainMCPBuilder

__all__ = ["DomainBuilder", "DomainMCPBuilder", "DataHubDomain"]

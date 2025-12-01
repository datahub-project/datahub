"""
Core DataHub RDF Package

This package contains the core functionality for:
- DataHub client operations
- Transpiler architecture for RDF to DataHub conversion
- Dependency injection factories for modular architecture
- Domain utilities
"""

from datahub.ingestion.source.rdf.core.datahub_client import DataHubClient
from datahub.ingestion.source.rdf.core.orchestrator import Orchestrator
from datahub.ingestion.source.rdf.core.query_factory import (
    CustomQuery,
    FilterQuery,
    PassThroughQuery,
    QueryFactory,
    QueryInterface,
    SPARQLQuery,
)

# Dependency Injection Factories
from datahub.ingestion.source.rdf.core.source_factory import (
    FileSource,
    FolderSource,
    MultiFileSource,
    ServerSource,
    SourceFactory,
    SourceInterface,
)
from datahub.ingestion.source.rdf.core.target_factory import (
    DataHubTarget,
    FileTarget,
    PrettyPrintTarget,
    TargetFactory,
    TargetInterface,
)
from datahub.ingestion.source.rdf.core.transpiler import RDFToDataHubTranspiler
from datahub.ingestion.source.rdf.core.urn_generator import (
    UrnGeneratorBase,
    extract_name_from_label,
)

__all__ = [
    "DataHubClient",
    "RDFToDataHubTranspiler",
    "UrnGeneratorBase",
    "extract_name_from_label",
    # Dependency Injection Factories
    "SourceFactory",
    "SourceInterface",
    "FileSource",
    "FolderSource",
    "ServerSource",
    "MultiFileSource",
    "QueryFactory",
    "QueryInterface",
    "SPARQLQuery",
    "PassThroughQuery",
    "FilterQuery",
    "CustomQuery",
    "TargetFactory",
    "TargetInterface",
    "DataHubTarget",
    "PrettyPrintTarget",
    "FileTarget",
    "Orchestrator",
]

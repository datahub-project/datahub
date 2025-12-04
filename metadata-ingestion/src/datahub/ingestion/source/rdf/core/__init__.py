"""
Core DataHub RDF Package

This package contains the core functionality for:
- DataHub client operations
- Transpiler architecture for RDF to DataHub conversion
- Dependency injection factories for modular architecture
- Domain utilities
"""

# DataHubClient removed - CLI-only, not used by ingestion source
from datahub.ingestion.source.rdf.core.orchestrator import Orchestrator

# Dependency Injection Factories
from datahub.ingestion.source.rdf.core.source_factory import (
    FileSource,
    FolderSource,
    MultiFileSource,
    ServerSource,
    SourceFactory,
    SourceInterface,
)
from datahub.ingestion.source.rdf.core.target_factory import TargetInterface
from datahub.ingestion.source.rdf.core.transpiler import RDFToDataHubTranspiler
from datahub.ingestion.source.rdf.core.urn_generator import (
    UrnGeneratorBase,
    extract_name_from_label,
)

__all__ = [
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
    "TargetInterface",
    "Orchestrator",
]

"""
Core DataHub RDF Package

This package contains the core functionality for:
- RDF graph loading
- URN generation utilities
"""

from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
from datahub.ingestion.source.rdf.core.urn_generator import (
    UrnGeneratorBase,
    extract_name_from_label,
)

__all__ = [
    "load_rdf_graph",
    "UrnGeneratorBase",
    "extract_name_from_label",
]

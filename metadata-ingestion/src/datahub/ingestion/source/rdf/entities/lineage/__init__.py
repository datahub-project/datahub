"""
Lineage Entity Module

Self-contained processing for dataset lineage:
- Extraction from RDF graphs (PROV-O patterns)
- Conversion to DataHub AST
- MCP creation for DataHub ingestion

Supports:
- prov:wasDerivedFrom - direct derivation
- prov:used / prov:wasGeneratedBy - activity-based lineage
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.lineage.ast import (
    DataHubLineageActivity,
    DataHubLineageRelationship,
    LineageType,
    RDFLineageActivity,
    RDFLineageRelationship,
)
from datahub.ingestion.source.rdf.entities.lineage.converter import LineageConverter
from datahub.ingestion.source.rdf.entities.lineage.extractor import LineageExtractor
from datahub.ingestion.source.rdf.entities.lineage.mcp_builder import LineageMCPBuilder

ENTITY_METADATA = EntityMetadata(
    entity_type="lineage",
    cli_names=["lineage"],
    rdf_ast_class=RDFLineageRelationship,
    datahub_ast_class=DataHubLineageRelationship,
    export_targets=["pretty_print", "file", "datahub"],
    processing_order=5,  # After datasets (lineage references datasets)
)

__all__ = [
    "LineageExtractor",
    "LineageConverter",
    "LineageMCPBuilder",
    "RDFLineageActivity",
    "RDFLineageRelationship",
    "DataHubLineageActivity",
    "DataHubLineageRelationship",
    "LineageType",
    "ENTITY_METADATA",
]

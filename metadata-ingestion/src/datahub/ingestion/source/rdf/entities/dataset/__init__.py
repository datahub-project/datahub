"""
Dataset Entity Module

Self-contained processing for datasets:
- Extraction from RDF graphs (void:Dataset, dcat:Dataset, schema:Dataset)
- Conversion to DataHub AST
- MCP creation for DataHub ingestion

Supports:
- Platform extraction via dcat:accessService
- Schema field extraction from SHACL shapes
- Field-to-glossary-term relationships
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.dataset.ast import (
    DataHubDataset,
    RDFDataset,
    RDFSchemaField,
)
from datahub.ingestion.source.rdf.entities.dataset.converter import DatasetConverter
from datahub.ingestion.source.rdf.entities.dataset.extractor import DatasetExtractor
from datahub.ingestion.source.rdf.entities.dataset.mcp_builder import DatasetMCPBuilder

# Entity type constant - part of the module contract
ENTITY_TYPE = "dataset"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["dataset", "datasets"],
    rdf_ast_class=RDFDataset,
    datahub_ast_class=DataHubDataset,
    export_targets=["pretty_print", "file", "datahub", "ddl"],
    dependencies=[],  # No dependencies - datasets are independent entities
)

__all__ = [
    "ENTITY_TYPE",
    "DatasetExtractor",
    "DatasetConverter",
    "DatasetMCPBuilder",
    "RDFDataset",
    "RDFSchemaField",
    "DataHubDataset",
    "ENTITY_METADATA",
]

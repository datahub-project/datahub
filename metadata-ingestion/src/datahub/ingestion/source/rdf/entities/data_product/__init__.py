"""
Data Product Entity Module

Self-contained processing for data products:
- Extraction from RDF graphs (dprod:DataProduct)
- Conversion to DataHub AST
- MCP creation for DataHub ingestion
"""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.data_product.ast import (
    DataHubDataProduct,
    RDFDataProduct,
    RDFDataProductAsset,
)
from datahub.ingestion.source.rdf.entities.data_product.converter import (
    DataProductConverter,
)
from datahub.ingestion.source.rdf.entities.data_product.extractor import (
    DataProductExtractor,
)
from datahub.ingestion.source.rdf.entities.data_product.mcp_builder import (
    DataProductMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.dataset import (
    ENTITY_TYPE as DATASET_ENTITY_TYPE,
)

# Entity type constant - part of the module contract
ENTITY_TYPE = "data_product"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["data_product", "data_products"],
    rdf_ast_class=RDFDataProduct,
    datahub_ast_class=DataHubDataProduct,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[
        DATASET_ENTITY_TYPE
    ],  # Depends on datasets (data products reference datasets)
)

__all__ = [
    "ENTITY_TYPE",
    "DataProductExtractor",
    "DataProductConverter",
    "DataProductMCPBuilder",
    "RDFDataProduct",
    "RDFDataProductAsset",
    "DataHubDataProduct",
    "ENTITY_METADATA",
]

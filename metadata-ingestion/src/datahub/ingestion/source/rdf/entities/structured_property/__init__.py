"""Structured Property Entity Module."""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
    DataHubStructuredPropertyValue,
    RDFStructuredProperty,
    RDFStructuredPropertyValue,
)
from datahub.ingestion.source.rdf.entities.structured_property.converter import (
    StructuredPropertyConverter,
)
from datahub.ingestion.source.rdf.entities.structured_property.extractor import (
    StructuredPropertyExtractor,
)
from datahub.ingestion.source.rdf.entities.structured_property.mcp_builder import (
    StructuredPropertyMCPBuilder,
)

ENTITY_METADATA = EntityMetadata(
    entity_type="structured_property",
    cli_names=["structured_property", "structured_properties", "properties"],
    rdf_ast_class=RDFStructuredProperty,
    datahub_ast_class=DataHubStructuredProperty,
    export_targets=["pretty_print", "file", "datahub"],
    processing_order=1,  # Must be processed first - definitions needed before value assignments
)

__all__ = [
    "StructuredPropertyExtractor",
    "StructuredPropertyConverter",
    "StructuredPropertyMCPBuilder",
    "RDFStructuredProperty",
    "RDFStructuredPropertyValue",
    "DataHubStructuredProperty",
    "DataHubStructuredPropertyValue",
    "ENTITY_METADATA",
]

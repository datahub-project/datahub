"""RDF structured property entity registration."""

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.rdf_structured_property.ast import (
    DataHubRdfStructuredProperty,
)
from datahub.ingestion.source.rdf.entities.rdf_structured_property.mcp_builder import (
    RdfStructuredPropertyMCPBuilder,
)

ENTITY_TYPE = "rdf_structured_property"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["rdf_structured_property", "structured_property"],
    rdf_ast_class=None,
    datahub_ast_class=DataHubRdfStructuredProperty,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[],
    processing_order=50,
)

__all__ = [
    "ENTITY_TYPE",
    "ENTITY_METADATA",
    "RdfStructuredPropertyMCPBuilder",
    "DataHubRdfStructuredProperty",
]

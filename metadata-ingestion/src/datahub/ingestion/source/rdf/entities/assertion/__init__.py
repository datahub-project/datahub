"""Assertion Entity Module."""

from datahub.ingestion.source.rdf.entities.assertion.ast import (
    CrossFieldConstraint,
    DataHubAssertion,
    DataHubCrossFieldConstraint,
    DataQualityRule,
    RDFAssertion,
)
from datahub.ingestion.source.rdf.entities.assertion.converter import AssertionConverter
from datahub.ingestion.source.rdf.entities.assertion.extractor import AssertionExtractor
from datahub.ingestion.source.rdf.entities.assertion.mcp_builder import (
    AssertionMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.base import EntityMetadata

ENTITY_METADATA = EntityMetadata(
    entity_type="assertion",
    cli_names=["assertion", "assertions"],
    rdf_ast_class=RDFAssertion,
    datahub_ast_class=DataHubAssertion,
    export_targets=["pretty_print", "file", "datahub"],
    processing_order=7,  # After datasets (assertions reference datasets/fields)
)

__all__ = [
    "AssertionExtractor",
    "AssertionConverter",
    "AssertionMCPBuilder",
    "RDFAssertion",
    "DataHubAssertion",
    "DataQualityRule",
    "CrossFieldConstraint",
    "DataHubCrossFieldConstraint",
    "ENTITY_METADATA",
]

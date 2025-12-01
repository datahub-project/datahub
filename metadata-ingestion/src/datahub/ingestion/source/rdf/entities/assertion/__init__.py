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
from datahub.ingestion.source.rdf.entities.dataset import (
    ENTITY_TYPE as DATASET_ENTITY_TYPE,
)

# Entity type constant - part of the module contract
ENTITY_TYPE = "assertion"

ENTITY_METADATA = EntityMetadata(
    entity_type=ENTITY_TYPE,
    cli_names=["assertion", "assertions"],
    rdf_ast_class=RDFAssertion,
    datahub_ast_class=DataHubAssertion,
    export_targets=["pretty_print", "file", "datahub"],
    dependencies=[
        DATASET_ENTITY_TYPE
    ],  # Depends on datasets (assertions reference datasets/fields)
)

__all__ = [
    "ENTITY_TYPE",
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

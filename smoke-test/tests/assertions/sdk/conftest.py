"""Shared fixtures and constants for SDK assertion tests."""

from typing import Any

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn


def get_test_dataset_urn(module_name: str) -> str:
    """Generate a unique dataset URN for each test module.

    This enables parallel test execution by ensuring each test file
    uses its own isolated dataset.

    Args:
        module_name: Short name like "column_metric", "freshness", etc.

    Returns:
        A unique dataset URN for that module.
    """
    return make_dataset_urn(platform="postgres", name=f"sdk_assertions_{module_name}")


# Schema fields for assertion tests - includes various types for comprehensive testing
TEST_SCHEMA_FIELDS = [
    # String columns
    SchemaFieldClass(
        fieldPath="user_id",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
    SchemaFieldClass(
        fieldPath="name",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
    SchemaFieldClass(
        fieldPath="email",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
        description="Email address field for regex validation testing",
    ),
    # Numeric columns
    SchemaFieldClass(
        fieldPath="price",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="DECIMAL",
    ),
    SchemaFieldClass(
        fieldPath="quantity",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="INTEGER",
    ),
    # Date/Time columns for freshness testing
    SchemaFieldClass(
        fieldPath="created_at",
        type=SchemaFieldDataTypeClass(type=DateTypeClass()),
        nativeDataType="DATE",
    ),
    SchemaFieldClass(
        fieldPath="updated_at",
        type=SchemaFieldDataTypeClass(type=TimeTypeClass()),
        nativeDataType="TIMESTAMP",
        description="Last modified timestamp for freshness assertion testing",
    ),
]


# Module scope balances performance (dataset created once per test file) with isolation
# (each file gets its own dataset). Function scope would be too slow; session scope
# would risk test interference across files.
@pytest.fixture(scope="module")
def test_data(graph_client: DataHubGraph, request: Any) -> Any:
    """Create test dataset for assertions with schema for column metric tests.

    Each test module gets its own unique dataset URN, enabling parallel execution.
    The URN is derived from the module name and yielded for tests to use.
    """
    # Derive module name: test_column_metric_assertion -> column_metric
    full_module_name = request.module.__name__
    # Extract the assertion type from module name like "tests.assertions.sdk.test_column_metric_assertion"
    module_suffix = full_module_name.split(".")[-1]  # "test_column_metric_assertion"
    module_name = module_suffix.replace("test_", "").replace(
        "_assertion", ""
    )  # "column_metric"
    dataset_urn = get_test_dataset_urn(module_name)

    # Emit status
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn, aspect=StatusClass(removed=False)
    )
    graph_client.emit(mcpw)

    # Emit schema for column metric assertion tests
    schema = SchemaMetadataClass(
        schemaName="test_schema",
        platform="urn:li:dataPlatform:postgres",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=TEST_SCHEMA_FIELDS,
    )
    schema_mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema)
    graph_client.emit(schema_mcpw)

    wait_for_writes_to_sync()
    yield dataset_urn  # Return the URN so tests can use it
    delete_urn(graph_client, dataset_urn)


@pytest.fixture(scope="module")
def datahub_client(graph_client: DataHubGraph) -> DataHubClient:
    """Create a DataHubClient from the existing graph_client fixture."""
    return DataHubClient(graph=graph_client)

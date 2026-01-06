"""Shared fixtures and constants for SDK assertion tests."""

from typing import Any

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn

TEST_DATASET_URN = make_dataset_urn(platform="postgres", name="foo_sdk_assertions")

# Schema fields for column metric assertion tests
TEST_SCHEMA_FIELDS = [
    SchemaFieldClass(
        fieldPath="user_id",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
    SchemaFieldClass(
        fieldPath="price",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="DECIMAL",
    ),
    SchemaFieldClass(
        fieldPath="name",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
]


@pytest.fixture(scope="module")
def test_data(graph_client: DataHubGraph) -> Any:
    """Create test dataset for assertions with schema for column metric tests."""
    # Emit status
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN, aspect=StatusClass(removed=False)
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
    schema_mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN, aspect=schema
    )
    graph_client.emit(schema_mcpw)

    wait_for_writes_to_sync()
    yield
    delete_urn(graph_client, TEST_DATASET_URN)


@pytest.fixture(scope="module")
def datahub_client(graph_client: DataHubGraph) -> DataHubClient:
    """Create a DataHubClient from the existing graph_client fixture."""
    return DataHubClient(graph=graph_client)

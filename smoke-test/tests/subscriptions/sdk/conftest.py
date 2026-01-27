"""Shared fixtures and constants for SDK subscription tests."""

import logging
from typing import Generator

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
    TimeTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.helpers import cleanup_assertion
from tests.consistency_utils import wait_for_writes_to_sync
from tests.subscriptions.sdk.helpers import generate_unique_test_id
from tests.utils import delete_urn, get_admin_username

logger = logging.getLogger(__name__)


def get_test_dataset_urn(module_name: str) -> str:
    """Generate a unique dataset URN for each test module.

    Args:
        module_name: Short name like "subscription", etc.

    Returns:
        A unique dataset URN for that module.
    """
    return make_dataset_urn(
        platform="postgres", name=f"sdk_subscriptions_{module_name}"
    )


# Schema fields for subscription tests
TEST_SCHEMA_FIELDS = [
    SchemaFieldClass(
        fieldPath="id",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
    SchemaFieldClass(
        fieldPath="name",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
    ),
    SchemaFieldClass(
        fieldPath="amount",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="DECIMAL",
    ),
    SchemaFieldClass(
        fieldPath="updated_at",
        type=SchemaFieldDataTypeClass(type=TimeTypeClass()),
        nativeDataType="TIMESTAMP",
    ),
]


@pytest.fixture(scope="module")
def test_id() -> str:
    """Generate a unique test ID for this module run."""
    return generate_unique_test_id()


@pytest.fixture
def isolated_dataset(
    graph_client: DataHubGraph, request: pytest.FixtureRequest
) -> Generator[str, None, None]:
    """Create a unique, isolated dataset for a single test.

    This fixture creates a fresh dataset for each test function that uses it,
    ensuring complete isolation from other tests. Use this for tests that
    need to verify subscription state via GraphQL without interference.
    """
    test_name = request.node.name
    unique_id = generate_unique_test_id()
    dataset_urn = make_dataset_urn(
        platform="postgres", name=f"sdk_subscriptions_isolated_{test_name}_{unique_id}"
    )
    logger.info(f"Creating isolated test dataset: {dataset_urn}")

    # Emit status
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn, aspect=StatusClass(removed=False)
    )
    graph_client.emit(mcpw)

    # Emit schema
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
    logger.info(f"Isolated test dataset created: {dataset_urn}")

    yield dataset_urn

    logger.info(f"Cleaning up isolated test dataset: {dataset_urn}")
    delete_urn(graph_client, dataset_urn)


@pytest.fixture
def isolated_assertion(
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
    isolated_dataset: str,
) -> Generator[tuple[str, str], None, None]:
    """Create an isolated assertion for a single test.

    This fixture creates a fresh assertion on an isolated dataset, ensuring
    complete isolation from other tests. Use this for assertion subscription
    tests that need to verify state via GraphQL without interference.

    Yields:
        A tuple of (dataset_urn, assertion_urn)
    """
    dataset_urn = isolated_dataset
    unique_id = generate_unique_test_id()

    logger.info(f"Creating isolated assertion for dataset: {dataset_urn}")

    assertion = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Isolated Assertion {unique_id}",
        schedule="0 * * * *",
        lookback_window={"unit": "HOUR", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism="datahub_operation",
    )

    assertion_urn = str(assertion.urn)
    logger.info(f"Isolated assertion created: {assertion_urn}")
    wait_for_writes_to_sync()

    yield dataset_urn, assertion_urn

    logger.info(f"Cleaning up isolated assertion: {assertion_urn}")
    cleanup_assertion(graph_client, assertion_urn)


@pytest.fixture(scope="module")
def test_data(graph_client: DataHubGraph, test_id: str) -> Generator[dict, None, None]:
    """Create test dataset for subscription tests.

    Each test module gets its own unique dataset URN, enabling parallel execution.
    The URN is derived from the module name and yielded for tests to use.
    """
    dataset_urn = get_test_dataset_urn(f"test_{test_id}")
    logger.info(f"Creating test dataset: {dataset_urn}")

    # Emit status
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn, aspect=StatusClass(removed=False)
    )
    graph_client.emit(mcpw)

    # Emit schema
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
    logger.info(f"Test dataset created: {dataset_urn}")

    yield {"dataset_urn": dataset_urn, "test_id": test_id}

    logger.info(f"Cleaning up test dataset: {dataset_urn}")
    delete_urn(graph_client, dataset_urn)


@pytest.fixture(scope="module")
def test_assertion(
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
    test_data: dict,
) -> Generator[str, None, None]:
    """Create a test assertion for assertion-level subscription tests.

    Creates a simple freshness assertion on the test dataset.
    """
    dataset_urn = test_data["dataset_urn"]
    test_id = test_data["test_id"]

    logger.info(f"Creating test assertion for dataset: {dataset_urn}")

    assertion = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Subscription Test Assertion {test_id}",
        schedule="0 * * * *",
        lookback_window={"unit": "HOUR", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism="datahub_operation",
    )

    assertion_urn = str(assertion.urn)
    logger.info(f"Test assertion created: {assertion_urn}")
    wait_for_writes_to_sync()

    yield assertion_urn

    logger.info(f"Cleaning up test assertion: {assertion_urn}")
    cleanup_assertion(graph_client, assertion_urn)


@pytest.fixture(scope="module")
def admin_user_urn() -> str:
    """Get the admin user URN for subscription tests."""
    admin_username = get_admin_username()
    return f"urn:li:corpuser:{admin_username}"


@pytest.fixture(scope="module")
def datahub_client(graph_client: DataHubGraph) -> DataHubClient:
    """Create a DataHubClient from the existing graph_client fixture."""
    return DataHubClient(graph=graph_client)

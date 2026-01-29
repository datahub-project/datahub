"""
Integration tests for end-to-end assertion execution.

Tests that assertions execute successfully using detection mechanisms
that don't require a database connection:
- Freshness assertions with datahub_operation mode (checks for Operation aspects)
- Volume assertions with dataset_profile mode (checks for DatasetProfile aspects)

These tests emit the required metadata (Operation and DatasetProfile timeseries
aspects) to DataHub, then run assertions against them to validate full e2e
execution with SUCCESS or FAILURE results (not errors).
"""

import logging
import time
from typing import Any, Dict, Generator, List

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    NumberTypeClass,
    OperationClass,
    OperationSourceTypeClass,
    OperationTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    TimeTypeClass,
    TimeWindowSizeClass,
)
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.helpers import (
    assert_no_error,
    assert_result_type,
    generate_unique_test_id,
    managed_assertions,
    managed_freshness_assertion,
    managed_volume_assertion,
    wait_for_assertion_sync,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn

logger = logging.getLogger(__name__)

# Test constants
TEST_MODULE_ID = "run_assertions"
KNOWN_ROW_COUNT = 1000  # Row count we emit in the profile


def get_test_dataset_urn(module_suffix: str) -> str:
    """Generate a unique dataset URN for this test module."""
    return make_dataset_urn(
        platform="postgres",
        name=f"sdk_assertions_{module_suffix}",
    )


@pytest.fixture(scope="module")
def test_id() -> str:
    """Generate a unique test ID for this module run."""
    return generate_unique_test_id()


@pytest.fixture(scope="module")
def dataset_urn(test_id) -> str:
    """Get the dataset URN for testing."""
    return get_test_dataset_urn(f"{TEST_MODULE_ID}_{test_id}")


@pytest.fixture(scope="module")
def test_data(
    graph_client: DataHubGraph,
    dataset_urn,
) -> Generator[Dict[str, Any], None, None]:
    """Create test infrastructure: dataset with Operation and DatasetProfile aspects.

    This sets up the metadata that allows assertions to execute end-to-end:
    1. Dataset with status and schema
    2. Operation timeseries aspect (for freshness with datahub_operation mode)
    3. DatasetProfile timeseries aspect (for volume with dataset_profile mode)

    When freshness assertions run with datahub_operation mode, the executor will:
    - Query for Operation aspects within the lookback window
    - Return SUCCESS if an operation exists, FAILURE otherwise

    When volume assertions run with dataset_profile mode, the executor will:
    - Fetch the latest DatasetProfile aspect
    - Evaluate the rowCount against the assertion criteria
    """
    logger.info(
        f"Setting up test data for run_assertions test (dataset_urn={dataset_urn})"
    )

    now_ms = int(time.time() * 1000)

    # 1. Create the dataset with status
    status_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=StatusClass(removed=False),
    )
    graph_client.emit(status_mcpw)

    # 2. Create schema for the dataset
    schema = SchemaMetadataClass(
        schemaName="test_schema",
        platform="urn:li:dataPlatform:postgres",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
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
        ],
    )
    schema_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema,
    )
    graph_client.emit(schema_mcpw)
    logger.info(f"Created dataset with schema: {dataset_urn}")

    # 3. Emit an Operation timeseries aspect (for freshness tests)
    # This simulates an ingestion/update operation on the dataset
    operation = OperationClass(
        timestampMillis=now_ms,
        lastUpdatedTimestamp=now_ms,
        operationType=OperationTypeClass.INSERT,
        sourceType=OperationSourceTypeClass.DATA_PLATFORM,
    )
    operation_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=operation,
    )
    graph_client.emit(operation_mcpw)
    logger.info(f"Emitted Operation aspect for dataset: {dataset_urn}")

    # 4. Emit a DatasetProfile timeseries aspect (for volume tests)
    # This provides the row count for volume assertions
    profile = DatasetProfileClass(
        timestampMillis=now_ms,
        rowCount=KNOWN_ROW_COUNT,
        columnCount=4,
        eventGranularity=TimeWindowSizeClass(unit="DAY", multiple=1),
    )
    profile_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=profile,
    )
    graph_client.emit(profile_mcpw)
    logger.info(
        f"Emitted DatasetProfile aspect with rowCount={KNOWN_ROW_COUNT} for dataset: {dataset_urn}"
    )

    wait_for_writes_to_sync()

    yield {
        "dataset_urn": dataset_urn,
        "row_count": KNOWN_ROW_COUNT,
    }

    # Cleanup
    logger.info("Cleaning up test data...")
    delete_urn(graph_client, dataset_urn)
    wait_for_writes_to_sync()


@pytest.fixture(scope="module")
def datahub_client(graph_client: DataHubGraph) -> DataHubClient:
    """Create a DataHubClient from the existing graph_client fixture."""
    return DataHubClient(graph=graph_client)


# =============================================================================
# Freshness Assertion Tests (DataHub Operation mode)
# =============================================================================


def test_freshness_datahub_operation_success(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test freshness assertion with datahub_operation mode returns SUCCESS.

    Creates a freshness assertion that checks for DataHub operations.
    Since we emitted an Operation aspect in the fixture, the assertion should
    find it within the lookback window and return SUCCESS.
    """
    dataset_urn = test_data["dataset_urn"]

    with managed_freshness_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Freshness DataHub Op {generate_unique_test_id()}",
        lookback_window={"unit": "DAY", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism="datahub_operation",
    ) as assertion_urn:
        # Run the assertion - should succeed because we emitted an Operation
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
        )

        logger.info(f"Freshness datahub_operation result: {result}")

        # Validate we got SUCCESS (not an error)
        assert_no_error(result, "Freshness datahub_operation assertion")
        assert_result_type(result, "SUCCESS", "Freshness datahub_operation assertion")


# =============================================================================
# Volume Assertion Tests (Dataset Profile mode)
# =============================================================================


def test_volume_dataset_profile_success(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test volume assertion with dataset_profile mode returns SUCCESS.

    Creates a volume assertion that checks row count from DatasetProfile.
    Since we emitted a profile with rowCount=1000, an assertion checking
    for rowCount > 500 should return SUCCESS.
    """
    dataset_urn = test_data["dataset_urn"]

    with managed_volume_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Volume Profile Success {generate_unique_test_id()}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=501,  # 1000 >= 501 = SUCCESS
        detection_mechanism="dataset_profile",
    ) as assertion_urn:
        # Run the assertion - should succeed because 1000 > 500
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
        )

        logger.info(f"Volume dataset_profile success result: {result}")

        # Validate we got SUCCESS (not an error)
        assert_no_error(result, "Volume dataset_profile assertion (success case)")
        assert_result_type(
            result, "SUCCESS", "Volume dataset_profile assertion (success case)"
        )


def test_volume_dataset_profile_failure(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test volume assertion with dataset_profile mode returns FAILURE.

    Creates a volume assertion that checks row count from DatasetProfile.
    Since we emitted a profile with rowCount=1000, an assertion checking
    for rowCount > 2000 should return FAILURE.
    """
    dataset_urn = test_data["dataset_urn"]

    with managed_volume_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Volume Profile Failure {generate_unique_test_id()}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=2001,  # 1000 >= 2001 = FAILURE
        detection_mechanism="dataset_profile",
    ) as assertion_urn:
        # Run the assertion - should fail because 1000 is NOT > 2000
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
        )

        logger.info(f"Volume dataset_profile failure result: {result}")

        # Validate we got FAILURE (not an error, not success)
        assert_no_error(result, "Volume dataset_profile assertion (failure case)")
        assert_result_type(
            result, "FAILURE", "Volume dataset_profile assertion (failure case)"
        )


def test_volume_dataset_profile_within_range(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test volume assertion with range condition returns SUCCESS.

    Creates a volume assertion checking if rowCount is within [500, 1500].
    Since we have 1000 rows, this should return SUCCESS.
    """
    dataset_urn = test_data["dataset_urn"]

    with managed_volume_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Volume Profile Range {generate_unique_test_id()}",
        criteria_condition="ROW_COUNT_IS_WITHIN_A_RANGE",
        criteria_parameters=(500, 1500),  # 1000 is within [500, 1500]
        detection_mechanism="dataset_profile",
    ) as assertion_urn:
        # Run the assertion - should succeed because 1000 is in range [500, 1500]
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
        )

        logger.info(f"Volume dataset_profile range result: {result}")

        # Validate we got SUCCESS
        assert_no_error(result, "Volume dataset_profile assertion (range case)")
        assert_result_type(
            result, "SUCCESS", "Volume dataset_profile assertion (range case)"
        )


# =============================================================================
# Batch Execution Tests
# =============================================================================


def test_run_assertions_batch_e2e(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test batch run_assertions with e2e execution.

    Creates multiple assertions and runs them in batch.
    Validates that all return appropriate SUCCESS/FAILURE results.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = generate_unique_test_id()
    assertion_urns: List[str] = []

    # Create freshness assertion (should succeed)
    freshness_assertion = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Batch Freshness {test_suffix}",
        lookback_window={"unit": "DAY", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism="datahub_operation",
        schedule="0 * * * *",
    )
    assertion_urns.append(str(freshness_assertion.urn))

    # Create volume assertion (should succeed - 1000 >= 501)
    volume_success = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Batch Volume Success {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=501,
        detection_mechanism="dataset_profile",
        schedule="0 * * * *",
    )
    assertion_urns.append(str(volume_success.urn))

    # Create volume assertion (should fail - 1000 is NOT >= 2001)
    volume_fail = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Batch Volume Fail {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=2001,
        detection_mechanism="dataset_profile",
        schedule="0 * * * *",
    )
    assertion_urns.append(str(volume_fail.urn))

    logger.info(f"Created batch assertions: {assertion_urns}")
    wait_for_assertion_sync()

    with managed_assertions(graph_client, assertion_urns):
        # Run all assertions in batch
        result = graph_client.run_assertions(
            urns=assertion_urns,
            save_result=False,
        )

        logger.info(f"Batch assertion result: {result}")

        # Validate batch result structure
        assert "results" in result, "Expected 'results' in batch response"
        assert len(result["results"]) == 3, (
            f"Expected 3 results, got {len(result['results'])}"
        )

        # We expect 2 passing and 1 failing
        assert result.get("passingCount") == 2, (
            f"Expected 2 passing, got {result.get('passingCount')}"
        )
        assert result.get("failingCount") == 1, (
            f"Expected 1 failing, got {result.get('failingCount')}"
        )
        assert result.get("errorCount") == 0, (
            f"Expected 0 errors, got {result.get('errorCount')}"
        )


def test_run_assertions_for_asset_e2e(
    test_data: Dict[str, Any],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test run_assertions_for_asset with e2e execution.

    Creates assertions and runs them using the asset-based method.
    """
    dataset_urn = test_data["dataset_urn"]
    row_count = test_data["row_count"]
    test_suffix = generate_unique_test_id()
    assertion_urns: List[str] = []

    # Create a volume assertion (should succeed)
    volume_assertion = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Asset Volume {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=row_count,  # Exactly 1000, should succeed
        detection_mechanism="dataset_profile",
        schedule="0 * * * *",
    )
    assertion_urns.append(str(volume_assertion.urn))

    logger.info(f"Created assertion for asset test: {assertion_urns}")
    wait_for_assertion_sync()

    with managed_assertions(graph_client, assertion_urns):
        # Run assertions for asset
        result = graph_client.run_assertions_for_asset(
            urn=dataset_urn,
        )

        logger.info(f"Asset assertion result: {result}")

        # Validate results
        assert "results" in result, "Expected 'results' in asset response"
        assert len(result["results"]) >= 1, "Expected at least 1 result"

        # Find our assertion in the results
        matching_result = None
        for assertion_result in result["results"]:
            if assertion_result.get("assertion", {}).get("urn") == assertion_urns[0]:
                matching_result = assertion_result
                break

        assert matching_result is not None, (
            f"Could not find our assertion {assertion_urns[0]} in results"
        )

        nested_result = matching_result.get("result", {})
        assert_no_error(nested_result, "Asset volume assertion")
        assert_result_type(nested_result, "SUCCESS", "Asset volume assertion")

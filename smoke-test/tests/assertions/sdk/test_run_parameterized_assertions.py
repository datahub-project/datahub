"""
Integration tests for parameterized assertion execution.

Tests that runtime parameters (${var}) are properly substituted in:
- SQL assertions (templated SQL statements)
- Volume assertions (templated filter clauses)
- Freshness assertions (templated filter clauses with field values mode)

These tests use a mock BigQuery ingestion source to enable testing the
parameter substitution flow without requiring a real database connection.
The tests validate that:
- When parameters are provided, substitution succeeds and the error is SOURCE_QUERY_FAILED
  (i.e., we reached the point of trying to query the source)
- When parameters are missing, the error is INVALID_PARAMETERS
"""

import json
import logging
import time
from typing import Dict, Generator, List

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    DataHubIngestionSourceKeyClass,
    DateTypeClass,
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestSourceClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SystemMetadataClass,
    TimeTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.helpers import (
    assert_error_type,
    assert_error_type_in,
    generate_unique_test_id,
    managed_assertions,
    managed_freshness_assertion,
    managed_sql_assertion,
    managed_volume_assertion,
    wait_for_assertion_sync,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn

logger = logging.getLogger(__name__)

# Test identifiers
TEST_MODULE_ID = "run_parameterized_assertions"

# =============================================================================
# IMPORTANT: Deliberately Invalid Test Credentials
# =============================================================================
# The following BigQuery recipe contains FAKE credentials that are intentionally
# malformed and will NEVER work as real credentials. This is by design:
#
# 1. The private_key is truncated/invalid - it's not a real RSA key
# 2. The project_id, client_email, and client_id are all fake
# 3. These credentials exist ONLY to test parameter substitution flow
#
# The test goal is to verify that ${var} parameters are properly substituted
# BEFORE the connection attempt fails with SOURCE_QUERY_FAILED or UNKNOWN_ERROR.
# Security scanners should recognize this as a test fixture, not a credential leak.
# =============================================================================
BIGQUERY_RECIPE = json.dumps(
    {
        "source": {
            "type": "bigquery",
            "config": {
                "project_id": "fake-project-for-testing",
                "credential": {
                    "private_key_id": "fake-key-id",
                    # This is NOT a real private key - intentionally malformed for testing
                    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIBOgIBAAJBALRoMLAH+HpXc2Xk2Rq/2kP5T3X9O7dVsfe/A8F9nqpT9kP8XFP1\nFAKE_KEY_FOR_TESTING_ONLY_NOT_REAL\n-----END PRIVATE KEY-----\n",
                    "client_email": "fake-service-account@fake-project.iam.gserviceaccount.com",
                    "client_id": "123456789012345678901",
                },
            },
        }
    }
)


def get_test_dataset_urn() -> str:
    """Generate a unique dataset URN for this test module."""
    return make_dataset_urn(
        platform="bigquery",
        name=f"fake-project-for-testing.test_dataset.{TEST_MODULE_ID}",
    )


def get_ingestion_source_urn(test_id: str) -> str:
    """Generate a unique ingestion source URN."""
    return f"urn:li:dataHubIngestionSource:test-bq-source-{test_id}"


def get_execution_request_id(test_id: str) -> str:
    """Generate a unique execution request ID."""
    return f"test-exec-{test_id}"


def get_execution_request_urn(test_id: str) -> str:
    """Generate the execution request URN from the ID."""
    return f"urn:li:dataHubExecutionRequest:{get_execution_request_id(test_id)}"


@pytest.fixture(scope="module")
def test_id() -> str:
    """Generate a unique test ID for this module run."""
    return generate_unique_test_id()


@pytest.fixture(scope="module")
def ingestion_source_urn(test_id) -> str:
    """Get the ingestion source URN for this test run."""
    return get_ingestion_source_urn(test_id)


@pytest.fixture(scope="module")
def execution_request_id(test_id) -> str:
    """Get the execution request ID for this test run."""
    return get_execution_request_id(test_id)


@pytest.fixture(scope="module")
def dataset_urn() -> str:
    """Get the dataset URN for testing."""
    return get_test_dataset_urn()


@pytest.fixture(scope="module")
def test_data(
    graph_client: DataHubGraph,
    test_id,
    ingestion_source_urn,
    execution_request_id,
    dataset_urn,
) -> Generator[Dict[str, str], None, None]:
    """Create test infrastructure: ingestion source, execution request, and dataset.

    This sets up the metadata chain that allows the executor to resolve
    the connection for the dataset:
    1. DataHubIngestionSource - Contains the BigQuery recipe
    2. ExecutionRequest - Links run ID to ingestion source
    3. Dataset - Has systemMetadata with runId pointing to execution request

    When assertions run against the dataset, the executor will:
    - Look up the dataset's systemMetadata to get the runId
    - Find the ExecutionRequest by runId
    - Get the ingestionSource from the ExecutionRequest
    - Extract the BigQuery connection from the recipe
    - Attempt to run queries (which will fail with SOURCE_QUERY_FAILED due to fake creds)
    """
    logger.info(
        f"Setting up test data for parameterized assertions test (test_id={test_id})"
    )

    execution_request_urn = get_execution_request_urn(test_id)

    # 1. Create the ingestion source
    ingestion_source_info = DataHubIngestionSourceInfoClass(
        name=f"Test BigQuery Source {test_id}",
        type="bigquery",
        config=DataHubIngestionSourceConfigClass(
            recipe=BIGQUERY_RECIPE,
            executorId="default",  # Use embedded executor
            extraArgs={},
            debugMode=False,
        ),
    )
    ingestion_source_mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=DataHubIngestionSourceKeyClass(id=f"test-bq-source-{test_id}"),
        entityUrn=ingestion_source_urn,
        entityType="dataHubIngestionSource",
        aspectName="dataHubIngestionSourceInfo",
        aspect=ingestion_source_info,
        changeType="UPSERT",
    )
    graph_client.emit_mcp(ingestion_source_mcpw)
    logger.info(f"Created ingestion source: {ingestion_source_urn}")

    # 2. Create the execution request
    execution_request_input = ExecutionRequestInputClass(
        task="RUN_INGEST",
        args={
            "recipe": BIGQUERY_RECIPE,
            "version": "1.0.0.0",
            "debug_mode": "false",
        },
        executorId="default",
        attempts=0,
        actorUrn=None,
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="MANUAL_INGESTION_SOURCE",
            ingestionSource=ingestion_source_urn,
        ),
    )
    execution_request_mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=execution_request_id),
        entityUrn=execution_request_urn,
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=execution_request_input,
        changeType="UPSERT",
    )
    graph_client.emit_mcp(execution_request_mcpw)
    logger.info(f"Created execution request: {execution_request_urn}")

    # 3. Create the dataset with status
    status_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn, aspect=StatusClass(removed=False)
    )
    graph_client.emit(status_mcpw)

    # 4. Create the dataset schema with systemMetadata containing the runId
    schema = SchemaMetadataClass(
        schemaName="test_schema",
        platform="urn:li:dataPlatform:bigquery",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="STRING",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="STRING",
            ),
            SchemaFieldClass(
                fieldPath="amount",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="FLOAT64",
            ),
            SchemaFieldClass(
                fieldPath="created_date",
                type=SchemaFieldDataTypeClass(type=DateTypeClass()),
                nativeDataType="DATE",
            ),
            SchemaFieldClass(
                fieldPath="updated_at",
                type=SchemaFieldDataTypeClass(type=TimeTypeClass()),
                nativeDataType="TIMESTAMP",
            ),
        ],
    )

    # Create SystemMetadata with runId pointing to our execution request
    system_metadata = SystemMetadataClass(
        runId=execution_request_id,
        lastObserved=int(time.time() * 1000),
    )

    schema_mcpw = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema,
        systemMetadata=system_metadata,
    )
    graph_client.emit(schema_mcpw)
    logger.info(f"Created dataset with schema: {dataset_urn}")

    wait_for_writes_to_sync()

    yield {
        "dataset_urn": dataset_urn,
        "ingestion_source_urn": ingestion_source_urn,
        "execution_request_urn": execution_request_urn,
        "execution_request_id": execution_request_id,
        "test_id": test_id,
    }

    # Cleanup
    logger.info("Cleaning up test data...")
    delete_urn(graph_client, dataset_urn)
    delete_urn(graph_client, execution_request_urn)
    delete_urn(graph_client, ingestion_source_urn)
    wait_for_writes_to_sync()


@pytest.fixture(scope="module")
def datahub_client(graph_client: DataHubGraph) -> DataHubClient:
    """Create a DataHubClient from the existing graph_client fixture."""
    return DataHubClient(graph=graph_client)


# Valid error types when parameters are provided and substitution succeeds.
# Both indicate we got past parameter substitution:
# - SOURCE_QUERY_FAILED: Query execution failed (valid credential format)
# - UNKNOWN_ERROR: Credential loading failed (invalid key format, but params worked)
PARAM_SUBSTITUTION_SUCCESS_ERRORS = ["SOURCE_QUERY_FAILED", "UNKNOWN_ERROR"]


# =============================================================================
# SQL Assertion Tests
# =============================================================================


@pytest.mark.remote_executor
def test_sql_assertion_with_parameters_substituted(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that SQL assertion with parameters reaches query execution.

    Creates a SQL assertion with ${filter_value} parameter in the statement.
    When run with the parameter provided, it should reach the query execution
    stage (SOURCE_QUERY_FAILED) rather than failing on parameter substitution.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_sql_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Parameterized SQL {test_suffix}",
        statement="SELECT COUNT(*) FROM `fake-project-for-testing.test_dataset.run_parameterized_assertions` WHERE name = '${filter_value}'",
        criteria_condition="IS_GREATER_THAN",
        criteria_parameters=0,
    ) as assertion_urn:
        # Run assertion with parameters - should fail with SOURCE_QUERY_FAILED
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={"filter_value": "test_name"},
        )

        logger.info(f"Assertion result with parameters: {result}")

        # Validate we reached query execution (not parameter substitution error)
        assert_error_type_in(
            result,
            PARAM_SUBSTITUTION_SUCCESS_ERRORS,
            "SQL assertion with parameters",
        )


@pytest.mark.remote_executor
def test_sql_assertion_missing_parameters(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that SQL assertion without required parameters fails with INVALID_PARAMETERS.

    Creates a SQL assertion with ${filter_value} parameter in the statement.
    When run without the parameter, it should fail with INVALID_PARAMETERS.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_sql_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Missing Params SQL {test_suffix}",
        statement="SELECT COUNT(*) FROM `fake-project-for-testing.test_dataset.run_parameterized_assertions` WHERE id = '${missing_param}'",
        criteria_condition="IS_GREATER_THAN",
        criteria_parameters=0,
    ) as assertion_urn:
        # Run assertion without parameters - should fail with INVALID_PARAMETERS
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={},  # Empty parameters
        )

        logger.info(f"Assertion result without parameters: {result}")

        # Validate we got parameter substitution error
        assert_error_type(
            result,
            "INVALID_PARAMETERS",
            "SQL assertion without parameters",
        )


# =============================================================================
# Volume Assertion Tests
# =============================================================================


@pytest.mark.remote_executor
def test_volume_assertion_with_parameters_substituted(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that volume assertion with filter parameters reaches query execution.

    Creates a volume assertion with a parameterized filter clause.
    When run with the parameter provided, it should reach the query execution
    stage (SOURCE_QUERY_FAILED) rather than failing on parameter substitution.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_volume_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Parameterized Volume {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=0,
        detection_mechanism={
            "type": "query",
            "additional_filter": "created_date >= '${start_date}'",
        },
    ) as assertion_urn:
        # Run assertion with parameters - should fail with SOURCE_QUERY_FAILED
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={"start_date": "2024-01-01"},
        )

        logger.info(f"Volume assertion result with parameters: {result}")

        # Validate we reached query execution (not parameter substitution error)
        assert_error_type_in(
            result,
            PARAM_SUBSTITUTION_SUCCESS_ERRORS,
            "Volume assertion with parameters",
        )


@pytest.mark.remote_executor
def test_volume_assertion_missing_parameters(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that volume assertion without required parameters fails with INVALID_PARAMETERS.

    Creates a volume assertion with a parameterized filter clause.
    When run without the parameter, it should fail with INVALID_PARAMETERS.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_volume_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Missing Params Volume {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=0,
        detection_mechanism={
            "type": "query",
            "additional_filter": "amount > ${min_amount}",
        },
    ) as assertion_urn:
        # Run assertion without parameters - should fail with INVALID_PARAMETERS
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={},  # Empty parameters
        )

        logger.info(f"Volume assertion result without parameters: {result}")

        # Validate we got parameter substitution error
        assert_error_type(
            result,
            "INVALID_PARAMETERS",
            "Volume assertion without parameters",
        )


# =============================================================================
# Freshness Assertion Tests (Field Values Mode)
# =============================================================================


@pytest.mark.remote_executor
def test_freshness_assertion_with_parameters_substituted(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that freshness assertion with filter parameters reaches query execution.

    Creates a freshness assertion using field values mode with a parameterized filter.
    When run with the parameter provided, it should reach the query execution
    stage (SOURCE_QUERY_FAILED) rather than failing on parameter substitution.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_freshness_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Parameterized Freshness {test_suffix}",
        lookback_window={"unit": "DAY", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism={
            "type": "last_modified_column",
            "column_name": "updated_at",
            "additional_filter": "name = '${partition_name}'",
        },
    ) as assertion_urn:
        # Run assertion with parameters - should fail with SOURCE_QUERY_FAILED
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={"partition_name": "production"},
        )

        logger.info(f"Freshness assertion result with parameters: {result}")

        # Validate we reached query execution (not parameter substitution error)
        assert_error_type_in(
            result,
            PARAM_SUBSTITUTION_SUCCESS_ERRORS,
            "Freshness assertion with parameters",
        )


@pytest.mark.remote_executor
def test_freshness_assertion_missing_parameters(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test that freshness assertion without required parameters fails with INVALID_PARAMETERS.

    Creates a freshness assertion using field values mode with a parameterized filter.
    When run without the parameter, it should fail with INVALID_PARAMETERS.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]

    with managed_freshness_assertion(
        graph_client=graph_client,
        datahub_client=datahub_client,
        dataset_urn=dataset_urn,
        display_name=f"Missing Params Freshness {test_suffix}",
        lookback_window={"unit": "DAY", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
        detection_mechanism={
            "type": "last_modified_column",
            "column_name": "updated_at",
            "additional_filter": "id = '${record_id}'",
        },
    ) as assertion_urn:
        # Run assertion without parameters - should fail with INVALID_PARAMETERS
        result = graph_client.run_assertion(
            urn=assertion_urn,
            save_result=False,
            parameters={},  # Empty parameters
        )

        logger.info(f"Freshness assertion result without parameters: {result}")

        # Validate we got parameter substitution error
        assert_error_type(
            result,
            "INVALID_PARAMETERS",
            "Freshness assertion without parameters",
        )


# =============================================================================
# run_assertions (batch) Tests
# =============================================================================


@pytest.mark.remote_executor
def test_run_assertions_batch_with_parameters(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test batch run_assertions with parameters across multiple assertions.

    Creates multiple assertions with parameters and runs them in batch.
    Validates that parameters are properly passed to all assertions.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]
    assertion_urns: List[str] = []

    # Create SQL assertion with parameter
    sql_assertion = datahub_client.assertions.sync_sql_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Batch SQL {test_suffix}",
        statement="SELECT COUNT(*) FROM `fake-project-for-testing.test_dataset.run_parameterized_assertions` WHERE name = '${shared_param}'",
        criteria_condition="IS_GREATER_THAN",
        criteria_parameters=0,
        schedule="0 * * * *",
    )
    assertion_urns.append(str(sql_assertion.urn))

    # Create volume assertion with same parameter
    volume_assertion = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Batch Volume {test_suffix}",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=0,
        detection_mechanism={
            "type": "query",
            "additional_filter": "name = '${shared_param}'",
        },
        schedule="0 * * * *",
    )
    assertion_urns.append(str(volume_assertion.urn))

    logger.info(f"Created batch assertions: {assertion_urns}")
    wait_for_assertion_sync()

    with managed_assertions(graph_client, assertion_urns):
        # Run assertions in batch with shared parameter
        result = graph_client.run_assertions(
            urns=assertion_urns,
            save_result=False,
            parameters={"shared_param": "test_value"},
        )

        logger.info(f"Batch assertion result with parameters: {result}")

        # Validate that all assertions reached query execution
        assert "results" in result, "Expected 'results' in batch response"
        assert len(result["results"]) == 2, (
            f"Expected 2 results, got {len(result['results'])}"
        )

        for assertion_result in result["results"]:
            nested_result = assertion_result.get("result", {})
            assert_error_type_in(
                nested_result,
                PARAM_SUBSTITUTION_SUCCESS_ERRORS,
                f"Batch assertion {assertion_result.get('assertion', {}).get('urn')}",
            )


@pytest.mark.remote_executor
def test_run_assertions_for_asset_with_parameters(
    test_data: Dict[str, str],
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
) -> None:
    """Test run_assertions_for_asset with parameters.

    Creates assertions with parameters and runs them using the asset-based method.
    """
    dataset_urn = test_data["dataset_urn"]
    test_suffix = test_data["test_id"]
    assertion_urns: List[str] = []

    # Create SQL assertion with parameter
    sql_assertion = datahub_client.assertions.sync_sql_assertion(
        dataset_urn=dataset_urn,
        display_name=f"Asset SQL {test_suffix}",
        statement="SELECT COUNT(*) FROM `fake-project-for-testing.test_dataset.run_parameterized_assertions` WHERE name = '${asset_param}'",
        criteria_condition="IS_GREATER_THAN",
        criteria_parameters=0,
        schedule="0 * * * *",
    )
    assertion_urns.append(str(sql_assertion.urn))

    logger.info(f"Created assertion for asset test: {assertion_urns}")
    wait_for_assertion_sync()

    with managed_assertions(graph_client, assertion_urns):
        # Run assertions for asset with parameter
        result = graph_client.run_assertions_for_asset(
            urn=dataset_urn,
            parameters={"asset_param": "asset_test_value"},
        )

        logger.info(f"Asset assertion result with parameters: {result}")

        # Validate results
        assert "results" in result, "Expected 'results' in asset response"
        # There should be at least one result (our created assertion)
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
        assert_error_type_in(
            nested_result,
            PARAM_SUBSTITUTION_SUCCESS_ERRORS,
            "Asset assertion with parameters",
        )

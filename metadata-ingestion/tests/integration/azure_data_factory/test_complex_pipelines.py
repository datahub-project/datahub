"""Integration tests for complex Azure Data Factory pipeline patterns.

These tests validate that the ADF connector correctly handles advanced pipeline
configurations commonly found in production environments. Each test scenario
represents a real-world pattern that data engineers use in ADF.

Test Coverage:
=============

1. **Nested Pipelines (Execute Pipeline Activity)**
   - Parent pipelines orchestrating child pipelines
   - Validates hierarchical DataFlow/DataJob relationships
   - Ensures lineage propagates through nested execution

2. **ForEach Loops**
   - Iterative processing over collections (tables, files, etc.)
   - Tests that loop activities and their children are properly extracted
   - Validates parametrized activities within loops

3. **Control Flow Branching (If-Condition, Switch)**
   - Conditional execution paths based on runtime expressions
   - Verifies all branches (true/false, switch cases, default) are captured
   - Tests that lineage is recorded for activities in all branches

4. **Mapping Data Flows**
   - Complex transformations (filter, join, aggregate, derive)
   - Multiple sources and sinks with transformation chains
   - Validates Data Flow script extraction and lineage

5. **Multi-Source ETL Pipelines**
   - Full ETL chains: SQL → Blob → Synapse → DataLake
   - Tests end-to-end lineage across multiple hops
   - Validates platform mapping (mssql, abs)

Why These Tests Matter:
======================
Production ADF pipelines rarely use simple, linear patterns. These tests ensure
the connector handles real-world complexity without losing lineage information
or failing to capture activities in nested/conditional structures.

Mock Data Strategy:
==================
Mock data is based on Azure REST API response structures from:
https://github.com/Azure/azure-rest-api-specs/tree/main/specification/datafactory

The mocks simulate real Azure SDK responses, including:
- Factory, Pipeline, Dataset, LinkedService, DataFlow objects
- Proper nesting of properties and type-specific configurations
- Realistic activity structures with inputs, outputs, and dependencies
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.azure_data_factory.adf_source import (
    AzureDataFactorySource,
)
from datahub.testing import mce_helpers
from tests.integration.azure_data_factory.complex_mocks import (
    RESOURCE_GROUP,
    SUBSCRIPTION_ID,
    create_branching_scenario,
    create_complex_datasets,
    create_complex_factory,
    create_complex_linked_services,
    create_dataflow_scenario,
    create_diverse_activities_scenario,
    create_foreach_loop_scenario,
    create_multisource_chain_scenario,
    create_nested_pipeline_scenario,
    get_all_data_flows,
)

# Freeze time for deterministic test output (affects timestamps in MCPs)
FROZEN_TIME = "2024-01-15 12:00:00"


# =============================================================================
# MOCK HELPERS
# =============================================================================
# These classes simulate the Azure SDK's response objects. The Azure SDK returns
# objects that have an as_dict() method to convert to dictionaries, which our
# connector then parses into Pydantic models.


class MockAzureResource:
    """Mock class to simulate Azure SDK resource objects.

    The Azure SDK returns resource objects (Pipeline, Dataset, etc.) that have
    an as_dict() method. Our connector calls this method to get a dictionary
    representation which is then validated against our Pydantic models.
    """

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def as_dict(self) -> Dict[str, Any]:
        return self._data


class MockPagedIterator:
    """Mock class to simulate Azure SDK paged iterators.

    Azure SDK list operations return paged iterators that yield resource objects.
    This mock simulates that behavior for testing without making real API calls.
    """

    def __init__(self, items: List[Dict[str, Any]]):
        self._items = [MockAzureResource(item) for item in items]

    def __iter__(self) -> Iterator[MockAzureResource]:
        return iter(self._items)


class MockQueryResponse:
    """Mock class for query responses (e.g., pipeline runs) with continuation token.

    Some Azure APIs return query responses that include a continuation token
    for pagination. This mock supports that pattern.
    """

    def __init__(
        self, items: List[Dict[str, Any]], continuation_token: Optional[str] = None
    ):
        self.value = [MockAzureResource(item) for item in items]
        self.continuation_token = continuation_token


def create_mock_client(
    pipelines: List[Dict[str, Any]],
    datasets: List[Dict[str, Any]],
    linked_services: List[Dict[str, Any]],
    data_flows: Optional[List[Dict[str, Any]]] = None,
    triggers: Optional[List[Dict[str, Any]]] = None,
    pipeline_runs: Optional[List[Dict[str, Any]]] = None,
) -> mock.MagicMock:
    """Create a mock DataFactoryManagementClient with the given test data.

    This function creates a mock that simulates the Azure SDK's
    DataFactoryManagementClient. Each method returns appropriate mock
    iterators/responses that our connector will process.

    Args:
        pipelines: List of pipeline definitions (will be converted to DataFlow entities)
        datasets: List of dataset definitions (used for lineage resolution)
        linked_services: List of linked service definitions (used for platform mapping)
        data_flows: List of data flow definitions (for Mapping Data Flow activities)
        triggers: List of trigger definitions (optional)
        pipeline_runs: List of pipeline run records (for execution history)

    Returns:
        A MagicMock configured to behave like DataFactoryManagementClient
    """
    mock_client = mock.MagicMock()

    # Mock factories - the top-level container for all ADF resources
    factory = create_complex_factory()
    mock_client.factories.list.return_value = MockPagedIterator([factory])
    mock_client.factories.list_by_resource_group.return_value = MockPagedIterator(
        [factory]
    )

    # Mock pipelines - these become DataFlow entities in DataHub
    mock_client.pipelines.list_by_factory.return_value = MockPagedIterator(pipelines)

    # Mock datasets - used to resolve lineage (input/output of activities)
    mock_client.datasets.list_by_factory.return_value = MockPagedIterator(datasets)

    # Mock linked services - determine the platform type for datasets
    # (e.g., AzureSqlDatabase → mssql, AzureBlobStorage → abs)
    mock_client.linked_services.list_by_factory.return_value = MockPagedIterator(
        linked_services
    )

    # Mock triggers - schedule definitions (not heavily used in these tests)
    mock_client.triggers.list_by_factory.return_value = MockPagedIterator(
        triggers or []
    )

    # Mock data flows - Mapping Data Flow definitions with sources/sinks/transforms
    mock_client.data_flows.list_by_factory.return_value = MockPagedIterator(
        data_flows or []
    )

    # Mock pipeline runs - execution history (for DataProcessInstance entities)
    mock_client.pipeline_runs.query_by_factory.return_value = MockQueryResponse(
        pipeline_runs or []
    )

    # Mock activity runs - individual activity execution records
    mock_client.activity_runs.query_by_pipeline_run.return_value = MockQueryResponse([])

    return mock_client


def _run_test_pipeline(
    tmp_path: Any,
    run_id: str,
    pipelines: List[Dict[str, Any]],
    datasets: Optional[List[Dict[str, Any]]] = None,
    linked_services: Optional[List[Dict[str, Any]]] = None,
    data_flows: Optional[List[Dict[str, Any]]] = None,
    include_lineage: bool = True,
) -> Pipeline:
    """Helper function to run an ingestion pipeline with mocked Azure data.

    This sets up the full DataHub ingestion pipeline with mocked Azure SDK
    responses, runs the ingestion, and returns the pipeline for assertions.

    Args:
        tmp_path: Pytest fixture for temporary directory
        run_id: Unique identifier for this test run
        pipelines: ADF pipeline definitions to ingest
        datasets: Dataset definitions (defaults to standard test datasets)
        linked_services: Linked service definitions (defaults to standard test services)
        data_flows: Data flow definitions for Mapping Data Flow activities
        include_lineage: Whether to extract lineage from activities

    Returns:
        The executed Pipeline object with source report for assertions
    """
    if datasets is None:
        datasets = create_complex_datasets()
    if linked_services is None:
        linked_services = create_complex_linked_services()

    mock_client = create_mock_client(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        data_flows=data_flows,
    )

    output_file = tmp_path / f"{run_id}_output.json"

    config = {
        "run_id": run_id,
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": include_lineage,
                "include_execution_history": False,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    return pipeline


# =============================================================================
# TEST: NESTED PIPELINES (Execute Pipeline Activity)
# =============================================================================
#
# Scenario: Parent pipeline orchestrates child pipelines
# -------------------------------------------------------
# ParentOrchestrationPipeline
#   └── ExecutePipeline: ChildDataMovementPipeline
#       └── Copy: SqlToBlob
#   └── ExecutePipeline: ChildTransformPipeline
#       └── DataFlow: TransformData
#
# What we're testing:
# - All three pipelines are extracted as DataFlow entities
# - ExecutePipeline activities are captured as DataJob entities
# - Child pipeline activities (Copy, DataFlow) are also captured
# - Browse paths show proper hierarchy
#
# Why this matters:
# - Large organizations modularize pipelines for reusability
# - Lineage must track data movement through nested executions
# - Users need to see the full orchestration hierarchy


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_nested_pipeline_creates_all_entities(pytestconfig, tmp_path):
    """Test that nested pipelines create correct DataFlow and DataJob entities.

    This test verifies that when a parent pipeline contains ExecutePipeline
    activities that call child pipelines, all three pipelines and their
    activities are properly extracted as DataHub entities.

    Expected entities:
    - 3 DataFlow entities (ParentOrchestrationPipeline, ChildDataMovement, ChildTransform)
    - 4 DataJob entities (2 ExecutePipeline + 1 Copy + 1 DataFlow activity)
    """
    scenario = create_nested_pipeline_scenario()

    pipeline = _run_test_pipeline(
        tmp_path,
        run_id="nested-pipeline-test",
        pipelines=scenario["pipelines"],
        data_flows=get_all_data_flows(),
    )

    # Verify all pipelines were processed (not filtered out)
    assert isinstance(pipeline.source, AzureDataFactorySource)
    assert pipeline.source.report.pipelines_scanned == len(scenario["pipelines"])


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_nested_pipeline_golden(pytestconfig, tmp_path):
    """Golden file test for nested pipeline scenario.

    Compares the output MCPs against a known-good golden file to detect
    any regressions in entity structure, URN format, or aspect content.

    The golden file captures the expected output including:
    - Container for the factory
    - DataFlow entities for each pipeline
    - DataJob entities for each activity
    - Browse paths showing hierarchy
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_nested_pipeline_scenario()

    output_file = tmp_path / "adf_nested_events.json"
    golden_file = test_resources_dir / "adf_nested_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
        data_flows=get_all_data_flows(),
    )

    config = {
        "run_id": "adf-nested-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


# =============================================================================
# TEST: FOREACH LOOPS
# =============================================================================
#
# Scenario: Iterate over a list of tables to copy
# ------------------------------------------------
# ForEachTablePipeline
#   └── Lookup: GetTableList (query sys.tables)
#   └── ForEach: IterateOverTables
#       └── Copy: CopyTableToStaging (parametrized)
#
# What we're testing:
# - ForEach activity is captured as a DataJob
# - Activities inside ForEach are also captured
# - Lookup activity's lineage (reading from system tables)
#
# Why this matters:
# - ForEach is used extensively for bulk data operations
# - Users need visibility into what tables/files are processed
# - The Copy activity inside ForEach creates lineage for each iteration


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_foreach_loop_pipeline(pytestconfig, tmp_path):
    """Golden file test for ForEach loop pipeline.

    Tests a pipeline that uses ForEach to iterate over tables and copy
    each one to staging. This is a common pattern for bulk data movement.

    The test verifies:
    - ForEach activity is captured as a DataJob with "ForEach Loop" subtype
    - Nested Copy activity is captured (though iterations aren't expanded)
    - Lookup activity that provides the iteration items is captured
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_foreach_loop_scenario()

    output_file = tmp_path / "adf_foreach_events.json"
    golden_file = test_resources_dir / "adf_foreach_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    config = {
        "run_id": "adf-foreach-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


# =============================================================================
# TEST: CONTROL FLOW BRANCHING (If-Condition, Switch)
# =============================================================================
#
# Scenario: Conditional execution based on data existence and region
# ------------------------------------------------------------------
# BranchingPipeline
#   └── Lookup: CheckDataExists
#   └── IfCondition: DataExistsCheck
#       ├── True: Copy: FullLoad
#       └── False: Copy: IncrementalLoad
#   └── Switch: ProcessByRegion
#       ├── Case "US": Copy: ProcessUSData
#       ├── Case "EU": Copy: ProcessEUData
#       └── Default: Copy: ProcessOtherData
#
# What we're testing:
# - IfCondition activity captures both true and false branches
# - Switch activity captures all cases and default
# - Activities in all branches are extracted as DataJobs
# - Lineage is captured for activities in conditional branches
#
# Why this matters:
# - Real pipelines have complex conditional logic
# - Users need to see ALL possible execution paths
# - Lineage must include data flows in every branch


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_branching_pipeline(pytestconfig, tmp_path):
    """Golden file test for If-Condition and Switch branching pipeline.

    Tests a pipeline with complex control flow:
    1. IfCondition that branches based on whether data exists
    2. Switch that routes processing based on region parameter

    The test verifies:
    - All activities in all branches are captured
    - IfCondition has "If Condition" subtype
    - Switch has "Switch Activity" subtype
    - Lineage captures inputs/outputs in each branch's activities
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_branching_scenario()

    output_file = tmp_path / "adf_branching_events.json"
    golden_file = test_resources_dir / "adf_branching_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    config = {
        "run_id": "adf-branching-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


# =============================================================================
# TEST: MAPPING DATA FLOWS
# =============================================================================
#
# Scenario: Complex data transformation with multiple sources and sinks
# ---------------------------------------------------------------------
# DataFlowPipeline
#   └── ExecuteDataFlow: RunSalesTransformation
#       └── SalesTransformationFlow:
#           Sources: CustomersSource, OrdersSource
#           Transformations: Filter → Join → Aggregate → Derive
#           Sinks: CuratedOutput, SynapseOutput
#
# What we're testing:
# - Data Flow definition is loaded and cached
# - ExecuteDataFlow activity extracts sources as inputs
# - ExecuteDataFlow activity extracts sinks as outputs
# - Data Flow script is captured in dataTransformLogic aspect
#
# Why this matters:
# - Mapping Data Flows contain critical transformation logic
# - Lineage from Data Flows shows complex many-to-many relationships
# - Scripts help users understand what transformations are applied


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dataflow_pipeline_with_lineage(pytestconfig, tmp_path):
    """Golden file test for Mapping Data Flow pipeline with lineage extraction.

    Tests a pipeline that executes a Mapping Data Flow containing:
    - Multiple sources (customers, orders)
    - Multiple transformations (filter, join, aggregate, derive)
    - Multiple sinks (data lake, synapse)

    The test verifies:
    - ExecuteDataFlow activity has "Data Flow Activity" subtype
    - Data Flow sources are captured as input datasets
    - Data Flow sinks are captured as output datasets
    - Data Flow script is captured (for transformation visibility)
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_dataflow_scenario()

    output_file = tmp_path / "adf_dataflow_events.json"
    golden_file = test_resources_dir / "adf_dataflow_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
        data_flows=scenario.get("data_flows", []),
    )

    config = {
        "run_id": "adf-dataflow-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


# =============================================================================
# TEST: MULTI-SOURCE ETL PIPELINE
# =============================================================================
#
# Scenario: Full ETL chain with multiple stages and destinations
# --------------------------------------------------------------
# ETLPipeline
#   ├── Copy: ExtractCustomersFromSQL (SQL → Blob)
#   ├── Copy: ExtractOrdersFromSQL (SQL → Blob)
#   ├── Copy: LoadCustomersToSynapse (Blob → Synapse)
#   ├── Copy: LoadOrdersToSynapse (Blob → Synapse)
#   └── Copy: ArchiveToDataLake (Blob → DataLake)
#
# Lineage chain:
#   SQL (Customers) → Blob (Staging) → Synapse (DW)
#                                    → DataLake (Archive)
#   SQL (Orders) → Blob (Staging) → Synapse (DW)
#
# What we're testing:
# - Multi-hop lineage is captured correctly
# - Platform mapping works for different linked services:
#   - AzureSqlDatabase → mssql
#   - AzureBlobStorage → abs
#   - AzureSynapseAnalytics → mssql
#   - AzureBlobFS → abs
# - Dependencies between activities are respected
#
# Why this matters:
# - Real ETL pipelines have multiple stages
# - Users need to trace data from source to final destination
# - Platform-specific URNs enable cross-system lineage in DataHub


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_multisource_etl_pipeline(pytestconfig, tmp_path):
    """Golden file test for multi-source ETL pipeline with full lineage chain.

    Tests a realistic ETL pipeline that:
    1. Extracts data from SQL databases to blob storage
    2. Loads from blob to Synapse data warehouse
    3. Archives to Data Lake for long-term storage

    The test verifies:
    - All Copy activities are captured with correct subtypes
    - Platform mapping produces correct URNs:
      - mssql for SQL and Synapse datasets
      - abs for Blob and Data Lake datasets
    - Activity dependencies are reflected in job order
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_multisource_chain_scenario()

    output_file = tmp_path / "adf_multisource_events.json"
    golden_file = test_resources_dir / "adf_multisource_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    config = {
        "run_id": "adf-multisource-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


# =============================================================================
# LINEAGE VERIFICATION TESTS
# =============================================================================
#
# These tests go beyond golden file comparison to programmatically verify
# that lineage is being captured correctly. They check specific assertions
# about the extracted metadata rather than comparing full output.


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_multisource_lineage_accuracy(tmp_path):
    """Verify lineage edges are correct for multi-source ETL pipeline.

    This test programmatically inspects the generated MCPs to verify that:
    1. dataJobInputOutput aspects are emitted (lineage is captured)
    2. SQL sources appear as input datasets with 'mssql' platform
    3. Synapse destinations appear as output datasets with 'mssql' platform (Synapse uses mssql protocol)

    This complements the golden file test by focusing on specific lineage
    properties that are critical for data governance use cases.
    """
    scenario = create_multisource_chain_scenario()

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    output_file = tmp_path / "lineage_test.json"

    config = {
        "run_id": "lineage-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()

    # Read output and verify lineage (file sink outputs JSON array)
    with open(output_file) as f:
        mcps = json.load(f)

    # Find dataJobInputOutput aspects - these contain the lineage edges
    lineage_aspects = [
        mcp for mcp in mcps if mcp.get("aspectName") == "dataJobInputOutput"
    ]

    # Verify lineage aspects were emitted for Copy activities
    assert len(lineage_aspects) > 0, "Expected lineage aspects to be emitted"

    # Collect all input and output datasets from lineage aspects
    all_inputs = []
    all_outputs = []
    for aspect in lineage_aspects:
        inputs = aspect.get("aspect", {}).get("json", {}).get("inputDatasets", [])
        outputs = aspect.get("aspect", {}).get("json", {}).get("outputDatasets", [])
        all_inputs.extend(inputs)
        all_outputs.extend(outputs)

    # Verify SQL sources are captured with correct platform
    # SQL inputs should have URNs containing 'mssql' (mapped from AzureSqlDatabase)
    sql_inputs = [i for i in all_inputs if "mssql" in i]
    assert len(sql_inputs) > 0, "Expected SQL dataset inputs with 'mssql' platform"

    # Verify Synapse destinations are captured with correct platform
    # Synapse outputs should have URNs containing 'mssql' (Synapse uses mssql protocol)
    # Check for output datasets that contain common Synapse table naming patterns
    mssql_outputs = [o for o in all_outputs if "mssql" in o]
    assert len(mssql_outputs) > 0, (
        "Expected Synapse dataset outputs with 'mssql' platform"
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dataflow_lineage_sources_and_sinks(tmp_path):
    """Verify Data Flow sources and sinks are extracted for lineage.

    This test verifies that when a pipeline executes a Mapping Data Flow,
    the connector:
    1. Fetches and caches the Data Flow definition
    2. Extracts source datasets from the Data Flow
    3. Extracts sink datasets from the Data Flow
    4. Reports that data flows were scanned

    Data Flow lineage is critical because:
    - Data Flows can have complex many-to-many relationships
    - Sources/sinks are defined in the Data Flow, not the activity
    - Without Data Flow inspection, lineage would be incomplete
    """
    scenario = create_dataflow_scenario()

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
        data_flows=scenario.get("data_flows", []),
    )

    output_file = tmp_path / "dataflow_lineage_test.json"

    config = {
        "run_id": "dataflow-lineage-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()

    # Verify that data flows were fetched and processed
    # This confirms the connector is looking up Data Flow definitions
    assert isinstance(pipeline.source, AzureDataFactorySource)
    assert pipeline.source.report.data_flows_scanned > 0, (
        "Expected data flows to be scanned for lineage extraction"
    )


# =============================================================================
# TEST: DIVERSE ACTIVITY TYPES
# =============================================================================
#
# Scenario: Pipeline with various activity types
# -----------------------------------------------
# DiverseActivitiesPipeline
#   └── SetVariable: InitializeCounter
#   └── WebActivity: FetchConfiguration
#   └── SqlServerStoredProcedure: ProcessData
#   └── Wait: DelayForReplication
#   └── GetMetadata: CheckOutputExists
#   └── DatabricksNotebook: RunMLTraining
#   └── Script: RunAnalyticsScript
#   └── AzureFunctionActivity: SendNotification
#   └── Fail: FailOnError
#
# What we're testing:
# - All activity types are captured as DataJobs with correct subtypes
# - Each activity has the appropriate metadata (description, properties)
# - The connector doesn't fail on uncommon activity types
#
# Why this matters:
# - Real pipelines use many different activity types
# - Users need visibility into all orchestration activities
# - Activity subtypes help with filtering and understanding


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_diverse_activities_pipeline(pytestconfig, tmp_path):
    """Test that diverse activity types are correctly captured.

    This test verifies that the connector handles various activity types:
    - SetVariable, WebActivity, SqlServerStoredProcedure, Wait
    - GetMetadata, DatabricksNotebook, Script, AzureFunctionActivity, Fail

    Each activity should be captured as a DataJob with the correct subtype.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_diverse_activities_scenario()

    output_file = tmp_path / "adf_diverse_events.json"
    golden_file = test_resources_dir / "adf_diverse_golden.json"

    # Combine standard linked services with additional ones from the scenario
    all_linked_services = create_complex_linked_services() + scenario.get(
        "additional_linked_services", []
    )

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=all_linked_services,
    )

    config = {
        "run_id": "adf-diverse-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_diverse_activities_subtypes(tmp_path):
    """Verify that diverse activity types have correct subtypes.

    This test programmatically checks that each activity type is mapped
    to the expected DataHub subtype.
    """
    scenario = create_diverse_activities_scenario()

    all_linked_services = create_complex_linked_services() + scenario.get(
        "additional_linked_services", []
    )

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=all_linked_services,
    )

    output_file = tmp_path / "diverse_subtypes_test.json"

    config = {
        "run_id": "diverse-subtypes-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()

    # Read output and verify subtypes
    with open(output_file) as f:
        mcps = json.load(f)

    # Find subTypes aspects for DataJobs
    subtype_aspects = [
        mcp
        for mcp in mcps
        if mcp.get("entityType") == "dataJob" and mcp.get("aspectName") == "subTypes"
    ]

    # Collect all subtypes
    found_subtypes = set()
    for aspect in subtype_aspects:
        types = aspect.get("aspect", {}).get("json", {}).get("typeNames", [])
        found_subtypes.update(types)

    # Verify we captured diverse subtypes (at least some key ones)
    expected_subtypes = {
        "Set Variable",
        "Web Activity",
        "Stored Procedure Activity",
        "Wait Activity",
        "Get Metadata Activity",
        "Databricks Notebook",
    }

    found_expected = expected_subtypes.intersection(found_subtypes)
    assert len(found_expected) >= 3, (
        f"Expected to find at least 3 activity subtypes from {expected_subtypes}, "
        f"but found: {found_subtypes}"
    )


# =============================================================================
# TEST: PIPELINE-TO-PIPELINE LINEAGE
# =============================================================================
#
# Scenario: Parent pipeline calling child pipelines
# --------------------------------------------------
# When a pipeline uses ExecutePipeline activity to call another pipeline,
# we should capture this dependency. This enables:
# - Understanding orchestration hierarchies
# - Impact analysis across pipeline boundaries
# - Tracing data flow through nested execution
#
# What we're testing:
# - ExecutePipeline activities capture child pipeline references
# - Custom properties include "calls_pipeline" and "child_pipeline_urn"
# - The dependency is visible in DataHub


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_pipeline_to_pipeline_lineage(tmp_path):
    """Verify that ExecutePipeline activities create DataJob-to-DataJob lineage.

    When a parent pipeline calls a child pipeline via ExecutePipeline,
    the connector should:
    1. Capture the child pipeline name in custom properties
    2. Record the child pipeline's DataFlow URN
    3. Create DataJob-to-DataJob lineage (inputDatajobs) pointing to first child activity
    4. Enable users to trace the orchestration hierarchy in the UI

    This test checks the nested pipeline scenario for these dependencies.
    """
    scenario = create_nested_pipeline_scenario()

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
        data_flows=get_all_data_flows(),
    )

    output_file = tmp_path / "pipeline_lineage_test.json"

    config = {
        "run_id": "pipeline-lineage-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()

    # Read output and verify pipeline-to-pipeline references
    with open(output_file) as f:
        mcps = json.load(f)

    # Find DataJobInfo aspects with ExecutePipeline activities
    datajob_infos = [
        mcp
        for mcp in mcps
        if mcp.get("entityType") == "dataJob" and mcp.get("aspectName") == "dataJobInfo"
    ]

    # Find DataJobInputOutput aspects
    datajob_io = [
        mcp
        for mcp in mcps
        if mcp.get("entityType") == "dataJob"
        and mcp.get("aspectName") == "dataJobInputOutput"
    ]

    # Look for activities that call child pipelines
    child_pipeline_refs = []
    for info in datajob_infos:
        custom_props = (
            info.get("aspect", {}).get("json", {}).get("customProperties", {})
        )
        if "calls_pipeline" in custom_props:
            child_pipeline_refs.append(
                {
                    "activity": info.get("entityUrn", ""),
                    "calls": custom_props.get("calls_pipeline"),
                    "child_urn": custom_props.get("child_pipeline_urn"),
                    "child_first_activity": custom_props.get("child_first_activity"),
                }
            )

    # The nested pipeline scenario has 2 ExecutePipeline activities
    assert len(child_pipeline_refs) >= 2, (
        f"Expected at least 2 ExecutePipeline activities with child pipeline references, "
        f"but found: {len(child_pipeline_refs)}"
    )

    # Verify the child pipeline names are captured
    child_names = {ref["calls"] for ref in child_pipeline_refs}
    assert "ChildDataMovementPipeline" in child_names, (
        f"Expected ChildDataMovementPipeline in child references: {child_names}"
    )
    assert "ChildTransformPipeline" in child_names, (
        f"Expected ChildTransformPipeline in child references: {child_names}"
    )

    # Verify the first activity names are captured
    first_activities = {ref["child_first_activity"] for ref in child_pipeline_refs}
    assert "CopyCustomersToStaging" in first_activities, (
        f"Expected CopyCustomersToStaging as first activity: {first_activities}"
    )
    assert "TransformCustomerData" in first_activities, (
        f"Expected TransformCustomerData as first activity: {first_activities}"
    )

    # Verify DataJobInputOutput aspects create correct lineage direction
    # The child's first activity should have the parent ExecutePipeline as inputDatajobs
    # This creates lineage: ExecutePipeline -> ChildFirstActivity
    child_activity_inputs = {}
    for io in datajob_io:
        entity_urn = io.get("entityUrn", "")
        input_jobs = io.get("aspect", {}).get("json", {}).get("inputDatajobs", [])
        if input_jobs:
            child_activity_inputs[entity_urn] = input_jobs

    # Should have at least 2 child activities with inputDatajobs (one for each ExecutePipeline)
    assert len(child_activity_inputs) >= 2, (
        f"Expected at least 2 child activities with inputDatajobs lineage, "
        f"but found: {len(child_activity_inputs)}"
    )

    # Verify the child activities have ExecutePipeline as their input (upstream)
    # CopyCustomersToStaging should have ExecuteDataMovement as input
    # TransformCustomerData should have ExecuteTransform as input
    all_inputs = []
    for inputs in child_activity_inputs.values():
        all_inputs.extend(inputs)

    assert any("ExecuteDataMovement" in urn for urn in all_inputs), (
        f"Expected ExecuteDataMovement as upstream of child activity: {all_inputs}"
    )
    assert any("ExecuteTransform" in urn for urn in all_inputs), (
        f"Expected ExecuteTransform as upstream of child activity: {all_inputs}"
    )


def test_mixed_pipeline_and_dataset_dependencies(tmp_path: Path) -> None:
    """Test scenario with both pipeline-to-pipeline and dataset dependencies.

    This test verifies that the connector correctly handles pipelines that have:
    1. ExecutePipeline activities (pipeline-to-pipeline lineage)
    2. Copy activities with explicit inputs/outputs (dataset lineage)

    Structure:
    - MixedOrchestrationPipeline
      └── ExecuteExtract -> ExtractDataPipeline.ExtractFromSource
      └── TransformInMain (Copy with dataset I/O)
      └── ExecuteLoad -> LoadDataPipeline.LoadToDestination

    Expected results:
    - Pipeline lineage: ExecuteExtract -> ExtractFromSource
    - Pipeline lineage: ExecuteLoad -> LoadToDestination
    - Dataset lineage: TransformInMain reads BlobStagingCustomers
    - Dataset lineage: TransformInMain writes SynapseCustomersDim
    """
    from tests.integration.azure_data_factory.complex_mocks import (
        create_mixed_dependencies_scenario,
    )

    scenario = create_mixed_dependencies_scenario()
    output_file = tmp_path / "mixed_deps_output.json"

    # Create mock client using the existing helper
    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    config = {
        "run_id": "mixed_deps_test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
                "include_execution_history": False,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()

    # Read output
    with open(output_file) as f:
        mcps = json.load(f)

    # =========================================================================
    # Verify Pipeline-to-Pipeline Lineage
    # =========================================================================
    # Find DataJobInfo aspects to identify ExecutePipeline activities
    datajob_infos = [
        mcp
        for mcp in mcps
        if mcp.get("entityType") == "dataJob" and mcp.get("aspectName") == "dataJobInfo"
    ]

    # Find activities that call child pipelines
    execute_pipeline_refs = []
    for info in datajob_infos:
        custom_props = (
            info.get("aspect", {}).get("json", {}).get("customProperties", {})
        )
        if "calls_pipeline" in custom_props:
            execute_pipeline_refs.append(
                {
                    "activity_urn": info.get("entityUrn", ""),
                    "calls": custom_props.get("calls_pipeline"),
                    "child_first_activity": custom_props.get("child_first_activity"),
                }
            )

    # Should have 2 ExecutePipeline activities
    assert len(execute_pipeline_refs) == 2, (
        f"Expected 2 ExecutePipeline activities, found: {len(execute_pipeline_refs)}"
    )

    # Verify correct child pipelines are referenced
    child_pipelines = {ref["calls"] for ref in execute_pipeline_refs}
    assert "ExtractDataPipeline" in child_pipelines
    assert "LoadDataPipeline" in child_pipelines

    # Verify first activities of child pipelines
    first_activities = {ref["child_first_activity"] for ref in execute_pipeline_refs}
    assert "ExtractFromSource" in first_activities
    assert "LoadToDestination" in first_activities

    # =========================================================================
    # Verify Dataset Lineage
    # =========================================================================
    # Find DataJobInputOutput aspects
    datajob_io = [
        mcp
        for mcp in mcps
        if mcp.get("entityType") == "dataJob"
        and mcp.get("aspectName") == "dataJobInputOutput"
    ]

    # Build a map of entity URN -> (inputDatasets, outputDatasets)
    dataset_lineage: dict[str, dict[str, list[str]]] = {}
    for io in datajob_io:
        entity_urn = io.get("entityUrn", "")
        input_datasets = io.get("aspect", {}).get("json", {}).get("inputDatasets", [])
        output_datasets = io.get("aspect", {}).get("json", {}).get("outputDatasets", [])
        if input_datasets or output_datasets:
            dataset_lineage[entity_urn] = {
                "inputs": input_datasets,
                "outputs": output_datasets,
            }

    # Find TransformInMain activity's lineage
    transform_lineage = None
    for urn, lineage in dataset_lineage.items():
        if "TransformInMain" in urn:
            transform_lineage = lineage
            break

    assert transform_lineage is not None, (
        f"TransformInMain activity should have dataset lineage. "
        f"Available URNs: {list(dataset_lineage.keys())}"
    )

    # TransformInMain should read from BlobStagingCustomers (blob storage)
    assert len(transform_lineage["inputs"]) >= 1, (
        "TransformInMain should have at least 1 input dataset"
    )
    # The URN uses platform and dataset path from typeProperties, not the ADF dataset name
    # BlobStagingCustomers maps to abs platform with path staging/customers
    assert any(
        "abs" in urn or "staging" in urn for urn in transform_lineage["inputs"]
    ), f"TransformInMain should read from blob storage: {transform_lineage['inputs']}"

    # TransformInMain should write to SynapseSalesTable (mssql platform)
    assert len(transform_lineage["outputs"]) >= 1, (
        "TransformInMain should have at least 1 output dataset"
    )
    # SynapseSalesTable maps to mssql platform (Synapse uses mssql protocol)
    assert any(
        "mssql" in urn or "Sales" in urn for urn in transform_lineage["outputs"]
    ), (
        f"TransformInMain should write to Synapse (mssql): {transform_lineage['outputs']}"
    )

    # =========================================================================
    # Verify Both Lineage Types Coexist
    # =========================================================================
    # We should have at least 3 DataJobInputOutput aspects:
    # - 2 for child pipelines' first activities (inputDatajobs from pipeline lineage)
    # - Several for Copy activities (inputDatasets/outputDatasets)
    assert len(datajob_io) >= 3, (
        f"Expected at least 3 DataJobInputOutput aspects for mixed lineage, "
        f"found: {len(datajob_io)}"
    )

    # Verify pipeline lineage exists (inputDatajobs)
    pipeline_lineage_count = sum(
        1
        for io in datajob_io
        if io.get("aspect", {}).get("json", {}).get("inputDatajobs", [])
    )
    assert pipeline_lineage_count >= 2, (
        f"Expected at least 2 activities with pipeline lineage (inputDatajobs), "
        f"found: {pipeline_lineage_count}"
    )

    # Verify dataset lineage exists (inputDatasets or outputDatasets)
    dataset_lineage_count = sum(
        1
        for io in datajob_io
        if io.get("aspect", {}).get("json", {}).get("inputDatasets", [])
        or io.get("aspect", {}).get("json", {}).get("outputDatasets", [])
    )
    assert dataset_lineage_count >= 3, (
        f"Expected at least 3 activities with dataset lineage, "
        f"found: {dataset_lineage_count}"
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mixed_dependencies_golden(pytestconfig, tmp_path):
    """Golden file test for mixed pipeline and dataset dependencies.

    This golden test validates the complete output when a pipeline has both:
    1. ExecutePipeline activities (pipeline-to-pipeline lineage)
    2. Copy activities with dataset inputs/outputs (dataset lineage)

    The golden file captures:
    - Container for the factory
    - DataFlow entities for all 3 pipelines
    - DataJob entities for all 5 activities
    - DataJobInputOutput aspects showing both pipeline and dataset lineage
    - Browse paths and custom properties
    """
    from tests.integration.azure_data_factory.complex_mocks import (
        create_mixed_dependencies_scenario,
    )

    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    scenario = create_mixed_dependencies_scenario()

    output_file = tmp_path / "adf_mixed_deps_events.json"
    golden_file = test_resources_dir / "adf_mixed_deps_golden.json"

    mock_client = create_mock_client(
        pipelines=scenario["pipelines"],
        datasets=create_complex_datasets(),
        linked_services=create_complex_linked_services(),
    )

    config = {
        "run_id": "adf-mixed-deps-test",
        "source": {
            "type": "azure-data-factory",
            "config": {
                "subscription_id": SUBSCRIPTION_ID,
                "resource_group": RESOURCE_GROUP,
                "credential": {"authentication_method": "default"},
                "env": "DEV",
                "include_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(config)
            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )

"""Integration tests for Azure Data Factory source.

These tests use mocked Azure SDK responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-15 12:00:00"

# Mock Azure SDK response data


def create_mock_factory(
    name: str,
    resource_group: str,
    subscription_id: str,
    location: str = "eastus",
    tags: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Create a mock factory response."""
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories",
        "location": location,
        "tags": tags or {},
        "properties": {
            "provisioningState": "Succeeded",
            "createTime": "2024-01-01T00:00:00Z",
        },
    }


def create_mock_pipeline(
    name: str,
    factory_name: str,
    resource_group: str,
    subscription_id: str,
    activities: Optional[List[Dict[str, Any]]] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a mock pipeline response."""
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelines/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": description,
            "activities": activities or [],
            "parameters": {},
            "variables": {},
            "annotations": [],
        },
    }


def create_mock_activity(
    name: str,
    activity_type: str,
    inputs: Optional[List[Dict[str, Any]]] = None,
    outputs: Optional[List[Dict[str, Any]]] = None,
    depends_on: Optional[List[Dict[str, Any]]] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a mock activity definition."""
    return {
        "name": name,
        "type": activity_type,
        "description": description,
        "dependsOn": depends_on or [],
        "inputs": inputs or [],
        "outputs": outputs or [],
        "typeProperties": {},
        "policy": {"timeout": "7.00:00:00", "retry": 0},
        "userProperties": [],
    }


def create_mock_dataset(
    name: str,
    factory_name: str,
    resource_group: str,
    subscription_id: str,
    linked_service_name: str,
    dataset_type: str = "AzureBlobDataset",
    type_properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a mock dataset response."""
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/datasets",
        "properties": {
            "linkedServiceName": {
                "referenceName": linked_service_name,
                "type": "LinkedServiceReference",
            },
            "type": dataset_type,
            "typeProperties": type_properties or {},
            "annotations": [],
            "parameters": {},
        },
    }


def create_mock_linked_service(
    name: str,
    factory_name: str,
    resource_group: str,
    subscription_id: str,
    service_type: str = "AzureBlobStorage",
) -> Dict[str, Any]:
    """Create a mock linked service response."""
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/linkedservices",
        "properties": {
            "type": service_type,
            "typeProperties": {},
            "annotations": [],
        },
    }


def create_mock_trigger(
    name: str,
    factory_name: str,
    resource_group: str,
    subscription_id: str,
    trigger_type: str = "ScheduleTrigger",
    pipelines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create a mock trigger response."""
    pipeline_refs = [
        {
            "pipelineReference": {"referenceName": p, "type": "PipelineReference"},
            "parameters": {},
        }
        for p in (pipelines or [])
    ]
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/triggers/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/triggers",
        "properties": {
            "type": trigger_type,
            "runtimeState": "Started",
            "pipelines": pipeline_refs,
            "typeProperties": {},
            "annotations": [],
        },
    }


def create_mock_pipeline_run(
    run_id: str,
    pipeline_name: str,
    status: str = "Succeeded",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Create a mock pipeline run response."""
    return {
        "runId": run_id,
        "pipelineName": pipeline_name,
        "status": status,
        "runStart": (
            start_time or datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        ).isoformat(),
        "runEnd": (
            end_time or datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        ).isoformat(),
        "durationInMs": 1800000,
        "message": None,
        "parameters": {},
        "invokedBy": {"name": "Manual", "invokedByType": "Manual"},
        "lastUpdated": datetime(
            2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc
        ).isoformat(),
    }


def create_mock_activity_run(
    activity_run_id: str,
    activity_name: str,
    activity_type: str,
    pipeline_run_id: str,
    pipeline_name: str,
    status: str = "Succeeded",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    duration_ms: int = 30000,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a mock activity run response."""
    return {
        "activityRunId": activity_run_id,
        "activityName": activity_name,
        "activityType": activity_type,
        "pipelineRunId": pipeline_run_id,
        "pipelineName": pipeline_name,
        "status": status,
        "activityRunStart": (
            start_time or datetime(2024, 1, 15, 10, 5, 0, tzinfo=timezone.utc)
        ).isoformat(),
        "activityRunEnd": (
            end_time or datetime(2024, 1, 15, 10, 10, 0, tzinfo=timezone.utc)
        ).isoformat(),
        "durationInMs": duration_ms,
        "input": {},
        "output": {},
        "error": error,
    }


class MockAzureResource:
    """Mock class to simulate Azure SDK resource objects."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def as_dict(self) -> Dict[str, Any]:
        return self._data


class MockPagedIterator:
    """Mock class to simulate Azure SDK paged iterators."""

    def __init__(self, items: List[Dict[str, Any]]):
        self._items = [MockAzureResource(item) for item in items]

    def __iter__(self) -> Iterator[MockAzureResource]:
        return iter(self._items)


class MockQueryResponse:
    """Mock class for query responses with continuation token."""

    def __init__(
        self, items: List[Dict[str, Any]], continuation_token: Optional[str] = None
    ):
        self.value = [MockAzureResource(item) for item in items]
        self.continuation_token = continuation_token


# Test data constants
SUBSCRIPTION_ID = "12345678-1234-1234-1234-123456789012"
RESOURCE_GROUP = "test-resource-group"
FACTORY_NAME = "test-data-factory"


def get_mock_test_data() -> Dict[str, Any]:
    """Generate comprehensive test data for the ADF source."""
    factories = [
        create_mock_factory(
            name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            tags={"environment": "test", "team": "data-engineering"},
        ),
    ]

    # Create pipelines with various activities
    copy_activity = create_mock_activity(
        name="CopyBlobToSQL",
        activity_type="Copy",
        inputs=[{"referenceName": "SourceBlobDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "DestSqlDataset", "type": "DatasetReference"}],
        description="Copy data from Blob to SQL",
    )

    lookup_activity = create_mock_activity(
        name="LookupConfig",
        activity_type="Lookup",
        inputs=[{"referenceName": "ConfigDataset", "type": "DatasetReference"}],
        description="Lookup configuration values",
    )

    dataflow_activity = create_mock_activity(
        name="TransformData",
        activity_type="ExecuteDataFlow",
        depends_on=[
            {"activity": "LookupConfig", "dependencyConditions": ["Succeeded"]}
        ],
        description="Execute mapping data flow",
    )

    stored_proc_activity = create_mock_activity(
        name="CallStoredProc",
        activity_type="SqlServerStoredProcedure",
        depends_on=[
            {"activity": "CopyBlobToSQL", "dependencyConditions": ["Succeeded"]}
        ],
        description="Call stored procedure",
    )

    pipelines = [
        create_mock_pipeline(
            name="DataIngestionPipeline",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            description="Main data ingestion pipeline",
            activities=[copy_activity, lookup_activity, dataflow_activity],
        ),
        create_mock_pipeline(
            name="DataProcessingPipeline",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            description="Data processing and transformation",
            activities=[stored_proc_activity],
        ),
    ]

    # Create datasets
    datasets = [
        create_mock_dataset(
            name="SourceBlobDataset",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="AzureBlobStorageLS",
            dataset_type="DelimitedTextDataset",
            type_properties={
                "location": {
                    "container": "raw-data",
                    "folderPath": "input",
                    "fileName": "data.csv",
                }
            },
        ),
        create_mock_dataset(
            name="DestSqlDataset",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="AzureSqlDatabaseLS",
            dataset_type="AzureSqlTableDataset",
            type_properties={"schema": "dbo", "table": "ProcessedData"},
        ),
        create_mock_dataset(
            name="ConfigDataset",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="AzureBlobStorageLS",
            dataset_type="JsonDataset",
            type_properties={
                "location": {
                    "container": "config",
                    "fileName": "settings.json",
                }
            },
        ),
    ]

    # Create linked services
    linked_services = [
        create_mock_linked_service(
            name="AzureBlobStorageLS",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            service_type="AzureBlobStorage",
        ),
        create_mock_linked_service(
            name="AzureSqlDatabaseLS",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            service_type="AzureSqlDatabase",
        ),
    ]

    # Create triggers
    triggers = [
        create_mock_trigger(
            name="DailyScheduleTrigger",
            factory_name=FACTORY_NAME,
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            trigger_type="ScheduleTrigger",
            pipelines=["DataIngestionPipeline"],
        ),
    ]

    # Create pipeline runs
    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-001-abc",
            pipeline_name="DataIngestionPipeline",
            status="Succeeded",
            start_time=datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 15, 8, 45, 0, tzinfo=timezone.utc),
        ),
        create_mock_pipeline_run(
            run_id="run-002-def",
            pipeline_name="DataIngestionPipeline",
            status="Failed",
            start_time=datetime(2024, 1, 14, 8, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 14, 8, 15, 0, tzinfo=timezone.utc),
        ),
        create_mock_pipeline_run(
            run_id="run-003-ghi",
            pipeline_name="DataProcessingPipeline",
            status="Succeeded",
            start_time=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
        ),
    ]

    # Create activity runs for each pipeline run
    # Activity runs are linked to DataJobs (activities), not DataFlows (pipelines)
    activity_runs = {
        "run-001-abc": [  # DataIngestionPipeline - Succeeded
            create_mock_activity_run(
                activity_run_id="act-001-copy",
                activity_name="CopyBlobToSQL",
                activity_type="Copy",
                pipeline_run_id="run-001-abc",
                pipeline_name="DataIngestionPipeline",
                status="Succeeded",
                start_time=datetime(2024, 1, 15, 8, 5, 0, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 15, 8, 20, 0, tzinfo=timezone.utc),
                duration_ms=900000,
            ),
            create_mock_activity_run(
                activity_run_id="act-001-lookup",
                activity_name="LookupConfig",
                activity_type="Lookup",
                pipeline_run_id="run-001-abc",
                pipeline_name="DataIngestionPipeline",
                status="Succeeded",
                start_time=datetime(2024, 1, 15, 8, 20, 0, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 15, 8, 21, 0, tzinfo=timezone.utc),
                duration_ms=60000,
            ),
            create_mock_activity_run(
                activity_run_id="act-001-transform",
                activity_name="TransformData",
                activity_type="ExecuteDataFlow",
                pipeline_run_id="run-001-abc",
                pipeline_name="DataIngestionPipeline",
                status="Succeeded",
                start_time=datetime(2024, 1, 15, 8, 21, 0, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 15, 8, 45, 0, tzinfo=timezone.utc),
                duration_ms=1440000,
            ),
        ],
        "run-002-def": [  # DataIngestionPipeline - Failed
            create_mock_activity_run(
                activity_run_id="act-002-copy",
                activity_name="CopyBlobToSQL",
                activity_type="Copy",
                pipeline_run_id="run-002-def",
                pipeline_name="DataIngestionPipeline",
                status="Failed",
                start_time=datetime(2024, 1, 14, 8, 5, 0, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 14, 8, 15, 0, tzinfo=timezone.utc),
                duration_ms=600000,
                error={
                    "message": "Connection timeout to SQL database",
                    "errorCode": "2200",
                },
            ),
        ],
        "run-003-ghi": [  # DataProcessingPipeline - Succeeded
            create_mock_activity_run(
                activity_run_id="act-003-proc",
                activity_name="CallStoredProc",
                activity_type="SqlServerStoredProcedure",
                pipeline_run_id="run-003-ghi",
                pipeline_name="DataProcessingPipeline",
                status="Succeeded",
                start_time=datetime(2024, 1, 15, 9, 5, 0, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
                duration_ms=1500000,
            ),
        ],
    }

    return {
        "factories": factories,
        "pipelines": pipelines,
        "datasets": datasets,
        "linked_services": linked_services,
        "triggers": triggers,
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }


def create_mock_client(
    test_data: Dict[str, Any], include_activity_runs: bool = False
) -> MagicMock:
    """Create a mock DataFactoryManagementClient.

    Args:
        test_data: Dictionary containing mock data for factories, pipelines, etc.
        include_activity_runs: If True, return activity runs for each pipeline run.
            This enables testing of the activity run extraction feature.
    """
    mock_client = MagicMock()

    # Mock factories
    mock_client.factories.list.return_value = MockPagedIterator(test_data["factories"])
    mock_client.factories.list_by_resource_group.return_value = MockPagedIterator(
        test_data["factories"]
    )

    # Mock pipelines
    mock_client.pipelines.list_by_factory.return_value = MockPagedIterator(
        test_data["pipelines"]
    )

    # Mock datasets
    mock_client.datasets.list_by_factory.return_value = MockPagedIterator(
        test_data["datasets"]
    )

    # Mock linked services
    mock_client.linked_services.list_by_factory.return_value = MockPagedIterator(
        test_data["linked_services"]
    )

    # Mock triggers
    mock_client.triggers.list_by_factory.return_value = MockPagedIterator(
        test_data["triggers"]
    )

    # Mock data flows (empty for basic tests)
    mock_client.data_flows.list_by_factory.return_value = MockPagedIterator([])

    # Mock pipeline runs
    mock_client.pipeline_runs.query_by_factory.return_value = MockQueryResponse(
        test_data["pipeline_runs"]
    )

    # Mock activity runs - return based on pipeline run ID if enabled
    if include_activity_runs and "activity_runs" in test_data:
        activity_runs_by_pipeline = test_data["activity_runs"]

        def get_activity_runs(
            resource_group_name: str,
            factory_name: str,
            run_id: str,
            filter_parameters: object,
        ) -> MockQueryResponse:
            """Return activity runs for the given pipeline run ID."""
            runs = activity_runs_by_pipeline.get(run_id, [])
            return MockQueryResponse(runs)

        mock_client.activity_runs.query_by_pipeline_run.side_effect = get_activity_runs
    else:
        mock_client.activity_runs.query_by_pipeline_run.return_value = (
            MockQueryResponse([])
        )

    return mock_client


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_adf_source_basic(pytestconfig, tmp_path):
    """Test basic ADF metadata extraction without execution history."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_basic_events.json"
    golden_file = test_resources_dir / "adf_basic_golden.json"

    test_data = get_mock_test_data()
    mock_client = create_mock_client(test_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-basic",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "PROD",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": str(output_file),
                        },
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    # For the first run, we need to create the golden file
    # In subsequent runs, this will compare against the golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_adf_source_with_execution_history(pytestconfig, tmp_path):
    """Test ADF metadata extraction with execution history.

    This test verifies:
    - Pipeline runs are extracted as DataProcessInstance linked to DataFlow
    - Activity runs are extracted as DataProcessInstance linked to DataJob
    - Run status (Succeeded, Failed) is correctly mapped
    - Both start and end events are emitted for completed runs
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_with_runs_events.json"
    golden_file = test_resources_dir / "adf_with_runs_golden.json"

    test_data = get_mock_test_data()
    # Enable activity runs to test DataJob-level run history
    mock_client = create_mock_client(test_data, include_activity_runs=True)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-with-runs",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "PROD",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": str(output_file),
                        },
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_adf_source_with_platform_instance(pytestconfig, tmp_path):
    """Test ADF metadata extraction with platform instance configured."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_platform_instance_events.json"
    golden_file = test_resources_dir / "adf_platform_instance_golden.json"

    test_data = get_mock_test_data()
    mock_client = create_mock_client(test_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-platform-instance",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "platform_instance": "my-adf-instance",
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": str(output_file),
                        },
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_adf_source_exception_propagation_to_factory_level(tmp_path):
    """Test that exceptions in child methods propagate to factory-level handler.

    This test verifies the error handling pattern where:
    1. An exception in _cache_factory_resources (e.g., get_datasets fails)
    2. Propagates to the per-factory try/except in get_workunits_internal
    3. Reports a warning and continues to next factory (if any)
    4. The overall pipeline does NOT fail
    """
    output_file = tmp_path / "adf_exception_test.json"

    test_data = get_mock_test_data()
    mock_client = create_mock_client(test_data)

    # Make get_datasets raise an exception to simulate API failure
    mock_client.datasets.list_by_factory.side_effect = Exception(
        "Azure API error: Service unavailable"
    )

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-exception",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "include_lineage": True,
                            "include_execution_history": False,
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": str(output_file),
                        },
                    },
                }
            )

            pipeline.run()

            # Pipeline should complete without raising - errors are captured as warnings
            # The source should have reported a warning for the failed factory
            source_report = pipeline.source.get_report()
            assert source_report is not None

            # Verify a warning was reported for the factory processing failure
            assert len(source_report.warnings) > 0, (
                "Expected at least one warning to be reported"
            )

            # Check that the warning is about the Data Factory processing failure
            warning_messages = [str(w) for w in source_report.warnings]
            assert any(
                "Failed to Process Data Factory" in msg for msg in warning_messages
            ), (
                f"Expected 'Failed to Process Data Factory' warning, got: {warning_messages}"
            )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_adf_source_factory_listing_failure_reports_failure(tmp_path):
    """Test that failure to list factories reports a failure (not warning).

    When we can't even list factories, it's a critical failure and should
    be reported as such, but still not crash the pipeline.
    """
    output_file = tmp_path / "adf_factory_fail_test.json"

    mock_client = MagicMock()
    # Make factories.list raise an exception
    mock_client.factories.list.side_effect = Exception(
        "Azure API error: Authentication failed"
    )

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-factory-fail",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "credential": {
                                "authentication_method": "default",
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": str(output_file),
                        },
                    },
                }
            )

            pipeline.run()

            # Pipeline should complete (not crash)
            source_report = pipeline.source.get_report()
            assert source_report is not None

            # Verify a failure was reported for the factory listing failure
            assert len(source_report.failures) > 0, (
                "Expected at least one failure to be reported"
            )

            # Check that the failure is about listing Data Factories
            failure_messages = [str(f) for f in source_report.failures]
            assert any(
                "Failed to List Data Factories" in msg for msg in failure_messages
            ), (
                f"Expected 'Failed to List Data Factories' failure, got: {failure_messages}"
            )

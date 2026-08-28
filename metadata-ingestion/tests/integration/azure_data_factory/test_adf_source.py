"""Integration tests for Azure Data Factory source.

These tests use mocked Azure SDK responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, cast
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.azure_data_factory.adf_report import (
    AzureDataFactorySourceReport,
)
from datahub.testing import mce_helpers
from tests.integration.azure_data_factory.complex_mocks import (
    RESOURCE_GROUP as COMPLEX_RESOURCE_GROUP,
    SUBSCRIPTION_ID as COMPLEX_SUBSCRIPTION_ID,
    create_branching_scenario,
    create_complex_datasets,
    create_complex_factory,
    create_complex_linked_services,
    create_dataflow_scenario,
    create_diverse_activities_scenario,
    create_foreach_loop_scenario,
    create_mixed_dependencies_scenario,
    create_multisource_chain_scenario,
    create_nested_pipeline_scenario,
)

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
    dataset: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a mock activity definition.

    "dataset" models a LookupActivity's own dataset reference - unlike
    Copy, the real SDK's LookupActivity has no "inputs"/"outputs" fields
    at all, only "dataset".
    """
    result: Dict[str, Any] = {
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
    if dataset is not None:
        result["dataset"] = dataset
    return result


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
    connection_string: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    type_properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a mock linked service response."""
    type_properties = dict(type_properties or {})
    if connection_string is not None:
        type_properties["connectionString"] = connection_string
    return {
        "id": f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/linkedservices",
        "properties": {
            "type": service_type,
            "typeProperties": type_properties,
            "annotations": [],
            "parameters": parameters or {},
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
    parameters: Optional[Dict[str, str]] = None,
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
        "parameters": parameters or {},
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
    """Mock class to simulate Azure SDK resource objects.

    Exposes dictionary data as attributes (like Azure SDK models) and supports
    nested attribute access for properties like `linked_service_name.reference_name`.

    The Azure SDK models expose properties at the top level (not nested under
    a 'properties' dict), so this mock looks in both the top level AND the
    'properties' dict for backwards compatibility with test data.

    For activities, also checks 'typeProperties' since SDK activity subclasses
    expose typeProperties contents as direct attributes (e.g., activity.pipeline
    instead of activity.typeProperties.pipeline).
    """

    # Attributes that should return the raw dict (for dict-like access)
    _DICT_PASSTHROUGH_ATTRS = {
        "tags",
        "parameters",
        "invoked_by",
        "error",
        "input",
        "output",
    }

    # The real SDK returns these as datetime objects; the mock stores them as
    # ISO strings, so parse them back here to match (production code calls
    # .timestamp() on them).
    _DATETIME_ATTRS = {
        "run_start",
        "run_end",
        "activity_run_start",
        "activity_run_end",
        "last_updated",
    }

    def __init__(self, data: Dict[str, Any]):
        self._data = data
        # Extract properties to top level for SDK-like access
        self._properties = data.get("properties", {})
        # For activities, typeProperties are also exposed at top level
        self._type_properties = data.get("typeProperties", {})

    def __getattr__(self, name: str) -> Any:
        # Convert snake_case to camelCase for Azure API compatibility
        # e.g., linked_service_name -> linkedServiceName
        camel_name = "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(name.split("_"))
        )

        value = None
        found = False

        # Try top-level first (snake_case then camelCase)
        if name in self._data:
            value = self._data[name]
            found = True
        elif camel_name in self._data:
            value = self._data[camel_name]
            found = True
        # Then try properties dict (SDK models expose these at top level)
        elif name in self._properties:
            value = self._properties[name]
            found = True
        elif camel_name in self._properties:
            value = self._properties[camel_name]
            found = True
        # Also try typeProperties (SDK activity subclasses expose these at top level)
        elif name in self._type_properties:
            value = self._type_properties[name]
            found = True
        elif camel_name in self._type_properties:
            value = self._type_properties[camel_name]
            found = True

        if not found:
            # Return None for missing attributes (like SDK does for optional fields)
            return None

        # For known dict-like attributes, return raw dict to support .get()/.items()
        if (
            name in self._DICT_PASSTHROUGH_ATTRS
            or camel_name in self._DICT_PASSTHROUGH_ATTRS
        ):
            return value

        if name in self._DATETIME_ATTRS and isinstance(value, str):
            return datetime.fromisoformat(value)

        # ADF "dynamic content" typeProperties (e.g. table names driven by
        # "@dataset().table_name") are returned by the real SDK as raw
        # dicts {"value": ..., "type": "Expression"}, not a typed model -
        # mirror that instead of wrapping them like other nested objects.
        if isinstance(value, dict) and value.get("type") == "Expression":
            return value

        # Recursively wrap nested dicts as MockAzureResource
        if isinstance(value, dict):
            return MockAzureResource(value)
        if isinstance(value, list):
            return [
                MockAzureResource(item) if isinstance(item, dict) else item
                for item in value
            ]
        return value

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

    # Mock global parameters - unlike other resources, the real SDK types
    # GlobalParameterResource.properties as a plain Dict[str, ...] rather
    # than a nested properties model, so this bypasses MockAzureResource's
    # generic nested-dict wrapping and builds that shape directly.
    mock_client.global_parameters.list_by_factory.return_value = [
        SimpleNamespace(
            properties={
                name: SimpleNamespace(value=value, type="String")
                for name, value in test_data.get("global_parameters", {}).items()
            }
        )
    ]

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


@time_machine.travel(FROZEN_TIME, tick=False)
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


@time_machine.travel(FROZEN_TIME, tick=False)
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


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_parameterized_dataset_literal_resolution(tmp_path):
    """Regression test: a dataset whose table typeProperty is ADF dynamic
    content (e.g. "@dataset().table_name") must resolve to the real table
    name supplied by the activity's DatasetReference.parameters, never to
    the raw Python dict repr of the Expression object.
    """
    output_file = tmp_path / "adf_parameterized_literal_events.json"

    factory_name = "param-test-factory"
    resource_group = "param-test-rg"

    copy_activity = create_mock_activity(
        name="CopyParameterizedTable",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "ParameterizedSqlDataset",
                "type": "DatasetReference",
                # Real ADF data wraps even literal dynamic-content values
                # (authored via the "Add dynamic content" UI) as an
                # Expression dict rather than a bare string.
                "parameters": {"table_name": {"value": "Orders", "type": "Expression"}},
            }
        ],
        outputs=[{"referenceName": "DestSqlDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="ParameterizedCopyPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    parameterized_dataset = create_mock_dataset(
        name="ParameterizedSqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    dest_dataset = create_mock_dataset(
        name="DestSqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "Staging"},
    )

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [parameterized_dataset, dest_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            )
        ],
        "triggers": [],
        "pipeline_runs": [],
        "activity_runs": {},
    }
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
                    "run_id": "adf-test-param-literal",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyParameterizedTable" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) == 1
    input_datasets = lineage_aspects[0]["aspect"]["json"]["inputDatasets"]
    assert len(input_datasets) == 1
    assert ",Orders," in input_datasets[0]
    assert "{'value'" not in input_datasets[0]
    assert "Expression" not in input_datasets[0]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_dynamic_lineage_via_pipeline_run_parameters(tmp_path):
    """A dataset parameter driven by "@pipeline().parameters.X" can only be
    resolved using a specific run's actual parameter values (execution
    history). Two runs with different resolved values should both appear,
    unioned, in the DataJob's static lineage.
    """
    output_file = tmp_path / "adf_dynamic_lineage_events.json"

    factory_name = "dynamic-test-factory"
    resource_group = "dynamic-test-rg"

    copy_activity = create_mock_activity(
        name="CopyDynamicTable",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "DynamicSqlDataset",
                "type": "DatasetReference",
                # Real ADF data wraps dynamic-content values (even ones
                # authored via the "Add dynamic content" UI) as an
                # Expression dict rather than a bare string - exercise
                # that shape here rather than a plain string.
                "parameters": {
                    "table_name": {
                        "value": "@pipeline().parameters.SourceTable",
                        "type": "Expression",
                    }
                },
            }
        ],
        outputs=[{"referenceName": "DestSqlDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="DynamicCopyPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    dynamic_dataset = create_mock_dataset(
        name="DynamicSqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    dest_dataset = create_mock_dataset(
        name="DestSqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "Staging"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-a",
            pipeline_name="DynamicCopyPipeline",
            parameters={"SourceTable": "Orders"},
        ),
        create_mock_pipeline_run(
            run_id="run-b",
            pipeline_name="DynamicCopyPipeline",
            parameters={"SourceTable": "Customers"},
        ),
    ]
    activity_runs = {
        "run-a": [
            create_mock_activity_run(
                activity_run_id="act-run-a",
                activity_name="CopyDynamicTable",
                activity_type="Copy",
                pipeline_run_id="run-a",
                pipeline_name="DynamicCopyPipeline",
            )
        ],
        "run-b": [
            create_mock_activity_run(
                activity_run_id="act-run-b",
                activity_name="CopyDynamicTable",
                activity_type="Copy",
                pipeline_run_id="run-b",
                pipeline_name="DynamicCopyPipeline",
            )
        ],
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [dynamic_dataset, dest_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            )
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-dynamic-lineage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyDynamicTable" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    # The augmentation MCP (emitted after execution-history processing)
    # carries the union of both runs' resolved tables.
    input_datasets = lineage_aspects[-1]["aspect"]["json"]["inputDatasets"]
    assert any(",Orders," in d for d in input_datasets)
    assert any(",Customers," in d for d in input_datasets)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_dynamic_lineage_via_query_parsing(tmp_path):
    """A Copy activity inside a ForEach loop (driven by "@item().X", which
    no Azure API field ever resolves) can still have its real source table
    recovered by parsing the resolved SQL query recorded on each specific
    ActivityRun. Two runs with different queries should both appear,
    unioned, in the DataJob's lineage.
    """
    output_file = tmp_path / "adf_query_lineage_events.json"

    factory_name = "query-test-factory"
    resource_group = "query-test-rg"

    copy_activity = create_mock_activity(
        name="CopyViaForEach",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "QuerySqlDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().TableName"},
            }
        ],
        outputs=[{"referenceName": "DestSqlDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="ForEachQueryPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    query_dataset = create_mock_dataset(
        name="QuerySqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    dest_dataset = create_mock_dataset(
        name="DestSqlDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "Staging"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(run_id="run-x", pipeline_name="ForEachQueryPipeline"),
    ]
    activity_runs = {
        "run-x": [
            create_mock_activity_run(
                activity_run_id="act-iter-1",
                activity_name="CopyViaForEach",
                activity_type="Copy",
                pipeline_run_id="run-x",
                pipeline_name="ForEachQueryPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-iter-2",
                activity_name="CopyViaForEach",
                activity_type="Copy",
                pipeline_run_id="run-x",
                pipeline_name="ForEachQueryPipeline",
            ),
        ]
    }
    # Simulate the resolved per-iteration query recorded on each ActivityRun
    # (real ADF payload shape confirmed via a live tenant: {"source": {"query": "..."}}).
    activity_runs["run-x"][0]["input"] = {
        "source": {"query": "SELECT * FROM dbo.orders_table"}
    }
    activity_runs["run-x"][1]["input"] = {
        "source": {"query": "SELECT * FROM dbo.customers_table"}
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [query_dataset, dest_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            )
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-query-lineage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyViaForEach" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    input_datasets = lineage_aspects[-1]["aspect"]["json"]["inputDatasets"]
    assert any("orders_table" in d for d in input_datasets)
    assert any("customers_table" in d for d in input_datasets)
    # Never the ADF dataset-name placeholder fallback once query parsing succeeds.
    assert not any("QuerySqlDataset" in d for d in input_datasets)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_query_parsing_fully_qualified_and_sink_ddl(tmp_path):
    """Two extensions to query-based lineage recovery, mirroring a
    real-world "mirror many source tables into equally many destination
    tables" pattern:
    1. A Databricks source's resolved query only ever contains a 2-part
       "schema.table" reference (Databricks datasets in ADF expose no
       catalog at all) - it should be fully qualified with the
       configured default catalog to match DataHub's own Unity Catalog
       source's naming convention.
    2. The destination side has no query of its own, but a Copy
       activity's "preCopyScript" (e.g. "TRUNCATE TABLE schema.table")
       exposes the real per-iteration destination table the same way
       source.query does for the source - parsing it should produce as
       many distinct downstream tables as sources, not a single static
       fallback.
    """
    output_file = tmp_path / "adf_fqn_sink_ddl_events.json"

    factory_name = "fqn-test-factory"
    resource_group = "fqn-test-rg"

    copy_activity = create_mock_activity(
        name="MirrorManyTables",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "DatabricksSourceDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
        outputs=[
            {
                "referenceName": "MssqlSinkDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
    )
    pipeline_def = create_mock_pipeline(
        name="MirrorPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )

    pipeline_runs = [
        create_mock_pipeline_run(run_id="run-y", pipeline_name="MirrorPipeline"),
    ]
    activity_runs = {
        "run-y": [
            create_mock_activity_run(
                activity_run_id="act-mirror-1",
                activity_name="MirrorManyTables",
                activity_type="Copy",
                pipeline_run_id="run-y",
                pipeline_name="MirrorPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-mirror-2",
                activity_name="MirrorManyTables",
                activity_type="Copy",
                pipeline_run_id="run-y",
                pipeline_name="MirrorPipeline",
            ),
        ]
    }
    activity_runs["run-y"][0]["input"] = {
        "source": {"query": "SELECT * FROM sales.orders_table"},
        "sink": {"preCopyScript": "truncate table sales.orders_table"},
    }
    activity_runs["run-y"][1]["input"] = {
        "source": {"query": "SELECT * FROM sales.customers_table"},
        "sink": {"preCopyScript": "truncate table sales.customers_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
                # A "parameterized" linked service, mirroring a real-world
                # pattern: the connection string references the linked
                # service's own parameter rather than a literal value, and
                # that parameter declares the real database name as its
                # default.
                connection_string="Initial Catalog=@{linkedService().target_db_param}",
                parameters={
                    "target_db_param": {"type": "String", "defaultValue": "WarehouseDB"}
                },
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-fqn-sink-ddl",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                            # ADF exposes no catalog anywhere for Databricks
                            # datasets/linked services, so it's opt-in config
                            # rather than an assumed default - see
                            # test_adf_source_databricks_catalog_unset_by_default
                            # for the (safer) zero-config behavior.
                            "databricks_default_catalog": "hive_metastore",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorManyTables" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    input_datasets = aspect["inputDatasets"]
    output_datasets = aspect["outputDatasets"]

    # Databricks source: fully qualified with the explicitly configured
    # default catalog, since ADF's Databricks datasets never expose one
    # themselves.
    assert any(
        d.startswith(
            "urn:li:dataset:(urn:li:dataPlatform:databricks,hive_metastore.sales.orders_table,"
        )
        for d in input_datasets
    )
    assert any(
        d.startswith(
            "urn:li:dataset:(urn:li:dataPlatform:databricks,hive_metastore.sales.customers_table,"
        )
        for d in input_datasets
    )

    # MSSQL sink: two distinct destination tables recovered from
    # preCopyScript, mirroring the two source tables - not a single
    # static fallback, and fully qualified with the linked service's own
    # declared database parameter default (a real value from the API,
    # not a config-file setting).
    assert any(
        d.startswith(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,WarehouseDB.sales.orders_table,"
        )
        for d in output_datasets
    )
    assert any(
        d.startswith(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,WarehouseDB.sales.customers_table,"
        )
        for d in output_datasets
    )
    assert len(output_datasets) >= 2
    assert not any("MssqlSinkDataset" in d for d in output_datasets)
    assert not any("linkedService" in d for d in output_datasets)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_databricks_catalog_unset_by_default(tmp_path):
    """Regression test: ADF exposes no catalog anywhere for Databricks
    datasets or linked services, and there's no way to tell a legacy
    hive_metastore workspace from a Unity Catalog workspace with an
    arbitrary catalog name apart. Without an explicit
    "databricks_default_catalog" config, the connector must not guess
    one - it should emit a 2-part schema.table reference rather than a
    3-part reference that may point at the wrong catalog."""
    output_file = tmp_path / "adf_databricks_no_catalog_events.json"

    factory_name = "no-catalog-test-factory"
    resource_group = "no-catalog-test-rg"

    copy_activity = create_mock_activity(
        name="CopyFromDatabricks",
        activity_type="Copy",
        inputs=[
            {"referenceName": "DatabricksSourceDataset", "type": "DatasetReference"}
        ],
        outputs=[{"referenceName": "MssqlSinkDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="NoCatalogPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={"table": "orders_table", "database": "sales"},
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "orders_table", "schema": "sales"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(run_id="run-z", pipeline_name="NoCatalogPipeline"),
    ]
    activity_runs = {
        "run-z": [
            create_mock_activity_run(
                activity_run_id="act-no-catalog-1",
                activity_name="CopyFromDatabricks",
                activity_type="Copy",
                pipeline_run_id="run-z",
                pipeline_name="NoCatalogPipeline",
            ),
        ]
    }
    activity_runs["run-z"][0]["input"] = {
        "source": {"query": "SELECT * FROM sales.orders_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-no-catalog",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyFromDatabricks" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    input_datasets = aspect["inputDatasets"]

    assert any(
        d == "urn:li:dataset:(urn:li:dataPlatform:databricks,sales.orders_table,DEV)"
        for d in input_datasets
    )
    assert not any("hive_metastore" in d for d in input_datasets)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_databricks_platform_instance_derived_from_workspace_url(
    tmp_path,
):
    """Regression test: without any "platform_instance_map" config, the
    Databricks workspace instance ID (e.g. "adb-1234567890123456") should
    be auto-derived from the linked service's workspace URL and folded
    into the emitted URN - mirroring exactly how DataHub's own Unity
    Catalog source derives its platform_instance from workspace_url."""
    output_file = tmp_path / "adf_databricks_platform_instance_events.json"

    factory_name = "platform-instance-test-factory"
    resource_group = "platform-instance-test-rg"

    copy_activity = create_mock_activity(
        name="CopyFromDatabricks",
        activity_type="Copy",
        inputs=[
            {"referenceName": "DatabricksSourceDataset", "type": "DatasetReference"}
        ],
        outputs=[{"referenceName": "MssqlSinkDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="PlatformInstancePipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={"table": "orders_table", "database": "sales"},
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "orders_table", "schema": "sales"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-platform-instance", pipeline_name="PlatformInstancePipeline"
        ),
    ]
    activity_runs = {
        "run-platform-instance": [
            create_mock_activity_run(
                activity_run_id="act-platform-instance-1",
                activity_name="CopyFromDatabricks",
                activity_type="Copy",
                pipeline_run_id="run-platform-instance",
                pipeline_name="PlatformInstancePipeline",
            ),
        ]
    }
    activity_runs["run-platform-instance"][0]["input"] = {
        "source": {"query": "SELECT * FROM sales.orders_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
                type_properties={
                    "domain": "https://adb-1234567890123456.4.azuredatabricks.net/"
                },
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-platform-instance",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyFromDatabricks" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    input_datasets = aspect["inputDatasets"]

    assert any(
        d
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,adb-1234567890123456.sales.orders_table,DEV)"
        for d in input_datasets
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_databricks_catalog_map_full_urn(tmp_path):
    """End-to-end regression test: with an explicit databricks_catalog_map
    entry supplying both catalog and metastore for a linked service, the
    emitted URN should fold platform_instance (auto-derived workspace ID)
    and metastore.catalog.schema.table together into the same shape
    DataHub's own Unity Catalog source produces when ingested with
    include_metastore enabled."""
    output_file = tmp_path / "adf_databricks_catalog_map_events.json"

    factory_name = "catalog-map-test-factory"
    resource_group = "catalog-map-test-rg"

    copy_activity = create_mock_activity(
        name="CopyFromDatabricks",
        activity_type="Copy",
        inputs=[
            {"referenceName": "DatabricksSourceDataset", "type": "DatasetReference"}
        ],
        outputs=[{"referenceName": "MssqlSinkDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="CatalogMapPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={"table": "orders_table", "database": "sales"},
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "orders_table", "schema": "sales"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-catalog-map", pipeline_name="CatalogMapPipeline"
        ),
    ]
    activity_runs = {
        "run-catalog-map": [
            create_mock_activity_run(
                activity_run_id="act-catalog-map-1",
                activity_name="CopyFromDatabricks",
                activity_type="Copy",
                pipeline_run_id="run-catalog-map",
                pipeline_name="CatalogMapPipeline",
            ),
        ]
    }
    activity_runs["run-catalog-map"][0]["input"] = {
        "source": {"query": "SELECT * FROM sales.orders_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
                type_properties={
                    "domain": "https://adb-1234567890123456.4.azuredatabricks.net/"
                },
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-catalog-map",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                            "databricks_catalog_map": {
                                "DatabricksLS": {
                                    "catalog": "prod_catalog",
                                    "metastore": "prod_metastore",
                                }
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyFromDatabricks" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    input_datasets = aspect["inputDatasets"]

    assert any(
        d == "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        "adb-1234567890123456.prod_metastore.prod_catalog.sales.orders_table,DEV)"
        for d in input_datasets
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_lookup_activity_never_produces_garbage_urn(tmp_path):
    """Regression test: a Lookup activity's dataset reference is exposed
    by the real SDK only via LookupActivity.dataset (never "inputs" -
    the base Activity model's inputs/outputs fields don't exist on
    LookupActivity at all). When that dataset's typeProperty uses
    Expression-typed dynamic content resolved via a literal override
    supplied on the Lookup's own dataset reference parameters, the
    emitted lineage must be a clean URN, never dict-repr garbage like
    "{'value': '@dataset().table_name', 'type': 'Expression'}"."""
    output_file = tmp_path / "adf_lookup_no_garbage_events.json"

    factory_name = "lookup-garbage-test-factory"
    resource_group = "lookup-garbage-test-rg"

    lookup_activity = create_mock_activity(
        name="LookupConfig",
        activity_type="Lookup",
        dataset={
            "referenceName": "ConfigDataset",
            "type": "DatasetReference",
            "parameters": {"table_name": "dbo.AppConfig"},
        },
    )
    pipeline_def = create_mock_pipeline(
        name="LookupGarbagePipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[lookup_activity],
    )
    config_dataset = create_mock_dataset(
        name="ConfigDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [config_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            ),
        ],
        "triggers": [],
        "pipeline_runs": [],
        "activity_runs": {},
    }
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
                    "run_id": "adf-test-lookup-no-garbage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "LookupConfig" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]

    assert aspect["inputDatasets"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,dbo.AppConfig,DEV)"
    ]
    for urn in aspect["inputDatasets"]:
        assert "{'value'" not in urn
        assert "Expression" not in urn


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_skips_unresolved_adf_expression_in_ddl_statement(tmp_path):
    """Regression test: some ADF-recorded sink DDL statements (e.g.
    preCopyScript) retain unevaluated ADF templating syntax (observed on
    a live tenant as "@{linkedService().someParam}.schema.table") even
    though the paired source.query is always fully resolved. Parsing that
    literally would reproduce the exact class of garbage-URN bug this
    feature was built to fix - it must be skipped cleanly instead.
    """
    output_file = tmp_path / "adf_unresolved_expression_events.json"

    factory_name = "unresolved-expr-factory"
    resource_group = "unresolved-expr-rg"

    copy_activity = create_mock_activity(
        name="CopyWithUnresolvedSink",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "SourceDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
        outputs=[
            {
                "referenceName": "SinkDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
    )
    pipeline_def = create_mock_pipeline(
        name="UnresolvedExprPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    source_dataset = create_mock_dataset(
        name="SourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    sink_dataset = create_mock_dataset(
        name="SinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-z", pipeline_name="UnresolvedExprPipeline"
        ),
    ]
    activity_runs = {
        "run-z": [
            create_mock_activity_run(
                activity_run_id="act-unresolved-1",
                activity_name="CopyWithUnresolvedSink",
                activity_type="Copy",
                pipeline_run_id="run-z",
                pipeline_name="UnresolvedExprPipeline",
            ),
        ]
    }
    activity_runs["run-z"][0]["input"] = {
        "source": {"query": "SELECT * FROM sales.orders_table"},
        # Real ADF quirk: this templating syntax is sometimes left
        # unevaluated in preCopyScript specifically, unlike source.query.
        "sink": {
            "preCopyScript": "truncate table @{linkedService().db_name}.sales.orders_table"
        },
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [source_dataset, sink_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureSqlDatabase",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-unresolved-expr",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            source_report = cast(
                AzureDataFactorySourceReport, pipeline.source.get_report()
            )
            assert any(
                "Unresolved ADF Expression" in (w.title or "")
                for w in source_report.warnings
            )

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyWithUnresolvedSink" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    output_datasets = lineage_aspects[-1]["aspect"]["json"]["outputDatasets"]
    # Never the unresolved "@{...}" templating syntax or its percent-encoded
    # form leaking into a URN - the exact class of bug this feature fixes.
    assert not any("linkedService" in d for d in output_datasets)
    assert not any("%28%29" in d for d in output_datasets)


@time_machine.travel(FROZEN_TIME, tick=False)
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


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_platform_instance_map_applies_to_every_lineage_platform(tmp_path):
    """Regression test: "platform_instance_map" (per-linked-service
    lineage-target overrides) had never actually been exercised with a
    real value in any existing test - only ever set to {} or covered by
    the unrelated singular "platform_instance" (the ADF factory's own
    instance). Confirm the manual override works uniformly across
    multiple different lineage-target platforms in one pipeline, not
    just the platform(s) that happen to have auto-derivation."""
    output_file = tmp_path / "adf_platform_instance_map_events.json"

    factory_name = "platform-instance-map-test-factory"
    resource_group = "platform-instance-map-test-rg"

    copy_activity = create_mock_activity(
        name="CopyFromMySql",
        activity_type="Copy",
        inputs=[{"referenceName": "MySqlSourceDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "MssqlSinkDataset", "type": "DatasetReference"}],
    )
    pipeline_def = create_mock_pipeline(
        name="PlatformInstanceMapPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    mysql_dataset = create_mock_dataset(
        name="MySqlSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="MySqlLS",
        dataset_type="MySqlTable",
        type_properties={"table": "orders_table"},
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "sales.orders_table"},
    )

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [mysql_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="MySqlLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="MySql",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": [],
        "activity_runs": {},
    }
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
                    "run_id": "adf-test-platform-instance-map",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                            "platform_instance_map": {
                                "MySqlLS": "prod_mysql",
                                "SqlSinkLS": "prod_mssql",
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "CopyFromMySql" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]

    assert aspect["inputDatasets"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,prod_mysql.orders_table,DEV)"
    ]
    assert aspect["outputDatasets"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,prod_mssql.sales.orders_table,DEV)"
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
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


@time_machine.travel(FROZEN_TIME, tick=False)
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


# =============================================================================
# Column-Level Lineage Integration Tests
# =============================================================================


def create_copy_activity_with_column_mappings(
    name: str,
    inputs: List[Dict[str, Any]],
    outputs: List[Dict[str, Any]],
    translator: Dict[str, Any],
) -> Dict[str, Any]:
    """Create a mock Copy activity with column mapping configuration."""
    return {
        "name": name,
        "type": "Copy",
        "inputs": inputs,
        "outputs": outputs,
        "typeProperties": {
            "source": {"type": "AzureSqlSource"},
            "sink": {"type": "AzureSqlSink"},
            "translator": translator,
        },
        "dependsOn": [],
        "policy": {"timeout": "7.00:00:00", "retry": 0},
    }


def get_column_lineage_test_data() -> Dict[str, Any]:
    """Generate test data for column lineage extraction tests."""
    factories = [
        create_mock_factory(
            name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
        ),
    ]

    # Copy activity with dictionary format column mappings
    copy_with_dict_mappings = create_copy_activity_with_column_mappings(
        name="CopyWithDictMappings",
        inputs=[{"referenceName": "SourceSqlDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "DestSqlDataset", "type": "DatasetReference"}],
        translator={
            "type": "TabularTranslator",
            "columnMappings": {
                "source_id": "target_id",
                "source_name": "target_name",
                "source_email": "target_email",
            },
        },
    )

    # Copy activity with list format column mappings
    copy_with_list_mappings = create_copy_activity_with_column_mappings(
        name="CopyWithListMappings",
        inputs=[{"referenceName": "SourceBlobDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "DestBlobDataset", "type": "DatasetReference"}],
        translator={
            "type": "TabularTranslator",
            "mappings": [
                {"source": {"name": "col_a"}, "sink": {"name": "column_x"}},
                {"source": {"name": "col_b"}, "sink": {"name": "column_y"}},
            ],
        },
    )

    pipelines = [
        create_mock_pipeline(
            name="ColumnLineagePipeline",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            activities=[copy_with_dict_mappings, copy_with_list_mappings],
        ),
    ]

    datasets = [
        create_mock_dataset(
            name="SourceSqlDataset",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="AzureSqlLS",
            dataset_type="AzureSqlTable",
            type_properties={"schema": "dbo", "table": "SourceTable"},
        ),
        create_mock_dataset(
            name="DestSqlDataset",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="AzureSqlLS",
            dataset_type="AzureSqlTable",
            type_properties={"schema": "dbo", "table": "DestTable"},
        ),
        create_mock_dataset(
            name="SourceBlobDataset",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="BlobStorageLS",
            dataset_type="DelimitedText",
            type_properties={
                "location": {"container": "source", "fileName": "data.csv"}
            },
        ),
        create_mock_dataset(
            name="DestBlobDataset",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            linked_service_name="BlobStorageLS",
            dataset_type="DelimitedText",
            type_properties={
                "location": {"container": "dest", "fileName": "output.csv"}
            },
        ),
    ]

    linked_services = [
        create_mock_linked_service(
            name="AzureSqlLS",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            service_type="AzureSqlDatabase",
        ),
        create_mock_linked_service(
            name="BlobStorageLS",
            factory_name="cll-test-factory",
            resource_group=RESOURCE_GROUP,
            subscription_id=SUBSCRIPTION_ID,
            service_type="AzureBlobStorage",
        ),
    ]

    triggers: List[Dict[str, Any]] = []
    pipeline_runs: List[Dict[str, Any]] = []

    return {
        "factories": factories,
        "pipelines": pipelines,
        "datasets": datasets,
        "linked_services": linked_services,
        "triggers": triggers,
        "pipeline_runs": pipeline_runs,
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_with_column_lineage(pytestconfig, tmp_path):
    """Test ADF metadata extraction with column-level lineage enabled.

    Verifies:
    - Column mappings are extracted from Copy activities
    - Both dictionary and list format translators are parsed
    - FineGrainedLineage aspects are emitted
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_column_lineage_events.json"
    golden_file = test_resources_dir / "adf_column_lineage_golden.json"

    test_data = get_column_lineage_test_data()
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
                    "run_id": "adf-test-column-lineage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "include_lineage": True,
                            "include_column_lineage": True,
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

            # Verify column lineage was extracted
            source_report = cast(
                AzureDataFactorySourceReport, pipeline.source.get_report()
            )
            assert source_report.column_lineage_extracted > 0, (
                "Expected column lineage mappings to be extracted"
            )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_column_lineage_disabled(tmp_path):
    """Test that column lineage is not extracted when disabled.

    Verifies:
    - No column lineage when include_column_lineage=False
    - Table-level lineage still works
    """
    output_file = tmp_path / "adf_no_column_lineage_events.json"

    test_data = get_column_lineage_test_data()
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
                    "run_id": "adf-test-no-column-lineage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": RESOURCE_GROUP,
                            "credential": {
                                "authentication_method": "default",
                            },
                            "include_lineage": True,
                            "include_column_lineage": False,  # Disabled
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

            # Verify no column lineage was extracted
            source_report = cast(
                AzureDataFactorySourceReport, pipeline.source.get_report()
            )
            assert source_report.column_lineage_extracted == 0, (
                "Expected no column lineage when disabled"
            )

            # But table-level lineage should still work
            assert source_report.dataset_lineage_extracted > 0, (
                "Expected table-level lineage to still work"
            )


# =============================================================================
# Complex Scenario Integration Tests
# =============================================================================


def create_complex_scenario_mock_client(
    scenario_data: Dict[str, Any],
) -> MagicMock:
    """Create a mock client for a complex scenario.

    Args:
        scenario_data: Dictionary with "pipelines" key from scenario functions
    """
    mock_client = MagicMock()

    # Mock factory
    mock_client.factories.list.return_value = MockPagedIterator(
        [create_complex_factory()]
    )
    mock_client.factories.list_by_resource_group.return_value = MockPagedIterator(
        [create_complex_factory()]
    )

    # Mock pipelines from scenario
    mock_client.pipelines.list_by_factory.return_value = MockPagedIterator(
        scenario_data["pipelines"]
    )

    # Mock datasets - use shared complex datasets
    mock_client.datasets.list_by_factory.return_value = MockPagedIterator(
        create_complex_datasets()
    )

    # Mock linked services - use shared complex linked services
    mock_client.linked_services.list_by_factory.return_value = MockPagedIterator(
        create_complex_linked_services()
    )

    # Mock data flows - check if scenario has them
    data_flows = scenario_data.get("data_flows", [])
    mock_client.data_flows.list_by_factory.return_value = MockPagedIterator(data_flows)

    # Mock triggers (empty for these tests)
    mock_client.triggers.list_by_factory.return_value = MockPagedIterator([])

    # Mock pipeline runs (empty - no execution history)
    mock_client.pipeline_runs.query_by_factory.return_value = MockQueryResponse([])
    mock_client.activity_runs.query_by_pipeline_run.return_value = MockQueryResponse([])

    return mock_client


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_nested_pipelines(pytestconfig, tmp_path):
    """Test nested pipeline scenario with ExecutePipeline activities.

    Verifies:
    - Parent and child pipelines are extracted
    - ExecutePipeline activities create pipeline-to-pipeline lineage
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_nested_events.json"
    golden_file = test_resources_dir / "adf_nested_golden.json"

    scenario_data = create_nested_pipeline_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-nested",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_foreach_loop(pytestconfig, tmp_path):
    """Test ForEach loop scenario with iteration activities.

    Verifies:
    - ForEach activity is extracted
    - Activities inside ForEach are extracted
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_foreach_events.json"
    golden_file = test_resources_dir / "adf_foreach_golden.json"

    scenario_data = create_foreach_loop_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-foreach",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_branching(pytestconfig, tmp_path):
    """Test branching scenario with If-Condition and Switch activities.

    Verifies:
    - Control flow activities are extracted
    - Conditional branches are represented
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_branching_events.json"
    golden_file = test_resources_dir / "adf_branching_golden.json"

    scenario_data = create_branching_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-branching",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_dataflow(pytestconfig, tmp_path):
    """Test Data Flow scenario with mapping data flows.

    Verifies:
    - ExecuteDataFlow activities extract sources/sinks
    - Data Flow transformation script is captured
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_dataflow_events.json"
    golden_file = test_resources_dir / "adf_dataflow_golden.json"

    scenario_data = create_dataflow_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-dataflow",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_multisource_chain(pytestconfig, tmp_path):
    """Test multi-source chain scenario with SQL -> Blob -> Synapse.

    Verifies:
    - Multiple Copy activities create chained lineage
    - Different platform types are mapped correctly
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_multisource_events.json"
    golden_file = test_resources_dir / "adf_multisource_golden.json"

    scenario_data = create_multisource_chain_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-multisource",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_diverse_activities(pytestconfig, tmp_path):
    """Test diverse activity types scenario.

    Verifies:
    - Various activity types are extracted with correct subtypes
    - Web, Azure Function, Databricks activities are represented
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_diverse_events.json"
    golden_file = test_resources_dir / "adf_diverse_golden.json"

    scenario_data = create_diverse_activities_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-diverse",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_mixed_dependencies(pytestconfig, tmp_path):
    """Test mixed dependencies scenario with complex activity dependencies.

    Verifies:
    - Activity dependencies are correctly represented
    - ExecutePipeline combined with Copy activities
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/azure_data_factory"
    output_file = tmp_path / "adf_mixed_deps_events.json"
    golden_file = test_resources_dir / "adf_mixed_deps_golden.json"

    scenario_data = create_mixed_dependencies_scenario()
    mock_client = create_complex_scenario_mock_client(scenario_data)

    with mock.patch(
        "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
    ) as MockClientClass:
        MockClientClass.return_value = mock_client

        with mock.patch(
            "datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": "adf-test-mixed-deps",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": COMPLEX_SUBSCRIPTION_ID,
                            "resource_group": COMPLEX_RESOURCE_GROUP,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_execution_history": False,
                            "env": "DEV",
                        },
                    },
                    "sink": {"type": "file", "config": {"filename": str(output_file)}},
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_dynamic_column_lineage_via_query_parsing(tmp_path):
    """Regression test: for a ForEach-looped Copy activity with no
    explicit ADF translator (column mapping), the real per-iteration
    column-level lineage should be recovered from the resolved query's
    own selected column list, the same way per-iteration table names are
    already recovered - "@item()"-driven per-iteration values can only
    ever be read off a specific ActivityRun, never resolved statically.
    Two different iterations copying different tables should each get
    their own column-level lineage, not a single static fallback."""
    output_file = tmp_path / "adf_dynamic_column_lineage_events.json"

    factory_name = "dynamic-column-lineage-test-factory"
    resource_group = "dynamic-column-lineage-test-rg"

    copy_activity = create_mock_activity(
        name="MirrorManyTablesWithColumns",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "DatabricksSourceDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
        outputs=[
            {
                "referenceName": "MssqlSinkDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
    )
    pipeline_def = create_mock_pipeline(
        name="MirrorColumnsPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-cols", pipeline_name="MirrorColumnsPipeline"
        ),
    ]
    activity_runs = {
        "run-cols": [
            create_mock_activity_run(
                activity_run_id="act-cols-1",
                activity_name="MirrorManyTablesWithColumns",
                activity_type="Copy",
                pipeline_run_id="run-cols",
                pipeline_name="MirrorColumnsPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-cols-2",
                activity_name="MirrorManyTablesWithColumns",
                activity_type="Copy",
                pipeline_run_id="run-cols",
                pipeline_name="MirrorColumnsPipeline",
            ),
        ]
    }
    activity_runs["run-cols"][0]["input"] = {
        "source": {"query": "SELECT id, name, amount FROM sales.orders_table"},
        "sink": {"preCopyScript": "truncate table sales.orders_table"},
    }
    activity_runs["run-cols"][1]["input"] = {
        "source": {"query": "SELECT id, email FROM sales.customers_table"},
        "sink": {"preCopyScript": "truncate table sales.customers_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-dynamic-column-lineage",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_column_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorManyTablesWithColumns" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    fine_grained_lineages = aspect.get("fineGrainedLineages")
    assert fine_grained_lineages, "expected column-level lineage to be emitted"

    # 3 columns for orders_table + 2 columns for customers_table = 5 total
    # distinct column-level mappings, each pointing at its own real table
    # pair - not a single static/generic fallback.
    assert len(fine_grained_lineages) == 5

    mappings = {
        (fgl["upstreams"][0], fgl["downstreams"][0]) for fgl in fine_grained_lineages
    }
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,sales.orders_table,DEV),id)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,sales.orders_table,DEV),id)",
    ) in mappings
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,sales.customers_table,DEV),email)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,sales.customers_table,DEV),email)",
    ) in mappings


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_dynamic_column_lineage_skipped_with_explicit_translator(tmp_path):
    """Regression test: when the Copy activity has its own explicit ADF
    translator (column mapping), the query-based column-lineage recovery
    must stay out of the way entirely - inventing a same-name mapping
    here could silently contradict the user's own explicit one."""
    output_file = tmp_path / "adf_dynamic_column_lineage_translator_events.json"

    factory_name = "translator-skip-test-factory"
    resource_group = "translator-skip-test-rg"

    copy_activity = create_mock_activity(
        name="MirrorWithExplicitTranslator",
        activity_type="Copy",
        inputs=[
            {
                "referenceName": "DatabricksSourceDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
        outputs=[
            {
                "referenceName": "MssqlSinkDataset",
                "type": "DatasetReference",
                "parameters": {"table_name": "@item().table_name"},
            }
        ],
    )
    copy_activity["typeProperties"]["translator"] = {
        "type": "TabularTranslator",
        "columnMappings": {"id": "id", "name": "full_name"},
    }
    pipeline_def = create_mock_pipeline(
        name="TranslatorSkipPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    databricks_dataset = create_mock_dataset(
        name="DatabricksSourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="DatabricksLS",
        dataset_type="AzureDatabricksDeltaLakeDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )
    mssql_dataset = create_mock_dataset(
        name="MssqlSinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSinkLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={
            "table": {"value": "@dataset().table_name", "type": "Expression"},
        },
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-translator", pipeline_name="TranslatorSkipPipeline"
        ),
    ]
    activity_runs = {
        "run-translator": [
            create_mock_activity_run(
                activity_run_id="act-translator-1",
                activity_name="MirrorWithExplicitTranslator",
                activity_type="Copy",
                pipeline_run_id="run-translator",
                pipeline_name="TranslatorSkipPipeline",
            ),
        ]
    }
    activity_runs["run-translator"][0]["input"] = {
        "source": {"query": "SELECT id, name FROM sales.orders_table"},
        "sink": {"preCopyScript": "truncate table sales.orders_table"},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [databricks_dataset, mssql_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="DatabricksLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="AzureDatabricksDeltaLake",
            ),
            create_mock_linked_service(
                name="SqlSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-translator-skip",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_column_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorWithExplicitTranslator" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    fine_grained_lineages = aspect.get("fineGrainedLineages")

    # The query-based per-run mechanism must not have contributed any
    # mapping pointing at the real per-iteration "sales.orders_table"
    # dataset - that would mean it ignored the explicit translator.
    if fine_grained_lineages:
        for fgl in fine_grained_lineages:
            assert "sales.orders_table" not in fgl["upstreams"][0]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_cross_activity_translator_resolution(tmp_path):
    """Regression test: a Copy activity's own translator can itself be a
    dynamic ADF expression referencing a sibling activity's run output
    (e.g. a Lookup activity that reads a data-driven column-mapping
    table from a control table) rather than a static config. The real
    mapping is only ever exposed on that sibling's own ActivityRun for
    this specific pipeline run - resolve it from there instead of
    falling back to a same-name guess, which could silently produce the
    wrong column edges."""
    output_file = tmp_path / "adf_cross_activity_translator_events.json"

    factory_name = "cross-activity-translator-test-factory"
    resource_group = "cross-activity-translator-test-rg"

    lookup_activity = create_mock_activity(
        name="GetMapping",
        activity_type="Lookup",
        dataset={
            "referenceName": "MappingConfigDataset",
            "type": "DatasetReference",
        },
    )
    copy_activity = create_mock_activity(
        name="MirrorWithDynamicTranslator",
        activity_type="Copy",
        inputs=[{"referenceName": "SourceDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "SinkDataset", "type": "DatasetReference"}],
        depends_on=[{"activity": "GetMapping"}],
    )
    copy_activity["typeProperties"]["translator"] = {
        "type": "Expression",
        "value": (
            "@if(equals(coalesce(activity('GetMapping').output.firstRow.mapping_json,'empty'),'empty'),"
            "activity('GetMapping').output.firstRow.mapping_json,"
            "json(activity('GetMapping').output.firstRow.mapping_json))"
        ),
    }
    pipeline_def = create_mock_pipeline(
        name="CrossActivityTranslatorPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[lookup_activity, copy_activity],
    )
    source_dataset = create_mock_dataset(
        name="SourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "source_table"},
    )
    sink_dataset = create_mock_dataset(
        name="SinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="PostgresSinkLS",
        dataset_type="AzurePostgreSqlTableDataset",
        type_properties={"table": "sink_table"},
    )
    mapping_config_dataset = create_mock_dataset(
        name="MappingConfigDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "column_mapping_config"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-cross-activity",
            pipeline_name="CrossActivityTranslatorPipeline",
        ),
    ]
    activity_runs = {
        "run-cross-activity": [
            create_mock_activity_run(
                activity_run_id="act-lookup-1",
                activity_name="GetMapping",
                activity_type="Lookup",
                pipeline_run_id="run-cross-activity",
                pipeline_name="CrossActivityTranslatorPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-copy-1",
                activity_name="MirrorWithDynamicTranslator",
                activity_type="Copy",
                pipeline_run_id="run-cross-activity",
                pipeline_name="CrossActivityTranslatorPipeline",
            ),
        ]
    }
    activity_runs["run-cross-activity"][0]["output"] = {
        "firstRow": {
            "mapping_json": json.dumps({"id": "id", "name": "full_name"}),
        },
        "count": 1,
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [source_dataset, sink_dataset, mapping_config_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
            create_mock_linked_service(
                name="PostgresSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="PostgreSql",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-cross-activity-translator",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_column_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorWithDynamicTranslator" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    fine_grained_lineages = aspect.get("fineGrainedLineages")
    assert fine_grained_lineages, (
        "expected column-level lineage resolved via the sibling activity's output"
    )

    mappings = {
        (fgl["upstreams"][0], fgl["downstreams"][0]) for fgl in fine_grained_lineages
    }
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,source_table,DEV),id)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,sink_table,DEV),id)",
    ) in mappings
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,source_table,DEV),name)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,sink_table,DEV),full_name)",
    ) in mappings


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_cross_activity_translator_unresolvable_emits_no_column_lineage(
    tmp_path,
):
    """Regression test: when a Copy activity's translator references a
    sibling activity's run output, but that sibling never ran (e.g. it
    was skipped, or execution history doesn't cover it), there IS a
    real, data-driven mapping intended - just not resolvable this run.
    No column-level lineage should be emitted; falling back to a
    same-name guess here could silently produce the wrong column edges."""
    output_file = tmp_path / "adf_cross_activity_unresolvable_events.json"

    factory_name = "cross-activity-unresolvable-test-factory"
    resource_group = "cross-activity-unresolvable-test-rg"

    copy_activity = create_mock_activity(
        name="MirrorWithDynamicTranslator",
        activity_type="Copy",
        inputs=[{"referenceName": "SourceDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "SinkDataset", "type": "DatasetReference"}],
    )
    copy_activity["typeProperties"]["translator"] = {
        "type": "Expression",
        "value": "@activity('GetMapping').output.firstRow.mapping_json",
    }
    pipeline_def = create_mock_pipeline(
        name="CrossActivityUnresolvablePipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    source_dataset = create_mock_dataset(
        name="SourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "source_table"},
    )
    sink_dataset = create_mock_dataset(
        name="SinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="PostgresSinkLS",
        dataset_type="AzurePostgreSqlTableDataset",
        type_properties={"table": "sink_table"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-unresolvable",
            pipeline_name="CrossActivityUnresolvablePipeline",
        ),
    ]
    activity_runs = {
        "run-unresolvable": [
            # Note: no "GetMapping" activity run at all - it never ran.
            create_mock_activity_run(
                activity_run_id="act-copy-1",
                activity_name="MirrorWithDynamicTranslator",
                activity_type="Copy",
                pipeline_run_id="run-unresolvable",
                pipeline_name="CrossActivityUnresolvablePipeline",
            ),
        ]
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [source_dataset, sink_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
            create_mock_linked_service(
                name="PostgresSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="PostgreSql",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-cross-activity-unresolvable",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_column_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorWithDynamicTranslator" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    assert not aspect.get("fineGrainedLineages"), (
        "must not guess column lineage when the referenced sibling run is unavailable"
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_adf_source_cross_activity_translator_ambiguous_with_multiple_sibling_runs(
    tmp_path,
):
    """Regression test: a parallel ForEach can run the same-named sibling
    activity once per iteration, each with its own resolved output (e.g.
    a Lookup returning a different column-mapping per table). The
    Activity Runs API exposes no per-iteration correlation between
    siblings, so picking any one of several same-named runs risks
    pairing the wrong iteration's mapping with this run's actual
    source/sink tables. No column-level lineage should be emitted in
    that case, for either iteration."""
    output_file = tmp_path / "adf_cross_activity_ambiguous_events.json"

    factory_name = "cross-activity-ambiguous-test-factory"
    resource_group = "cross-activity-ambiguous-test-rg"

    copy_activity = create_mock_activity(
        name="MirrorWithDynamicTranslator",
        activity_type="Copy",
        inputs=[{"referenceName": "SourceDataset", "type": "DatasetReference"}],
        outputs=[{"referenceName": "SinkDataset", "type": "DatasetReference"}],
    )
    copy_activity["typeProperties"]["translator"] = {
        "type": "Expression",
        "value": "@activity('GetMapping').output.firstRow.mapping_json",
    }
    pipeline_def = create_mock_pipeline(
        name="CrossActivityAmbiguousPipeline",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        activities=[copy_activity],
    )
    source_dataset = create_mock_dataset(
        name="SourceDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="SqlSourceLS",
        dataset_type="AzureSqlTableDataset",
        type_properties={"table": "source_table"},
    )
    sink_dataset = create_mock_dataset(
        name="SinkDataset",
        factory_name=factory_name,
        resource_group=resource_group,
        subscription_id=SUBSCRIPTION_ID,
        linked_service_name="PostgresSinkLS",
        dataset_type="AzurePostgreSqlTableDataset",
        type_properties={"table": "sink_table"},
    )

    pipeline_runs = [
        create_mock_pipeline_run(
            run_id="run-ambiguous",
            pipeline_name="CrossActivityAmbiguousPipeline",
        ),
    ]
    activity_runs = {
        "run-ambiguous": [
            # Two concurrent ForEach iterations, each with its own
            # "GetMapping" run carrying a different resolved mapping.
            create_mock_activity_run(
                activity_run_id="act-lookup-1",
                activity_name="GetMapping",
                activity_type="Lookup",
                pipeline_run_id="run-ambiguous",
                pipeline_name="CrossActivityAmbiguousPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-lookup-2",
                activity_name="GetMapping",
                activity_type="Lookup",
                pipeline_run_id="run-ambiguous",
                pipeline_name="CrossActivityAmbiguousPipeline",
            ),
            create_mock_activity_run(
                activity_run_id="act-copy-1",
                activity_name="MirrorWithDynamicTranslator",
                activity_type="Copy",
                pipeline_run_id="run-ambiguous",
                pipeline_name="CrossActivityAmbiguousPipeline",
            ),
        ]
    }
    activity_runs["run-ambiguous"][0]["output"] = {
        "firstRow": {"mapping_json": json.dumps({"id": "id", "name": "full_name"})},
    }
    activity_runs["run-ambiguous"][1]["output"] = {
        "firstRow": {"mapping_json": json.dumps({"id": "user_id"})},
    }

    test_data = {
        "factories": [
            create_mock_factory(factory_name, resource_group, SUBSCRIPTION_ID)
        ],
        "pipelines": [pipeline_def],
        "datasets": [source_dataset, sink_dataset],
        "linked_services": [
            create_mock_linked_service(
                name="SqlSourceLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="SqlServer",
            ),
            create_mock_linked_service(
                name="PostgresSinkLS",
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=SUBSCRIPTION_ID,
                service_type="PostgreSql",
            ),
        ],
        "triggers": [],
        "pipeline_runs": pipeline_runs,
        "activity_runs": activity_runs,
    }
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
                    "run_id": "adf-test-cross-activity-ambiguous",
                    "source": {
                        "type": "azure-data-factory",
                        "config": {
                            "subscription_id": SUBSCRIPTION_ID,
                            "resource_group": resource_group,
                            "credential": {"authentication_method": "default"},
                            "include_lineage": True,
                            "include_column_lineage": True,
                            "include_execution_history": True,
                            "execution_history_days": 7,
                            "env": "DEV",
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(output_file)},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

    events = json.loads(output_file.read_text())
    lineage_aspects = [
        e
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and "MirrorWithDynamicTranslator" in e.get("entityUrn", "")
    ]
    assert len(lineage_aspects) >= 1
    aspect = lineage_aspects[-1]["aspect"]["json"]
    assert not aspect.get("fineGrainedLineages"), (
        "must not guess which iteration's mapping applies when the "
        "referenced sibling activity ran more than once in this pipeline run"
    )

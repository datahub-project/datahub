"""Reusable test factory helpers for Fabric Data Factory integration tests."""

from typing import Any, Dict, List, Optional

from datahub.ingestion.source.fabric.common.models import (
    FabricConnection,
    FabricItem,
    FabricJobInstance,
    FabricWorkspace,
)
from datahub.ingestion.source.fabric.data_factory.models import (
    ActivityDependency,
    PipelineActivity,
    PipelineActivityRun,
)

FROZEN_TIME = "2024-01-15 12:00:00"

# Deterministic GUIDs
WORKSPACE_ID_1 = "ws-11111111-1111-1111-1111-111111111111"
WORKSPACE_ID_2 = "ws-22222222-2222-2222-2222-222222222222"
PIPELINE_ID_A = "pl-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PIPELINE_ID_B = "pl-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
CONNECTION_ID_LAKEHOUSE = "conn-lh-00000000-0000-0000-0000-000000000001"
CONNECTION_ID_SNOWFLAKE = "conn-sf-00000000-0000-0000-0000-000000000002"
PIPELINE_RUN_ID_1 = "run-11111111-1111-1111-1111-111111111111"
ACTIVITY_RUN_ID_1 = "ar-11111111-1111-1111-1111-111111111111"
ACTIVITY_RUN_ID_2 = "ar-22222222-2222-2222-2222-222222222222"
LAKEHOUSE_ARTIFACT_ID = "lh-artifact-0000-0000-0000-000000000001"


def create_workspace(
    workspace_id: str, name: str, description: Optional[str] = None
) -> FabricWorkspace:
    return FabricWorkspace(id=workspace_id, name=name, description=description)


def create_pipeline_item(
    pipeline_id: str,
    name: str,
    workspace_id: str,
    description: Optional[str] = None,
) -> FabricItem:
    return FabricItem(
        id=pipeline_id,
        name=name,
        type="DataPipeline",
        workspace_id=workspace_id,
        description=description,
    )


def create_connection(
    connection_id: str,
    display_name: str,
    connection_type: str,
    path: Optional[str] = None,
) -> FabricConnection:
    """Create a FabricConnection matching list_item_connections() output."""
    return FabricConnection(
        id=connection_id,
        display_name=display_name,
        connection_type=connection_type,
        connection_path=path,
    )


def _make_depends_on(
    depends_on: Optional[List[str]],
) -> List[ActivityDependency]:
    if not depends_on:
        return []
    return [
        ActivityDependency(activity=dep, dependency_conditions=["Succeeded"])
        for dep in depends_on
    ]


def create_copy_activity(
    name: str,
    source_settings: Dict[str, Any],
    sink_settings: Dict[str, Any],
    depends_on: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> PipelineActivity:
    return PipelineActivity(
        name=name,
        type="Copy",
        description=description,
        state="Active",
        depends_on=_make_depends_on(depends_on),
        type_properties={
            "source": {"datasetSettings": source_settings},
            "sink": {"datasetSettings": sink_settings},
        },
    )


def create_lookup_activity(
    name: str,
    depends_on: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> PipelineActivity:
    return PipelineActivity(
        name=name,
        type="Lookup",
        description=description,
        state="Active",
        depends_on=_make_depends_on(depends_on),
        type_properties={},
    )


def create_set_variable_activity(
    name: str,
    depends_on: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> PipelineActivity:
    return PipelineActivity(
        name=name,
        type="SetVariable",
        description=description,
        state="Active",
        depends_on=_make_depends_on(depends_on),
        type_properties={},
    )


def create_invoke_pipeline_activity(
    name: str,
    child_pipeline_id: str,
    child_workspace_id: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> PipelineActivity:
    type_properties: Dict[str, Any] = {
        "operationType": "InvokeFabricPipeline",
        "pipelineId": child_pipeline_id,
    }
    if child_workspace_id:
        type_properties["workspaceId"] = child_workspace_id
    return PipelineActivity(
        name=name,
        type="InvokePipeline",
        description=description,
        state="Active",
        depends_on=_make_depends_on(depends_on),
        type_properties=type_properties,
    )


def create_pipeline_run(
    run_id: str,
    item_id: str,
    workspace_id: str,
    status: str,
    start_time: str,
    end_time: Optional[str] = None,
    invoke_type: Optional[str] = None,
    failure_reason: Optional[str] = None,
) -> FabricJobInstance:
    return FabricJobInstance(
        id=run_id,
        item_id=item_id,
        workspace_id=workspace_id,
        status=status,
        start_time_utc=start_time,
        end_time_utc=end_time,
        invoke_type=invoke_type,
        failure_reason=failure_reason,
    )


def create_activity_run(
    activity_name: str,
    activity_type: str,
    activity_run_id: str,
    pipeline_run_id: str,
    status: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    retry_attempt: Optional[int] = None,
) -> PipelineActivityRun:
    return PipelineActivityRun(
        activity_name=activity_name,
        activity_type=activity_type,
        activity_run_id=activity_run_id,
        pipeline_run_id=pipeline_run_id,
        status=status,
        activity_run_start=start,
        activity_run_end=end,
        duration_in_ms=duration_ms,
        error_message=error_message,
        retry_attempt=retry_attempt,
    )


def create_lakehouse_dataset_settings(
    artifact_id: str,
    workspace_id: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    """Create datasetSettings for a Fabric-native Lakehouse source/sink."""
    type_properties: Dict[str, Any] = {
        "artifactId": artifact_id,
    }
    if workspace_id:
        type_properties["workspaceId"] = workspace_id
    if schema:
        type_properties["schema"] = schema
    if table:
        type_properties["table"] = table

    return {
        "connectionSettings": {
            "properties": {
                "type": "Lakehouse",
                "typeProperties": {
                    "artifactId": artifact_id,
                    **({"workspaceId": workspace_id} if workspace_id else {}),
                },
            },
        },
        "typeProperties": type_properties,
    }


def create_external_dataset_settings(
    connection_id: str,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    """Create datasetSettings for an external connection (e.g. Snowflake)."""
    type_properties: Dict[str, Any] = {}
    if schema:
        type_properties["schema"] = schema
    if table:
        type_properties["table"] = table

    return {
        "externalReferences": {"connection": connection_id},
        "typeProperties": type_properties,
    }

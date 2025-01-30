import asyncio
import json
import logging
from typing import Dict, List, Optional, cast
from unittest.mock import MagicMock, Mock, patch
from uuid import UUID

import pytest
from prefect.client.schemas import FlowRun, TaskRun, Workspace
from prefect.futures import PrefectFuture
from prefect.server.schemas.core import Flow
from prefect.task_runners import SequentialTaskRunner
from requests.models import Response

from datahub.api.entities.datajob import DataJob
from datahub.utilities.urns.dataset_urn import DatasetUrn
from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset, _Entity

mock_transform_task_json: Dict = {
    "name": "transform",
    "description": "Transform the actual data",
    "task_key": "__main__.transform",
    "tags": ["etl flow task"],
}

mock_extract_task_run_json: Dict = {
    "id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b",
    "created": "2023-06-06T05:51:54.822707+00:00",
    "updated": "2023-06-06T05:51:55.126000+00:00",
    "name": "Extract-0",
    "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
    "task_key": "__main__.extract",
    "dynamic_key": "0",
    "cache_key": None,
    "cache_expiration": None,
    "task_version": None,
    "empirical_policy": {
        "max_retries": 0,
        "retry_delay_seconds": 0.0,
        "retries": 0,
        "retry_delay": 0,
        "retry_jitter_factor": None,
    },
    "tags": [],
    "state_id": "e280decd-2cc8-4428-a70f-149bcaf95b3c",
    "task_inputs": {},
    "state_type": "COMPLETED",
    "state_name": "Completed",
    "run_count": 1,
    "flow_run_run_count": 1,
    "expected_start_time": "2023-06-06T05:51:54.822183+00:00",
    "next_scheduled_start_time": None,
    "start_time": "2023-06-06T05:51:55.016264+00:00",
    "end_time": "2023-06-06T05:51:55.096534+00:00",
    "total_run_time": 0.08027,
    "estimated_run_time": 0.08027,
    "estimated_start_time_delta": 0.194081,
    "state": {
        "id": "e280decd-2cc8-4428-a70f-149bcaf95b3c",
        "type": "COMPLETED",
        "name": "Completed",
        "timestamp": "2023-06-06T05:51:55.096534+00:00",
        "message": None,
        "data": {"type": "unpersisted"},
        "state_details": {
            "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
            "task_run_id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b",
            "child_flow_run_id": None,
            "scheduled_time": None,
            "cache_key": None,
            "cache_expiration": None,
            "untrackable_result": False,
            "pause_timeout": None,
            "pause_reschedule": False,
            "pause_key": None,
            "refresh_cache": None,
        },
    },
}

mock_transform_task_run_json: Dict = {
    "id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7",
    "created": "2023-06-06T05:51:55.160372+00:00",
    "updated": "2023-06-06T05:51:55.358000+00:00",
    "name": "transform-0",
    "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
    "task_key": "__main__.transform",
    "dynamic_key": "0",
    "cache_key": None,
    "cache_expiration": None,
    "task_version": None,
    "empirical_policy": {
        "max_retries": 0,
        "retry_delay_seconds": 0.0,
        "retries": 0,
        "retry_delay": 0,
        "retry_jitter_factor": None,
    },
    "tags": [],
    "state_id": "971ad82e-6e5f-4691-abab-c900358e96c2",
    "task_inputs": {
        "actual_data": [
            {"input_type": "task_run", "id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b"}
        ]
    },
    "state_type": "COMPLETED",
    "state_name": "Completed",
    "run_count": 1,
    "flow_run_run_count": 1,
    "expected_start_time": "2023-06-06T05:51:55.159416+00:00",
    "next_scheduled_start_time": None,
    "start_time": "2023-06-06T05:51:55.243159+00:00",
    "end_time": "2023-06-06T05:51:55.332950+00:00",
    "total_run_time": 0.089791,
    "estimated_run_time": 0.089791,
    "estimated_start_time_delta": 0.083743,
    "state": {
        "id": "971ad82e-6e5f-4691-abab-c900358e96c2",
        "type": "COMPLETED",
        "name": "Completed",
        "timestamp": "2023-06-06T05:51:55.332950+00:00",
        "message": None,
        "data": {"type": "unpersisted"},
        "state_details": {
            "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
            "task_run_id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7",
            "child_flow_run_id": None,
            "scheduled_time": None,
            "cache_key": None,
            "cache_expiration": None,
            "untrackable_result": False,
            "pause_timeout": None,
            "pause_reschedule": False,
            "pause_key": None,
            "refresh_cache": None,
        },
    },
}
mock_load_task_run_json: Dict = {
    "id": "f19f83ea-316f-4781-8cbe-1d5d8719afc3",
    "created": "2023-06-06T05:51:55.389823+00:00",
    "updated": "2023-06-06T05:51:55.566000+00:00",
    "name": "Load_task-0",
    "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
    "task_key": "__main__.load",
    "dynamic_key": "0",
    "cache_key": None,
    "cache_expiration": None,
    "task_version": None,
    "empirical_policy": {
        "max_retries": 0,
        "retry_delay_seconds": 0.0,
        "retries": 0,
        "retry_delay": 0,
        "retry_jitter_factor": None,
    },
    "tags": [],
    "state_id": "0cad13c8-84e4-4bcf-8616-c5904e10dcb4",
    "task_inputs": {
        "data": [
            {"input_type": "task_run", "id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7"}
        ]
    },
    "state_type": "COMPLETED",
    "state_name": "Completed",
    "run_count": 1,
    "flow_run_run_count": 1,
    "expected_start_time": "2023-06-06T05:51:55.389075+00:00",
    "next_scheduled_start_time": None,
    "start_time": "2023-06-06T05:51:55.461812+00:00",
    "end_time": "2023-06-06T05:51:55.535954+00:00",
    "total_run_time": 0.074142,
    "estimated_run_time": 0.074142,
    "estimated_start_time_delta": 0.072737,
    "state": {
        "id": "0cad13c8-84e4-4bcf-8616-c5904e10dcb4",
        "type": "COMPLETED",
        "name": "Completed",
        "timestamp": "2023-06-06T05:51:55.535954+00:00",
        "message": None,
        "data": {"type": "unpersisted"},
        "state_details": {
            "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
            "task_run_id": "f19f83ea-316f-4781-8cbe-1d5d8719afc3",
            "child_flow_run_id": None,
            "scheduled_time": None,
            "cache_key": None,
            "cache_expiration": None,
            "untrackable_result": True,
            "pause_timeout": None,
            "pause_reschedule": False,
            "pause_key": None,
            "refresh_cache": None,
        },
    },
}
mock_flow_json: Dict = {
    "id": "cc65498f-d950-4114-8cc1-7af9e8fdf91b",
    "created": "2023-06-02T12:31:10.988697+00:00",
    "updated": "2023-06-02T12:31:10.988710+00:00",
    "name": "etl",
    "description": "Extract transform load flow",
    "tags": [],
}
mock_flow_run_json: Dict = {
    "id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
    "created": "2023-06-06T05:51:54.544266+00:00",
    "updated": "2023-06-06T05:51:55.622000+00:00",
    "name": "olivine-beagle",
    "flow_id": "cc65498f-d950-4114-8cc1-7af9e8fdf91b",
    "state_id": "ca2db325-d98f-40e7-862e-449cd0cc9a6e",
    "deployment_id": None,
    "work_queue_name": None,
    "flow_version": "3ba54dfa31a7c9af4161aa4cd020a527",
    "parameters": {},
    "idempotency_key": None,
    "context": {},
    "empirical_policy": {
        "max_retries": 0,
        "retry_delay_seconds": 0.0,
        "retries": 0,
        "retry_delay": 0,
        "pause_keys": [],
        "resuming": False,
    },
    "tags": [],
    "parent_task_run_id": None,
    "state_type": "COMPLETED",
    "state_name": "Completed",
    "run_count": 1,
    "expected_start_time": "2023-06-06T05:51:54.543357+00:00",
    "next_scheduled_start_time": None,
    "start_time": "2023-06-06T05:51:54.750523+00:00",
    "end_time": "2023-06-06T05:51:55.596446+00:00",
    "total_run_time": 0.845923,
    "estimated_run_time": 0.845923,
    "estimated_start_time_delta": 0.207166,
    "auto_scheduled": False,
    "infrastructure_document_id": None,
    "infrastructure_pid": None,
    "created_by": None,
    "work_pool_name": None,
    "state": {
        "id": "ca2db325-d98f-40e7-862e-449cd0cc9a6e",
        "type": "COMPLETED",
        "name": "Completed",
        "timestamp": "2023-06-06T05:51:55.596446+00:00",
        "message": "All states completed.",
        "data": {"type": "unpersisted"},
        "state_details": {
            "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
            "task_run_id": None,
            "child_flow_run_id": None,
            "scheduled_time": None,
            "cache_key": None,
            "cache_expiration": None,
            "untrackable_result": False,
            "pause_timeout": None,
            "pause_reschedule": False,
            "pause_key": None,
            "refresh_cache": None,
        },
    },
}
mock_graph_json: List[Dict] = [
    {
        "id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b",
        "name": "Extract-0",
        "upstream_dependencies": [],
        "state": {
            "id": "e280decd-2cc8-4428-a70f-149bcaf95b3c",
            "type": "COMPLETED",
            "name": "Completed",
            "timestamp": "2023-06-06T05:51:55.096534+00:00",
            "message": None,
            "data": {"type": "unpersisted"},
            "state_details": {
                "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
                "task_run_id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b",
                "child_flow_run_id": None,
                "scheduled_time": None,
                "cache_key": None,
                "cache_expiration": None,
                "untrackable_result": False,
                "pause_timeout": None,
                "pause_reschedule": False,
                "pause_key": None,
                "refresh_cache": None,
            },
        },
        "expected_start_time": "2023-06-06T05:51:54.822183+00:00",
        "start_time": "2023-06-06T05:51:55.016264+00:00",
        "end_time": "2023-06-06T05:51:55.096534+00:00",
        "total_run_time": 0.08027,
        "estimated_run_time": 0.08027,
        "untrackable_result": False,
    },
    {
        "id": "f19f83ea-316f-4781-8cbe-1d5d8719afc3",
        "name": "Load_task-0",
        "upstream_dependencies": [
            {"input_type": "task_run", "id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7"}
        ],
        "state": {
            "id": "0cad13c8-84e4-4bcf-8616-c5904e10dcb4",
            "type": "COMPLETED",
            "name": "Completed",
            "timestamp": "2023-06-06T05:51:55.535954+00:00",
            "message": None,
            "data": {"type": "unpersisted"},
            "state_details": {
                "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
                "task_run_id": "f19f83ea-316f-4781-8cbe-1d5d8719afc3",
                "child_flow_run_id": None,
                "scheduled_time": None,
                "cache_key": None,
                "cache_expiration": None,
                "untrackable_result": True,
                "pause_timeout": None,
                "pause_reschedule": False,
                "pause_key": None,
                "refresh_cache": None,
            },
        },
        "expected_start_time": "2023-06-06T05:51:55.389075+00:00",
        "start_time": "2023-06-06T05:51:55.461812+00:00",
        "end_time": "2023-06-06T05:51:55.535954+00:00",
        "total_run_time": 0.074142,
        "estimated_run_time": 0.074142,
        "untrackable_result": True,
    },
    {
        "id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7",
        "name": "transform-0",
        "upstream_dependencies": [
            {"input_type": "task_run", "id": "fa14a52b-d271-4c41-99cb-6b42ca7c070b"}
        ],
        "state": {
            "id": "971ad82e-6e5f-4691-abab-c900358e96c2",
            "type": "COMPLETED",
            "name": "Completed",
            "timestamp": "2023-06-06T05:51:55.332950+00:00",
            "message": None,
            "data": {"type": "unpersisted"},
            "state_details": {
                "flow_run_id": "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e",
                "task_run_id": "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7",
                "child_flow_run_id": None,
                "scheduled_time": None,
                "cache_key": None,
                "cache_expiration": None,
                "untrackable_result": False,
                "pause_timeout": None,
                "pause_reschedule": False,
                "pause_key": None,
                "refresh_cache": None,
            },
        },
        "expected_start_time": "2023-06-06T05:51:55.159416+00:00",
        "start_time": "2023-06-06T05:51:55.243159+00:00",
        "end_time": "2023-06-06T05:51:55.332950+00:00",
        "total_run_time": 0.089791,
        "estimated_run_time": 0.089791,
        "untrackable_result": False,
    },
]
mock_workspace_json: Dict = {
    "account_id": "33e98cfe-ad06-4ceb-a500-c11148499f75",
    "account_name": "shubhamjagtapgslabcom",
    "account_handle": "shubhamjagtapgslabcom",
    "workspace_id": "157eb822-1b3b-4338-ae80-98edd5d00cb9",
    "workspace_name": "datahub",
    "workspace_description": "",
    "workspace_handle": "datahub",
}


async def mock_task_run_future():
    extract_prefect_future: PrefectFuture = PrefectFuture(
        name=mock_extract_task_run_json["name"],
        key=UUID("4552629a-ac04-4590-b286-27642292739f"),
        task_runner=SequentialTaskRunner(),
    )
    extract_prefect_future.task_run = cast(
        None, TaskRun.parse_obj(mock_extract_task_run_json)
    )
    transform_prefect_future: PrefectFuture = PrefectFuture(
        name=mock_transform_task_run_json["name"],
        key=UUID("40fff3e5-5ef4-4b8b-9cc8-786f91bcc656"),
        task_runner=SequentialTaskRunner(),
    )
    transform_prefect_future.task_run = cast(
        None, TaskRun.parse_obj(mock_transform_task_run_json)
    )
    load_prefect_future: PrefectFuture = PrefectFuture(
        name=mock_load_task_run_json["name"],
        key=UUID("7565f596-9eb0-4330-ba34-963e7839883e"),
        task_runner=SequentialTaskRunner(),
    )
    load_prefect_future.task_run = cast(
        None, TaskRun.parse_obj(mock_load_task_run_json)
    )
    return [extract_prefect_future, transform_prefect_future, load_prefect_future]


@pytest.fixture(scope="module")
def mock_run_logger():
    with patch(
        "prefect_datahub.datahub_emitter.get_run_logger",
        return_value=logging.getLogger(),
    ) as mock_logger:
        yield mock_logger


@pytest.fixture(scope="module")
def mock_run_context(mock_run_logger):
    task_run_ctx = MagicMock()
    task_run_ctx.task.task_key = mock_transform_task_json["task_key"]
    task_run_ctx.task.name = mock_transform_task_json["name"]
    task_run_ctx.task.description = mock_transform_task_json["description"]
    task_run_ctx.task.tags = mock_transform_task_json["tags"]

    flow_run_ctx = MagicMock()
    flow_run_ctx.flow.name = mock_flow_json["name"]
    flow_run_ctx.flow.description = mock_flow_json["description"]
    flow_run_obj = FlowRun.parse_obj(mock_flow_run_json)
    flow_run_ctx.flow_run.id = flow_run_obj.id
    flow_run_ctx.flow_run.name = flow_run_obj.name
    flow_run_ctx.flow_run.flow_id = flow_run_obj.flow_id
    flow_run_ctx.flow_run.start_time = flow_run_obj.start_time
    flow_run_ctx.task_run_futures = asyncio.run(mock_task_run_future())

    with patch(
        "prefect_datahub.datahub_emitter.TaskRunContext"
    ) as mock_task_run_ctx, patch(
        "prefect_datahub.datahub_emitter.FlowRunContext"
    ) as mock_flow_run_ctx:
        mock_task_run_ctx.get.return_value = task_run_ctx
        mock_flow_run_ctx.get.return_value = flow_run_ctx
        yield (task_run_ctx, flow_run_ctx)


async def mock_task_run(*args, **kwargs):
    task_run_id = str(kwargs["task_run_id"])
    if task_run_id == "fa14a52b-d271-4c41-99cb-6b42ca7c070b":
        return TaskRun.parse_obj(mock_extract_task_run_json)
    elif task_run_id == "dd15ee83-5d28-4bf1-804f-f84eab9f9fb7":
        return TaskRun.parse_obj(mock_transform_task_run_json)
    elif task_run_id == "f19f83ea-316f-4781-8cbe-1d5d8719afc3":
        return TaskRun.parse_obj(mock_load_task_run_json)
    return None


async def mock_flow(*args, **kwargs):
    return Flow.parse_obj(mock_flow_json)


async def mock_flow_run(*args, **kwargs):
    return FlowRun.parse_obj(mock_flow_run_json)


async def mock_flow_run_graph(*args, **kwargs):
    response = Response()
    response.status_code = 200
    response._content = json.dumps(mock_graph_json, separators=(",", ":")).encode(
        "utf-8"
    )
    return response


async def mock_api_healthcheck(*args, **kwargs):
    return None


async def mock_read_workspaces(*args, **kwargs):
    return [Workspace.parse_obj(mock_workspace_json)]


@pytest.fixture(scope="module")
def mock_prefect_client():
    prefect_client_mock = MagicMock()
    prefect_client_mock.read_flow.side_effect = mock_flow
    prefect_client_mock.read_flow_run.side_effect = mock_flow_run
    prefect_client_mock.read_task_run.side_effect = mock_task_run
    prefect_client_mock._client.get.side_effect = mock_flow_run_graph
    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_client:
        mock_client.get_client.return_value = prefect_client_mock
        yield prefect_client_mock


@pytest.fixture(scope="module")
def mock_prefect_cloud_client():
    prefect_cloud_client_mock = MagicMock()
    prefect_cloud_client_mock.api_healthcheck.side_effect = mock_api_healthcheck
    prefect_cloud_client_mock.read_workspaces.side_effect = mock_read_workspaces
    with patch("prefect_datahub.datahub_emitter.cloud") as mock_client, patch(
        "prefect_datahub.datahub_emitter.PREFECT_API_URL.value",
        return_value="https://api.prefect.cloud/api/accounts/33e98cfe-ad06-4ceb-"
        "a500-c11148499f75/workspaces/157eb822-1b3b-4338-ae80-98edd5d00cb9",
    ):
        mock_client.get_cloud_client.return_value = prefect_cloud_client_mock
        yield prefect_cloud_client_mock


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_entities_to_urn_list(mock_emit):
    dataset_urn_list = DatahubEmitter()._entities_to_urn_list(
        [Dataset("snowflake", "mydb.schema.tableA")]
    )
    for dataset_urn in dataset_urn_list:
        assert isinstance(dataset_urn, DatasetUrn)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_get_flow_run_graph(mock_emit, mock_prefect_client):
    graph_json = asyncio.run(
        DatahubEmitter()._get_flow_run_graph("c3b947e5-3fa1-4b46-a2e2-58d50c938f2e")
    )
    assert isinstance(graph_json, list)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test__get_workspace(mock_emit, mock_prefect_cloud_client):
    workspace_name = DatahubEmitter()._get_workspace()
    assert workspace_name == "datahub"


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_add_task(mock_emit, mock_run_context):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    datahub_emitter = DatahubEmitter()
    inputs: Optional[List[_Entity]] = [Dataset("snowflake", "mydb.schema.tableA")]
    outputs: Optional[List[_Entity]] = [Dataset("snowflake", "mydb.schema.tableC")]
    datahub_emitter.add_task(
        inputs=inputs,
        outputs=outputs,
    )

    task_run_ctx = mock_run_context[0]
    flow_run_ctx = mock_run_context[1]

    expected_datajob_urn = (
        f"urn:li:dataJob:(urn:li:dataFlow:"
        f"(prefect,{flow_run_ctx.flow.name},PROD),{task_run_ctx.task.task_key})"
    )

    assert expected_datajob_urn in datahub_emitter._datajobs_to_emit.keys()
    actual_datajob = datahub_emitter._datajobs_to_emit[expected_datajob_urn]
    assert isinstance(actual_datajob, DataJob)
    assert str(actual_datajob.flow_urn) == "urn:li:dataFlow:(prefect,etl,PROD)"
    assert actual_datajob.name == task_run_ctx.task.name
    assert actual_datajob.description == task_run_ctx.task.description
    assert actual_datajob.tags == task_run_ctx.task.tags
    assert (
        str(actual_datajob.inlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableA,PROD)"
    )
    assert (
        str(actual_datajob.outlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
    )
    assert mock_emit.emit.call_count == 0


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_emit_flow(
    mock_emit, mock_run_context, mock_prefect_client, mock_prefect_cloud_client
):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    platform_instance = "datahub_workspace"

    datahub_emitter = DatahubEmitter(platform_instance=platform_instance)
    datahub_emitter.add_task()
    datahub_emitter.emit_flow()

    task_run_ctx = mock_run_context[0]
    flow_run_ctx = mock_run_context[1]

    expected_dataflow_urn = (
        f"urn:li:dataFlow:(prefect,{platform_instance}.{flow_run_ctx.flow.name},PROD)"
    )

    expected_dataflow_urn = (
        f"urn:li:dataFlow:(prefect,{platform_instance}.{flow_run_ctx.flow.name},PROD)"
    )

    # Ignore the first call (index 0) which is a connection call
    # DataFlow assertions
    assert mock_emitter.method_calls[1][1][0].aspectName == "dataFlowInfo"
    assert mock_emitter.method_calls[1][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[2][1][0].aspectName == "status"
    assert mock_emitter.method_calls[2][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[3][1][0].aspectName == "ownership"
    assert mock_emitter.method_calls[3][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[4][1][0].aspectName == "globalTags"
    assert mock_emitter.method_calls[4][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[5][1][0].aspectName == "browsePaths"
    assert mock_emitter.method_calls[5][1][0].entityUrn == expected_dataflow_urn

    # DataProcessInstance assertions for the flow
    assert (
        mock_emitter.method_calls[10][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[10][1][0].entityUrn
        == "urn:li:dataProcessInstance:56231547bcc2781e0c14182ceab6c9ac"
    )
    assert (
        mock_emitter.method_calls[11][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[11][1][0].entityUrn
        == "urn:li:dataProcessInstance:56231547bcc2781e0c14182ceab6c9ac"
    )
    assert (
        mock_emitter.method_calls[12][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[12][1][0].entityUrn
        == "urn:li:dataProcessInstance:56231547bcc2781e0c14182ceab6c9ac"
    )

    # DataJob assertions for extract
    assert mock_emitter.method_calls[13][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[13][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[14][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[14][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[15][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[15][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[16][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[16][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[17][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[17][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[18][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[18][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )

    # DataProcessInstance assertions for extract
    assert (
        mock_emitter.method_calls[19][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[19][1][0].entityUrn
        == "urn:li:dataProcessInstance:b048ba729c1403f229a0760f8765d691"
    )
    assert (
        mock_emitter.method_calls[20][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[20][1][0].entityUrn
        == "urn:li:dataProcessInstance:b048ba729c1403f229a0760f8765d691"
    )
    assert (
        mock_emitter.method_calls[21][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[21][1][0].entityUrn
        == "urn:li:dataProcessInstance:b048ba729c1403f229a0760f8765d691"
    )
    assert (
        mock_emitter.method_calls[22][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[22][1][0].entityUrn
        == "urn:li:dataProcessInstance:b048ba729c1403f229a0760f8765d691"
    )

    # DataJob assertions for load
    assert mock_emitter.method_calls[23][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[23][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[24][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[24][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[25][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[25][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[26][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[26][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[27][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[27][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[28][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[28][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )

    # DataProcessInstance assertions for load
    assert (
        mock_emitter.method_calls[29][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[29][1][0].entityUrn
        == "urn:li:dataProcessInstance:e7df9fe09bb4da19687b8199e5ee5038"
    )
    assert (
        mock_emitter.method_calls[30][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[30][1][0].entityUrn
        == "urn:li:dataProcessInstance:e7df9fe09bb4da19687b8199e5ee5038"
    )
    assert (
        mock_emitter.method_calls[31][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[31][1][0].entityUrn
        == "urn:li:dataProcessInstance:e7df9fe09bb4da19687b8199e5ee5038"
    )
    assert (
        mock_emitter.method_calls[32][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[32][1][0].entityUrn
        == "urn:li:dataProcessInstance:e7df9fe09bb4da19687b8199e5ee5038"
    )

    # DataJob assertions for transform
    assert mock_emitter.method_calls[33][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[33][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[34][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[34][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[35][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[35][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[36][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[36][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[37][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[37][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert (
        mock_emitter.method_calls[37][1][0].aspect.tags[0].tag
        == f"urn:li:tag:{task_run_ctx.task.tags[0]}"
    )
    assert mock_emitter.method_calls[38][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[38][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )

    # DataProcessInstance assertions for transform
    assert (
        mock_emitter.method_calls[39][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[39][1][0].entityUrn
        == "urn:li:dataProcessInstance:bfa255d4d1fba52d23a52c9de4f6d0a6"
    )
    assert (
        mock_emitter.method_calls[40][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[40][1][0].entityUrn
        == "urn:li:dataProcessInstance:bfa255d4d1fba52d23a52c9de4f6d0a6"
    )
    assert (
        mock_emitter.method_calls[41][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[41][1][0].entityUrn
        == "urn:li:dataProcessInstance:bfa255d4d1fba52d23a52c9de4f6d0a6"
    )
    assert (
        mock_emitter.method_calls[42][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[42][1][0].entityUrn
        == "urn:li:dataProcessInstance:bfa255d4d1fba52d23a52c9de4f6d0a6"
    )

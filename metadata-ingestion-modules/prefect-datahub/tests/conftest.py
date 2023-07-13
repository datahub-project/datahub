import asyncio
import json
import logging
from typing import Dict, List
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from prefect.client.schemas import FlowRun, TaskRun, Workspace
from prefect.futures import PrefectFuture
from prefect.server.schemas.core import Flow
from requests.models import Response

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
    extract_prefect_future = PrefectFuture(
        name=mock_extract_task_run_json["name"],
        key=UUID("4552629a-ac04-4590-b286-27642292739f"),
        task_runner=None,
    )
    extract_prefect_future.task_run = TaskRun.parse_obj(mock_extract_task_run_json)
    transform_prefect_future = PrefectFuture(
        name=mock_transform_task_run_json["name"],
        key=UUID("40fff3e5-5ef4-4b8b-9cc8-786f91bcc656"),
        task_runner=None,
    )
    transform_prefect_future.task_run = TaskRun.parse_obj(mock_transform_task_run_json)
    load_prefect_future = PrefectFuture(
        name=mock_load_task_run_json["name"],
        key=UUID("7565f596-9eb0-4330-ba34-963e7839883e"),
        task_runner=None,
    )
    load_prefect_future.task_run = TaskRun.parse_obj(mock_load_task_run_json)
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

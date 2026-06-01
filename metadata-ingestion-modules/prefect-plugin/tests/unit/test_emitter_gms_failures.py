"""Tests for GMS emit() failures: emitter must swallow and isolate per-task failures."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import List
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

import pytest
from prefect.client.schemas import FlowRun, TaskRun
from prefect.client.schemas.objects import Flow
from requests.models import Response

from datahub.configuration.common import OperationalError

FLOW_ID = "cc65498f-d950-4114-8cc1-7af9e8fdf91b"
FLOW_RUN_ID = "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e"


def _task_run_dict(task_run_id: str, task_key: str) -> dict:
    return {
        "id": task_run_id,
        "name": task_key.split(".")[-1],
        "flow_run_id": FLOW_RUN_ID,
        "task_key": task_key,
        "dynamic_key": "0",
        "state_type": "COMPLETED",
        "state_name": "Completed",
        "run_count": 1,
        "flow_run_run_count": 1,
        "total_run_time": 0.1,
        "estimated_run_time": 0.1,
        "estimated_start_time_delta": 0.0,
        "start_time": "2024-01-01T00:00:00+00:00",
        "end_time": "2024-01-01T00:00:01+00:00",
    }


def _flow_dict() -> dict:
    return {
        "id": FLOW_ID,
        "name": "test_flow",
        "tags": [],
        "labels": {},
    }


def _flow_run_dict() -> dict:
    return {
        "id": FLOW_RUN_ID,
        "name": "test_flow_run",
        "flow_id": FLOW_ID,
        "state_type": "COMPLETED",
        "state_name": "Completed",
        "run_count": 1,
        "expected_start_time": "2024-01-01T00:00:00+00:00",
        "start_time": "2024-01-01T00:00:00+00:00",
        "end_time": "2024-01-01T00:00:05+00:00",
        "total_run_time": 5.0,
        "estimated_run_time": 5.0,
        "estimated_start_time_delta": 0.0,
    }


def _build_two_task_chain():
    """Two-task linear chain: a -> b. Returns (task_runs, graph_json)."""
    a_id, b_id = str(uuid4()), str(uuid4())
    task_runs = [
        TaskRun.model_validate(_task_run_dict(a_id, "mod.task_a")),
        TaskRun.model_validate(_task_run_dict(b_id, "mod.task_b")),
    ]
    graph_json = [
        {"id": a_id, "upstream_dependencies": []},
        {"id": b_id, "upstream_dependencies": [{"input_type": "task_run", "id": a_id}]},
    ]
    return task_runs, graph_json


def _make_failing_rest_emitter(failure_pattern):
    """`failure_pattern(call_index, mcp) -> Optional[Exception]`."""
    call_log: List = []
    rest = Mock()
    rest.test_connection.return_value = None

    def fake_emit(mcp, *args, **kwargs):
        idx = len(call_log)
        call_log.append(mcp)
        exc = failure_pattern(idx, mcp)
        if exc is not None:
            raise exc

    rest.emit.side_effect = fake_emit
    return rest, call_log


def _run_flow(rest_emitter_factory, debug_logger):
    from prefect_datahub.datahub_emitter import DatahubEmitter

    task_runs, graph_json = _build_two_task_chain()
    flow_obj = Flow.model_validate(_flow_dict())
    flow_run_obj = FlowRun.model_validate(_flow_run_dict())
    flow_run_ctx = SimpleNamespace(
        flow=SimpleNamespace(name="test_flow", description=None),
        flow_run=SimpleNamespace(
            id=flow_run_obj.id,
            name=flow_run_obj.name,
            flow_id=flow_run_obj.flow_id,
            start_time=flow_run_obj.start_time,
        ),
    )

    async def fake_read_task_runs(*a, **kw):
        offset = kw.get("offset", 0)
        limit = kw.get("limit", 200)
        return list(task_runs[offset : offset + limit])

    async def fake_read_flow(*a, **kw):
        return flow_obj

    async def fake_read_flow_run(*a, **kw):
        return flow_run_obj

    async def fake_graph_get(*a, **kw):
        import json as _j

        resp = Response()
        resp.status_code = 200
        resp._content = _j.dumps(graph_json, separators=(",", ":")).encode("utf-8")
        return resp

    async def fake_no_sleep(_s):
        return None

    async def fake_api_healthcheck(*a, **kw):
        return None

    async def fake_read_workspaces(*a, **kw):
        return []

    prefect_client = MagicMock()
    prefect_client.read_flow.side_effect = fake_read_flow
    prefect_client.read_flow_run.side_effect = fake_read_flow_run
    prefect_client.read_task_runs.side_effect = fake_read_task_runs
    prefect_client._client.get.side_effect = fake_graph_get

    cloud_client = MagicMock()
    cloud_client.api_healthcheck.side_effect = fake_api_healthcheck
    cloud_client.read_workspaces.side_effect = fake_read_workspaces

    with (
        patch(
            "prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True
        ) as mock_rest_cls,
        patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch,
        patch("prefect_datahub.datahub_emitter.cloud") as mock_cloud,
        patch(
            "prefect_datahub.datahub_emitter.PREFECT_API_URL.value",
            return_value="https://prefect.local/api",
        ),
        patch("prefect_datahub.datahub_emitter.TaskRunContext") as mock_task_ctx,
        patch("prefect_datahub.datahub_emitter.FlowRunContext") as mock_flow_ctx,
        patch(
            "prefect_datahub.datahub_emitter.asyncio.sleep", side_effect=fake_no_sleep
        ),
        patch(
            "prefect_datahub.datahub_emitter.get_run_logger", return_value=debug_logger
        ),
    ):
        mock_orch.get_client.return_value = prefect_client
        mock_cloud.get_cloud_client.return_value = cloud_client
        mock_task_ctx.get.return_value = None
        mock_flow_ctx.get.return_value = flow_run_ctx

        rest_emitter, call_log = rest_emitter_factory()
        mock_rest_cls.return_value = rest_emitter

        DatahubEmitter().emit_flow()

    return call_log


@pytest.fixture
def debug_logger():
    return MagicMock(spec=logging.Logger)


def test_first_aspect_failure_aborts_dataflow_branch(debug_logger):
    def factory():
        return _make_failing_rest_emitter(
            lambda idx, mcp: OperationalError("503", {"status": 503})
            if idx == 0
            else None
        )

    call_log = _run_flow(factory, debug_logger)
    assert len(call_log) == 1
    debug_logger.debug.assert_called()


def test_per_task_failure_does_not_block_other_tasks(debug_logger):
    # First task's first aspect fails; second task must still emit fully.
    state = {"failed_once": False}

    def pattern(idx, mcp):
        urn = getattr(mcp, "entityUrn", "") or ""
        if "dataJob:" in urn and not state["failed_once"]:
            state["failed_once"] = True
            return OperationalError("503", {"status": 503})
        return None

    def factory():
        return _make_failing_rest_emitter(pattern)

    call_log = _run_flow(factory, debug_logger)

    task_urns = {
        getattr(m, "entityUrn", None)
        for m in call_log
        if "dataJob:" in (getattr(m, "entityUrn", "") or "")
    }
    assert len(task_urns) == 2, f"second task did not emit: {task_urns}"
    debug_logger.debug.assert_called()


def test_total_gms_outage_emits_no_panic(debug_logger):
    def factory():
        return _make_failing_rest_emitter(
            lambda idx, mcp: OperationalError("503", {"status": 503})
        )

    call_log = _run_flow(factory, debug_logger)
    # Finite emission count; no retry loop.
    assert 0 < len(call_log) < 200
    debug_logger.debug.assert_called()

"""Negative-path tests: emitter must not raise; failures should be logged at debug."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from prefect_datahub.datahub_emitter import DatahubEmitter

VALID_FLOW_RUN_ID = "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e"
VALID_FLOW_ID = "cc65498f-d950-4114-8cc1-7af9e8fdf91b"


@pytest.fixture
def debug_logger(monkeypatch):
    logger = MagicMock(spec=logging.Logger)
    monkeypatch.setattr(
        "prefect_datahub.datahub_emitter.get_run_logger", lambda: logger
    )
    return logger


@pytest.fixture
def emitter(monkeypatch):
    with patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True):
        yield DatahubEmitter()


# ---------------------------------------------------------------------------
# _get_workspace error paths
# ---------------------------------------------------------------------------


def test_get_workspace_returns_none_when_cloud_healthcheck_raises(
    emitter, debug_logger
):
    async def boom(*a, **k):
        raise ConnectionError("no cloud here")

    cloud_client = MagicMock()
    cloud_client.api_healthcheck = AsyncMock(side_effect=boom)
    with patch("prefect_datahub.datahub_emitter.cloud") as mock_cloud:
        mock_cloud.get_cloud_client.return_value = cloud_client
        assert emitter._get_workspace() is None
    debug_logger.debug.assert_called()


def test_get_workspace_returns_none_when_api_url_lacks_workspace(emitter, debug_logger):
    # Local Prefect (no /workspaces/ in PREFECT_API_URL) short-circuits to None.
    async def ok(*a, **k):
        return None

    cloud_client = MagicMock()
    cloud_client.api_healthcheck = AsyncMock(side_effect=ok)
    with (
        patch("prefect_datahub.datahub_emitter.cloud") as mock_cloud,
        patch(
            "prefect_datahub.datahub_emitter.PREFECT_API_URL.value",
            return_value="http://127.0.0.1:4200/api",
        ),
    ):
        mock_cloud.get_cloud_client.return_value = cloud_client
        assert emitter._get_workspace() is None


def test_get_workspace_returns_none_when_id_does_not_match(emitter, debug_logger):
    # Cloud reachable, but no workspace matches the URL's id.
    async def ok(*a, **k):
        return None

    async def workspaces(*a, **k):
        from prefect.client.schemas import Workspace

        return [
            Workspace.model_validate(
                {
                    "account_id": "00000000-0000-0000-0000-000000000001",
                    "account_name": "x",
                    "account_handle": "x",
                    "workspace_id": "00000000-0000-0000-0000-000000000099",
                    "workspace_name": "real_ws",
                    "workspace_description": "",
                    "workspace_handle": "real_ws",
                }
            )
        ]

    cloud_client = MagicMock()
    cloud_client.api_healthcheck = AsyncMock(side_effect=ok)
    cloud_client.read_workspaces = AsyncMock(side_effect=workspaces)

    with (
        patch("prefect_datahub.datahub_emitter.cloud") as mock_cloud,
        patch(
            "prefect_datahub.datahub_emitter.PREFECT_API_URL.value",
            return_value="https://api.prefect.cloud/api/accounts/00000000-0000-0000-0000-000000000001/workspaces/00000000-0000-0000-0000-000000000002",
        ),
    ):
        mock_cloud.get_cloud_client.return_value = cloud_client
        assert emitter._get_workspace() is None


# ---------------------------------------------------------------------------
# _get_flow_run_graph error paths
# ---------------------------------------------------------------------------


def test_get_flow_run_graph_returns_none_on_http_failure(emitter, debug_logger):
    client = MagicMock()
    client._client.get = AsyncMock(side_effect=RuntimeError("server exploded"))
    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch:
        mock_orch.get_client.return_value = client
        result = asyncio.run(emitter._get_flow_run_graph(VALID_FLOW_RUN_ID))
    assert result is None
    debug_logger.debug.assert_called()


def test_get_flow_run_graph_raises_on_response_without_json(emitter, debug_logger):
    bad_response = object()

    async def bad_get(*a, **k):
        return bad_response

    client = MagicMock()
    client._client.get = AsyncMock(side_effect=bad_get)
    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch:
        mock_orch.get_client.return_value = client
        result = asyncio.run(emitter._get_flow_run_graph(VALID_FLOW_RUN_ID))
    assert result is None
    debug_logger.debug.assert_called()


# ---------------------------------------------------------------------------
# _generate_dataflow error paths
# ---------------------------------------------------------------------------


def test_generate_dataflow_returns_none_when_read_flow_raises(emitter, debug_logger):
    flow_run_ctx = MagicMock()
    flow_run_ctx.flow.name = "etl"
    flow_run_ctx.flow.description = "desc"
    flow_run_ctx.flow_run.flow_id = UUID(VALID_FLOW_ID)

    async def boom(**kwargs):
        raise RuntimeError("flow disappeared")

    client = MagicMock()
    client.read_flow = AsyncMock(side_effect=boom)
    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch:
        mock_orch.get_client.return_value = client
        result = emitter._generate_dataflow(flow_run_ctx=flow_run_ctx)

    assert result is None
    debug_logger.debug.assert_called()


# ---------------------------------------------------------------------------
# _emit_task_run error paths
# ---------------------------------------------------------------------------


def test_emit_task_run_raises_when_state_unknown():
    """Unknown task_run.state_name should raise — not silently swallowed."""
    from prefect.client.schemas import TaskRun

    from prefect_datahub.datahub_emitter import DatahubEmitter

    with patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True):
        emitter = DatahubEmitter()

    task_run = TaskRun.model_validate(
        {
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "task-1",
            "flow_run_id": VALID_FLOW_RUN_ID,
            "task_key": "k",
            "dynamic_key": "0",
            "state_type": "PAUSED",
            "state_name": "MYSTERIOUS_NEW_STATE",
            "run_count": 1,
            "flow_run_run_count": 1,
            "total_run_time": 0.0,
            "estimated_run_time": 0.0,
            "estimated_start_time_delta": 0.0,
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-01-01T00:00:01+00:00",
        }
    )

    from datahub.api.entities.datajob import DataJob
    from datahub.utilities.urns.data_flow_urn import DataFlowUrn

    flow_urn = DataFlowUrn.create_from_ids(
        orchestrator="prefect", flow_id="f", env="PROD"
    )
    datajob = DataJob(id="k", flow_urn=flow_urn, name="k")

    with pytest.raises(Exception, match="State"):
        emitter._emit_task_run(
            datajob=datajob,
            flow_run_name="fr",
            task_run=task_run,
        )


# ---------------------------------------------------------------------------
# add_task error paths
# ---------------------------------------------------------------------------


def test_add_task_swallows_missing_context(emitter, debug_logger):
    with (
        patch("prefect_datahub.datahub_emitter.FlowRunContext") as mflow,
        patch("prefect_datahub.datahub_emitter.TaskRunContext") as mtask,
    ):
        mflow.get.return_value = None
        mtask.get.return_value = None
        emitter.add_task()
    debug_logger.debug.assert_called()


# ---------------------------------------------------------------------------
# emit_flow error paths
# ---------------------------------------------------------------------------


def test_emit_flow_swallows_missing_context(emitter, debug_logger):
    with patch("prefect_datahub.datahub_emitter.FlowRunContext") as mflow:
        mflow.get.return_value = None
        emitter.emit_flow()
    debug_logger.debug.assert_called()


# ---------------------------------------------------------------------------
# _emit_tasks error path: read_task_runs raises
# ---------------------------------------------------------------------------


def test_emit_tasks_swallows_read_task_runs_failure(emitter, debug_logger):
    flow_run_ctx = MagicMock()
    flow_run_ctx.flow_run.id = UUID(VALID_FLOW_RUN_ID)
    dataflow = MagicMock()

    with (
        patch.object(emitter, "_get_flow_run_graph", new=AsyncMock(return_value=[])),
        patch.object(
            emitter,
            "_fetch_all_task_runs_stable",
            new=AsyncMock(side_effect=RuntimeError("backend gone")),
        ),
    ):
        emitter._emit_tasks(flow_run_ctx=flow_run_ctx, dataflow=dataflow)
    debug_logger.debug.assert_called()


# ---------------------------------------------------------------------------
# _stringify_property — Prefect 2 -> 3 pendulum ISO format stability
# ---------------------------------------------------------------------------


def test_stringify_property_datetime_uses_isoformat_with_t_separator():
    from datetime import datetime, timezone

    from prefect_datahub.datahub_emitter import _stringify_property

    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert _stringify_property(dt) == "2024-01-02T03:04:05+00:00"


# ---------------------------------------------------------------------------
# Orphan upstream: graph_json references a task_run id not in task_run_key_map
# ---------------------------------------------------------------------------


def test_emit_single_task_skips_orphan_upstream(emitter, debug_logger):
    from prefect.client.schemas import TaskRun

    from datahub.api.entities.datajob import DataFlow, DataJob

    task_a_id = "11111111-1111-1111-1111-111111111111"
    task_b_id = "22222222-2222-2222-2222-222222222222"

    task_b = TaskRun.model_validate(
        {
            "id": task_b_id,
            "name": "task_b",
            "flow_run_id": VALID_FLOW_RUN_ID,
            "task_key": "mod.task_b",
            "dynamic_key": "0",
            "state_type": "COMPLETED",
            "state_name": "Completed",
            "run_count": 1,
            "flow_run_run_count": 1,
            "total_run_time": 0.1,
            "estimated_run_time": 0.1,
            "estimated_start_time_delta": 0.0,
        }
    )

    task_run_key_map = {task_b_id: "mod.task_b"}
    graph_node_map = {
        task_b_id: {
            "id": task_b_id,
            "upstream_dependencies": [{"input_type": "task_run", "id": task_a_id}],
        }
    }

    flow_run_ctx = MagicMock()
    flow_run_ctx.flow_run = MagicMock()
    flow_run_ctx.flow.name = "etl"
    flow_run_ctx.flow.description = "desc"

    dataflow = DataFlow(
        orchestrator="prefect", id="etl", env="PROD", platform_instance=None
    )
    datajob = DataJob(id="mod.task_b", flow_urn=dataflow.urn, name="mod.task_b")

    with (
        patch.object(emitter, "_generate_datajob", return_value=datajob),
        patch.object(emitter, "_emit_task_run"),
    ):
        emitter._emit_single_task(
            task_run=task_b,
            flow_run_ctx=flow_run_ctx,
            dataflow=dataflow,
            task_run_key_map=task_run_key_map,
            graph_node_map=graph_node_map,
            workspace_name=None,
        )

    assert datajob.upstream_urns == []


# ---------------------------------------------------------------------------
# _stringify_property — bare date type (not datetime)
# ---------------------------------------------------------------------------


def test_stringify_property_date_uses_isoformat():
    from datetime import date

    from prefect_datahub.datahub_emitter import _stringify_property

    d = date(2024, 6, 15)
    assert _stringify_property(d) == "2024-06-15"


# ---------------------------------------------------------------------------
# Task absent from graph_node_map: should emit without upstreams, not skip
# ---------------------------------------------------------------------------


def test_emit_single_task_absent_from_graph_emits_without_upstreams(
    emitter, debug_logger
):
    """When a task_run is not in graph_node_map (async persistence lag), the task
    should still be emitted with empty upstreams rather than being silently dropped."""
    from prefect.client.schemas import TaskRun

    from datahub.api.entities.datajob import DataFlow, DataJob

    task_id = "33333333-3333-3333-3333-333333333333"
    task_run = TaskRun.model_validate(
        {
            "id": task_id,
            "name": "task_a",
            "flow_run_id": VALID_FLOW_RUN_ID,
            "task_key": "mod.task_a",
            "dynamic_key": "0",
            "state_type": "COMPLETED",
            "state_name": "Completed",
            "run_count": 1,
            "flow_run_run_count": 1,
            "total_run_time": 0.1,
            "estimated_run_time": 0.1,
            "estimated_start_time_delta": 0.0,
        }
    )

    dataflow = DataFlow(
        orchestrator="prefect", id="etl", env="PROD", platform_instance=None
    )
    datajob = DataJob(id="mod.task_a", flow_urn=dataflow.urn, name="mod.task_a")

    flow_run_ctx = MagicMock()
    flow_run_ctx.flow_run = MagicMock()
    flow_run_ctx.flow.name = "etl"

    emit_task_run_calls = []

    def capture_emit_task_run(**kwargs):
        emit_task_run_calls.append(kwargs)

    with (
        patch.object(emitter, "_generate_datajob", return_value=datajob),
        patch.object(emitter, "_emit_task_run", side_effect=capture_emit_task_run),
    ):
        emitter._emit_single_task(
            task_run=task_run,
            flow_run_ctx=flow_run_ctx,
            dataflow=dataflow,
            task_run_key_map={task_id: "mod.task_a"},
            graph_node_map={},  # task absent from graph
            workspace_name=None,
        )

    assert datajob.upstream_urns == []
    assert len(emit_task_run_calls) == 1, (
        "task must still be emitted even when absent from graph"
    )

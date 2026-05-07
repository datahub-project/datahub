"""Tests for pipeline run history tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.runs import (
    _millis_to_iso,
    _summarize_run,
    get_runs,
)

DATAJOB_URN = "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,prod),my_task)"


def _make_run(
    urn: str = "urn:li:dataProcessInstance:abc123",
    name: str = "my_dag_my_task_run_2026-05-07",
    created_time: int = 1746614400000,  # 2026-05-07T12:00:00Z
    finished_time: int = 1746614700000,  # 2026-05-07T12:05:00Z
    duration_ms: int = 300000,
    status: str = "SUCCESS",
    external_url: str = "https://airflow.example.com/runs/1",
) -> dict:
    return {
        "urn": urn,
        "externalUrl": external_url,
        "properties": {
            "name": name,
            "externalUrl": None,
            "created": {"time": created_time},
        },
        "state": [
            {
                "status": "COMPLETE",
                "timestampMillis": finished_time,
                "durationMillis": duration_ms,
                "attempt": 1,
                "result": {
                    "resultType": status,
                    "nativeResultType": "airflow",
                },
            }
        ],
    }


def _make_gql_response(runs: list, total: int = None) -> dict:
    return {
        "entity": {
            "runs": {
                "total": total if total is not None else len(runs),
                "start": 0,
                "count": len(runs),
                "runs": runs,
            }
        }
    }


# --- Unit tests for helper functions ---


def test_millis_to_iso_valid():
    result = _millis_to_iso(1746614400000)
    assert result == "2026-05-07T12:00:00+00:00"


def test_millis_to_iso_none():
    assert _millis_to_iso(None) is None


def test_summarize_run_success():
    raw = _make_run()
    summary = _summarize_run(raw)

    assert summary["urn"] == "urn:li:dataProcessInstance:abc123"
    assert summary["name"] == "my_dag_my_task_run_2026-05-07"
    assert summary["status"] == "SUCCESS"
    assert summary["startedAt"] == "2026-05-07T12:00:00+00:00"
    assert summary["finishedAt"] == "2026-05-07T12:05:00+00:00"
    assert summary["durationMs"] == 300000
    assert summary["externalUrl"] == "https://airflow.example.com/runs/1"


def test_summarize_run_prefers_complete_state_event():
    """COMPLETE event should be preferred over STARTED event for status."""
    raw = _make_run()
    raw["state"] = [
        {
            "status": "STARTED",
            "timestampMillis": 1746614400000,
            "durationMillis": None,
            "attempt": 1,
            "result": None,
        },
        {
            "status": "COMPLETE",
            "timestampMillis": 1746614700000,
            "durationMillis": 300000,
            "attempt": 1,
            "result": {"resultType": "SUCCESS", "nativeResultType": "airflow"},
        },
    ]
    summary = _summarize_run(raw)
    assert summary["status"] == "SUCCESS"
    assert summary["durationMs"] == 300000


def test_summarize_run_no_state():
    """Run with no state events should still return basic info."""
    raw = _make_run()
    raw["state"] = []
    summary = _summarize_run(raw)

    assert summary["urn"] == "urn:li:dataProcessInstance:abc123"
    assert "status" not in summary
    assert "finishedAt" not in summary


def test_summarize_run_strips_none_values():
    raw = _make_run()
    raw["externalUrl"] = None
    raw["properties"]["externalUrl"] = None
    summary = _summarize_run(raw)
    assert "externalUrl" not in summary


# --- Integration-style tests for get_runs ---


def _mock_graph():
    graph = MagicMock()
    graph.frontend_base_url = None
    return graph


def test_get_runs_datajob_success():
    run = _make_run()
    mock_response = _make_gql_response([run], total=1)

    with patch(
        "datahub_agent_context.mcp_tools.runs.execute_graphql",
        return_value=mock_response,
    ), DataHubContext(_mock_graph()):
        result = get_runs(urn=DATAJOB_URN, count=5)

    assert result["success"] is True
    assert result["data"]["total"] == 1
    assert result["data"]["count"] == 1
    runs = result["data"]["runs"]
    assert len(runs) == 1
    assert runs[0]["status"] == "SUCCESS"
    assert "Last run:" in result["message"]
    assert "SUCCESS" in result["message"]


def test_get_runs_empty():
    mock_response = _make_gql_response([], total=0)

    with patch(
        "datahub_agent_context.mcp_tools.runs.execute_graphql",
        return_value=mock_response,
    ), DataHubContext(_mock_graph()):
        result = get_runs(urn=DATAJOB_URN)

    assert result["success"] is True
    assert result["data"]["total"] == 0
    assert result["data"]["runs"] == []


def test_get_runs_entity_not_datajob():
    """Non-DataJob/DataFlow entity returns failure response."""
    mock_response = {"entity": {}}

    with patch(
        "datahub_agent_context.mcp_tools.runs.execute_graphql",
        return_value=mock_response,
    ), DataHubContext(_mock_graph()):
        result = get_runs(urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)")

    assert result["success"] is False
    assert result["data"]["runs"] == []


def test_get_runs_clamps_count():
    mock_response = _make_gql_response([], total=0)

    with patch(
        "datahub_agent_context.mcp_tools.runs.execute_graphql",
        return_value=mock_response,
    ) as mock_gql, DataHubContext(_mock_graph()):
        get_runs(urn=DATAJOB_URN, count=999)

    variables = mock_gql.call_args.kwargs["variables"]
    assert variables["count"] == 50  # MAX_COUNT


def test_get_runs_propagates_error():
    with patch(
        "datahub_agent_context.mcp_tools.runs.execute_graphql",
        side_effect=Exception("GMS unavailable"),
    ), DataHubContext(_mock_graph()):
        with pytest.raises(RuntimeError, match="Error fetching run history"):
            get_runs(urn=DATAJOB_URN)

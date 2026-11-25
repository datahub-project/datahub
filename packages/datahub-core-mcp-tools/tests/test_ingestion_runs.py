"""run_ingestion, list_ingestion_runs, list_ingestion_run_entities and
rollback_ingestion_run. Reads go through a test ReadBackend; writes reach a
mocked graph."""

import json
from typing import Iterator, Optional, cast
from unittest.mock import MagicMock

import pytest
import requests
from fastmcp.exceptions import ToolError
from pydantic import JsonValue

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools import ingestion_runs
from datahub_core_mcp_tools.tools.ingestion_runs import (
    list_ingestion_run_entities,
    list_ingestion_runs,
    rollback_ingestion_run,
    run_ingestion,
    summarize_run,
)

JsonObject = dict[str, JsonValue]
SOURCE_URN = "urn:li:dataHubIngestionSource:my-source"
EXECUTION_URN = "urn:li:dataHubExecutionRequest:abc-123"
RECIPE = "source:\n  type: postgres\n  config:\n    password: ${PG_PASSWORD}\n"
STORED_RECIPE = {"source": {"type": "postgres", "config": {"host_port": "db"}}}


class _Runs:
    def __init__(self) -> None:
        self.sources: dict[str, JsonObject] = {}
        self.executions: dict[str, JsonObject] = {}
        self.statuses: list[Optional[str]] = []
        self.source_runs: Optional[JsonObject] = None
        self.executions_calls: list[tuple[str, int, int]] = []
        self.rows: list[JsonObject] = []
        self.rows_calls: list[tuple[str, int, int]] = []

    def ingestion_source(self, urn: str) -> Optional[JsonObject]:
        return self.sources.get(urn)

    def ingestion_executions(
        self, urn: str, start: int, count: int
    ) -> Optional[JsonObject]:
        self.executions_calls.append((urn, start, count))
        return self.source_runs

    def execution_request(
        self, urn: str, *, report: bool = True
    ) -> Optional[JsonObject]:
        if not report:
            status = self.statuses.pop(0) if self.statuses else "SUCCESS"
            return {"urn": urn, "result": {"status": status} if status else None}
        return self.executions.get(urn)

    def ingestion_run_rows(
        self, run_id: str, start: int, count: int
    ) -> list[JsonObject]:
        self.rows_calls.append((run_id, start, count))
        return self.rows


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> _Runs:
    monkeypatch.setattr(ingestion_runs, "POLL_SECONDS", 0.0)
    runs = _Runs()
    runs.sources[SOURCE_URN] = {
        "urn": SOURCE_URN,
        "name": "pg",
        "type": "postgres",
        "config": {"recipe": json.dumps(STORED_RECIPE), "version": "1.6.0"},
    }
    return runs


@pytest.fixture
def graph(backend: _Runs) -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph._gms_server = "http://gms:8080"
    context = ToolContext([ReadBackendBinding(cast(ReadBackend, backend))])
    with with_datahub_client(client, context):
        yield client._graph


def _write(graph: MagicMock, operation: str) -> dict:
    assert graph.execute_graphql.call_count == 1
    kwargs = graph.execute_graphql.call_args.kwargs
    assert kwargs["operation_name"] == operation
    return kwargs["variables"]["input"]


def test_a_run_of_a_saved_source(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {
        "createIngestionExecutionRequest": EXECUTION_URN
    }

    result = run_ingestion(source_urn=SOURCE_URN)

    assert result["execution_urn"] == EXECUTION_URN
    assert _write(graph, "createIngestionExecutionRequest") == {
        "ingestionSourceUrn": SOURCE_URN
    }


def test_a_run_needs_an_existing_saved_source(graph: MagicMock) -> None:
    # GMS answers a run of a missing source with a 500.
    with pytest.raises(ToolError, match="No ingestion source exists"):
        run_ingestion(source_urn="urn:li:dataHubIngestionSource:gone")
    with pytest.raises(ToolError, match="create_ingestion_source"):
        run_ingestion(recipe=RECIPE)
    with pytest.raises(ToolError, match="exactly one"):
        run_ingestion()
    with pytest.raises(ToolError, match="exactly one"):
        run_ingestion(source_urn=SOURCE_URN, recipe=RECIPE)
    graph.execute_graphql.assert_not_called()


def test_a_run_with_no_reply_says_how_to_check(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError, match="may or may not") as e:
        run_ingestion(source_urn=SOURCE_URN)
    assert "list_ingestion_runs(source_urn=" in str(e.value)


def test_a_connection_test_of_a_recipe_waits_for_its_report(
    graph: MagicMock, backend: _Runs
) -> None:
    graph.execute_graphql.return_value = {"createTestConnectionRequest": EXECUTION_URN}
    backend.statuses = [None, "RUNNING", "SUCCESS"]
    backend.executions[EXECUTION_URN] = {
        "result": {
            "status": "SUCCESS",
            "structuredReport": {
                "serializedValue": json.dumps(
                    {
                        "basic_connectivity": {
                            "capable": False,
                            "failure_reason": "password authentication failed",
                        }
                    }
                )
            },
        }
    }

    result = run_ingestion(recipe=RECIPE, test_source_connection=True)

    test_input = _write(graph, "createTestConnectionRequest")
    assert json.loads(test_input["recipe"])["source"]["config"]["password"] == (
        "${PG_PASSWORD}"
    )
    assert "version" not in test_input
    assert backend.statuses == []
    assert result["status"] == "SUCCESS"
    assert result["report"]["basic_connectivity"]["capable"] is False
    assert "password authentication failed" in result["message"]


def test_a_connection_test_of_a_source_uses_its_recipe_and_version(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.return_value = {"createTestConnectionRequest": EXECUTION_URN}

    run_ingestion(source_urn=SOURCE_URN, test_source_connection=True)

    test_input = _write(graph, "createTestConnectionRequest")
    assert json.loads(test_input["recipe"]) == STORED_RECIPE
    assert test_input["version"] == "1.6.0"


def test_an_unfinished_connection_test_says_where_to_read_it(
    graph: MagicMock, backend: _Runs, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph.execute_graphql.return_value = {"createTestConnectionRequest": EXECUTION_URN}
    clock = iter([0.0, 0.5, 2.0])
    monkeypatch.setattr(ingestion_runs.time, "monotonic", lambda: next(clock))
    backend.statuses = ["RUNNING"]

    result = run_ingestion(
        recipe=RECIPE, test_source_connection=True, timeout_seconds=1
    )

    assert result["status"] == "RUNNING"
    assert "report" not in result
    assert f"list_ingestion_runs(execution_urn='{EXECUTION_URN}')" in result["message"]


def test_a_connection_test_refuses_literal_credentials(graph: MagicMock) -> None:
    with pytest.raises(ToolError, match="literal credentials"):
        run_ingestion(
            recipe="source:\n  type: postgres\n  config:\n    password: pw\n",
            test_source_connection=True,
        )
    graph.execute_graphql.assert_not_called()


def test_one_run_returns_its_status_and_first_failures(
    graph: MagicMock, backend: _Runs
) -> None:
    failures = [{"message": f"f{i}"} for i in range(15)]
    backend.executions[EXECUTION_URN] = {
        "urn": EXECUTION_URN,
        "input": {
            "task": "RUN_INGEST",
            "source": {"ingestionSource": SOURCE_URN},
        },
        "result": {
            "status": "FAILURE",
            "structuredReport": {
                "serializedValue": json.dumps(
                    {
                        "cli": {"cli_version": "1.6.0"},
                        "source": {"report": {"failures": failures}},
                    }
                )
            },
        },
    }

    result = list_ingestion_runs(execution_urn=EXECUTION_URN)

    assert result["status"] == "FAILURE"
    assert result["source_urn"] == SOURCE_URN
    assert result["cli_version"] == "1.6.0"
    assert result["errors_count"] == 15
    assert len(result["failures"]) == ingestion_runs.MAX_REPORT_ITEMS
    with pytest.raises(ToolError, match="No run exists"):
        list_ingestion_runs(execution_urn="urn:li:dataHubExecutionRequest:gone")
    with pytest.raises(ToolError, match="not a run's execution URN"):
        list_ingestion_runs(execution_urn="abc-123")


def test_a_status_filter_reads_the_widest_window(
    graph: MagicMock, backend: _Runs
) -> None:
    runs: list[JsonValue] = [
        {"urn": f"urn:li:dataHubExecutionRequest:{i}", "result": {"status": s}}
        for i, s in enumerate(["SUCCESS", "SUCCESS", "FAILURE", "FAILURE"])
    ]
    backend.source_runs = {"name": "pg", "executions": {"executionRequests": runs}}

    result = list_ingestion_runs(source_urn=SOURCE_URN, status="failure", limit=1)

    assert backend.executions_calls == [(SOURCE_URN, 0, ingestion_runs.MAX_LISTED_RUNS)]
    assert [r["urn"] for r in result["runs"]] == ["urn:li:dataHubExecutionRequest:2"]

    backend.source_runs = None
    with pytest.raises(ToolError, match="No ingestion source exists"):
        list_ingestion_runs(source_urn=SOURCE_URN)
    with pytest.raises(ToolError, match="exactly one"):
        list_ingestion_runs()


@pytest.mark.parametrize(
    ("report", "warnings", "errors", "written"),
    [
        # A clean run's empty failure list counts as zero, not unknown.
        ('{"source": {"report": {"failures": [], "warnings": []}}}', 0, 0, None),
        # Older reports key warnings by message.
        ('{"warnings": {"w": ["ctx"]}, "failures": {"a": [], "b": []}}', 1, 2, None),
        # A log line scan has bare counts.
        ("workunits_produced: 7\nwarnings: 3\nfailures: 1", 3, 1, 7),
    ],
)
def test_run_counts_from_each_report_shape(
    report: str, warnings: int, errors: int, written: Optional[int]
) -> None:
    summary = summarize_run({"result": {"status": "SUCCESS", "report": report}})

    assert summary["warnings_count"] == warnings
    assert summary["errors_count"] == errors
    assert summary["entities_written"] == written


def test_run_entities_page_without_aspect_values(
    graph: MagicMock, backend: _Runs
) -> None:
    backend.rows = [
        {
            "urn": "urn:li:dataset:a",
            "aspectName": "schemaMetadata",
            "version": 0,
            "timestamp": 1,
            "keyAspect": False,
            "runId": "abc-123",
            "metadata": '{"big": "value"}',
        }
    ]

    result = list_ingestion_run_entities(EXECUTION_URN, start=-5, count=10_000)

    assert backend.rows_calls == [("abc-123", 0, ingestion_runs.MAX_RUN_ENTITY_ROWS)]
    assert result["rows"] == [
        {
            "urn": "urn:li:dataset:a",
            "aspectName": "schemaMetadata",
            "version": 0,
            "timestamp": 1,
            "keyAspect": False,
        }
    ]


def test_rollback_defaults_to_a_dry_run_of_the_execution_id(graph: MagicMock) -> None:
    graph._post_generic.return_value = {
        "value": {
            "entitiesAffected": 2,
            "aspectsReverted": 3,
            "aspectRowSummaries": [
                {"runId": "abc-123", "urn": "urn:li:dataset:a", "metadata": "{}"},
                {"runId": "other-run", "urn": "urn:li:dataset:b"},
            ],
        }
    }

    result = rollback_ingestion_run(EXECUTION_URN)

    url, payload = graph._post_generic.call_args.args
    assert url == "http://gms:8080/runs?action=rollback"
    assert payload == {"runId": "abc-123", "dryRun": True, "safe": True}
    assert result["dry_run"] is True
    assert result["entities_affected"] == 2
    assert result["reverted_aspects"] == [{"urn": "urn:li:dataset:a"}]


def test_a_rollback_with_no_reply_says_how_to_check(graph: MagicMock) -> None:
    graph._post_generic.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError, match="may or may not") as e:
        rollback_ingestion_run("my-cli-run", dry_run=False)
    assert "dry_run=true" in str(e.value)

"""Run tools of the opt-in ``ingestion`` group: start a source's run on the
executor or test a recipe's connection (``datahub ingest run``), read runs
(``list-source-runs``), what a run wrote (``show``) and undo it (``rollback``).

DataHub Core's executor runs saved sources and connection tests only: an
unsaved recipe, a dry run or a preview needs DataHub Cloud's recipe runs.
"""

import json
import re
import time
from typing import Optional, cast

from fastmcp.exceptions import ToolError
from pydantic import JsonValue

from .. import graphql_helpers
from ..read_backend import get_read_backend
from ..version_requirements import destructive, open_world, read_only
from .ingestion_recipes import checked_recipe
from .ingestion_sources import existing_source, stored_recipe

JsonObject = dict[str, JsonValue]

EXECUTION_URN_PREFIX = "urn:li:dataHubExecutionRequest:"
MAX_LISTED_RUNS = 50
MAX_REPORT_ITEMS = 10
MAX_RUN_ENTITY_ROWS = 500
MAX_ROLLBACK_ROWS = 50
# A connection test holds a worker thread while it waits, so the wait is
# bounded, below the service's 300-second tool time limit; a longer test is
# read afterwards with list_ingestion_runs.
MAX_TEST_WAIT_SECONDS = 240
POLL_SECONDS = 2.0
_RUNNING = frozenset({"RUNNING", "PENDING", "UP_FOR_RETRY"})
_REPORT_INT_RE = re.compile(r"['\"]?(\w+)['\"]?\s*[:=]\s*(\d+)")


def _execution_urn(value: str) -> str:
    urn = value.strip()
    if not urn.startswith(EXECUTION_URN_PREFIX):
        raise ToolError(
            f"{value!r} is not a run's execution URN ({EXECUTION_URN_PREFIX}...). "
            "list_ingestion_runs(source_urn=...) lists a source's runs."
        )
    return urn


def _run_id(run_id_or_execution_urn: str) -> str:
    """A managed run's id is its execution request's id; a CLI run has its own."""
    value = run_id_or_execution_urn.strip()
    if not value:
        raise ToolError("`run_id` must not be empty.")
    if value.startswith(EXECUTION_URN_PREFIX):
        return value[len(EXECUTION_URN_PREFIX) :]
    return value


def parse_run_report(report: Optional[str]) -> JsonObject:
    """An execution report: JSON when it is, else its integer ``key: n`` lines."""
    if not report:
        return {}
    try:
        as_json = json.loads(report)
    except ValueError:
        as_json = None
    if isinstance(as_json, dict):
        return cast(JsonObject, as_json)
    summary: JsonObject = {}
    for match in _REPORT_INT_RE.finditer(report):
        summary.setdefault(match.group(1), int(match.group(2)))
    return summary


def _result(execution: JsonObject) -> JsonObject:
    result = execution.get("result")
    return result if isinstance(result, dict) else {}


def run_report_fields(execution: JsonObject) -> JsonObject:
    """A run's parsed report, with the source report's fields on the top level."""
    result = _result(execution)
    structured = result.get("structuredReport")
    serialized = (
        structured.get("serializedValue") if isinstance(structured, dict) else None
    )
    report = result.get("report")
    # The structured report is the pipeline's JSON; `report` is mostly the log,
    # whose line scan is a last resort.
    parsed = parse_run_report(
        serialized if isinstance(serialized, str) else None
    ) or parse_run_report(report if isinstance(report, str) else None)
    source = parsed.get("source")
    if isinstance(source, dict) and isinstance(source.get("report"), dict):
        parsed = {**parsed, **cast(JsonObject, source["report"])}
    return parsed


def summarize_run(execution: JsonObject) -> JsonObject:
    """The fields of a run an agent needs most."""
    result = _result(execution)
    parsed = run_report_fields(execution)

    def count(*keys: str) -> Optional[int]:
        for key in keys:
            value = parsed.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        return None

    warnings = parsed.get("warnings")
    # A clean run's "failures": [] counts as 0, not as absent.
    failures = parsed.get("failures")
    if failures is None:
        failures = parsed.get("errors")
    run_input = execution.get("input")
    return {
        "status": result.get("status"),
        "requested_at_ms": run_input.get("requestedAt")
        if isinstance(run_input, dict)
        else None,
        "start_time_ms": result.get("startTimeMs"),
        "duration_ms": result.get("durationMs"),
        "entities_written": count(
            "workunits_produced", "events_produced", "entities_written"
        ),
        # Lists (v2 reports) or {message: contexts} dicts (older ones) count by
        # length; a line-scanned log has bare counts.
        "warnings_count": len(warnings)
        if isinstance(warnings, (list, dict))
        else count("warnings", "warnings_count"),
        "errors_count": len(failures)
        if isinstance(failures, (list, dict))
        else count("failures", "errors", "errors_count", "failures_count"),
    }


_ROW_FIELDS = ("urn", "aspectName", "version", "timestamp", "keyAspect")


def _row(row: JsonObject) -> JsonObject:
    """An aspect row without its value (``metadata``), which can be large."""
    return {key: row[key] for key in _ROW_FIELDS if key in row}


def _first_items(value: JsonValue, limit: int = MAX_REPORT_ITEMS) -> JsonValue:
    if isinstance(value, list):
        return value[:limit]
    if isinstance(value, dict):
        return dict(list(value.items())[:limit])
    return value


def _wait_for_test(execution_urn: str, timeout_seconds: int) -> Optional[str]:
    """Poll a connection test until it finishes; its last status (None: not
    picked up yet)."""
    backend = get_read_backend()
    deadline = time.monotonic() + min(max(1, timeout_seconds), MAX_TEST_WAIT_SECONDS)
    status: Optional[str] = None
    while time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
        execution = backend.execution_request(execution_urn, report=False)
        value = _result(execution or {}).get("status")
        status = value if isinstance(value, str) else None
        if status is not None and status not in _RUNNING:
            break
    return status


def _test_connection(
    recipe_json: str, version: Optional[str], timeout_seconds: int
) -> dict:
    test_input: dict = {"recipe": recipe_json}
    if version:
        test_input["version"] = version
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("ingestion.gql"),
        operation_name="createTestConnectionRequest",
        variables={"input": test_input},
        unknown_outcome_hint="A connection test changes nothing: running it again "
        "is safe.",
    )
    execution_urn = result.get("createTestConnectionRequest")
    if not execution_urn:
        raise ToolError("createTestConnectionRequest returned no execution URN.")
    status = _wait_for_test(execution_urn, timeout_seconds)
    out: dict = {
        "test_source_connection": True,
        "execution_urn": execution_urn,
        "status": status or "PENDING",
    }
    if status is None or status in _RUNNING:
        out["message"] = (
            "The connection test has not finished yet (the executor may still be "
            "installing the connector). Read it later with "
            f"list_ingestion_runs(execution_urn={execution_urn!r})."
        )
        return out
    execution = get_read_backend().execution_request(execution_urn) or {}
    structured = _result(execution).get("structuredReport")
    serialized = (
        structured.get("serializedValue") if isinstance(structured, dict) else None
    )
    report = parse_run_report(serialized if isinstance(serialized, str) else None)
    out["report"] = report
    basic = report.get("basic_connectivity")
    if isinstance(basic, dict) and basic.get("capable") is True:
        out["message"] = "The connection succeeded."
    elif isinstance(basic, dict) and basic.get("capable") is False:
        out["message"] = f"The connection failed: {basic.get('failure_reason')}"
    elif report.get("internal_failure"):
        out["message"] = (
            f"The test could not run: {report.get('internal_failure_reason')}"
        )
    return out


@open_world
@destructive
def run_ingestion(
    source_urn: Optional[str] = None,
    recipe: Optional[str] = None,
    test_source_connection: bool = False,
    timeout_seconds: int = 30,
) -> dict:
    """Start a run of an ingestion source on the executor (`datahub ingest run`), or only test a recipe's connection.

    Pass `source_urn` to run a saved source: it returns at once with the run's
    `execution_urn`. Poll list_ingestion_runs(execution_urn=...) until `status`
    is no longer RUNNING; runs take minutes to hours. A run writes metadata to
    DataHub, and with stateful ingestion removes what the source no longer has.

    `test_source_connection` (`--test-source-connection`) only tests that a
    recipe can connect, and starts no run. Pass `recipe` (YAML or JSON, with
    ${SECRET} references) or `source_urn` (its stored recipe and CLI version).
    It waits up to `timeout_seconds` (at most 240) and returns the test report;
    a failed connection is a normal result, and `report.basic_connectivity`
    says why. An unsaved recipe can only be tested, not run: save it with
    create_ingestion_source first.
    """
    if (source_urn is None) == (recipe is None):
        raise ToolError("Pass exactly one of `source_urn` or `recipe`.")
    if test_source_connection:
        if recipe is not None:
            return _test_connection(
                json.dumps(checked_recipe(recipe)), None, timeout_seconds
            )
        assert source_urn is not None
        source = existing_source(source_urn)
        config = source.get("config")
        version = config.get("version") if isinstance(config, dict) else None
        return _test_connection(
            json.dumps(stored_recipe(source)),
            version if isinstance(version, str) else None,
            timeout_seconds,
        )
    if recipe is not None:
        raise ToolError(
            "DataHub runs only saved sources: save the recipe with "
            "create_ingestion_source and run that, or pass "
            "test_source_connection=true to test its connection."
        )
    assert source_urn is not None
    # GMS answers a run of a source that does not exist with a 500, not a 404.
    urn = str(existing_source(source_urn).get("urn") or source_urn.strip())
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("ingestion.gql"),
        operation_name="createIngestionExecutionRequest",
        variables={"input": {"ingestionSourceUrn": urn}},
        unknown_outcome_hint=(
            f"Look for a new run with list_ingestion_runs(source_urn={urn!r}) "
            "before starting another."
        ),
    )
    execution_urn = result.get("createIngestionExecutionRequest")
    if not execution_urn:
        raise ToolError("createIngestionExecutionRequest returned no execution URN.")
    return {
        "execution_urn": execution_urn,
        "source_urn": urn,
        "message": "The run has started. Poll list_ingestion_runs(execution_urn="
        f"{execution_urn!r}) until its status is no longer RUNNING.",
    }


@read_only
def list_ingestion_runs(
    source_urn: Optional[str] = None,
    execution_urn: Optional[str] = None,
    limit: int = 10,
    status: Optional[str] = None,
) -> dict:
    """List a source's recent runs, newest first (`datahub ingest list-source-runs`).

    `status` filters, for example "FAILURE" or "SUCCESS". Pass `execution_urn`
    instead to get one run's result: status, duration, counts, the first
    failures and warnings, and the CLI version. That works for ingestion runs
    and connection tests.
    """
    if (source_urn is None) == (execution_urn is None):
        raise ToolError("Pass exactly one of `source_urn` or `execution_urn`.")
    backend = get_read_backend()
    if source_urn is not None:
        urn = source_urn.strip()
        limit = min(max(1, limit), MAX_LISTED_RUNS)
        # GraphQL cannot filter runs by status, so a status filter reads the
        # largest window and filters it here.
        source = backend.ingestion_executions(
            urn, 0, MAX_LISTED_RUNS if status else limit
        )
        if source is None:
            raise ToolError(
                f"No ingestion source exists at {urn}. Call list_ingestion_sources "
                "to find its URN."
            )
        executions = source.get("executions")
        runs = (
            executions.get("executionRequests")
            if isinstance(executions, dict)
            else None
        )
        rows = (
            [r for r in runs if isinstance(r, dict)] if isinstance(runs, list) else []
        )
        if status:
            wanted = status.strip().upper()
            rows = [r for r in rows if _result(r).get("status") == wanted][:limit]
        return {"source_urn": urn, "source_name": source.get("name"), "runs": rows}

    assert execution_urn is not None
    urn = _execution_urn(execution_urn)
    execution = backend.execution_request(urn)
    if not execution:
        raise ToolError(f"No run exists at {urn}.")
    report = run_report_fields(execution)
    failures = report.get("failures")
    if failures is None:
        failures = report.get("errors")
    cli = report.get("cli")
    run_input = execution.get("input")
    run_source = run_input.get("source") if isinstance(run_input, dict) else None
    return {
        "execution_urn": urn,
        "task": run_input.get("task") if isinstance(run_input, dict) else None,
        "source_urn": run_source.get("ingestionSource")
        if isinstance(run_source, dict)
        else None,
        **summarize_run(execution),
        "cli_version": cli.get("cli_version") if isinstance(cli, dict) else None,
        "failures": _first_items(failures),
        "warnings": _first_items(report.get("warnings")),
    }


@read_only
def list_ingestion_run_entities(run_id: str, start: int = 0, count: int = 100) -> dict:
    """List the entities and aspects a run wrote (`datahub ingest show`).

    `run_id` is a run's execution URN, or the run id of a CLI run. Page with
    `start` and `count` (at most 500).
    """
    rid = _run_id(run_id)
    count = min(max(1, count), MAX_RUN_ENTITY_ROWS)
    rows = get_read_backend().ingestion_run_rows(rid, max(0, start), count)
    return {"run_id": rid, "rows": [_row(r) for r in rows], "count": len(rows)}


@destructive
def rollback_ingestion_run(
    run_id: str, dry_run: bool = True, safe: bool = True
) -> dict:
    """Undo a run: remove the entities it created and revert the aspects it changed (`datahub ingest rollback`).

    Defaults to `dry_run`, which only reports what would change. Pass
    dry_run=false to roll back: the entities the run created are marked
    removed (soft-deleted), and the change can take a few seconds to show. With `safe` (the default), entities that other
    runs also wrote to are kept, and only this run's aspects are removed.
    `run_id` is a run's execution URN, or the run id of a CLI run.
    """
    rid = _run_id(run_id)
    graph = graphql_helpers.get_datahub_client()._graph
    payload = {"runId": rid, "dryRun": dry_run, "safe": safe}
    try:
        response = graph._post_generic(
            f"{graph._gms_server}/runs?action=rollback", payload
        )
    except Exception as e:
        status = graphql_helpers.http_status(e)
        if dry_run or (status is not None and status < 500):
            raise ToolError(
                f"Rolling back run {rid} failed: "
                f"{graphql_helpers.describe_datahub_error(e)}"
            ) from e
        raise ToolError(
            f"Rolling back run {rid} got no reply from DataHub ({status or type(e).__name__}), "
            "so it may or may not have been applied. Call rollback_ingestion_run "
            "with dry_run=true to see what is left to roll back."
        ) from e
    summary = response.get("value") if isinstance(response, dict) else None
    summary = summary if isinstance(summary, dict) else {}
    rows = [
        _row(row)
        for row in summary.get("aspectRowSummaries") or []
        if isinstance(row, dict) and row.get("runId") == rid
    ]
    return {
        "run_id": rid,
        "dry_run": dry_run,
        "entities_affected": summary.get("entitiesAffected", 0),
        "entities_deleted": summary.get("entitiesDeleted", 0),
        "aspects_reverted": summary.get("aspectsReverted", 0),
        "other_aspects_affected": summary.get("aspectsAffected", 0),
        "unsafe_entities_count": summary.get("unsafeEntitiesCount", 0),
        "reverted_aspects": rows[:MAX_ROLLBACK_ROWS],
    }

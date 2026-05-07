"""Tools for getting pipeline run history for DataJobs and DataFlows."""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import clean_gql_response, execute_graphql

logger = logging.getLogger(__name__)

GET_RUNS_QUERY = """
query GetEntityRuns($urn: String!, $start: Int!, $count: Int!) {
    entity(urn: $urn) {
        ... on DataJob {
            runs(start: $start, count: $count) {
                total
                start
                count
                runs {
                    urn
                    externalUrl
                    properties {
                        name
                        externalUrl
                        created {
                            time
                        }
                    }
                    state(limit: 2) {
                        status
                        timestampMillis
                        durationMillis
                        attempt
                        result {
                            resultType
                            nativeResultType
                        }
                    }
                }
            }
        }
        ... on DataFlow {
            runs(start: $start, count: $count) {
                total
                start
                count
                runs {
                    urn
                    externalUrl
                    properties {
                        name
                        externalUrl
                        created {
                            time
                        }
                    }
                    state(limit: 2) {
                        status
                        timestampMillis
                        durationMillis
                        attempt
                        result {
                            resultType
                            nativeResultType
                        }
                    }
                }
            }
        }
    }
}
"""

DEFAULT_COUNT = 10
MAX_COUNT = 50


def _millis_to_iso(millis: Optional[int]) -> Optional[str]:
    if millis is None:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()


def _summarize_run(run: dict[str, Any]) -> dict[str, Any]:
    """Convert a raw DataProcessInstance into a concise summary."""
    props = run.get("properties") or {}
    state_events = run.get("state") or []

    # state events are returned newest-first; pick the terminal COMPLETE event
    # if present, otherwise fall back to the first (most recent) event.
    complete_event = next(
        (e for e in state_events if e.get("status") == "COMPLETE"), None
    )
    latest_event = complete_event or (state_events[0] if state_events else None)

    result_type: Optional[str] = None
    native_result: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None

    if latest_event:
        result_obj = latest_event.get("result") or {}
        result_type = result_obj.get("resultType")
        native_result = result_obj.get("nativeResultType")
        finished_at = _millis_to_iso(latest_event.get("timestampMillis"))
        duration_ms = latest_event.get("durationMillis")

    created_time: Optional[str] = None
    created = props.get("created") or {}
    if created.get("time"):
        created_time = _millis_to_iso(created["time"])

    summary: dict[str, Any] = {
        "urn": run.get("urn"),
        "name": props.get("name"),
        "startedAt": created_time,
        "finishedAt": finished_at,
        "status": result_type,
        "nativeStatus": native_result,
        "durationMs": duration_ms,
        "externalUrl": run.get("externalUrl") or props.get("externalUrl"),
    }
    # Drop None values for cleaner output
    return {k: v for k, v in summary.items() if v is not None}


def get_runs(
    urn: str,
    start: int = 0,
    count: int = DEFAULT_COUNT,
) -> dict[str, Any]:
    """Get the execution run history for a DataJob (task) or DataFlow (DAG/pipeline).

    Returns a list of recent runs ordered newest-first, with timestamp, status,
    and duration for each run. Use this to answer questions like:
    - "When did this task last run?"
    - "Did the pipeline succeed today?"
    - "What was the last run status of this task?"

    Args:
        urn: URN of a DataJob (task) or DataFlow (pipeline/DAG).
            Examples:
            - DataJob:  urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,prod),my_task)
            - DataFlow: urn:li:dataFlow:(airflow,my_dag,prod)
        start: Pagination offset (default 0)
        count: Number of runs to return (default 10, max 50)

    Returns:
        Dictionary with:
        - success: Whether the request was successful
        - data: Contains start, count, total, and runs list
        - message: Summary message with last-run status

    Each run includes:
        - urn: Unique identifier for this run instance
        - name: Run ID / name as reported by the orchestrator
        - startedAt: ISO-8601 UTC timestamp when the run started
        - finishedAt: ISO-8601 UTC timestamp when the run completed (if available)
        - status: Outcome — SUCCESS, FAILURE, SKIPPED, or UP_FOR_RETRY
        - nativeStatus: Raw status string from the source system (e.g. "success")
        - durationMs: Run duration in milliseconds (if available)
        - externalUrl: Link to view the run in the source orchestrator

    Examples:
        # Get last 10 runs for an Airflow task
        get_runs(urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,prod),my_task)")

        # Get last 5 runs for a DAG
        get_runs(
            urn="urn:li:dataFlow:(airflow,my_dag,prod)",
            count=5,
        )

        # Paginate through run history
        get_runs(urn="urn:li:dataJob:(...)", start=10, count=10)

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_runs(
                urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,prod),my_task)"
            )
    """
    graph = get_graph()

    start = max(0, start)
    count = max(1, min(count, MAX_COUNT))

    try:
        result = execute_graphql(
            graph,
            query=GET_RUNS_QUERY,
            variables={"urn": urn, "start": start, "count": count},
            operation_name="GetEntityRuns",
        )

        entity = result.get("entity") or {}
        runs_result = entity.get("runs")

        if runs_result is None:
            return {
                "success": False,
                "data": {"start": start, "count": 0, "total": 0, "runs": []},
                "message": (
                    "No run history found. The entity may not be a DataJob or DataFlow, "
                    "or no runs have been recorded yet."
                ),
            }

        raw_runs = runs_result.get("runs") or []
        total = runs_result.get("total", 0)
        summaries = [_summarize_run(clean_gql_response(r)) for r in raw_runs]

        latest = summaries[0] if summaries else None
        if latest:
            last_run_at = latest.get("finishedAt") or latest.get("startedAt", "unknown")
            last_status = latest.get("status", "UNKNOWN")
            message = f"Last run: {last_run_at} — {last_status}. Total runs available: {total}."
        else:
            message = f"No runs found (total recorded: {total})."

        return {
            "success": True,
            "data": {
                "start": runs_result.get("start", start),
                "count": len(summaries),
                "total": total,
                "runs": summaries,
            },
            "message": message,
        }

    except Exception as e:
        raise RuntimeError(f"Error fetching run history for {urn}: {e}") from e

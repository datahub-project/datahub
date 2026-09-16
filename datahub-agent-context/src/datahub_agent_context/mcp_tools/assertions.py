"""Tools for getting dataset assertion information."""

import logging
from typing import Any, Literal, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import clean_gql_response, execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    _select_results_within_budget,
    truncate_descriptions,
)

logger = logging.getLogger(__name__)

SEARCH_ASSERTIONS_QUERY = """
query SearchDatasetAssertions(
    $query: String!,
    $orFilters: [AndFilterInput!],
    $start: Int!,
    $count: Int!,
    $runEventsLimit: Int!
) {
    searchAcrossEntities(
        input: {
            query: $query
            types: [ASSERTION]
            orFilters: $orFilters
            start: $start
            count: $count
            searchFlags: { skipHighlighting: true }
        }
    ) {
        start
        count
        total
        searchResults {
            entity {
                ... on Assertion {
                    urn
                    platform {
                        urn
                        name
                        properties { displayName }
                    }
                    info {
                        type
                        description
                        externalUrl
                        source { type }
                        datasetAssertion {
                            datasetUrn
                            scope
                            fields { urn path }
                            aggregation
                            operator
                            parameters {
                                value { value type }
                                minValue { value type }
                                maxValue { value type }
                            }
                            nativeType
                            logic
                        }
                        freshnessAssertion {
                            entityUrn
                            type
                            schedule {
                                type
                                cron { cron timezone }
                                fixedInterval { unit multiple }
                            }
                            filter { type sql }
                        }
                        volumeAssertion {
                            entityUrn
                            type
                            filter { type sql }
                            rowCountTotal {
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                            rowCountChange {
                                type
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                        }
                        sqlAssertion {
                            type
                            entityUrn
                            statement
                            changeType
                            operator
                            parameters {
                                value { value type }
                                minValue { value type }
                                maxValue { value type }
                            }
                        }
                        fieldAssertion {
                            type
                            entityUrn
                            filter { type sql }
                            fieldValuesAssertion {
                                field { path type nativeType }
                                transform { type }
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                                failThreshold { type value }
                                excludeNulls
                            }
                            fieldMetricAssertion {
                                field { path type nativeType }
                                metric
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                        }
                        schemaAssertion {
                            entityUrn
                            compatibility
                            fields { path type nativeType }
                        }
                        customAssertion {
                            type
                            entityUrn
                            field { urn path }
                            logic
                        }
                    }
                    runEvents(status: COMPLETE, limit: $runEventsLimit) {
                        total
                        failed
                        succeeded
                        runEvents {
                            timestampMillis
                            status
                            result {
                                type
                                rowCount
                                missingCount
                                unexpectedCount
                                actualAggValue
                                externalUrl
                                nativeResults { key value }
                                error { type }
                            }
                        }
                    }
                    tags {
                        tags {
                            tag {
                                urn
                                properties { name }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

DEFAULT_PAGE_SIZE = 5
MAX_PAGE_SIZE = 20
MIN_RUN_EVENTS = 1
MAX_RUN_EVENTS = 10

AssertionType = Literal[
    "DATASET", "FRESHNESS", "VOLUME", "SQL", "FIELD", "DATA_SCHEMA", "CUSTOM"
]
AssertionStatus = Literal["PASSING", "FAILING", "ERROR", "INIT"]


def _build_search_filters(
    urn: str,
    column: Optional[str] = None,
    assertion_type: Optional[AssertionType] = None,
    status: Optional[AssertionStatus] = None,
) -> list[dict[str, Any]]:
    """Build orFilters for the searchAcrossEntities query."""
    filters: list[dict[str, Any]] = [
        {"field": "entity", "values": [urn], "condition": "EQUAL"},
    ]
    if column:
        filters.append({"field": "fieldPath", "values": [column], "condition": "EQUAL"})
    if assertion_type:
        filters.append(
            {
                "field": "assertionType",
                "values": [assertion_type],
                "condition": "EQUAL",
            }
        )
    if status:
        filters.append(
            {"field": "assertionStatus", "values": [status], "condition": "EQUAL"}
        )
    return [{"and": filters}]


def _get_column_path(assertion: dict[str, Any]) -> Optional[str]:
    """Extract the column/field path from an assertion, if applicable.

    Returns None for multi-field DATASET assertions since a single column
    path would be ambiguous.
    """
    info = assertion.get("info") or {}
    assertion_type = info.get("type")

    if assertion_type == "FIELD":
        field_assertion = info.get("fieldAssertion") or {}
        for key in ("fieldMetricAssertion", "fieldValuesAssertion"):
            sub = field_assertion.get(key) or {}
            field = sub.get("field") or {}
            if field.get("path"):
                return field["path"]
    elif assertion_type == "DATASET":
        dataset_assertion = info.get("datasetAssertion") or {}
        fields = dataset_assertion.get("fields") or []
        if len(fields) == 1:
            return fields[0].get("path")
    elif assertion_type == "CUSTOM":
        custom = info.get("customAssertion") or {}
        field = custom.get("field") or {}
        if field.get("path"):
            return field["path"]

    return None


def _build_assertion_summary(assertion: dict[str, Any]) -> dict[str, Any]:
    """Extract a concise summary from a raw assertion."""
    info = assertion.get("info") or {}
    run_events = assertion.get("runEvents") or {}

    events = run_events.get("runEvents") or []
    latest_result = events[0].get("result") if events else None

    summary: dict[str, Any] = {
        "urn": assertion.get("urn"),
        "type": info.get("type"),
        "description": info.get("description"),
        "externalUrl": info.get("externalUrl"),
        "platform": _extract_platform(assertion),
        "sourceType": _extract_source_type(info),
        "column": _get_column_path(assertion),
        "definition": _extract_definition(info),
        "latestResultType": latest_result.get("type") if latest_result else None,
        "runSummary": {
            "total": run_events.get("total", 0),
            "succeeded": run_events.get("succeeded", 0),
            "failed": run_events.get("failed", 0),
        },
        "runHistory": _extract_run_history(events),
        "tags": _extract_tags(assertion),
    }
    return summary


def _extract_platform(assertion: dict[str, Any]) -> Optional[str]:
    platform = assertion.get("platform") or {}
    props = platform.get("properties") or {}
    return props.get("displayName") or platform.get("name")


def _extract_source_type(info: dict[str, Any]) -> Optional[str]:
    source = info.get("source") or {}
    return source.get("type")


def _extract_definition(info: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return the type-specific assertion definition, whichever is populated."""
    for key in (
        "datasetAssertion",
        "freshnessAssertion",
        "volumeAssertion",
        "sqlAssertion",
        "fieldAssertion",
        "schemaAssertion",
        "customAssertion",
    ):
        value = info.get(key)
        if value is not None:
            return value
    return None


def _extract_run_history(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a slim run-event history list."""
    history: list[dict[str, Any]] = []
    for event in events:
        result = event.get("result") or {}
        entry: dict[str, Any] = {
            "timestampMillis": event.get("timestampMillis"),
            "resultType": result.get("type"),
        }
        error = result.get("error")
        if error:
            entry["error"] = error.get("type")
        history.append(entry)
    return history


def _extract_tags(assertion: dict[str, Any]) -> list[str]:
    tags_wrapper = assertion.get("tags") or {}
    tag_list = tags_wrapper.get("tags") or []
    result: list[str] = []
    for t in tag_list:
        tag = t.get("tag") or {}
        props = tag.get("properties") or {}
        name = props.get("name") or tag.get("urn")
        if name:
            result.append(name)
    return result


def get_dataset_assertions(
    urn: str,
    start: int = 0,
    count: int = DEFAULT_PAGE_SIZE,
    column: Optional[str] = None,
    assertion_type: Optional[AssertionType] = None,
    status: Optional[AssertionStatus] = None,
    run_events_count: int = 1,
) -> dict[str, Any]:
    """Get data quality assertions for a dataset, with their latest run results.

    Fetches assertions associated with a dataset including their type, definition,
    and recent run results (pass/fail). Use this to understand the data quality
    checks configured for a dataset and whether they are currently passing or failing.

    Args:
        urn: Dataset URN (e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))
        start: Pagination offset (default 0)
        count: Number of assertions to return per page (default 5, max 20)
        column: Optional column/field path to filter assertions by (e.g. "user_id")
        assertion_type: Optional type filter (DATASET, FRESHNESS, VOLUME, SQL, FIELD, DATA_SCHEMA, CUSTOM)
        status: Optional status filter (PASSING, FAILING, ERROR, INIT)
        run_events_count: Number of recent run events per assertion (default 1, min 1, max 10).
            Set to 1 for just the latest result, or higher to see recent pass/fail history.

    Returns:
        Dictionary with:
        - success: Whether the request was successful
        - data: Contains start, count, total, and assertions list
        - message: Summary message

    Each assertion includes: urn, type, description, externalUrl, platform, sourceType,
    column, definition, latestResultType, runSummary, runHistory, tags.

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_dataset_assertions(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
            )
    """
    graph = get_graph()

    start = max(0, start)
    count = max(1, min(count, MAX_PAGE_SIZE))
    run_limit = max(MIN_RUN_EVENTS, min(run_events_count, MAX_RUN_EVENTS))
    or_filters = _build_search_filters(urn, column, assertion_type, status)

    try:
        result = execute_graphql(
            graph,
            query=SEARCH_ASSERTIONS_QUERY,
            variables={
                "query": "*",
                "orFilters": or_filters,
                "start": start,
                "count": count,
                "runEventsLimit": run_limit,
            },
            operation_name="SearchDatasetAssertions",
        )

        search_result = result.get("searchAcrossEntities")
        if not search_result:
            return _empty_response(start)

        search_results = search_result.get("searchResults") or []
        assertions = [sr.get("entity") or {} for sr in search_results]
        summaries = [_build_assertion_summary(a) for a in assertions if a.get("urn")]

        summaries = [clean_gql_response(s) for s in summaries]
        truncate_descriptions(summaries)
        selected = list(
            _select_results_within_budget(
                results=iter(summaries),
                fetch_entity=lambda s: s,
                max_results=len(summaries),
            )
        )

        total = search_result.get("total", 0)
        response: dict[str, Any] = {
            "success": True,
            "data": {
                "start": search_result.get("start", start),
                "count": len(selected),
                "total": total,
                "assertions": selected,
            },
            "message": f"Found {total} assertions for dataset",
        }
        if len(selected) < len(summaries):
            response["data"]["truncatedDueToTokenBudget"] = True
        return response

    except Exception as e:
        raise RuntimeError(f"Error fetching assertions for dataset {urn}: {e}") from e


def _empty_response(start: int = 0) -> dict[str, Any]:
    return {
        "success": True,
        "data": {
            "start": start,
            "count": 0,
            "total": 0,
            "assertions": [],
        },
        "message": "No assertions found for this dataset",
    }

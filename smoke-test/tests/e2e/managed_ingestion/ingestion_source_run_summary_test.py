import logging
from typing import Iterator, List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceRunSummaryClass,
    ExecutionRequestResultClass,
)
from tests.e2e.utils import execute_graphql, unique_suffix, with_test_retry
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.INGESTION)

_RECIPE = '{"source":{"type":"mysql","config":{"host_port":"localhost:3306"}}}'

_CREATE_SOURCE = """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
    createIngestionSource(input: $input)
}"""

_DELETE_SOURCE = """mutation deleteIngestionSource($urn: String!) {
    deleteIngestionSource(urn: $urn)
}"""

_CREATE_RUN = """mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
    createIngestionExecutionRequest(input: $input)
}"""

_LIST_SOURCES = """query listIngestionSources($input: ListIngestionSourcesInput!) {
    listIngestionSources(input: $input) {
        ingestionSources {
            urn
        }
    }
}"""


def _create_source(auth_session, name: str) -> str:
    return execute_graphql(
        auth_session,
        _CREATE_SOURCE,
        # No executor picks up this executorId, so runs stay pending and the
        # test decides when a result arrives.
        {
            "input": {
                "name": name,
                "type": "mysql",
                "config": {"recipe": _RECIPE, "executorId": "smoke-no-executor"},
            }
        },
        no_sync_wait=True,
    )["data"]["createIngestionSource"]


def _run(auth_session, source_urn: str) -> str:
    return execute_graphql(
        auth_session, _CREATE_RUN, {"input": {"ingestionSourceUrn": source_urn}}
    )["data"]["createIngestionExecutionRequest"]


def _list_sources(
    auth_session, source_urns: List[str], status: Optional[str] = None
) -> List[str]:
    filters = [{"field": "urn", "values": source_urns}]
    if status is not None:
        filters.append({"field": "lastRunStatus", "values": [status]})
    listing = execute_graphql(
        auth_session,
        _LIST_SOURCES,
        {
            "input": {
                "query": "*",
                "start": 0,
                "count": len(source_urns),
                "filters": filters,
                "sort": {
                    "field": "lastRunRequestedAtMillis",
                    "sortOrder": "DESCENDING",
                },
            }
        },
    )["data"]["listIngestionSources"]
    return [source["urn"] for source in listing["ingestionSources"]]


@with_test_retry()
def _ensure_summary(
    graph_client: DataHubGraph,
    source_urn: str,
    run_urn: str,
    status: Optional[str] = None,
) -> DataHubIngestionSourceRunSummaryClass:
    summary = graph_client.get_aspect(source_urn, DataHubIngestionSourceRunSummaryClass)
    assert summary is not None
    assert summary.lastExecutionRequest == run_urn
    assert summary.lastRunStatus == status
    return summary


@with_test_retry()
def _ensure_listed(
    auth_session,
    source_urns: List[str],
    expected: List[str],
    status: Optional[str] = None,
) -> None:
    assert _list_sources(auth_session, source_urns, status) == expected


@pytest.fixture()
def source_urns(auth_session) -> Iterator[List[str]]:
    suffix = unique_suffix()
    urns = [
        _create_source(auth_session, f"SMOKE_INGESTION_RUN_SUMMARY_{i}_{suffix}")
        for i in range(2)
    ]
    yield urns
    for urn in urns:
        try:
            execute_graphql(auth_session, _DELETE_SOURCE, {"urn": urn})
        except Exception:
            logger.warning("cleanup failed for %s", urn, exc_info=True)


def test_list_sorts_by_last_run(auth_session, graph_client, source_urns):
    first, second = source_urns
    first_run = _run(auth_session, first)
    second_run = _run(auth_session, second)

    first_summary = _ensure_summary(graph_client, first, first_run)
    second_summary = _ensure_summary(graph_client, second, second_run)
    assert (
        second_summary.lastRunRequestedAtMillis
        >= first_summary.lastRunRequestedAtMillis
    )
    _ensure_listed(auth_session, source_urns, [second, first])

    newer_first_run = _run(auth_session, first)
    _ensure_summary(graph_client, first, newer_first_run)
    _ensure_listed(auth_session, source_urns, [first, second])


def test_list_filters_by_last_run_status(auth_session, graph_client, source_urns):
    succeeded, pending = source_urns
    succeeded_run = _run(auth_session, succeeded)
    pending_run = _run(auth_session, pending)
    _ensure_summary(graph_client, pending, pending_run)

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=succeeded_run,
            aspect=ExecutionRequestResultClass(status="SUCCESS"),
        ),
        emit_mode=EmitMode.SYNC_PRIMARY,
    )

    _ensure_summary(graph_client, succeeded, succeeded_run, status="SUCCESS")
    _ensure_listed(auth_session, source_urns, [succeeded], status="SUCCESS")


def test_summary_not_matching_its_run_is_ignored(
    auth_session, graph_client, source_urns
):
    source_urn = source_urns[0]
    run_urn = _run(auth_session, source_urn)
    summary = _ensure_summary(graph_client, source_urn, run_urn)

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=source_urn,
            aspect=DataHubIngestionSourceRunSummaryClass(
                lastExecutionRequest=run_urn,
                lastRunRequestedAtMillis=2**62,
                lastRunStatus="SUCCESS",
            ),
        ),
        emit_mode=EmitMode.SYNC_PRIMARY,
    )

    assert _ensure_summary(graph_client, source_urn, run_urn) == summary

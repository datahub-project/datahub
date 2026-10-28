import logging
from typing import Iterator, List, Optional

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceRunEventClass,
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
def _ensure_run_event(
    graph_client: DataHubGraph, source_urn: str, run_urn: str, status: str
) -> DataHubIngestionSourceRunEventClass:
    event = graph_client.get_latest_timeseries_value(
        source_urn,
        DataHubIngestionSourceRunEventClass,
        {"executionRequest": run_urn, "status": status},
    )
    assert event is not None
    return event


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
        _create_source(auth_session, f"SMOKE_INGESTION_RUN_EVENTS_{i}_{suffix}")
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

    first_event = _ensure_run_event(graph_client, first, first_run, "PENDING")
    second_event = _ensure_run_event(graph_client, second, second_run, "PENDING")
    assert second_event.requestedAtMillis >= first_event.requestedAtMillis
    _ensure_listed(auth_session, source_urns, [second, first])

    newer_first_run = _run(auth_session, first)
    _ensure_run_event(graph_client, first, newer_first_run, "PENDING")
    _ensure_listed(auth_session, source_urns, [first, second])


def test_list_filters_by_last_run_status(auth_session, graph_client, source_urns):
    succeeded, pending = source_urns
    succeeded_run = _run(auth_session, succeeded)
    pending_run = _run(auth_session, pending)
    _ensure_run_event(graph_client, pending, pending_run, "PENDING")

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=succeeded_run,
            aspect=ExecutionRequestResultClass(status="SUCCESS"),
        ),
        emit_mode=EmitMode.SYNC_PRIMARY,
    )

    event = _ensure_run_event(graph_client, succeeded, succeeded_run, "SUCCESS")
    assert event.successfulRunRequestedAtMillis == event.requestedAtMillis
    _ensure_listed(auth_session, source_urns, [succeeded], status="SUCCESS")


def test_run_event_does_not_create_missing_source(graph_client):
    missing_source = f"urn:li:dataHubIngestionSource:smoke-missing-{unique_suffix()}"

    with pytest.raises(OperationalError):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=missing_source,
                aspect=DataHubIngestionSourceRunEventClass(
                    timestampMillis=1,
                    executionRequest="urn:li:dataHubExecutionRequest:smoke-missing",
                    requestedAtMillis=1,
                    status="SUCCESS",
                ),
            ),
            emit_mode=EmitMode.SYNC_PRIMARY,
        )

    assert not graph_client.exists(missing_source)

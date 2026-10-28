"""GraphQL error-payload handling and pagination in TableauSiteSource.

The Tableau Metadata API reports many failures inside a 200 response's
``errors`` list rather than as HTTP errors. These tests pin down which of
those are warnings (data is still used), which are retried, and which abort
the query, plus how cursor pages and id-filter chunks are walked.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Union, cast
from unittest import mock

import pytest
from tableauserverclient.models import UserItem
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    NonXMLResponseError,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    REGULAR_AUTH_EXPIRY_PERIOD,
    SiteIdContentUrl,
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from tests.unit.tableau.test_tableau_config import default_config

CONNECTION = c.WORKBOOKS_CONNECTION
SITE_CONTENT_URL = "site-a"
TIMEOUT_MESSAGE = "Execution canceled because timeout of 30000 millis was reached"


class _AuthError(NonXMLResponseError):
    def __init__(self) -> None:
        Exception.__init__(self, '{"status":401,"error":"Unauthorized"}')


class _HttpError(InternalServerError):
    def __init__(self, code: int) -> None:
        Exception.__init__(self, f"Internal error {code}")
        self.code = code


def _page(
    nodes: List[dict],
    end_cursor: Optional[str] = None,
    has_next_page: bool = False,
    errors: Optional[List[dict]] = None,
) -> dict:
    payload: dict = {
        c.DATA: {
            CONNECTION: {
                c.NODES: nodes,
                c.PAGE_INFO: {c.HAS_NEXT_PAGE: has_next_page, "endCursor": end_cursor},
            }
        }
    }
    if errors is not None:
        payload[c.ERRORS] = errors
    return payload


def _warning(code: Optional[str] = None, message: str = "warning") -> dict:
    extensions: Dict[str, str] = {c.SEVERITY: c.WARNING}
    if code:
        extensions["code"] = code
    return {"message": message, c.EXTENSIONS: extensions}


def _error(message: str) -> dict:
    return {"message": message, c.EXTENSIONS: {c.SEVERITY: "ERROR"}}


def _make_source(
    responses: Sequence[Union[dict, Exception]], max_retries: int = 2
) -> TableauSiteSource:
    config = TableauConfig.model_validate(
        {**default_config, "max_retries": max_retries}
    )
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    server.metadata.query.side_effect = list(responses)
    return TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="site-1", site_content_url=SITE_CONTENT_URL),
        report=TableauSourceReport(),
        server=server,
    )


def _query_mock(source: TableauSiteSource) -> mock.MagicMock:
    return cast(mock.MagicMock, source.server).metadata.query


def _fetch_page(source: TableauSiteSource) -> dict:
    connection, _cursor, _has_next = source.get_connection_object_page(
        query="{ id }",
        connection_type=CONNECTION,
        query_filter="",
        current_cursor=None,
        fetch_size=10,
    )
    return connection


def test_warning_severity_errors_are_reported_without_aborting_the_query() -> None:
    errors = [
        _warning(code="PERMISSIONS_MODE_SWITCHED"),
        _warning(code="NODE_LIMIT_EXCEEDED"),
        _warning(message="some other partial failure"),
    ]
    source = _make_source([_page([{c.ID: "wb-1"}], errors=errors)])

    with mock.patch("time.sleep") as sleep:
        connection = _fetch_page(source)

    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    sleep.assert_not_called()
    assert _query_mock(source).call_count == 1
    titles = {entry.title for entry in source.report.warnings}
    assert {
        "Derived Permission Error",
        "Tableau Data Exceed Predefined Limit",
    } <= titles
    assert len(source.report.warnings) == 3


def test_timeout_error_is_retried_until_data_arrives() -> None:
    source = _make_source(
        [_page([], errors=[_error(TIMEOUT_MESSAGE)]), _page([{c.ID: "wb-1"}])]
    )

    with mock.patch("time.sleep") as sleep:
        connection = _fetch_page(source)

    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    assert sleep.call_count == 1
    assert _query_mock(source).call_count == 2
    assert source.report.warnings == []


def test_timeout_error_gives_up_after_max_retries() -> None:
    source = _make_source(
        [_page([], errors=[_error(TIMEOUT_MESSAGE)])] * 5, max_retries=2
    )

    with mock.patch("time.sleep"), pytest.raises(RuntimeError):
        _fetch_page(source)

    # The initial attempt plus one retry per configured retry.
    assert _query_mock(source).call_count == 3


def test_non_retryable_graphql_error_aborts_without_retry() -> None:
    source = _make_source(
        [
            _page([], errors=[_error("Validation error of type FieldUndefined")]),
            _page([{c.ID: "wb-1"}]),
        ]
    )

    with mock.patch("time.sleep") as sleep, pytest.raises(RuntimeError):
        _fetch_page(source)

    sleep.assert_not_called()
    assert _query_mock(source).call_count == 1


def test_non_retriable_http_status_is_raised_immediately() -> None:
    source = _make_source([_HttpError(400), _page([{c.ID: "wb-1"}])])

    with mock.patch("time.sleep") as sleep, pytest.raises(InternalServerError):
        _fetch_page(source)

    sleep.assert_not_called()
    assert _query_mock(source).call_count == 1
    assert source.report.tableau_server_error_stats[InternalServerError.__name__] == 1


def test_stale_session_is_reauthenticated_before_retrying() -> None:
    source = _make_source([_AuthError()])
    stale_login = (
        datetime.now(timezone.utc) - REGULAR_AUTH_EXPIRY_PERIOD - timedelta(minutes=1)
    )
    source.report.last_authenticated_at = stale_login
    fresh_server = mock.MagicMock()
    fresh_server.metadata.query.return_value = _page([{c.ID: "wb-1"}])

    with (
        mock.patch.object(
            TableauConfig, "make_tableau_client", return_value=fresh_server
        ) as make_client,
        mock.patch("time.sleep"),
    ):
        connection = _fetch_page(source)

    make_client.assert_called_once_with(SITE_CONTENT_URL)
    assert source.server is fresh_server
    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    assert source.report.last_authenticated_at is not None
    assert source.report.last_authenticated_at > stale_login


def test_recent_session_is_retried_on_the_same_client() -> None:
    source = _make_source([_AuthError(), _page([{c.ID: "wb-1"}])])
    source.report.last_authenticated_at = datetime.now(timezone.utc)
    original_server = source.server

    with (
        mock.patch.object(TableauConfig, "make_tableau_client") as make_client,
        mock.patch("time.sleep"),
    ):
        connection = _fetch_page(source)

    make_client.assert_not_called()
    assert source.server is original_server
    assert connection[c.NODES] == [{c.ID: "wb-1"}]


def test_get_connection_objects_walks_cursor_pages_and_id_filter_chunks() -> None:
    source = _make_source(
        [
            _page([{c.ID: "a"}], end_cursor="cursor-1", has_next_page=True),
            _page([{c.ID: "b"}]),
            _page([{c.ID: "c"}]),
        ]
    )

    nodes = list(
        source.get_connection_objects(
            query="{ id }",
            connection_type=CONNECTION,
            page_size=2,
            query_filter={c.ID_WITH_IN: ["a", "b", "c"]},
        )
    )

    assert [node[c.ID] for node in nodes] == ["a", "b", "c"]
    calls = _query_mock(source).call_args_list
    assert [call.kwargs["variables"]["after"] for call in calls] == [
        None,
        "cursor-1",
        None,
    ]
    # The id filter is chunked to the page size: ids a+b on the first filter
    # page (two cursor pages), id c alone on the second.
    assert '"c"' not in calls[0].kwargs["query"]
    assert '"c"' in calls[2].kwargs["query"]
    assert source.report.num_filter_queries_by_connection_type[CONNECTION] == 2
    assert source.report.num_paginated_queries_by_connection_type[CONNECTION] == 3

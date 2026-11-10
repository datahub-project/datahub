"""GraphQL error-payload handling and pagination in TableauSiteSource.

The Tableau Metadata API reports many failures inside a 200 response's
``errors`` list rather than as HTTP errors. These tests pin down which of
those are warnings (data is still used), which are retried, and which abort
the query, plus how cursor pages and id-filter chunks are walked.

HTTP-level retries (InternalServerError, NonXMLResponseError, OSError backoff)
are covered by test_tableau_retry.py.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Sequence, Union
from unittest import mock

import pytest
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    NonXMLResponseError,
)

from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    REGULAR_AUTH_EXPIRY_PERIOD,
    TableauConfig,
    TableauSiteSource,
)
from tests.unit.tableau.conftest import (
    SITE_CONTENT_URL,
    SiteSourceFactory,
    graphql_error,
    graphql_page,
    graphql_warning,
    mock_tableau_server,
    query_mock,
)

CONNECTION = c.WORKBOOKS_CONNECTION
TIMEOUT_MESSAGE = "Execution canceled because timeout of 30000 millis was reached"


class _AuthError(NonXMLResponseError):
    def __init__(self) -> None:
        Exception.__init__(self, '{"status":401,"error":"Unauthorized"}')


class _HttpError(InternalServerError):
    def __init__(self, code: int) -> None:
        Exception.__init__(self, f"Internal error {code}")
        self.code = code


def _source_answering(
    site_source: SiteSourceFactory,
    responses: Sequence[Union[dict, Exception]],
    max_retries: int = 2,
) -> TableauSiteSource:
    """A site source whose Metadata API returns (or raises) ``responses`` in order."""
    source = site_source(max_retries=max_retries)
    query_mock(source).side_effect = list(responses)
    return source


def _fetch_page(source: TableauSiteSource) -> dict:
    connection, _cursor, _has_next = source.get_connection_object_page(
        query="{ id }",
        connection_type=CONNECTION,
        query_filter="",
        current_cursor=None,
        fetch_size=10,
    )
    return connection


def test_warning_severity_errors_are_reported_without_aborting_the_query(
    site_source: SiteSourceFactory,
) -> None:
    """Warning-severity errors keep the page's data; the two known codes get
    their own actionable titles and anything else is reported generically."""
    errors = [
        graphql_warning(code="PERMISSIONS_MODE_SWITCHED"),
        graphql_warning(code="NODE_LIMIT_EXCEEDED"),
        graphql_warning(message="some other partial failure"),
    ]
    source = _source_answering(
        site_source, [graphql_page([{c.ID: "wb-1"}], errors=errors)]
    )

    with mock.patch("time.sleep") as sleep:
        connection = _fetch_page(source)

    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    sleep.assert_not_called()
    assert query_mock(source).call_count == 1
    warnings = list(source.report.warnings)
    assert {
        "Derived Permission Error",
        "Tableau Data Exceed Predefined Limit",
    } <= {entry.title for entry in warnings}
    assert any(
        "some other partial failure" in " ".join(entry.context) for entry in warnings
    )


def test_timeout_error_is_retried_until_data_arrives(
    site_source: SiteSourceFactory,
) -> None:
    source = _source_answering(
        site_source,
        [
            graphql_page([], errors=[graphql_error(TIMEOUT_MESSAGE)]),
            graphql_page([{c.ID: "wb-1"}]),
        ],
    )

    with mock.patch("time.sleep") as sleep:
        connection = _fetch_page(source)

    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    assert sleep.call_count == 1
    assert query_mock(source).call_count == 2
    assert source.report.warnings == []


def test_timeout_error_gives_up_after_max_retries(
    site_source: SiteSourceFactory,
) -> None:
    """After the initial attempt plus one retry per configured retry the
    GraphQL errors are raised, so the caller sees what the API said."""
    source = _source_answering(
        site_source,
        [graphql_page([], errors=[graphql_error(TIMEOUT_MESSAGE)])] * 5,
        max_retries=2,
    )

    with (
        mock.patch("time.sleep"),
        pytest.raises(RuntimeError, match=re.escape(TIMEOUT_MESSAGE)),
    ):
        _fetch_page(source)

    assert query_mock(source).call_count == 3


def test_non_retryable_graphql_error_aborts_without_retry(
    site_source: SiteSourceFactory,
) -> None:
    source = _source_answering(
        site_source,
        [
            graphql_page(
                [], errors=[graphql_error("Validation error of type FieldUndefined")]
            ),
            graphql_page([{c.ID: "wb-1"}]),
        ],
    )

    with mock.patch("time.sleep") as sleep, pytest.raises(RuntimeError):
        _fetch_page(source)

    sleep.assert_not_called()
    assert query_mock(source).call_count == 1


def test_non_retriable_http_status_is_raised_immediately(
    site_source: SiteSourceFactory,
) -> None:
    source = _source_answering(
        site_source, [_HttpError(400), graphql_page([{c.ID: "wb-1"}])]
    )

    with mock.patch("time.sleep") as sleep, pytest.raises(InternalServerError):
        _fetch_page(source)

    sleep.assert_not_called()
    assert query_mock(source).call_count == 1
    assert source.report.tableau_server_error_stats[InternalServerError.__name__] == 1


def test_stale_session_is_reauthenticated_before_retrying(
    site_source: SiteSourceFactory,
) -> None:
    """An auth error on a session older than the regular expiry period means
    the token lapsed: a fresh client is created and the query retried on it."""
    source = _source_answering(site_source, [_AuthError()])
    stale_login = (
        datetime.now(timezone.utc) - REGULAR_AUTH_EXPIRY_PERIOD - timedelta(minutes=1)
    )
    source.report.last_authenticated_at = stale_login
    fresh_server = mock_tableau_server()
    fresh_server.metadata.query.return_value = graphql_page([{c.ID: "wb-1"}])

    with (
        mock.patch.object(
            TableauConfig,
            "make_tableau_client",
            autospec=True,
            return_value=fresh_server,
        ) as make_client,
        mock.patch("time.sleep"),
    ):
        connection = _fetch_page(source)

    make_client.assert_called_once_with(source.config, SITE_CONTENT_URL)
    assert source.server is fresh_server
    assert connection[c.NODES] == [{c.ID: "wb-1"}]
    assert source.report.last_authenticated_at is not None
    assert source.report.last_authenticated_at > stale_login


def test_recent_session_is_retried_on_the_same_client(
    site_source: SiteSourceFactory,
) -> None:
    """Tableau sporadically answers 401 right after sign-in; a session that is
    still young is retried as is rather than re-authenticated."""
    source = _source_answering(
        site_source, [_AuthError(), graphql_page([{c.ID: "wb-1"}])]
    )
    source.report.last_authenticated_at = datetime.now(timezone.utc)
    original_server = source.server

    with (
        mock.patch.object(
            TableauConfig, "make_tableau_client", autospec=True
        ) as make_client,
        mock.patch("time.sleep"),
    ):
        connection = _fetch_page(source)

    make_client.assert_not_called()
    assert source.server is original_server
    assert connection[c.NODES] == [{c.ID: "wb-1"}]


def test_get_connection_objects_walks_cursor_pages_and_id_filter_chunks(
    site_source: SiteSourceFactory,
) -> None:
    """The id filter is chunked to the page size (ids a+b, then c) and each
    chunk is paged by cursor (a+b spans two cursor pages)."""
    source = _source_answering(
        site_source,
        [
            graphql_page([{c.ID: "a"}], end_cursor="cursor-1", has_next_page=True),
            graphql_page([{c.ID: "b"}]),
            graphql_page([{c.ID: "c"}]),
        ],
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
    calls = query_mock(source).call_args_list
    assert [call.kwargs["variables"]["after"] for call in calls] == [
        None,
        "cursor-1",
        None,
    ]
    assert '"c"' not in calls[0].kwargs["query"]
    assert '"c"' in calls[2].kwargs["query"]
    assert source.report.num_filter_queries_by_connection_type[CONNECTION] == 2
    assert source.report.num_paginated_queries_by_connection_type[CONNECTION] == 3

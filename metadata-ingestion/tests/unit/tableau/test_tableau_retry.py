from typing import List
from unittest import mock

import pytest
from freezegun import freeze_time
from tableauserverclient import Server
from tableauserverclient.models import UserItem
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    NonXMLResponseError,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauSiteSource,
    TableauSourceReport,
)

FROZEN_TIME = "2021-12-07 07:00:00"

# Common test data
SUCCESS_RESPONSE = {
    "data": {
        "workbooksConnection": {
            "nodes": [],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }
    }
}


class MockInternalServerError(InternalServerError):
    """Mock InternalServerError that can be instantiated easily for testing."""

    def __init__(self, message="Internal Server Error", code=503):
        Exception.__init__(self, message)
        self.code = code


class MockNonXMLResponseError(NonXMLResponseError):
    """Mock NonXMLResponseError that can be instantiated easily for testing."""

    def __init__(
        self, message='{"timestamp":"xxx","status":401,"error":"Unauthorized"}'
    ):
        Exception.__init__(self, message)


def create_mock_server(metadata_query_side_effect):
    """Create a mock Tableau server with required attributes."""
    mock_server = mock.MagicMock(spec=Server)
    mock_metadata = mock.Mock()
    mock_metadata.query.side_effect = metadata_query_side_effect
    mock_server.metadata = mock_metadata
    mock_server.user_id = "test_user_id"
    mock_server.site_id = "test_site_id"
    mock_server.users = mock.Mock()
    mock_server.users.get_by_id.return_value = UserItem(
        name="test_user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    return mock_server


def create_tableau_source(mock_config, mock_server):
    """Create a TableauSiteSource instance for testing."""
    return TableauSiteSource(
        platform="tableau",
        config=mock_config,
        ctx=PipelineContext(run_id="test", pipeline_name="test_tableau_retry"),
        site=SiteIdContentUrl(site_id="Site1", site_content_url="site1"),
        server=mock_server,
        report=TableauSourceReport(),
    )


@freeze_time(FROZEN_TIME)
def test_internal_server_error_with_backoff():
    """Test that InternalServerError (503) retries with exponential backoff."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 3

    ise_503 = MockInternalServerError(
        "Internal error 503 at https://us-west-2b.online.tableau.com/api/metadata/graphql",
        code=503,
    )
    mock_server = create_mock_server([ise_503, SUCCESS_RESPONSE])
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        result = tableau_source.get_connection_object_page(
            query="workbooksConnection { nodes { id } }",
            connection_type="workbooksConnection",
            query_filter="",
            current_cursor=None,
            retries_remaining=3,
            fetch_size=10,
        )

        # First retry: (3 - 3 + 1)^2 = 1 second
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 1
        assert (
            tableau_source.report.tableau_server_error_stats[
                InternalServerError.__name__
            ]
            == 1
        )
        assert result is not None


@freeze_time(FROZEN_TIME)
def test_internal_server_error_exhausts_retries():
    """Test that InternalServerError raises after exhausting retries."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 2

    ise_503 = MockInternalServerError("Internal error 503", code=503)
    mock_server = create_mock_server(ise_503)
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        with pytest.raises(InternalServerError):
            tableau_source.get_connection_object_page(
                query="workbooksConnection { nodes { id } }",
                connection_type="workbooksConnection",
                query_filter="",
                current_cursor=None,
                retries_remaining=2,
                fetch_size=10,
            )

        # First retry: (2 - 2 + 1)^2 = 1 second, Second: (2 - 1 + 1)^2 = 4 seconds
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 1
        assert mock_sleep.call_args_list[1][0][0] == 4


@freeze_time(FROZEN_TIME)
def test_internal_server_error_backoff_calculation():
    """Test that backoff time increases with each retry attempt."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 5

    ise_503 = MockInternalServerError("Internal error 503", code=503)
    mock_server = create_mock_server([ise_503, ise_503, ise_503, SUCCESS_RESPONSE])
    tableau_source = create_tableau_source(mock_config, mock_server)

    sleep_times: List[int] = []
    with mock.patch("time.sleep", side_effect=sleep_times.append):
        result = tableau_source.get_connection_object_page(
            query="workbooksConnection { nodes { id } }",
            connection_type="workbooksConnection",
            query_filter="",
            current_cursor=None,
            retries_remaining=5,
            fetch_size=10,
        )

        # Backoff progression: (5-5+1)²=1, (5-4+1)²=4, (5-3+1)²=9
        assert sleep_times == [1, 4, 9]
        assert result is not None


@freeze_time(FROZEN_TIME)
def test_internal_server_error_raises_after_max_retries():
    """Test that InternalServerError is raised after exactly max_retries attempts."""
    max_retries = 3
    mock_config = mock.MagicMock()
    mock_config.max_retries = max_retries

    ise_503 = MockInternalServerError("Internal error 503", code=503)
    mock_server = create_mock_server(ise_503)
    tableau_source = create_tableau_source(mock_config, mock_server)

    # Track how many times the query is called
    query_call_count = 0

    def track_query_calls(*args, **kwargs):
        nonlocal query_call_count
        query_call_count += 1
        raise ise_503

    mock_server.metadata.query.side_effect = track_query_calls

    sleep_times: List[int] = []
    with mock.patch("time.sleep", side_effect=sleep_times.append):
        with pytest.raises(InternalServerError) as exc_info:
            tableau_source.get_connection_object_page(
                query="workbooksConnection { nodes { id } }",
                connection_type="workbooksConnection",
                query_filter="",
                current_cursor=None,
                retries_remaining=max_retries,
                fetch_size=10,
            )

        # Verify exception is raised
        assert isinstance(exc_info.value, InternalServerError)
        assert exc_info.value.code == 503

        # Verify total attempts: initial call + max_retries retries
        # With max_retries=3: 1 initial + 3 retries = 4 total attempts
        assert query_call_count == max_retries + 1

        # Verify backoff was called max_retries times (once per retry)
        # With max_retries=3: 3 retries = 3 sleep calls
        assert len(sleep_times) == max_retries
        # Backoff progression: (3-3+1)²=1, (3-2+1)²=4, (3-1+1)²=9
        assert sleep_times == [1, 4, 9]

        # Verify error was tracked for each failed attempt
        assert (
            tableau_source.report.tableau_server_error_stats[
                InternalServerError.__name__
            ]
            == max_retries + 1
        )


@freeze_time(FROZEN_TIME)
def test_non_xml_response_error_with_backoff():
    """Test that NonXMLResponseError (REAUTHENTICATE_ERRORS) retries with exponential backoff."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 3

    auth_error = NonXMLResponseError(
        '{"timestamp":"xxx","status":401,"error":"Unauthorized","path":"/relationship-service-war/graphql"}'
    )
    mock_server = create_mock_server([auth_error, SUCCESS_RESPONSE])
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        result = tableau_source.get_connection_object_page(
            query="workbooksConnection { nodes { id } }",
            connection_type="workbooksConnection",
            query_filter="",
            current_cursor=None,
            retries_remaining=3,
            fetch_size=10,
        )

        # First retry: (3 - 3 + 1)^2 = 1 second
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 1
        assert (
            tableau_source.report.tableau_server_error_stats[
                NonXMLResponseError.__name__
            ]
            == 1
        )
        assert result is not None


@freeze_time(FROZEN_TIME)
def test_non_xml_response_error_exhausts_retries():
    """Test that NonXMLResponseError raises after exhausting retries."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 2

    auth_error = NonXMLResponseError(
        '{"timestamp":"xxx","status":401,"error":"Unauthorized"}'
    )
    mock_server = create_mock_server(auth_error)
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        with pytest.raises(NonXMLResponseError):
            tableau_source.get_connection_object_page(
                query="workbooksConnection { nodes { id } }",
                connection_type="workbooksConnection",
                query_filter="",
                current_cursor=None,
                retries_remaining=2,
                fetch_size=10,
            )

        # First retry: (2 - 2 + 1)^2 = 1 second, Second: (2 - 1 + 1)^2 = 4 seconds
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 1
        assert mock_sleep.call_args_list[1][0][0] == 4


@freeze_time(FROZEN_TIME)
def test_os_error_with_backoff():
    """Test that OSError retries with exponential backoff."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 3

    os_error = OSError("Response is not a http response?")
    mock_server = create_mock_server([os_error, SUCCESS_RESPONSE])
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        result = tableau_source.get_connection_object_page(
            query="workbooksConnection { nodes { id } }",
            connection_type="workbooksConnection",
            query_filter="",
            current_cursor=None,
            retries_remaining=3,
            fetch_size=10,
        )

        # First retry: (3 - 3 + 1)^2 = 1 second
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 1
        assert tableau_source.report.tableau_server_error_stats[OSError.__name__] == 1
        assert result is not None


@freeze_time(FROZEN_TIME)
def test_os_error_exhausts_retries():
    """Test that OSError raises after exhausting retries."""
    mock_config = mock.MagicMock()
    mock_config.max_retries = 2

    os_error = OSError("Response is not a http response?")
    mock_server = create_mock_server(os_error)
    tableau_source = create_tableau_source(mock_config, mock_server)

    with mock.patch("time.sleep") as mock_sleep:
        with pytest.raises(OSError):
            tableau_source.get_connection_object_page(
                query="workbooksConnection { nodes { id } }",
                connection_type="workbooksConnection",
                query_filter="",
                current_cursor=None,
                retries_remaining=2,
                fetch_size=10,
            )

        # First retry: (2 - 2 + 1)^2 = 1 second, Second: (2 - 1 + 1)^2 = 4 seconds
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 1
        assert mock_sleep.call_args_list[1][0][0] == 4

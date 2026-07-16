from datetime import datetime
from unittest.mock import Mock, patch

import requests
from requests.adapters import HTTPAdapter

from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import (
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import HightouchSync


def test_api_client_initialization():
    config = HightouchAPIConfig(
        api_key="test_key",
        base_url="https://api.hightouch.com/api/v1",
    )
    client = HightouchAPIClient(config)

    assert client.config == config
    assert client.session.headers["Authorization"] == "Bearer test_key"


@patch("requests.Session.request")
def test_make_paginated_request_single_page(mock_request):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [{"id": "1"}, {"id": "2"}],
        "hasMore": False,
    }
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    results = client._make_paginated_request("/test")

    assert len(results) == 2
    assert results[0]["id"] == "1"
    assert results[1]["id"] == "2"
    mock_request.assert_called_once()


@patch("requests.Session.request")
def test_make_paginated_request_multiple_pages(mock_request):
    response1 = Mock()
    response1.status_code = 200
    response1.json.return_value = {
        "data": [{"id": "1"}, {"id": "2"}],
        "hasMore": True,
    }
    response1.raise_for_status = Mock()

    response2 = Mock()
    response2.status_code = 200
    response2.json.return_value = {
        "data": [{"id": "3"}, {"id": "4"}],
        "hasMore": True,
    }
    response2.raise_for_status = Mock()

    response3 = Mock()
    response3.status_code = 200
    response3.json.return_value = {
        "data": [{"id": "5"}],
        "hasMore": False,
    }
    response3.raise_for_status = Mock()

    mock_request.side_effect = [response1, response2, response3]

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    results = client._make_paginated_request("/test")

    assert len(results) == 5
    assert [r["id"] for r in results] == ["1", "2", "3", "4", "5"]
    assert mock_request.call_count == 3


@patch("requests.Session.request")
def test_make_paginated_request_with_custom_limit(mock_request):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [{"id": "1"}],
        "hasMore": False,
    }
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    list(client._make_paginated_request("/test", {"limit": 50}))

    call_args = mock_request.call_args
    assert call_args[1]["params"]["limit"] == 50


def test_extract_field_mappings_from_field_mappings():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "mappings": [
                {
                    "from": "user_id",
                    "to": "UserId",
                    "isPrimaryKey": True,
                },
                {"from": "email", "to": "Email"},
                {"from": "name", "to": "Name"},
            ]
        },
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 3
    assert mappings[0].source_field == "user_id"
    assert mappings[0].destination_field == "UserId"
    assert mappings[0].is_primary_key is True
    assert mappings[1].source_field == "email"
    assert mappings[1].destination_field == "Email"
    assert mappings[1].is_primary_key is False


def test_extract_field_mappings_empty_configuration():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 0


def test_extract_field_mappings_no_configuration():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 0


def test_extract_field_mappings_skips_incomplete_mappings():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "mappings": [
                {"from": "user_id", "to": "UserId"},
                {"from": "email"},  # Missing destination
                {"to": "Name"},  # Missing source
                {"from": "valid", "to": "ValidField"},
            ]
        },
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 2
    assert mappings[0].source_field == "user_id"
    assert mappings[1].source_field == "valid"


@patch("requests.Session.request")
def test_get_contracts_404_handling(mock_request):
    mock_response = Mock()
    mock_response.status_code = 404

    http_error = requests.exceptions.HTTPError("404 Client Error")
    http_error.response = mock_response
    mock_response.raise_for_status.side_effect = http_error

    mock_request.return_value = mock_response

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    contracts = client.get_contracts()

    assert contracts == []


def test_session_retry_adapter_configured():
    """The session mounts an HTTPAdapter whose Retry policy matches our constants.
    Previous pagination tests patched Session.request and never touched the adapter,
    so the retry wiring itself was untested."""
    client = HightouchAPIClient(HightouchAPIConfig(api_key="test"))

    adapter = client.session.get_adapter("https://api.hightouch.com/api/v1")
    assert isinstance(adapter, HTTPAdapter)

    retry = adapter.max_retries
    assert retry.total == HTTP_RETRY_MAX_ATTEMPTS
    assert list(retry.status_forcelist) == HTTP_RETRY_STATUS_CODES


def test_pagination_cap_truncates_and_warns(monkeypatch):
    """When the API never reports hasMore=false, pagination stops at the page cap
    and surfaces a truncation warning to the operator."""
    from datahub.ingestion.source.hightouch import hightouch_api

    monkeypatch.setattr(hightouch_api, "API_PAGINATION_MAX_PAGES", 3)

    report = HightouchSourceReport()
    client = HightouchAPIClient(HightouchAPIConfig(api_key="test"), report)

    full_page = {"data": [{"id": "1"}], "hasMore": True}
    with patch.object(client, "_make_request", return_value=full_page):
        results = client._make_paginated_request("/test")

    assert len(results) == 3
    assert len(report.warnings) >= 1

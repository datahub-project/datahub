from datetime import datetime
from unittest.mock import Mock, patch

import requests

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig
from datahub.ingestion.source.hightouch.hightouch_api import (
    FieldMapping,
    HightouchAPIClient,
)
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
            "fieldMappings": [
                {
                    "sourceField": "user_id",
                    "destinationField": "UserId",
                    "isPrimaryKey": True,
                },
                {"sourceField": "email", "destinationField": "Email"},
                {"sourceField": "name", "destinationField": "Name"},
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


def test_extract_field_mappings_from_column_mappings():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "columnMappings": {
                "user_id": "id",
                "user_email": "email",
                "user_name": "name",
            }
        },
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 3
    assert any(
        m.source_field == "id" and m.destination_field == "user_id" for m in mappings
    )
    assert any(
        m.source_field == "email" and m.destination_field == "user_email"
        for m in mappings
    )
    assert any(
        m.source_field == "name" and m.destination_field == "user_name"
        for m in mappings
    )


def test_extract_field_mappings_from_columns():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "columns": [
                {"from": "user_id", "to": "UserId", "isPrimaryKey": True},
                {"from": "email", "to": "Email"},
            ]
        },
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 2
    assert mappings[0].source_field == "user_id"
    assert mappings[0].destination_field == "UserId"
    assert mappings[0].is_primary_key is True


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


def test_extract_field_mappings_with_snake_case_keys():
    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "field_mappings": [
                {"source_field": "user_id", "destination_field": "UserId"},
            ]
        },
    )

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    mappings = client.extract_field_mappings(sync)

    assert len(mappings) == 1
    assert mappings[0].source_field == "user_id"
    assert mappings[0].destination_field == "UserId"


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
            "fieldMappings": [
                {"sourceField": "user_id", "destinationField": "UserId"},
                {"sourceField": "email"},  # Missing destination
                {"destinationField": "Name"},  # Missing source
                {"sourceField": "valid", "destinationField": "ValidField"},
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


@patch("requests.Session.request")
def test_get_contract_runs_404_handling(mock_request):
    mock_response = Mock()
    mock_response.status_code = 404

    http_error = requests.exceptions.HTTPError("404 Client Error")
    http_error.response = mock_response
    mock_response.raise_for_status.side_effect = http_error

    mock_request.return_value = mock_response

    config = HightouchAPIConfig(api_key="test")
    client = HightouchAPIClient(config)

    runs = client.get_contract_runs(contract_id="contract_1", limit=10)

    assert runs == []


def test_field_mapping_model():
    mapping = FieldMapping(
        source_field="user_id",
        destination_field="UserId",
        is_primary_key=True,
    )

    assert mapping.source_field == "user_id"
    assert mapping.destination_field == "UserId"
    assert mapping.is_primary_key is True


def test_field_mapping_model_defaults():
    mapping = FieldMapping(
        source_field="email",
        destination_field="Email",
    )

    assert mapping.source_field == "email"
    assert mapping.destination_field == "Email"
    assert mapping.is_primary_key is False

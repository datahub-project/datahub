from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)


@pytest.fixture
def api_config():
    return HightouchAPIConfig(
        api_key="test_api_key",
        base_url="https://api.hightouch.com/api/v1",
        request_timeout_sec=30,
    )


@pytest.fixture
def api_client(api_config):
    return HightouchAPIClient(api_config)


def test_init(api_client, api_config):
    assert api_client.config == api_config
    assert api_client.session is not None
    assert api_client.session.headers["Authorization"] == "Bearer test_api_key"
    assert api_client.session.headers["Content-Type"] == "application/json"


@patch("requests.Session.request")
def test_get_sources_success(mock_request, api_client):
    """Test successful retrieval of sources."""
    mock_response_data = {
        "data": [
            {
                "id": "1",
                "name": "Test Snowflake",
                "slug": "test-snowflake",
                "type": "snowflake",
                "workspaceId": "100",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {"account": "test.snowflakecomputing.com"},
            }
        ]
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_sources()

    assert len(result) == 1
    assert isinstance(result[0], HightouchSourceConnection)
    assert result[0].id == "1"
    assert result[0].name == "Test Snowflake"
    assert result[0].type == "snowflake"
    assert result[0].slug == "test-snowflake"

    mock_request.assert_called_once()


@patch("requests.Session.request")
def test_get_models_success(mock_request, api_client):
    """Test successful retrieval of models."""
    mock_response_data = {
        "data": [
            {
                "id": "10",
                "name": "Customer Model",
                "slug": "customer-model",
                "workspaceId": "100",
                "sourceId": "1",
                "queryType": "raw_sql",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "customer_id",
                "description": "Customer data model",
                "isSchema": False,
            }
        ]
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_models()

    assert len(result) == 1
    assert isinstance(result[0], HightouchModel)
    assert result[0].id == "10"
    assert result[0].name == "Customer Model"
    assert result[0].slug == "customer-model"
    assert result[0].query_type == "raw_sql"
    assert result[0].primary_key == "customer_id"


@patch("requests.Session.request")
def test_get_destinations_success(mock_request, api_client):
    """Test successful retrieval of destinations."""
    mock_response_data = {
        "data": [
            {
                "id": "20",
                "name": "Salesforce Prod",
                "slug": "salesforce-prod",
                "type": "salesforce",
                "workspaceId": "100",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {"instance_url": "https://test.salesforce.com"},
            }
        ]
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_destinations()

    assert len(result) == 1
    assert isinstance(result[0], HightouchDestination)
    assert result[0].id == "20"
    assert result[0].name == "Salesforce Prod"
    assert result[0].type == "salesforce"


@patch("requests.Session.request")
def test_get_syncs_success(mock_request, api_client):
    """Test successful retrieval of syncs."""
    mock_response_data = {
        "data": [
            {
                "id": "30",
                "slug": "customer-to-salesforce",
                "workspaceId": "100",
                "modelId": "10",
                "destinationId": "20",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {"destinationTable": "Contact"},
                "schedule": {"type": "interval", "interval": 3600},
                "disabled": False,
            }
        ]
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_syncs()

    assert len(result) == 1
    assert isinstance(result[0], HightouchSync)
    assert result[0].id == "30"
    assert result[0].slug == "customer-to-salesforce"
    assert result[0].model_id == "10"
    assert result[0].destination_id == "20"
    assert result[0].disabled is False


@patch("requests.Session.request")
def test_get_sync_runs_success(mock_request, api_client):
    """Test successful retrieval of sync runs."""
    mock_response_data = {
        "data": [
            {
                "id": "100",
                "syncId": "30",
                "status": "success",
                "startedAt": "2023-01-03T00:00:00Z",
                "finishedAt": "2023-01-03T00:05:00Z",
                "createdAt": "2023-01-03T00:00:00Z",
                "completionRatio": 1.0,
                "plannedRows": {"added": 100, "changed": 50, "removed": 10},
                "successfulRows": {"added": 98, "changed": 49, "removed": 10},
                "failedRows": {"added": 2, "changed": 1, "removed": 0},
                "querySize": 1024000,
            }
        ]
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_sync_runs("30", limit=10)

    assert len(result) == 1
    assert isinstance(result[0], HightouchSyncRun)
    assert result[0].id == "100"
    assert result[0].sync_id == "30"
    assert result[0].status == "success"
    assert result[0].completion_ratio == 1.0
    assert result[0].planned_rows == {"added": 100, "changed": 50, "removed": 10}


@patch("requests.Session.request")
def test_get_user_by_id_success(mock_request, api_client):
    """Test successful retrieval of a user."""
    mock_response_data = {
        "id": "user123",
        "email": "test@example.com",
        "name": "Test User",
        "createdAt": "2023-01-01T00:00:00Z",
    }

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_user_by_id("user123")

    assert isinstance(result, HightouchUser)
    assert result.id == "user123"
    assert result.email == "test@example.com"
    assert result.name == "Test User"


def test_extract_field_mappings_format1(api_client):
    """Test extraction of field mappings - format 1."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "fieldMappings": [
                {
                    "sourceField": "customer_id",
                    "destinationField": "ContactId",
                    "isPrimaryKey": True,
                },
                {
                    "sourceField": "email",
                    "destinationField": "Email",
                    "isPrimaryKey": False,
                },
            ]
        },
    )

    result = api_client.extract_field_mappings(sync)

    assert len(result) == 2
    assert result[0].source_field == "customer_id"
    assert result[0].destination_field == "ContactId"
    assert result[0].is_primary_key is True
    assert result[1].source_field == "email"
    assert result[1].destination_field == "Email"
    assert result[1].is_primary_key is False


def test_extract_field_mappings_format2(api_client):
    """Test extraction of field mappings - format 2 (column mappings)."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "columnMappings": {
                "ContactId": "customer_id",
                "Email": "email",
                "FirstName": "first_name",
            }
        },
    )

    result = api_client.extract_field_mappings(sync)

    assert len(result) == 3
    field_dict = {fm.destination_field: fm.source_field for fm in result}
    assert field_dict["ContactId"] == "customer_id"
    assert field_dict["Email"] == "email"
    assert field_dict["FirstName"] == "first_name"


def test_extract_field_mappings_format3(api_client):
    """Test extraction of field mappings - format 3 (columns array)."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "columns": [
                {"from": "customer_id", "to": "ContactId", "isPrimaryKey": True},
                {"from": "email", "to": "Email", "isPrimaryKey": False},
            ]
        },
    )

    result = api_client.extract_field_mappings(sync)

    assert len(result) == 2
    assert result[0].source_field == "customer_id"
    assert result[0].destination_field == "ContactId"
    assert result[0].is_primary_key is True


def test_extract_field_mappings_empty(api_client):
    """Test extraction with no field mappings."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    result = api_client.extract_field_mappings(sync)

    assert len(result) == 0


@patch("requests.Session.request")
def test_get_source_by_id_not_found(mock_request, api_client):
    """Test handling of 404 when getting source by ID."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=Mock(status_code=404)
    )
    mock_request.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        api_client.get_source_by_id("nonexistent")


@patch("requests.Session.request")
def test_get_source_by_id_validation_error(mock_request, api_client):
    """Test handling of validation errors when getting source by ID."""
    mock_response_data = {"id": "1"}  # Missing required fields

    mock_response = Mock()
    mock_response.json.return_value = mock_response_data
    mock_response.raise_for_status = Mock()
    mock_request.return_value = mock_response

    result = api_client.get_source_by_id("1")

    assert result is None


@patch("requests.Session.request")
def test_get_model_by_id_server_error(mock_request, api_client):
    """Test handling of 500 server error when getting model by ID."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=Mock(status_code=500)
    )
    mock_request.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        api_client.get_model_by_id("10")


@patch("requests.Session.request")
def test_get_destination_by_id_unauthorized(mock_request, api_client):
    """Test handling of 401 unauthorized error when getting destination by ID."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=Mock(status_code=401)
    )
    mock_request.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        api_client.get_destination_by_id("20")


@patch("requests.Session.request")
def test_network_timeout(mock_request, api_client):
    """Test handling of network timeout."""
    mock_request.side_effect = requests.exceptions.Timeout("Request timed out")

    with pytest.raises(requests.exceptions.Timeout):
        api_client.get_sources()


def test_extract_field_mappings_malformed_data(api_client):
    """Test field mapping extraction with malformed data."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "fieldMappings": [
                # Missing destinationField
                {"sourceField": "customer_id", "isPrimaryKey": True},
                # Valid mapping
                {
                    "sourceField": "email",
                    "destinationField": "Email",
                    "isPrimaryKey": False,
                },
                # Non-dict item (should be skipped)
                "invalid_mapping",
            ]
        },
    )

    result = api_client.extract_field_mappings(sync)

    # Should only include the valid mapping
    assert len(result) == 1
    assert result[0].source_field == "email"
    assert result[0].destination_field == "Email"


def test_extract_field_mappings_non_string_values(api_client):
    """Test field mapping extraction with non-string field names."""
    sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "columnMappings": {
                "ContactId": 123,  # Non-string value
                "Email": "email",
            }
        },
    )

    result = api_client.extract_field_mappings(sync)

    # Should convert to strings
    assert len(result) == 2
    field_dict = {fm.destination_field: fm.source_field for fm in result}
    assert field_dict["ContactId"] == "123"
    assert field_dict["Email"] == "email"

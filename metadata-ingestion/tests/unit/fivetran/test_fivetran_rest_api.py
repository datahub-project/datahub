import json
from unittest.mock import Mock, patch

import pytest
import requests
from pydantic import ValidationError

from datahub.ingestion.source.fivetran.config import FivetranAPIConfig
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionDetails,
)


class TestFivetranAPIClient:
    """Test cases for FivetranAPIClient."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.config = FivetranAPIConfig(
            api_key="test_api_key",
            api_secret="test_api_secret",
            base_url="https://api.fivetran.com",
            request_timeout_sec=30,
        )
        self.client = FivetranAPIClient(self.config)

    def test_init(self):
        """Test FivetranAPIClient initialization."""
        assert self.client.config == self.config
        assert self.client._session is not None
        assert self.client._session.auth == ("test_api_key", "test_api_secret")
        assert self.client._session.headers["Content-Type"] == "application/json"
        assert self.client._session.headers["Accept"] == "application/json"

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_success(self, mock_get):
        """Test successful retrieval of connection details."""
        mock_response_data = {
            "code": "Success",
            "data": {
                "id": "test_connection_id",
                "group_id": "test_group_id",
                "service": "google_sheets",
                "created_at": "2025-01-01T00:00:00Z",
                "succeeded_at": "2025-01-01T01:00:00Z",
                "paused": False,
                "sync_frequency": 360,
                # Extra field that should be filtered out
                "status": {
                    "setup_state": "connected",
                    "schema_status": "ready",
                    "sync_state": "paused",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "warnings": [
                        {
                            "code": "test_warning",
                            "message": "Test warning message",
                            "details": {},
                        }
                    ],
                },
                "config": {
                    "auth_type": "ServiceAccount",
                    "sheet_id": "https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0",
                    "named_range": "Test_Range",
                },
                # Extra fields that should be filtered out
                "source_sync_details": {"last_synced": "2025-01-01T01:00:00Z"},
                "service_version": 1,
                "schema": "test_schema",
                "connected_by": "test_user",
            },
        }

        mock_response = Mock()
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        result = self.client.get_connection_details_by_id("test_connection_id")

        assert isinstance(result, FivetranConnectionDetails)
        assert result.id == "test_connection_id"
        assert result.group_id == "test_group_id"
        assert result.service == "google_sheets"
        assert result.paused is False
        assert result.sync_frequency == 360
        assert result.config.auth_type == "ServiceAccount"
        assert result.config.named_range == "Test_Range"

        mock_get.assert_called_once_with(
            "https://api.fivetran.com/v1/connections/test_connection_id", timeout=30
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_http_error(self, mock_get):
        """Test handling of HTTP error responses."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Client Error: Not Found"
        )
        mock_get.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_json_decode_error(self, mock_get):
        """Test handling of JSON decode errors."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_get.return_value = mock_response

        with pytest.raises(json.JSONDecodeError):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_missing_data_field(self, mock_get):
        """Test handling of missing data field in response."""
        mock_response_data = {
            "code": "Success",
            # Missing "data" field
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="missing 'data' field"):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_non_success_code(self, mock_get):
        """Test handling of non-success response code."""
        mock_response_data = {
            "code": "Error",
            "data": {
                "id": "test_connection_id",
            },
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="(?i)not 'success'"):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_missing_code_field(self, mock_get):
        """Test handling when code field is missing (should proceed normally)."""
        mock_response_data = {
            # Missing "code" field - should be allowed
            "data": {
                "id": "test_connection_id",
                "group_id": "test_group_id",
                "service": "google_sheets",
                "created_at": "2025-01-01T00:00:00Z",
                "succeeded_at": "2025-01-01T01:00:00Z",
                "paused": False,
                "sync_frequency": 360,
                # Extra field that should be filtered out
                "status": {
                    "setup_state": "connected",
                    "schema_status": "ready",
                    "sync_state": "paused",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "warnings": [],
                },
                "config": {
                    "auth_type": "ServiceAccount",
                    "sheet_id": "https://docs.google.com/spreadsheets/d/test123/edit",
                    "named_range": "Test_Range",
                },
                # Extra fields that should be filtered out
                "source_sync_details": {"last_synced": "2025-01-01T01:00:00Z"},
                "service_version": 1,
            },
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        result = self.client.get_connection_details_by_id("test_connection_id")
        assert isinstance(result, FivetranConnectionDetails)

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_request_exception(self, mock_get):
        """Test handling of request exceptions."""
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        with pytest.raises(requests.exceptions.RequestException):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_parse_error(self, mock_get):
        """Test handling of parsing errors when creating FivetranConnectionDetails."""
        mock_response_data = {
            "code": "Success",
            "data": {
                # Missing required fields to cause parsing error
                "id": "test_connection_id",
                # Missing other required fields: group_id, service, created_at, paused, sync_frequency, config
            },
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        # Now raises ValidationError since we use direct Pydantic parsing
        with pytest.raises(ValidationError):
            self.client.get_connection_details_by_id("test_connection_id")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_filters_extra_fields(self, mock_get):
        """Test that extra fields in API response are properly filtered out."""
        mock_response_data = {
            "code": "Success",
            "data": {
                "id": "test_connection_id",
                "group_id": "test_group_id",
                "service": "google_sheets",
                "created_at": "2025-01-01T00:00:00Z",
                "succeeded_at": "2025-01-01T01:00:00Z",
                "paused": False,
                "sync_frequency": 360,
                # Extra field that should be filtered out
                "status": {
                    "setup_state": "connected",
                    "schema_status": "ready",
                    "sync_state": "paused",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "warnings": [],
                    "tasks": ["task1", "task2"],
                },
                "config": {
                    "auth_type": "ServiceAccount",
                    "sheet_id": "https://docs.google.com/spreadsheets/d/test123/edit",
                    "named_range": "Test_Range",
                    # Extra field in config that should be filtered
                    "authorization_method": "User OAuth",
                },
                # Extra top-level fields that should be filtered
                "source_sync_details": {"last_synced": "2025-01-01T01:00:00Z"},
                "service_version": 1,
                "schema": "test_schema",
                "connected_by": "test_user",
                "failed_at": None,
                "pause_after_trial": False,
            },
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        result = self.client.get_connection_details_by_id("test_connection_id")
        assert isinstance(result, FivetranConnectionDetails)
        assert result.id == "test_connection_id"
        # Verify that extra fields are not accessible (they were filtered out)
        assert not hasattr(result, "source_sync_details")
        assert not hasattr(result, "service_version")
        assert not hasattr(result, "status")
        assert not hasattr(result.config, "authorization_method")

    @patch("requests.Session.get")
    def test_get_connection_details_by_id_with_null_succeeded_at(self, mock_get):
        """Test handling of null succeeded_at field."""
        mock_response_data = {
            "code": "Success",
            "data": {
                "id": "test_connection_id",
                "group_id": "test_group_id",
                "service": "google_sheets",
                "created_at": "2025-01-01T00:00:00Z",
                "succeeded_at": None,  # Null succeeded_at
                "paused": False,
                "sync_frequency": 360,
                # Extra field that should be filtered out
                "status": {
                    "setup_state": "connected",
                    "schema_status": "ready",
                    "sync_state": "paused",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "warnings": [],
                },
                "config": {
                    "auth_type": "ServiceAccount",
                    "sheet_id": "https://docs.google.com/spreadsheets/d/test123/edit",
                    "named_range": "Test_Range",
                },
            },
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        result = self.client.get_connection_details_by_id("test_connection_id")
        assert isinstance(result, FivetranConnectionDetails)
        assert result.succeeded_at is None

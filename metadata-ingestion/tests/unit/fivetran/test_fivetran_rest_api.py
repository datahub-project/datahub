from unittest import TestCase
from unittest.mock import Mock, patch

from datahub.ingestion.source.fivetran.config import FivetranAPIConfig
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionDetails,
)


class TestFivetranAPIClient(TestCase):
    """Test cases for FivetranAPIClient."""

    def setUp(self):
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
        # Mock response data
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
                "source_sync_details": {"last_synced": "2025-01-01T01:00:00Z"},
            },
        }

        # Mock the response
        mock_response = Mock()
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        # Call the method
        result = self.client.get_connection_details_by_id("test_connection_id")

        # Assertions
        assert isinstance(result, FivetranConnectionDetails)
        assert result.id == "test_connection_id"
        assert result.group_id == "test_group_id"
        assert result.service == "google_sheets"
        assert result.paused is False
        assert result.sync_frequency == 360
        assert result.config.auth_type == "ServiceAccount"
        assert result.config.named_range == "Test_Range"
        # assert result.config.sheet_id_from_url == "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo"  # Removed - no longer an attribute

        # Verify the API call
        mock_get.assert_called_once_with(
            "https://api.fivetran.com/v1/connections/test_connection_id", timeout=30
        )

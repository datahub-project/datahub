"""Unit tests for Snowplow BDP API client."""

from unittest.mock import patch

import pytest

from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
)


class TestSnowplowBDPClient:
    """Test the SnowplowBDPClient class."""

    @pytest.fixture
    def bdp_config(self):
        """Create a test BDP connection config."""
        return SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )

    @pytest.fixture
    def bdp_client(self, bdp_config):
        """Create a test BDP client with mocked authentication."""
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(bdp_config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_with_pagination(
        self, mock_request, bdp_client
    ):
        """Test that get_data_structure_deployments uses pagination parameters."""
        # Setup mock response - simulating the API response with pagination
        # Note: DataStructureDeployment model expects 'version' as top-level field
        mock_deployments = [
            {
                "version": "1-0-0",
                "env": "PROD",
                "ts": "2024-01-01T00:00:00Z",
                "initiator": "user1",
            },
            {
                "version": "1-1-0",
                "env": "PROD",
                "ts": "2024-02-01T00:00:00Z",
                "initiator": "user2",
            },
        ]
        mock_request.return_value = mock_deployments

        # Call the method
        data_structure_hash = "test-hash-123"
        result = bdp_client.get_data_structure_deployments(data_structure_hash)

        # Verify _request was called with correct parameters
        mock_request.assert_called_once_with(
            "GET",
            f"organizations/test-org-id/data-structures/v1/{data_structure_hash}/deployments",
            params={"from": 0, "size": 1000},
        )

        # Verify results
        assert len(result) == 2
        assert result[0].version == "1-0-0"
        assert result[1].version == "1-1-0"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_returns_full_history(
        self, mock_request, bdp_client
    ):
        """Test that pagination returns full deployment history, not just current state."""
        # Simulate response with both historical and current deployments
        # This includes deployments that have been replaced (e.g., v1-0-0 replaced by v1-1-0)
        mock_deployments = [
            {
                "version": "1-0-0",
                "env": "PROD",
                "ts": "2024-01-01T00:00:00Z",
                "initiator": "user1",
            },
            {
                "version": "1-0-0",
                "env": "DEV",
                "ts": "2024-01-01T01:00:00Z",
                "initiator": "user1",
            },
            {
                "version": "1-1-0",
                "env": "PROD",
                "ts": "2024-02-01T00:00:00Z",
                "initiator": "user2",
            },
            {
                "version": "1-1-0",
                "env": "DEV",
                "ts": "2024-02-01T01:00:00Z",
                "initiator": "user2",
            },
        ]
        mock_request.return_value = mock_deployments

        # Call the method
        result = bdp_client.get_data_structure_deployments("test-hash")

        # Verify all 4 deployments are returned (historical + current)
        assert len(result) == 4

        # Verify we have both versions in the history
        versions = [d.version for d in result]
        assert "1-0-0" in versions
        assert "1-1-0" in versions

        # Verify we have deployments for both environments
        environments = [d.env for d in result]
        assert "PROD" in environments
        assert "DEV" in environments

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_empty_response(
        self, mock_request, bdp_client
    ):
        """Test handling of empty deployment history."""
        mock_request.return_value = []

        result = bdp_client.get_data_structure_deployments("test-hash")

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_handles_dict_response(
        self, mock_request, bdp_client
    ):
        """Test handling of response wrapped in data object."""
        # Some API responses might still return {"data": [...]} format
        mock_deployments = {
            "data": [
                {
                    "version": "1-0-0",
                    "env": "PROD",
                    "ts": "2024-01-01T00:00:00Z",
                    "initiator": "user1",
                }
            ]
        }
        mock_request.return_value = mock_deployments

        result = bdp_client.get_data_structure_deployments("test-hash")

        assert len(result) == 1
        assert result[0].version == "1-0-0"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_handles_parse_error(
        self, mock_request, bdp_client, caplog
    ):
        """Test handling of malformed deployment data."""
        # Return invalid data that can't be parsed
        mock_request.return_value = [{"invalid": "data"}]

        result = bdp_client.get_data_structure_deployments("test-hash")

        # Should return empty list and log error
        assert result == []
        assert "Failed to parse deployments" in caplog.text

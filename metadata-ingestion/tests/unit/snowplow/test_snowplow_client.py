"""Unit tests for Snowplow BDP API client."""

from unittest.mock import Mock, patch

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


class TestSnowplowBDPClientAuthentication:
    """Test authentication logic and retry mechanisms."""

    @pytest.fixture
    def bdp_config(self):
        """Create a test BDP connection config."""
        return SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )

    @patch("requests.Session.get")
    def test_authenticate_success(self, mock_get, bdp_config):
        """Test successful authentication flow."""
        # Mock successful authentication response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()  # Don't raise
        mock_response.json.return_value = {"accessToken": "test-jwt-token"}
        mock_get.return_value = mock_response

        client = SnowplowBDPClient(bdp_config)

        # Verify JWT token was set
        assert client._jwt_token == "test-jwt-token"

        # Verify Authorization header was set
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == "Bearer test-jwt-token"

    @patch("requests.Session.get")
    def test_authenticate_with_401_error(self, mock_get, bdp_config):
        """Test authentication failure with 401 (invalid credentials)."""
        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid API credentials"):
            SnowplowBDPClient(bdp_config)

    @patch("requests.Session.get")
    def test_authenticate_with_403_error(self, mock_get, bdp_config):
        """Test authentication failure with 403 (forbidden)."""
        # Mock 403 response
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Forbidden"):
            SnowplowBDPClient(bdp_config)

    @patch("requests.Session.get")
    def test_authenticate_with_invalid_response_format(self, mock_get, bdp_config):
        """Test authentication failure when response format is invalid."""
        # Mock response with missing accessToken
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"wrongField": "value"}
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid authentication response format"):
            SnowplowBDPClient(bdp_config)

    @patch("requests.Session.get")
    def test_authenticate_with_network_timeout(self, mock_get, bdp_config):
        """Test authentication failure due to network timeout."""
        # Mock timeout exception
        mock_get.side_effect = pytest.importorskip("requests").exceptions.Timeout()

        with pytest.raises(ValueError, match="Authentication failed"):
            SnowplowBDPClient(bdp_config)


class TestSnowplowBDPClientRequestRetry:
    """Test request retry logic and error handling."""

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

    @patch("requests.Session.request")
    def test_request_success(self, mock_request, bdp_client):
        """Test successful API request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()  # Don't raise
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response

        result = bdp_client._request("GET", "test-endpoint")

        assert result == {"data": "test"}
        mock_request.assert_called_once()

    @patch("requests.Session.request")
    @patch.object(SnowplowBDPClient, "_authenticate")
    def test_request_retry_on_401(self, mock_authenticate, mock_request, bdp_client):
        """Test that 401 triggers re-authentication and retry."""
        # First request returns 401, second succeeds after re-auth
        mock_401_response = Mock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_401_response)

        mock_success_response = Mock()
        mock_success_response.status_code = 200
        mock_success_response.raise_for_status = Mock()  # Don't raise
        mock_success_response.json.return_value = {"data": "success"}

        mock_request.side_effect = [mock_401_response, mock_success_response]

        result = bdp_client._request("GET", "test-endpoint")

        # Should have re-authenticated and retried
        mock_authenticate.assert_called_once()
        assert result == {"data": "success"}
        assert mock_request.call_count == 2

    @patch("requests.Session.request")
    @patch.object(SnowplowBDPClient, "_authenticate")
    def test_request_no_double_retry_on_401(
        self, mock_authenticate, mock_request, bdp_client
    ):
        """Test that 401 after retry raises error (no infinite loop)."""
        # Both requests return 401
        mock_401_response = Mock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_401_response)

        mock_request.return_value = mock_401_response

        # Should raise after second 401 (no infinite retry)
        with pytest.raises(pytest.importorskip("requests").exceptions.HTTPError):
            bdp_client._request("GET", "test-endpoint")

        # Should only re-authenticate once
        mock_authenticate.assert_called_once()

    @patch("requests.Session.request")
    def test_request_raises_on_403(self, mock_request, bdp_client):
        """Test that 403 raises PermissionError."""
        mock_403_response = Mock()
        mock_403_response.status_code = 403
        mock_403_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_403_response)
        mock_request.return_value = mock_403_response

        with pytest.raises(PermissionError, match="Permission denied"):
            bdp_client._request("GET", "test-endpoint")

    @patch("requests.Session.request")
    def test_request_raises_on_404(self, mock_request, bdp_client):
        """Test that 404 raises ResourceNotFoundError."""
        mock_404_response = Mock()
        mock_404_response.status_code = 404
        mock_404_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_404_response)
        mock_request.return_value = mock_404_response

        from datahub.ingestion.source.snowplow.snowplow_client import (
            ResourceNotFoundError,
        )

        with pytest.raises(ResourceNotFoundError, match="Resource not found"):
            bdp_client._request("GET", "test-endpoint")

    @patch("requests.Session.request")
    def test_request_timeout(self, mock_request, bdp_client):
        """Test request timeout handling."""
        mock_request.side_effect = pytest.importorskip("requests").exceptions.Timeout()

        with pytest.raises(pytest.importorskip("requests").exceptions.Timeout):
            bdp_client._request("GET", "test-endpoint")

    @patch("requests.Session.request")
    def test_request_with_params_and_json(self, mock_request, bdp_client):
        """Test that request params and JSON body are passed correctly."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()  # Don't raise
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response

        params = {"page": 1, "size": 10}
        json_data = {"filter": "active"}

        bdp_client._request("POST", "test-endpoint", params=params, json_data=json_data)

        # Verify params and json were passed
        call_kwargs = mock_request.call_args[1]
        assert call_kwargs["params"] == params
        assert call_kwargs["json"] == json_data


class TestSnowplowBDPClientAPIMethods:
    """Test all API methods for comprehensive coverage."""

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

    # Organization API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_success(self, mock_request, bdp_client):
        """Test successful organization fetch."""
        mock_request.return_value = {
            "id": "test-org",
            "name": "Test Organization",
            "source": {"name": "snowflake", "type": "warehouse"},
        }

        result = bdp_client.get_organization()

        assert result is not None
        assert result.name == "Test Organization"
        mock_request.assert_called_once_with("GET", "organizations/test-org-id")

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_empty_response(self, mock_request, bdp_client):
        """Test organization fetch with empty response."""
        mock_request.return_value = None

        result = bdp_client.get_organization()

        assert result is None

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_parse_error(self, mock_request, bdp_client):
        """Test organization fetch with invalid data."""
        mock_request.return_value = {"invalid": "data"}

        result = bdp_client.get_organization()

        # Should return None and log error
        assert result is None

    # Data Structures API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structures_single_page(self, mock_request, bdp_client):
        """Test fetching data structures with single page."""
        mock_structures = [
            {"hash": "hash1", "vendor": "com.test", "name": "schema1"},
            {"hash": "hash2", "vendor": "com.test", "name": "schema2"},
        ]
        mock_request.return_value = mock_structures

        result = bdp_client.get_data_structures()

        assert len(result) == 2
        assert result[0].hash == "hash1"
        assert result[1].hash == "hash2"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structures_pagination(self, mock_request, bdp_client):
        """Test fetching data structures with pagination."""
        # First page: 2 items (page_size=2)
        # Second page: 1 item (last page)
        page1 = [
            {"hash": "hash1", "vendor": "com.test", "name": "schema1"},
            {"hash": "hash2", "vendor": "com.test", "name": "schema2"},
        ]
        page2 = [{"hash": "hash3", "vendor": "com.test", "name": "schema3"}]

        mock_request.side_effect = [page1, page2]

        result = bdp_client.get_data_structures(page_size=2)

        # Should make 2 requests (stops when page size < requested)
        assert mock_request.call_count == 2
        assert len(result) == 3

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structures_with_filters(self, mock_request, bdp_client):
        """Test fetching data structures with vendor/name filters."""
        mock_request.return_value = []

        bdp_client.get_data_structures(vendor="com.example", name="test_schema")

        # Verify filters were passed
        call_kwargs = mock_request.call_args[1]
        params = call_kwargs["params"]
        assert params["vendor"] == "com.example"
        assert params["name"] == "test_schema"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structures_non_list_response(self, mock_request, bdp_client):
        """Test handling of non-list response."""
        mock_request.return_value = {"error": "invalid"}

        result = bdp_client.get_data_structures()

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_success(self, mock_request, bdp_client):
        """Test fetching single data structure."""
        mock_request.return_value = {
            "hash": "test-hash",
            "vendor": "com.test",
            "name": "schema1",
        }

        result = bdp_client.get_data_structure("test-hash")

        assert result is not None
        assert result.hash == "test-hash"
        mock_request.assert_called_once_with(
            "GET", "organizations/test-org-id/data-structures/v1/test-hash"
        )

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_not_found(self, mock_request, bdp_client):
        """Test fetching non-existent data structure."""
        mock_request.return_value = None

        result = bdp_client.get_data_structure("non-existent")

        assert result is None

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_version_success(self, mock_request, bdp_client):
        """Test fetching specific version of data structure."""
        # Response has self and schema properties at root level
        mock_request.return_value = {
            "self": {
                "vendor": "com.test",
                "name": "schema1",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "type": "object",
            "properties": {"field1": {"type": "string"}},
        }

        result = bdp_client.get_data_structure_version("test-hash", "1-0-0")

        assert result is not None
        mock_request.assert_called_once()

    # Deployments API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_structure_deployments_success(self, mock_request, bdp_client):
        """Test fetching deployment history."""
        mock_deployments = [
            {
                "version": "1-0-0",
                "env": "PROD",
                "ts": "2024-01-01T00:00:00Z",
                "initiator": "user1",
            }
        ]
        mock_request.return_value = mock_deployments

        result = bdp_client.get_data_structure_deployments("test-hash")

        assert len(result) == 1
        assert result[0].version == "1-0-0"

    # Pipelines API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_pipelines_success(self, mock_request, bdp_client):
        """Test fetching pipelines."""
        mock_pipelines = [
            {"id": "pipeline1", "name": "Test Pipeline", "status": "ready"},
            {"id": "pipeline2", "name": "Test Pipeline 2", "status": "ready"},
        ]
        mock_request.return_value = {"pipelines": mock_pipelines}

        result = bdp_client.get_pipelines()

        assert len(result) == 2
        assert result[0].id == "pipeline1"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_pipelines_empty(self, mock_request, bdp_client):
        """Test fetching pipelines when none exist."""
        mock_request.return_value = {"pipelines": []}

        result = bdp_client.get_pipelines()

        assert result == []

    # Enrichments API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_enrichments_success(self, mock_request, bdp_client):
        """Test fetching enrichments for a pipeline."""
        mock_enrichments = [
            {
                "id": "enrich1",
                "schemaRef": "iglu:com.snowplowanalytics.snowplow.enrichments/ip_lookups/jsonschema/1-0-0",
                "filename": "ip_lookups.json",
                "enabled": True,
                "lastUpdate": "2024-01-01T00:00:00Z",
            }
        ]
        # Response is direct array, not wrapped
        mock_request.return_value = mock_enrichments

        result = bdp_client.get_enrichments("pipeline1")

        assert len(result) == 1
        mock_request.assert_called_once_with(
            "GET",
            "organizations/test-org-id/resources/v1/pipelines/pipeline1/configuration/enrichments",
        )

    # Event Specifications API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_event_specifications_success(self, mock_request, bdp_client):
        """Test fetching event specifications."""
        mock_specs = [
            {"id": "spec1", "name": "Test Event", "version": 1},
            {"id": "spec2", "name": "Test Event 2", "version": 1},
        ]
        # Event specs use wrapped format
        mock_request.return_value = {"data": mock_specs, "includes": [], "errors": []}

        result = bdp_client.get_event_specifications()

        assert len(result) == 2
        assert result[0].id == "spec1"

    # Tracking Scenarios API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_tracking_scenarios_success(self, mock_request, bdp_client):
        """Test fetching tracking scenarios."""
        mock_scenarios = [
            {"id": "scenario1", "name": "Test Scenario"},
            {"id": "scenario2", "name": "Test Scenario 2"},
        ]
        # Tracking scenarios use wrapped format
        mock_request.return_value = {
            "data": mock_scenarios,
            "includes": [],
            "errors": [],
        }

        result = bdp_client.get_tracking_scenarios()

        assert len(result) == 2
        assert result[0].id == "scenario1"

    # Data Products API tests
    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_products_success(self, mock_request, bdp_client):
        """Test fetching data products."""
        mock_products = [
            {"id": "product1", "name": "Test Product"},
            {"id": "product2", "name": "Test Product 2"},
        ]
        # Data products use wrapped format with includes as dict
        mock_request.return_value = {
            "data": mock_products,
            "includes": {"owners": [], "eventSpecs": [], "sourceApplications": []},
            "errors": [],
        }

        result = bdp_client.get_data_products()

        assert len(result) == 2
        assert result[0].id == "product1"

    # Context manager tests
    def test_context_manager_enter_exit(self, bdp_config):
        """Test context manager protocol."""
        with (
            patch.object(SnowplowBDPClient, "_authenticate", return_value=None),
            SnowplowBDPClient(bdp_config) as client,
        ):
            assert client is not None
            assert client.session is not None

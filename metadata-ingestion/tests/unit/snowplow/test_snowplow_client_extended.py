"""Extended unit tests for SnowplowBDPClient covering uncovered API methods."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.snowplow.snowplow_client import (
    ResourceNotFoundError,
    SnowplowBDPClient,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
)


class TestSnowplowBDPClientDataProducts:
    """Test data products API methods."""

    @pytest.fixture
    def bdp_config(self):
        """Create test BDP connection config."""
        return SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )

    @pytest.fixture
    def bdp_client(self, bdp_config):
        """Create test BDP client with mocked authentication."""
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(bdp_config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_products_success(self, mock_request, bdp_client):
        """Test successful data products retrieval."""
        mock_response = {
            "data": [
                {
                    "id": "product-1",
                    "name": "Checkout Events",
                    "description": "Checkout flow events",
                    "eventSpecIds": ["spec-1", "spec-2"],
                },
                {
                    "id": "product-2",
                    "name": "User Events",
                    "description": "User interaction events",
                    "eventSpecIds": ["spec-3"],
                },
            ],
            "includes": {
                "owners": [],
                "eventSpecs": [],
                "sourceApplications": [],
            },
            "errors": [],
        }
        mock_request.return_value = mock_response

        result = bdp_client.get_data_products()

        assert len(result) == 2
        assert result[0].id == "product-1"
        assert result[0].name == "Checkout Events"
        assert result[1].id == "product-2"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_products_handles_404(self, mock_request, bdp_client):
        """Test graceful handling when data products endpoint returns 404."""
        mock_request.side_effect = ResourceNotFoundError("Not found")

        result = bdp_client.get_data_products()

        # Should return empty list, not raise
        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_products_handles_empty_response(self, mock_request, bdp_client):
        """Test handling of empty response."""
        mock_request.return_value = None

        result = bdp_client.get_data_products()

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_product_by_id(self, mock_request, bdp_client):
        """Test retrieving specific data product by ID."""
        mock_response = {
            "data": {
                "id": "product-1",
                "name": "Checkout Events",
                "description": "Checkout flow events",
                "eventSpecIds": ["spec-1"],
            }
        }
        mock_request.return_value = mock_response

        result = bdp_client.get_data_product("product-1")

        assert result is not None
        assert result.id == "product-1"
        assert result.name == "Checkout Events"


class TestSnowplowBDPClientDataModels:
    """Test data models API methods."""

    @pytest.fixture
    def bdp_client(self):
        """Create test BDP client."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_models_success(self, mock_request, bdp_client):
        """Test successful data models retrieval."""
        mock_response = [
            {
                "id": "model-1",
                "organizationId": "test-org",
                "dataProductId": "product-123",
                "name": "Checkout Model",
                "query_engine": "snowflake",
                "destination": "dest-123",
                "table_name": "analytics.derived.checkouts",
            },
            {
                "id": "model-2",
                "organizationId": "test-org",
                "dataProductId": "product-123",
                "name": "User Model",
                "query_engine": "bigquery",
                "destination": "dest-456",
                "table_name": "analytics.derived.users",
            },
        ]
        mock_request.return_value = mock_response

        result = bdp_client.get_data_models("product-123")

        assert len(result) == 2
        assert result[0].id == "model-1"
        assert result[0].query_engine == "snowflake"
        assert result[0].table_name == "analytics.derived.checkouts"
        assert result[1].id == "model-2"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_models_empty_response(self, mock_request, bdp_client):
        """Test handling of data product with no data models."""
        mock_request.return_value = []

        result = bdp_client.get_data_models("product-123")

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_models_handles_parsing_errors(self, mock_request, bdp_client):
        """Test graceful handling when data model parsing fails."""
        mock_response = [
            {
                "id": "model-1",
                "organizationId": "test-org",
                "dataProductId": "product-123",
                "name": "Valid Model",
                "query_engine": "snowflake",
                "destination": "dest-123",
                "table_name": "analytics.derived.table",
            },
            {
                "id": "model-2",
                # Missing required fields - will fail validation
            },
        ]
        mock_request.return_value = mock_response

        result = bdp_client.get_data_models("product-123")

        # Only valid model should be returned
        assert len(result) == 1
        assert result[0].id == "model-1"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_data_models_handles_non_list_response(self, mock_request, bdp_client):
        """Test handling when API returns non-list response."""
        mock_request.return_value = {"error": "Invalid request"}

        result = bdp_client.get_data_models("product-123")

        assert result == []


class TestSnowplowBDPClientEnrichments:
    """Test enrichments API methods."""

    @pytest.fixture
    def bdp_client(self):
        """Create test BDP client."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_enrichments_success(self, mock_request, bdp_client):
        """Test successful enrichments retrieval."""
        mock_response = [
            {
                "id": "enrichment-1",
                "filename": "ip_lookup.json",
                "enabled": True,
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
            {
                "id": "enrichment-2",
                "filename": "ua_parser.json",
                "enabled": True,
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
        ]
        mock_request.return_value = mock_response

        result = bdp_client.get_enrichments("pipeline-123")

        assert len(result) == 2
        assert result[0].id == "enrichment-1"
        assert result[0].filename == "ip_lookup.json"
        assert result[0].enabled is True
        assert result[1].id == "enrichment-2"
        assert result[1].filename == "ua_parser.json"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_enrichments_handles_404(self, mock_request, bdp_client):
        """Test graceful handling when enrichments endpoint returns 404."""
        mock_request.side_effect = ResourceNotFoundError("Not found")

        result = bdp_client.get_enrichments("pipeline-123")

        # Should return empty list, not raise
        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_enrichments_empty_pipeline(self, mock_request, bdp_client):
        """Test handling of pipeline with no enrichments."""
        mock_request.return_value = []

        result = bdp_client.get_enrichments("pipeline-123")

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_enrichments_handles_parsing_errors(self, mock_request, bdp_client):
        """Test graceful handling when enrichment parsing fails."""
        mock_response = [
            {
                "id": "enrichment-1",
                "filename": "valid.json",
                "enabled": True,
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
            {
                # Missing required fields - will fail parsing
                "invalid": "data",
            },
        ]
        mock_request.return_value = mock_response

        result = bdp_client.get_enrichments("pipeline-123")

        # Should return empty list due to parsing errors (all or nothing approach)
        assert result == []


class TestSnowplowBDPClientDestinations:
    """Test destinations API methods."""

    @pytest.fixture
    def bdp_client(self):
        """Create test BDP client."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_destinations_success(self, mock_request, bdp_client):
        """Test successful destinations retrieval."""
        mock_response = [
            {
                "id": "dest-1",
                "pipelineId": "pipeline-1",
                "name": "Snowflake Production",
                "loaderName": "snowflake-loader",
                "loaderType": "snowflake",
                "destinationType": "warehouse",
                "status": "active",
                "target": {
                    "id": "target-1",
                    "label": "Snowflake Prod",
                    "name": "snowflake-prod",
                    "status": "active",
                    "envName": "production",
                    "targetType": "snowflake",
                    "config": {
                        "database": "analytics",
                        "schema": "events",
                        "table": "atomic_events",
                    },
                },
            },
            {
                "id": "dest-2",
                "pipelineId": "pipeline-2",
                "name": "BigQuery Development",
                "loaderName": "bigquery-loader",
                "loaderType": "bigquery",
                "destinationType": "warehouse",
                "status": "active",
                "target": {
                    "id": "target-2",
                    "label": "BigQuery Dev",
                    "name": "bigquery-dev",
                    "status": "active",
                    "envName": "development",
                    "targetType": "bigquery",
                    "config": {
                        "database": "analytics-dev",
                        "schema": "events",
                        "table": "atomic_events",
                    },
                },
            },
        ]
        mock_request.return_value = mock_response

        result = bdp_client.get_destinations()

        assert len(result) == 2
        assert result[0].id == "dest-1"
        assert result[0].loader_name == "snowflake-loader"
        assert result[1].id == "dest-2"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_destinations_handles_404(self, mock_request, bdp_client):
        """Test graceful handling when destinations endpoint returns 404."""
        mock_request.side_effect = ResourceNotFoundError("Not found")

        result = bdp_client.get_destinations()

        # Should return empty list, not raise
        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_destinations_empty_response(self, mock_request, bdp_client):
        """Test handling of empty destinations response."""
        mock_request.return_value = []

        result = bdp_client.get_destinations()

        assert result == []

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_destinations_handles_non_list_response(self, mock_request, bdp_client):
        """Test handling when API returns non-list response."""
        mock_request.return_value = {"error": "Invalid"}

        result = bdp_client.get_destinations()

        assert result == []


class TestSnowplowBDPClientOrganization:
    """Test organization API methods."""

    @pytest.fixture
    def bdp_client(self):
        """Create test BDP client."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_with_warehouse_source(self, mock_request, bdp_client):
        """Test organization retrieval with warehouse destination configured."""
        mock_response = {
            "id": "org-123",
            "name": "Test Organization",
            "source": {
                "id": "source-123",
                "name": "Snowflake Warehouse",
                "type": "snowflake",
                "database": "analytics",
                "schema": "atomic",
            },
        }
        mock_request.return_value = mock_response

        result = bdp_client.get_organization()

        assert result is not None
        assert result.id == "org-123"
        assert result.name == "Test Organization"
        assert result.source is not None
        assert result.source.name == "Snowflake Warehouse"

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_without_warehouse_source(self, mock_request, bdp_client):
        """Test organization retrieval without warehouse destination."""
        mock_response = {
            "id": "org-123",
            "name": "Test Organization",
            "source": None,
        }
        mock_request.return_value = mock_response

        result = bdp_client.get_organization()

        assert result is not None
        assert result.id == "org-123"
        assert result.source is None

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_handles_api_error(self, mock_request, bdp_client):
        """Test graceful handling when organization API fails."""
        mock_request.side_effect = Exception("API error")

        result = bdp_client.get_organization()

        # Should return None, not raise
        assert result is None

    @patch.object(SnowplowBDPClient, "_request")
    def test_get_organization_handles_empty_response(self, mock_request, bdp_client):
        """Test handling of empty organization response."""
        mock_request.return_value = None

        result = bdp_client.get_organization()

        assert result is None


class TestSnowplowBDPClientAuthenticationRetry:
    """Test authentication retry logic on 401 errors."""

    @pytest.fixture
    def bdp_client(self):
        """Create test BDP client."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )
        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            client._jwt_token = "test-token"
            yield client

    @patch.object(SnowplowBDPClient, "_authenticate")
    def test_request_retries_on_401_with_new_token(self, mock_authenticate, bdp_client):
        """Test that 401 triggers re-authentication and retry."""
        # Mock session to simulate 401 on first call, success on second
        first_call = True

        def side_effect(*args, **kwargs):
            nonlocal first_call
            if first_call:
                first_call = False
                response = Mock()
                response.status_code = 401
                response.raise_for_status.side_effect = Exception("401 Unauthorized")
                from requests.exceptions import HTTPError

                raise HTTPError(response=response)
            else:
                response = Mock()
                response.json.return_value = {"data": "success"}
                return response

        with patch.object(bdp_client.session, "request", side_effect=side_effect):
            # First call should trigger 401, re-auth, and retry
            result = bdp_client._request("GET", "test-endpoint")

            # Should have re-authenticated
            mock_authenticate.assert_called_once()

            # Should have succeeded on retry
            assert result == {"data": "success"}

    @patch.object(SnowplowBDPClient, "_authenticate")
    def test_request_does_not_retry_twice_on_401(self, mock_authenticate, bdp_client):
        """Test that request does not retry indefinitely on repeated 401s."""

        # Mock session to always return 401
        def side_effect(*args, **kwargs):
            response = Mock()
            response.status_code = 401
            from requests.exceptions import HTTPError

            raise HTTPError(response=response)

        from requests.exceptions import HTTPError

        with patch.object(bdp_client.session, "request", side_effect=side_effect):
            # Should raise after single retry
            with pytest.raises(HTTPError):
                bdp_client._request("GET", "test-endpoint")

            # Should have tried to re-authenticate once
            mock_authenticate.assert_called_once()


class TestSnowplowBDPClientContextManager:
    """Test context manager functionality."""

    def test_client_closes_session_on_exit(self):
        """Test that client properly closes session when used as context manager."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )

        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            with SnowplowBDPClient(config) as client:
                client._jwt_token = "test-token"
                session_mock = Mock()
                client.session = session_mock

            # Session should be closed after context exit
            session_mock.close.assert_called_once()

    def test_client_close_method(self):
        """Test that close method properly closes session."""
        config = SnowplowBDPConnectionConfig(
            organization_id="test-org-id",
            api_key_id="test-key-id",
            api_key="test-secret",
        )

        with patch.object(SnowplowBDPClient, "_authenticate", return_value=None):
            client = SnowplowBDPClient(config)
            session_mock = Mock()
            client.session = session_mock

            client.close()

            # Session should be closed
            session_mock.close.assert_called_once()

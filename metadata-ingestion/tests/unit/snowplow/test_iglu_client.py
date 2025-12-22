"""Unit tests for Iglu Schema Registry client."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.snowplow.iglu_client import IgluClient
from datahub.ingestion.source.snowplow.snowplow_config import IgluConnectionConfig


class TestIgluClientInitialization:
    """Test Iglu client initialization and setup."""

    def test_init_without_api_key(self):
        """Test initialization without API key."""
        config = IgluConnectionConfig(iglu_server_url="http://iglu.example.com")

        client = IgluClient(config)

        assert client.base_url == "http://iglu.example.com"
        assert client.session is not None
        # No API key set in session params (params should be empty dict or None)
        assert client.session.params == {} or client.session.params is None

    def test_init_with_api_key(self):
        """Test initialization with API key."""
        config = IgluConnectionConfig(
            iglu_server_url="http://iglu.example.com",
            api_key="test-api-key-uuid",
        )

        client = IgluClient(config)

        assert client.base_url == "http://iglu.example.com"
        assert client.session is not None
        # API key should be set in session params
        assert client.session.params == {"apikey": "test-api-key-uuid"}

    def test_session_has_retry_strategy(self):
        """Test that session is configured with retry strategy."""
        config = IgluConnectionConfig(iglu_server_url="http://iglu.example.com")

        client = IgluClient(config)

        # Verify adapters are mounted
        assert "http://" in client.session.adapters
        assert "https://" in client.session.adapters


class TestIgluClientContextManager:
    """Test context manager protocol."""

    def test_context_manager_enter_exit(self):
        """Test context manager enter and exit."""
        config = IgluConnectionConfig(iglu_server_url="http://iglu.example.com")

        with IgluClient(config) as client:
            assert client is not None
            assert client.session is not None


class TestIgluClientGetSchema:
    """Test schema retrieval from Iglu registry."""

    @pytest.fixture
    def iglu_config(self):
        """Create test Iglu config."""
        return IgluConnectionConfig(iglu_server_url="http://iglu.example.com")

    @pytest.fixture
    def iglu_client(self, iglu_config):
        """Create test Iglu client."""
        return IgluClient(iglu_config)

    @patch("requests.Session.get")
    def test_get_schema_success(self, mock_get, iglu_client):
        """Test successful schema retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.snowplowanalytics.snowplow",
                "name": "page_view",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "type": "object",
            "properties": {"page_url": {"type": "string"}},
        }
        mock_get.return_value = mock_response

        result = iglu_client.get_schema(
            vendor="com.snowplowanalytics.snowplow",
            name="page_view",
            format="jsonschema",
            version="1-0-0",
        )

        assert result is not None
        # Should make request to correct endpoint
        mock_get.assert_called_once()
        call_args = mock_get.call_args[0]
        assert (
            "api/schemas/com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0"
            in call_args[0]
        )

    @patch("requests.Session.get")
    def test_get_schema_not_found(self, mock_get, iglu_client):
        """Test schema not found (404)."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        result = iglu_client.get_schema(
            vendor="com.example",
            name="non_existent",
            format="jsonschema",
            version="1-0-0",
        )

        assert result is None

    @patch("requests.Session.get")
    def test_get_schema_server_error(self, mock_get, iglu_client):
        """Test server error (500)."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        # Should raise HTTPError for server errors
        with pytest.raises(pytest.importorskip("requests").exceptions.HTTPError):
            iglu_client.get_schema(
                vendor="com.example",
                name="schema",
                format="jsonschema",
                version="1-0-0",
            )

    @patch("requests.Session.get")
    def test_get_schema_parse_error(self, mock_get, iglu_client):
        """Test invalid response format."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"invalid": "format"}
        mock_get.return_value = mock_response

        result = iglu_client.get_schema(
            vendor="com.example",
            name="schema",
            format="jsonschema",
            version="1-0-0",
        )

        # Should return None when parsing fails
        assert result is None

    @patch("requests.Session.get")
    def test_get_schema_timeout(self, mock_get, iglu_client):
        """Test request timeout."""
        mock_get.side_effect = pytest.importorskip("requests").exceptions.Timeout()

        # Should raise Timeout exception
        with pytest.raises(pytest.importorskip("requests").exceptions.Timeout):
            iglu_client.get_schema(
                vendor="com.example",
                name="schema",
                format="jsonschema",
                version="1-0-0",
            )

    @patch("requests.Session.get")
    def test_get_schema_with_api_key(self, mock_get):
        """Test that API key is passed in requests."""
        config = IgluConnectionConfig(
            iglu_server_url="http://iglu.example.com",
            api_key="test-api-key",
        )
        client = IgluClient(config)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.test",
                "name": "schema",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "type": "object",
        }
        mock_get.return_value = mock_response

        client.get_schema(
            vendor="com.test",
            name="schema",
            format="jsonschema",
            version="1-0-0",
        )

        # API key should be in session params
        assert client.session.params == {"apikey": "test-api-key"}


class TestIgluClientErrorHandling:
    """Test error handling scenarios."""

    @pytest.fixture
    def iglu_client(self):
        """Create test Iglu client."""
        config = IgluConnectionConfig(iglu_server_url="http://iglu.example.com")
        return IgluClient(config)

    @patch("requests.Session.get")
    def test_connection_error(self, mock_get, iglu_client):
        """Test connection error handling."""
        mock_get.side_effect = pytest.importorskip(
            "requests"
        ).exceptions.ConnectionError()

        # Should raise ConnectionError (which is a RequestException)
        with pytest.raises(pytest.importorskip("requests").exceptions.ConnectionError):
            iglu_client.get_schema(
                vendor="com.test",
                name="schema",
                format="jsonschema",
                version="1-0-0",
            )

    @patch("requests.Session.get")
    def test_json_decode_error(self, mock_get, iglu_client):
        """Test JSON decode error handling."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        result = iglu_client.get_schema(
            vendor="com.test",
            name="schema",
            format="jsonschema",
            version="1-0-0",
        )

        # Should return None when JSON parsing fails
        assert result is None

    @patch("requests.Session.get")
    def test_unexpected_exception(self, mock_get, iglu_client):
        """Test unexpected exception handling."""
        mock_get.side_effect = Exception("Unexpected error")

        result = iglu_client.get_schema(
            vendor="com.test",
            name="schema",
            format="jsonschema",
            version="1-0-0",
        )

        # Should return None and log error
        assert result is None

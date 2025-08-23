from unittest.mock import Mock, patch

import pytest
import requests

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioAPIErrorHandling:
    @pytest.fixture
    def mock_session(self):
        """Create a mock session for testing"""
        mock_session = Mock()
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200
        mock_session.headers = {}  # Add headers as dict for curl command generation
        return mock_session

    @pytest.fixture
    def dremio_api(self, mock_session):
        """Create DremioAPIOperations instance with mocked session"""
        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )
        report = DremioSourceReport()

        with patch("requests.Session", return_value=mock_session):
            api = DremioAPIOperations(config, report)
            api.session = mock_session  # Ensure we're using the mock
            return api

    def test_fetch_all_results_missing_rows_key(self, dremio_api):
        """Test OOM scenario where 'rows' key is missing from response"""
        job_id = "test_job_123"

        # Mock get_job_result to return response without 'rows' key
        dremio_api.get_job_result = Mock(
            return_value={
                "rowCount": 100,
                # Missing 'rows' key - this simulates OOM scenario
            }
        )

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._fetch_all_results(job_id)

        assert "missing 'rows' key" in str(exc_info.value)
        assert "out-of-memory error" in str(exc_info.value)
        assert job_id in str(exc_info.value)

    def test_fetch_all_results_error_message_in_response(self, dremio_api):
        """Test handling of errorMessage in API response"""
        job_id = "test_job_123"
        error_msg = "Query execution failed due to insufficient memory"

        dremio_api.get_job_result = Mock(
            return_value={
                "errorMessage": error_msg,
                "rowCount": 0,
            }
        )

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._fetch_all_results(job_id)

        assert error_msg in str(exc_info.value)
        assert job_id in str(exc_info.value)

    def test_fetch_all_results_invalid_response_format(self, dremio_api):
        """Test handling of non-dict response"""
        job_id = "test_job_123"

        # Mock get_job_result to return string instead of dict
        dremio_api.get_job_result = Mock(return_value="Invalid response")

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._fetch_all_results(job_id)

        assert "Invalid response format" in str(exc_info.value)
        assert job_id in str(exc_info.value)

    def test_fetch_all_results_invalid_rows_format(self, dremio_api):
        """Test handling of non-list rows"""
        job_id = "test_job_123"

        dremio_api.get_job_result = Mock(
            return_value={
                "rows": "not_a_list",  # Invalid format
                "rowCount": 1,
            }
        )

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._fetch_all_results(job_id)

        assert "Invalid 'rows' format" in str(exc_info.value)
        assert job_id in str(exc_info.value)

    def test_fetch_all_results_missing_row_count(self, dremio_api):
        """Test handling of missing rowCount key"""
        job_id = "test_job_123"
        rows_data = [{"col1": "value1"}, {"col1": "value2"}]

        dremio_api.get_job_result = Mock(
            return_value={
                "rows": rows_data,
                # Missing 'rowCount' key
            }
        )

        # Should not raise exception, should use length of rows
        result = dremio_api._fetch_all_results(job_id)
        assert result == rows_data

    def test_fetch_all_results_successful_single_batch(self, dremio_api):
        """Test successful fetching with single batch"""
        job_id = "test_job_123"
        rows_data = [{"col1": "value1"}, {"col1": "value2"}]

        dremio_api.get_job_result = Mock(
            return_value={
                "rows": rows_data,
                "rowCount": 2,
            }
        )

        result = dremio_api._fetch_all_results(job_id)
        assert result == rows_data

    def test_fetch_all_results_successful_multiple_batches(self, dremio_api):
        """Test successful fetching with multiple batches"""
        job_id = "test_job_123"
        batch1 = [{"col1": f"value{i}"} for i in range(500)]
        batch2 = [{"col1": f"value{i}"} for i in range(500, 600)]

        # Mock to return different batches based on offset
        def mock_get_job_result(job_id, offset, limit):
            if offset == 0:
                return {"rows": batch1, "rowCount": 600}
            elif offset == 500:
                return {"rows": batch2, "rowCount": 600}
            else:
                return {"rows": [], "rowCount": 600}

        dremio_api.get_job_result = mock_get_job_result

        result = dremio_api._fetch_all_results(job_id)
        assert len(result) == 600
        assert result == batch1 + batch2

    def test_request_http_500_error(self, dremio_api):
        """Test handling of HTTP 500 errors (potential OOM)"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "HTTP 500 error" in str(exc_info.value)
        assert "Server Error - possibly OOM" in str(exc_info.value)

    def test_request_http_503_error(self, dremio_api):
        """Test handling of HTTP 503 errors"""
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.text = "Service Unavailable"

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "HTTP 503 error" in str(exc_info.value)
        assert "Service Unavailable" in str(exc_info.value)

    def test_request_http_429_error(self, dremio_api):
        """Test handling of HTTP 429 errors (rate limiting)"""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "HTTP 429 error" in str(exc_info.value)
        assert "Rate Limited" in str(exc_info.value)

    def test_request_json_decode_error(self, dremio_api):
        """Test handling of JSON decode errors"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError(
            "Invalid JSON", "", 0
        )
        mock_response.text = "Invalid JSON response"

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "Failed to parse JSON from response" in str(exc_info.value)
        assert "Invalid JSON response" in str(exc_info.value)

    def test_request_timeout_error(self, dremio_api):
        """Test handling of timeout errors"""
        dremio_api.session.request.side_effect = requests.exceptions.Timeout(
            "Request timed out"
        )

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "Request timeout" in str(exc_info.value)

    def test_request_connection_error(self, dremio_api):
        """Test handling of connection errors"""
        dremio_api.session.request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "Connection error" in str(exc_info.value)

    def test_request_successful_with_error_message_in_json(self, dremio_api):
        """Test handling of errorMessage in successful HTTP response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"errorMessage": "SQL execution failed"}

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "API error: SQL execution failed" in str(exc_info.value)

    def test_request_successful_with_error_field_in_json(self, dremio_api):
        """Test handling of error field in successful HTTP response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "Authentication failed"}

        dremio_api.session.request.return_value = mock_response

        with pytest.raises(DremioAPIException) as exc_info:
            dremio_api._request("GET", "/test")

        assert "API error: Authentication failed" in str(exc_info.value)

    def test_request_successful(self, dremio_api):
        """Test successful request handling"""
        expected_data = {"data": "success", "status": "ok"}

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = expected_data

        dremio_api.session.request.return_value = mock_response

        result = dremio_api._request("GET", "/test")
        assert result == expected_data

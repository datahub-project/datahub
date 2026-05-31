from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioAPIPagination:
    @pytest.fixture
    def dremio_api(self, monkeypatch):
        """Setup mock Dremio API for testing"""
        # Mock the requests.Session
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))

        # Mock the authentication response
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )
        report = DremioSourceReport()
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        return api

    def test_fetch_results_iter_missing_rows_key(self, dremio_api):
        """Test internal streaming method handling missing 'rows' key"""
        # Mock get_job_result to return response without 'rows' key
        dremio_api.get_job_result = Mock(return_value={"message": "No data available"})

        result_iterator = dremio_api._fetch_results_iter("test-job-id")
        results = list(result_iterator)

        # Should return empty iterator when no rows key is present
        assert results == []

    def test_fetch_results_iter_with_error(self, dremio_api):
        """Test internal streaming method handling error response"""
        # Mock get_job_result to return error response
        dremio_api.get_job_result = Mock(return_value={"errorMessage": "Query timeout"})

        result_iterator = dremio_api._fetch_results_iter("test-job-id")

        with pytest.raises(DremioAPIException, match="Query error: Query timeout"):
            list(result_iterator)

    def test_fetch_results_iter_normal_case(self, dremio_api):
        """Test internal streaming method with valid data"""
        # Mock get_job_result to return valid data in batches
        mock_responses = [
            {"rows": [{"col1": "val1"}, {"col1": "val2"}], "rowCount": 2},
            {"rows": [], "rowCount": 2},  # Empty response to end iteration
        ]
        dremio_api.get_job_result = Mock(side_effect=mock_responses)

        result_iterator = dremio_api._fetch_results_iter("test-job-id")
        results = list(result_iterator)

        expected = [
            {"col1": "val1"},
            {"col1": "val2"},
        ]
        assert results == expected

    def test_execute_query_iter_success(self, dremio_api):
        """Test execute_query_iter with successful job completion"""
        # Mock the POST response for job submission
        dremio_api.post = Mock(return_value={"id": "job-123"})

        # Mock job status progression
        status_responses = [
            {"jobState": "RUNNING"},
            {"jobState": "RUNNING"},
            {"jobState": "COMPLETED"},
        ]
        dremio_api.get_job_status = Mock(side_effect=status_responses)

        # Mock streaming results
        dremio_api._fetch_results_iter = Mock(return_value=iter([{"col1": "val1"}]))

        with patch("time.sleep"):  # Skip actual sleep delays
            result_iterator = dremio_api.execute_query_iter("SELECT * FROM test")
            results = list(result_iterator)

        assert results == [{"col1": "val1"}]
        dremio_api.post.assert_called_once()
        assert dremio_api.get_job_status.call_count == 3

    def test_execute_query_iter_job_failure(self, dremio_api):
        """Test execute_query_iter with job failure"""
        # Mock the POST response for job submission
        dremio_api.post = Mock(return_value={"id": "job-123"})

        # Mock job failure
        dremio_api.get_job_status = Mock(
            return_value={"jobState": "FAILED", "errorMessage": "SQL syntax error"}
        )

        with (
            pytest.raises(DremioAPIException),
            patch("time.sleep"),
        ):
            result_iterator = dremio_api.execute_query_iter("SELECT * FROM test")
            list(result_iterator)  # Force evaluation

    def test_execute_query_iter_timeout(self, dremio_api):
        """Test execute_query_iter with timeout"""
        # Mock the POST response for job submission
        dremio_api.post = Mock(return_value={"id": "job-123"})

        # Mock job that never completes
        dremio_api.get_job_status = Mock(return_value={"jobState": "RUNNING"})
        dremio_api.cancel_query = Mock()

        # Mock time.time to simulate timeout - need to patch where it's imported
        # First call: start_time = 0
        # Second call: time() - start_time check = 3700 (triggers timeout)
        mock_time_values = [0, 3700]

        with (
            patch(
                "datahub.ingestion.source.dremio.dremio_api.time",
                side_effect=mock_time_values,
            ),
            patch("datahub.ingestion.source.dremio.dremio_api.sleep"),
            pytest.raises(
                DremioAPIException,
                match="Query execution timed out after 3600 seconds",
            ),
        ):
            result_iterator = dremio_api.execute_query_iter("SELECT * FROM test")
            list(result_iterator)

        dremio_api.cancel_query.assert_called_once_with("job-123")

    def test_fetch_results_iter_incremental_yielding(self, dremio_api):
        """Test that internal iterator yields results incrementally and can be partially consumed"""
        # Verify that streaming yields results one at a time and iterator state is maintained

        mock_responses = [
            {"rows": [{"id": i} for i in range(100)], "rowCount": 1000},
            {"rows": [{"id": i} for i in range(100, 200)], "rowCount": 1000},
            {"rows": [], "rowCount": 1000},  # End iteration
        ]
        dremio_api.get_job_result = Mock(side_effect=mock_responses)

        # Test that streaming version yields results incrementally
        result_iterator = dremio_api._fetch_results_iter("test-job")

        # Get first few results
        first_batch = []
        for i, result in enumerate(result_iterator):
            first_batch.append(result)
            if i >= 50:  # Stop after getting 51 results
                break

        assert len(first_batch) == 51
        # Verify we can still get more results (iterator not exhausted)
        next_result = next(result_iterator, None)
        assert next_result is not None

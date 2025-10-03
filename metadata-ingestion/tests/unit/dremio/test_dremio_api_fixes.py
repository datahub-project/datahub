from collections import deque
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioAPIFixes:
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

    def test_fetch_all_results_missing_rows_key(self, dremio_api):
        """Test handling of API response missing 'rows' key"""
        # Mock get_job_result to return response without 'rows' key
        dremio_api.get_job_result = Mock(return_value={"message": "No data available"})

        result = dremio_api._fetch_all_results("test-job-id")

        # Should return empty list when no rows key is present
        assert result == []
        dremio_api.get_job_result.assert_called_once()

    def test_fetch_all_results_with_error_message(self, dremio_api):
        """Test handling of API response with errorMessage"""
        # Mock get_job_result to return error response
        dremio_api.get_job_result = Mock(return_value={"errorMessage": "Out of memory"})

        with pytest.raises(DremioAPIException, match="Query error: Out of memory"):
            dremio_api._fetch_all_results("test-job-id")

    def test_fetch_all_results_empty_rows(self, dremio_api):
        """Test handling of empty rows response"""
        # Mock get_job_result to return empty rows
        dremio_api.get_job_result = Mock(return_value={"rows": [], "rowCount": 0})

        result = dremio_api._fetch_all_results("test-job-id")

        assert result == []
        dremio_api.get_job_result.assert_called_once()

    def test_fetch_all_results_normal_case(self, dremio_api):
        """Test normal operation with valid rows"""
        # Mock get_job_result to return valid data
        # First response: 2 rows, rowCount=2 (offset will be 2, which equals rowCount, so stops)
        mock_responses = [
            {"rows": [{"col1": "val1"}, {"col1": "val2"}], "rowCount": 2},
        ]
        dremio_api.get_job_result = Mock(side_effect=mock_responses)

        result = dremio_api._fetch_all_results("test-job-id")

        expected = [{"col1": "val1"}, {"col1": "val2"}]
        assert result == expected
        assert dremio_api.get_job_result.call_count == 1

    def test_fetch_results_iter_missing_rows_key(self, dremio_api):
        """Test streaming version handling missing 'rows' key"""
        # Mock get_job_result to return response without 'rows' key
        dremio_api.get_job_result = Mock(return_value={"message": "No data available"})

        result_iterator = dremio_api._fetch_results_iter("test-job-id")
        results = list(result_iterator)

        # Should return empty iterator when no rows key is present
        assert results == []

    def test_fetch_results_iter_with_error(self, dremio_api):
        """Test streaming version handling error response"""
        # Mock get_job_result to return error response
        dremio_api.get_job_result = Mock(return_value={"errorMessage": "Query timeout"})

        result_iterator = dremio_api._fetch_results_iter("test-job-id")

        with pytest.raises(DremioAPIException, match="Query error: Query timeout"):
            list(result_iterator)

    def test_fetch_results_iter_normal_case(self, dremio_api):
        """Test streaming version with valid data"""
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
            pytest.raises(RuntimeError, match="Query failed: SQL syntax error"),
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

    def test_get_all_tables_and_columns_iter(self, dremio_api):
        """Test streaming version of get_all_tables_and_columns"""
        from datahub.ingestion.source.dremio.dremio_api import DremioEdition

        # Set up test data
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        # Mock container
        mock_container = Mock()
        mock_container.container_name = "test_source"
        containers = deque([mock_container])

        # Mock streaming query results
        mock_results = [
            {
                "COLUMN_NAME": "col1",
                "FULL_TABLE_PATH": "test.table1",
                "TABLE_NAME": "table1",
                "TABLE_SCHEMA": "test",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": "res1",
            }
        ]
        dremio_api.execute_query_iter = Mock(return_value=iter(mock_results))

        # Test streaming method
        result_iterator = dremio_api.get_all_tables_and_columns_iter(containers)
        tables = list(result_iterator)

        assert len(tables) == 1
        table = tables[0]
        assert table["TABLE_NAME"] == "table1"
        assert table["TABLE_SCHEMA"] == "test"
        assert len(table["COLUMNS"]) == 1
        assert table["COLUMNS"][0]["name"] == "col1"

    def test_extract_all_queries_iter(self, dremio_api):
        """Test streaming version of extract_all_queries"""

        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.start_time = None
        dremio_api.end_time = None

        # Mock streaming query execution
        mock_query_results = [
            {"query_id": "q1", "sql": "SELECT * FROM table1"},
            {"query_id": "q2", "sql": "SELECT * FROM table2"},
        ]
        dremio_api.execute_query_iter = Mock(return_value=iter(mock_query_results))

        result_iterator = dremio_api.extract_all_queries_iter()
        queries = list(result_iterator)

        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[1]["query_id"] == "q2"
        dremio_api.execute_query_iter.assert_called_once()

    def test_memory_usage_comparison(self, dremio_api):
        """Test that streaming uses less memory than batch processing"""
        # This is a conceptual test - in practice, we'd need memory profiling tools
        # Here we just verify that streaming yields results one at a time

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

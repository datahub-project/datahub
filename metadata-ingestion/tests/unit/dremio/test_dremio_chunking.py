from typing import Dict, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries


class TestDremioChunking:
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
        report = Mock(spec=DremioSourceReport)
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        return api

    def test_chunk_size_default(self, dremio_api):
        """Test that chunk size defaults to 1000"""
        assert dremio_api._chunk_size == 1000

    def test_sql_queries_have_limit_clause_placeholder(self):
        """Test that all SQL queries have {limit_clause} placeholder"""
        # Check dataset queries
        assert "{limit_clause}" in DremioSQLQueries.QUERY_DATASETS_CE
        assert "{limit_clause}" in DremioSQLQueries.QUERY_DATASETS_EE
        assert "{limit_clause}" in DremioSQLQueries.QUERY_DATASETS_CLOUD

        # Check job queries
        jobs_query = DremioSQLQueries.get_query_all_jobs()
        assert "{limit_clause}" in jobs_query

        jobs_query_cloud = DremioSQLQueries.get_query_all_jobs_cloud()
        assert "{limit_clause}" in jobs_query_cloud

    def test_get_container_tables_chunked_single_chunk(self, dremio_api):
        """Test chunked table fetching with a single chunk"""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        # Mock container
        mock_container = Mock()
        mock_container.container_name = "test_source"

        # Mock query results - less than chunk size
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

        # Mock execute_query_iter to return our test data
        dremio_api.execute_query_iter = Mock(return_value=iter(mock_results))

        # Test chunked method
        result_iterator = dremio_api._get_container_tables_chunked(
            mock_container,
            DremioSQLQueries.QUERY_DATASETS_EE,
            "",  # schema_condition
            "",  # deny_schema_condition
        )

        tables = list(result_iterator)

        # Should get one table
        assert len(tables) == 1
        assert tables[0]["TABLE_NAME"] == "table1"

        # Verify the query was called with LIMIT and OFFSET
        dremio_api.execute_query_iter.assert_called_once()
        query_arg = dremio_api.execute_query_iter.call_args[1]["query"]
        assert "LIMIT 1000 OFFSET 0" in query_arg

    def test_get_container_tables_chunked_multiple_chunks(self, dremio_api):
        """Test chunked table fetching with multiple chunks"""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []
        dremio_api._chunk_size = 2  # Small chunk size for testing

        # Mock container
        mock_container = Mock()
        mock_container.container_name = "test_source"

        # Mock query results - simulate multiple chunks
        chunk1_results = [
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
            },
            {
                "COLUMN_NAME": "col1",
                "FULL_TABLE_PATH": "test.table2",
                "TABLE_NAME": "table2",
                "TABLE_SCHEMA": "test",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": "res2",
            },
        ]

        chunk2_results = [
            {
                "COLUMN_NAME": "col1",
                "FULL_TABLE_PATH": "test.table3",
                "TABLE_NAME": "table3",
                "TABLE_SCHEMA": "test",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": "res3",
            }
        ]

        # Mock execute_query_iter to return different results for each call
        dremio_api.execute_query_iter = Mock(
            side_effect=[
                iter(chunk1_results),  # First chunk (full)
                iter(chunk2_results),  # Second chunk (partial, triggers completion)
            ]
        )

        # Test chunked method
        result_iterator = dremio_api._get_container_tables_chunked(
            mock_container,
            DremioSQLQueries.QUERY_DATASETS_EE,
            "",  # schema_condition
            "",  # deny_schema_condition
        )

        tables = list(result_iterator)

        # Should get three tables total
        assert len(tables) == 3
        table_names = [table["TABLE_NAME"] for table in tables]
        assert "table1" in table_names
        assert "table2" in table_names
        assert "table3" in table_names

        # Verify the query was called twice (two chunks)
        assert dremio_api.execute_query_iter.call_count == 2

        # Check that OFFSET was incremented properly
        first_call_query = dremio_api.execute_query_iter.call_args_list[0][1]["query"]
        second_call_query = dremio_api.execute_query_iter.call_args_list[1][1]["query"]

        assert "LIMIT 2 OFFSET 0" in first_call_query
        assert "LIMIT 2 OFFSET 2" in second_call_query

    def test_get_container_tables_chunked_oom_error(self, dremio_api):
        """Test chunked table fetching handles Dremio crash (KeyError: 'rows') gracefully"""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        # Mock container
        mock_container = Mock()
        mock_container.container_name = "test_source"

        # Mock execute_query_iter to raise the actual error we see when Dremio crashes
        # This simulates what happens when Dremio runs out of memory and returns
        # a response without the expected 'rows' key
        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException("Query error: 'rows'")
        )

        # Test chunked method
        result_iterator = dremio_api._get_container_tables_chunked(
            mock_container,
            DremioSQLQueries.QUERY_DATASETS_EE,
            "",  # schema_condition
            "",  # deny_schema_condition
        )

        # Should handle error gracefully and return empty results
        tables = list(result_iterator)
        assert len(tables) == 0

        # Verify that a warning was reported to the structured reporting system
        dremio_api.report.report_warning.assert_called_once()
        warning_call = dremio_api.report.report_warning.call_args
        assert (
            "Dremio crash detected" in warning_call[0][0]
        )  # First positional arg (message)
        assert "KeyError: 'rows'" in warning_call[0][0]
        assert (
            warning_call[1]["context"] == "container:test_source"
        )  # Named arg (context)

    def test_get_queries_chunked_single_chunk(self, dremio_api):
        """Test chunked query fetching with a single chunk"""
        # Mock query results - less than chunk size
        mock_results = [
            {"query_id": "q1", "sql": "SELECT * FROM table1"},
            {"query_id": "q2", "sql": "SELECT * FROM table2"},
        ]

        dremio_api.execute_query_iter = Mock(return_value=iter(mock_results))

        # Test chunked method
        base_query = "SELECT * FROM jobs {limit_clause}"
        result_iterator = dremio_api._get_queries_chunked(base_query)

        queries = list(result_iterator)

        # Should get two queries
        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[1]["query_id"] == "q2"

        # Verify the query was called with LIMIT and OFFSET
        dremio_api.execute_query_iter.assert_called_once()
        query_arg = dremio_api.execute_query_iter.call_args[1]["query"]
        assert "LIMIT 1000 OFFSET 0" in query_arg

    def test_get_queries_chunked_multiple_chunks(self, dremio_api):
        """Test chunked query fetching with multiple chunks"""
        dremio_api._chunk_size = 1  # Small chunk size for testing

        # Mock query results for multiple chunks
        chunk1_results = [{"query_id": "q1", "sql": "SELECT * FROM table1"}]
        chunk2_results = [{"query_id": "q2", "sql": "SELECT * FROM table2"}]
        chunk3_results: List[Dict] = []  # Empty to trigger completion

        dremio_api.execute_query_iter = Mock(
            side_effect=[
                iter(chunk1_results),
                iter(chunk2_results),
                iter(chunk3_results),
            ]
        )

        # Test chunked method
        base_query = "SELECT * FROM jobs {limit_clause}"
        result_iterator = dremio_api._get_queries_chunked(base_query)

        queries = list(result_iterator)

        # Should get two queries total
        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[1]["query_id"] == "q2"

        # Verify the query was called three times (including empty result)
        assert dremio_api.execute_query_iter.call_count == 3

    def test_get_queries_chunked_keyerror_rows(self, dremio_api):
        """Test chunked query fetching handles Dremio crash (KeyError: 'rows') gracefully"""
        # Mock execute_query_iter to raise the actual error we see when Dremio crashes
        # This simulates the original issue: KeyError: 'rows' when Dremio runs out of memory
        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException("Query error: 'rows'")
        )

        # Test chunked method
        base_query = "SELECT * FROM jobs {limit_clause}"
        result_iterator = dremio_api._get_queries_chunked(base_query)

        # Should handle error gracefully and return empty results
        queries = list(result_iterator)
        assert len(queries) == 0

        # Verify that a warning was reported to the structured reporting system
        dremio_api.report.report_warning.assert_called_once()
        warning_call = dremio_api.report.report_warning.call_args
        assert (
            "Dremio crash detected" in warning_call[0][0]
        )  # First positional arg (message)
        assert "KeyError: 'rows'" in warning_call[0][0]
        assert warning_call[1]["context"] == "query_extraction"  # Named arg (context)

    def test_extract_all_queries_iter_uses_chunking(self, dremio_api):
        """Test that extract_all_queries_iter uses chunked execution"""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.start_time = None
        dremio_api.end_time = None

        # Mock the chunked method
        mock_results = [{"query_id": "q1", "sql": "SELECT * FROM table1"}]
        dremio_api._get_queries_chunked = Mock(return_value=iter(mock_results))

        # Test that extract_all_queries_iter calls the chunked method
        result_iterator = dremio_api.extract_all_queries_iter()
        queries = list(result_iterator)

        # Verify chunked method was called
        dremio_api._get_queries_chunked.assert_called_once()

        # Verify results
        assert len(queries) == 1
        assert queries[0]["query_id"] == "q1"

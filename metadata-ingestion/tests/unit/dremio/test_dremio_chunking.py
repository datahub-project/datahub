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

    def test_get_all_tables_and_columns_uses_global_query(self, dremio_api):
        """get_all_tables_and_columns should use the global query path, not per-container."""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        mock_results = [
            {
                "COLUMN_NAME": "col1",
                "FULL_TABLE_PATH": "source.schema.table1",
                "TABLE_NAME": "table1",
                "TABLE_SCHEMA": "source.schema",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": "res1",
                "LOCATION_ID": "loc1",
                "VIEW_DEFINITION": None,
                "OWNER": "user1",
                "OWNER_TYPE": "USER",
                "CREATED": "2024-01-01",
                "FORMAT_TYPE": None,
            }
        ]

        dremio_api._get_all_tables_global_chunked = Mock(
            return_value=iter(mock_results)
        )

        tables = list(dremio_api.get_all_tables_and_columns())

        dremio_api._get_all_tables_global_chunked.assert_called_once()
        assert len(tables) == 1

    def test_get_all_tables_global_chunked_single_chunk(self, dremio_api):
        """Test global chunked table fetching with a single chunk."""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        mock_results = [
            {
                "COLUMN_NAME": "col1",
                "FULL_TABLE_PATH": "source.table1",
                "TABLE_NAME": "table1",
                "TABLE_SCHEMA": "source",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": "res1",
                "LOCATION_ID": "loc1",
                "VIEW_DEFINITION": None,
                "OWNER": None,
                "OWNER_TYPE": None,
                "CREATED": None,
                "FORMAT_TYPE": None,
            }
        ]

        dremio_api.execute_query_iter = Mock(return_value=iter(mock_results))

        tables = list(
            dremio_api._get_all_tables_global_chunked(
                DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
                "",
                "",
            )
        )

        assert len(tables) == 1
        assert tables[0]["TABLE_NAME"] == "table1"

        query_arg = dremio_api.execute_query_iter.call_args[1]["query"]
        assert "LIMIT 1000 OFFSET 0" in query_arg
        # Global query must not have LOCATE container filter
        assert "LOCATE" not in query_arg

    def test_get_all_tables_global_chunked_oom_error(self, dremio_api):
        """Global chunked fetch handles Dremio OOM crash gracefully."""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []

        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException("Query error: 'rows'")
        )

        tables = list(
            dremio_api._get_all_tables_global_chunked(
                DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
                "",
                "",
            )
        )

        assert len(tables) == 0
        dremio_api.report.report_warning.assert_called_once()
        warning_call = dremio_api.report.report_warning.call_args
        assert "Dremio crash detected" in warning_call[0][0]
        assert warning_call[1]["context"] == "global_dataset_fetch"

    def test_extract_all_queries_uses_chunking(self, dremio_api):
        """Test that extract_all_queries uses chunked execution"""
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.start_time = None
        dremio_api.end_time = None

        # Mock the chunked method
        mock_results = [{"query_id": "q1", "sql": "SELECT * FROM table1"}]
        dremio_api._get_queries_chunked = Mock(return_value=iter(mock_results))

        # Test that extract_all_queries calls the chunked method
        result_iterator = dremio_api.extract_all_queries()
        queries = list(result_iterator)

        # Verify chunked method was called
        dremio_api._get_queries_chunked.assert_called_once()

        # Verify results
        assert len(queries) == 1
        assert queries[0]["query_id"] == "q1"

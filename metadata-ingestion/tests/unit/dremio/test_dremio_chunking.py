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
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))

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
        mock_results = [
            {"query_id": "q1", "sql": "SELECT * FROM table1"},
            {"query_id": "q2", "sql": "SELECT * FROM table2"},
        ]

        dremio_api.execute_query_iter = Mock(return_value=iter(mock_results))

        base_query = "SELECT * FROM jobs {limit_clause}"
        queries = list(dremio_api._get_queries_chunked(base_query))

        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[1]["query_id"] == "q2"

        dremio_api.execute_query_iter.assert_called_once()
        query_arg = dremio_api.execute_query_iter.call_args[1]["query"]
        assert "LIMIT 1000 OFFSET 0" in query_arg

    def test_get_queries_chunked_multiple_chunks(self, dremio_api):
        dremio_api._chunk_size = 1

        chunk1_results = [{"query_id": "q1", "sql": "SELECT * FROM table1"}]
        chunk2_results = [{"query_id": "q2", "sql": "SELECT * FROM table2"}]
        empty_chunk: List[Dict] = []

        dremio_api.execute_query_iter = Mock(
            side_effect=[
                iter(chunk1_results),
                iter(chunk2_results),
                iter(empty_chunk),
            ]
        )

        base_query = "SELECT * FROM jobs {limit_clause}"
        queries = list(dremio_api._get_queries_chunked(base_query))

        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[1]["query_id"] == "q2"

        # Three calls: two chunks of data, then an empty chunk that signals completion.
        assert dremio_api.execute_query_iter.call_count == 3

    def test_get_queries_chunked_keyerror_rows(self, dremio_api):
        # KeyError: 'rows' is the symptom of a Dremio OOM crash mid-iteration.
        # Surface it as a structured warning rather than letting it bubble.
        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException("Query error: 'rows'")
        )

        base_query = "SELECT * FROM jobs {limit_clause}"
        queries = list(dremio_api._get_queries_chunked(base_query))
        assert len(queries) == 0

        dremio_api.report.report_warning.assert_called_once()
        warning_call = dremio_api.report.report_warning.call_args
        assert "Dremio crash detected" in warning_call[0][0]
        assert "KeyError: 'rows'" in warning_call[0][0]
        assert warning_call[1]["context"] == "query_extraction"

    def test_get_all_tables_and_columns_uses_global_query(self, dremio_api):
        # get_all_tables_and_columns must use the global query path, not per-container.
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
        # Global queries must not contain a LOCATE container filter.
        assert "LOCATE" not in query_arg

    def test_get_all_tables_global_chunked_straddling_table(self, dremio_api):
        # A wide table whose columns cross a chunk boundary must surface
        # once with all columns, not split or duplicated.
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.allow_schema_pattern = [".*"]
        dremio_api.deny_schema_pattern = []
        dremio_api._chunk_size = 2

        def _row(table: str, ordinal: int, col: str) -> Dict:
            return {
                "COLUMN_NAME": col,
                "FULL_TABLE_PATH": f"source.{table}",
                "TABLE_NAME": table,
                "TABLE_SCHEMA": "source",
                "ORDINAL_POSITION": ordinal,
                "IS_NULLABLE": "YES",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_SIZE": 255,
                "RESOURCE_ID": f"res-{table}",
                "LOCATION_ID": f"loc-{table}",
                "VIEW_DEFINITION": None,
                "OWNER": None,
                "OWNER_TYPE": None,
                "CREATED": None,
                "FORMAT_TYPE": None,
            }

        # chunk1+chunk2 split table1's columns; chunk3 is empty so table2
        # exercises the final flush after the loop exits.
        chunk1 = [_row("table1", 1, "a"), _row("table1", 2, "b")]
        chunk2 = [_row("table1", 3, "c"), _row("table2", 1, "x")]
        chunk3: List[Dict] = []

        dremio_api.execute_query_iter = Mock(
            side_effect=[iter(chunk1), iter(chunk2), iter(chunk3)]
        )

        tables = list(
            dremio_api._get_all_tables_global_chunked(
                DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
                "",
                "",
            )
        )

        names_to_cols = {
            t["TABLE_NAME"]: [c["name"] for c in t["COLUMNS"]] for t in tables
        }
        assert names_to_cols == {"table1": ["a", "b", "c"], "table2": ["x"]}
        # No table emitted twice.
        assert [t["TABLE_NAME"] for t in tables] == ["table1", "table2"]

    def test_get_all_tables_global_chunked_oom_error(self, dremio_api):
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
        dremio_api.edition = DremioEdition.ENTERPRISE
        dremio_api.start_time = None
        dremio_api.end_time = None

        mock_results = [{"query_id": "q1", "sql": "SELECT * FROM table1"}]
        dremio_api._get_queries_chunked = Mock(return_value=iter(mock_results))

        queries = list(dremio_api.extract_all_queries())

        dremio_api._get_queries_chunked.assert_called_once()
        assert len(queries) == 1
        assert queries[0]["query_id"] == "q1"

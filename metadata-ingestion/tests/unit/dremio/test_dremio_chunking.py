from typing import Dict, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DREMIO_MAX_JOB_OUTPUT_ROWS,
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.utilities.file_backed_collections import FileBackedDict


def _make_api_with_batch_size(monkeypatch, batch_size):
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
        batch_size=batch_size,
    )
    report = Mock(spec=DremioSourceReport)
    return DremioAPIOperations(config, report), report


@pytest.mark.parametrize(
    "configured,expected",
    [
        (10000, 10000),
        (500, 500),
        # 0 means "as much as possible" -> the safety ceiling.
        (0, DREMIO_MAX_JOB_OUTPUT_ROWS),
        # Above the ceiling is clamped so short-page pagination stays correct.
        (5_000_000, DREMIO_MAX_JOB_OUTPUT_ROWS),
    ],
)
def test_batch_size_resolves_to_chunk_size(monkeypatch, configured, expected):
    api, _ = _make_api_with_batch_size(monkeypatch, configured)
    assert api._chunk_size == expected


def test_batch_size_over_ceiling_warns(monkeypatch):
    api, report = _make_api_with_batch_size(monkeypatch, DREMIO_MAX_JOB_OUTPUT_ROWS + 1)

    assert api._chunk_size == DREMIO_MAX_JOB_OUTPUT_ROWS
    report.warning.assert_called_once()


def test_batch_size_at_ceiling_does_not_warn(monkeypatch):
    # The clamp warning uses `>` while the page-cap nudge uses `>=`; they
    # intentionally differ. Exactly at the ceiling is a valid value, not a clamp.
    api, report = _make_api_with_batch_size(monkeypatch, DREMIO_MAX_JOB_OUTPUT_ROWS)

    assert api._chunk_size == DREMIO_MAX_JOB_OUTPUT_ROWS
    report.warning.assert_not_called()


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
        assert "LIMIT 10000 OFFSET 0" in query_arg

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

    def test_get_queries_chunked_stops_on_short_nonempty_final_page(self, dremio_api):
        # A final page shorter than chunk_size but non-empty must terminate the
        # loop without an extra probing call, and without a truncation warning
        # (the page is well under the job ceiling).
        dremio_api._chunk_size = 2

        full_page = [{"query_id": "q1"}, {"query_id": "q2"}]
        short_page = [{"query_id": "q3"}]

        dremio_api.execute_query_iter = Mock(
            side_effect=[iter(full_page), iter(short_page)]
        )

        queries = list(
            dremio_api._get_queries_chunked("SELECT * FROM jobs {limit_clause}")
        )

        assert [q["query_id"] for q in queries] == ["q1", "q2", "q3"]
        assert dremio_api.execute_query_iter.call_count == 2
        dremio_api.report.warning.assert_not_called()

    def test_warn_if_page_at_job_cap_fires_once(self, dremio_api):
        # A page filled to the ceiling is indistinguishable from a truncated one,
        # so warn — but only once per run, no matter how many pages hit it.
        dremio_api._warn_if_page_at_job_cap(DREMIO_MAX_JOB_OUTPUT_ROWS)
        dremio_api._warn_if_page_at_job_cap(DREMIO_MAX_JOB_OUTPUT_ROWS)

        dremio_api.report.warning.assert_called_once()

    def test_warn_if_page_at_job_cap_silent_below_ceiling(self, dremio_api):
        dremio_api._warn_if_page_at_job_cap(DREMIO_MAX_JOB_OUTPUT_ROWS - 1)

        dremio_api.report.warning.assert_not_called()

    def test_get_queries_chunked_keyerror_rows(self, dremio_api):
        # KeyError: 'rows' is the symptom of a Dremio OOM crash mid-iteration.
        # Surface it as a structured warning rather than letting it bubble.
        dremio_api.execute_query_iter = Mock(
            side_effect=DremioAPIException("Query error: 'rows'")
        )

        base_query = "SELECT * FROM jobs {limit_clause}"
        queries = list(dremio_api._get_queries_chunked(base_query))
        assert len(queries) == 0

        dremio_api.report.warning.assert_called_once()
        warning_call = dremio_api.report.warning.call_args
        assert "Dremio crash detected" in warning_call[1]["message"]
        assert "KeyError: 'rows'" in warning_call[1]["message"]
        assert "query_extraction" in warning_call[1]["context"]

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
                "OWNER": "user1",
                "OWNER_TYPE": "USER",
                "CREATED": "2024-01-01",
                "FORMAT_TYPE": None,
            }
        ]

        dremio_api._get_all_tables_global_chunked = Mock(
            return_value=iter(mock_results)
        )
        view_definitions: FileBackedDict[str] = FileBackedDict()
        view_definitions["source.schema.table1"] = "SELECT 1"
        dremio_api._get_view_definitions = Mock(return_value=view_definitions)

        tables = list(dremio_api.get_all_tables_and_columns())

        dremio_api._get_all_tables_global_chunked.assert_called_once()
        dremio_api._get_view_definitions.assert_called_once()
        # The separately-fetched view definitions must be threaded through to the
        # fetch so they can be merged back by path.
        assert (
            dremio_api._get_all_tables_global_chunked.call_args.kwargs[
                "view_definitions"
            ]
            is view_definitions
        )
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
                {},
            )
        )

        assert len(tables) == 1
        assert tables[0]["TABLE_NAME"] == "table1"

        query_arg = dremio_api.execute_query_iter.call_args[1]["query"]
        assert "LIMIT 10000 OFFSET 0" in query_arg
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
                {},
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
                {},
            )
        )

        assert len(tables) == 0
        # A dropped chunk must be reported as a failure, not silently swallowed.
        dremio_api.report.failure.assert_called_once()
        failure_call = dremio_api.report.failure.call_args
        assert "'rows'" in failure_call.kwargs["context"]

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

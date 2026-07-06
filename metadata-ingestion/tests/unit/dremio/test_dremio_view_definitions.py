from typing import Dict, List
from unittest.mock import Mock

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries


def _make_api(monkeypatch, **config_overrides) -> DremioAPIOperations:
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
        **config_overrides,
    )
    report = Mock(spec=DremioSourceReport)
    api = DremioAPIOperations(config, report)
    api.session = mock_session
    api.allow_schema_pattern = [".*"]
    api.deny_schema_pattern = []
    return api


class TestViewDefinitionQueries:
    def test_datasets_query_does_not_carry_view_definition(self):
        # The per-column datasets query must not select VIEW_DEFINITION — that
        # duplication per column is what overflowed Dremio's 2 GiB vector.
        for query in (
            DremioSQLQueries.QUERY_DATASETS_CE_GLOBAL,
            DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
            DremioSQLQueries.QUERY_DATASETS_CLOUD_GLOBAL,
        ):
            assert "VIEW_DEFINITION" not in query
            assert "SQL_DEFINITION" not in query

    def test_view_definition_queries_are_one_row_per_view(self):
        # The dedicated view queries carry the definition but must not join
        # against COLUMNS (which is what caused the per-column fan-out).
        for query in (
            DremioSQLQueries.QUERY_VIEW_DEFINITIONS_CE,
            DremioSQLQueries.QUERY_VIEW_DEFINITIONS_EE,
            DremioSQLQueries.QUERY_VIEW_DEFINITIONS_CLOUD,
        ):
            assert "VIEW_DEFINITION" in query
            assert "INFORMATION_SCHEMA.COLUMNS" not in query


class TestViewDefinitionSelect:
    def test_community_uses_information_schema_column(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.COMMUNITY
        assert api._get_view_definition_select() == "VIEW_DEFINITION"

    def test_enterprise_uses_sql_definition(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE
        assert api._get_view_definition_select() == "SQL_DEFINITION"

    def test_truncation_wraps_in_substr(self, monkeypatch):
        api = _make_api(monkeypatch, max_view_definition_length=100)
        api.edition = DremioEdition.ENTERPRISE
        assert api._get_view_definition_select() == "SUBSTR(SQL_DEFINITION, 1, 100)"

    def test_truncation_applies_to_community_column(self, monkeypatch):
        api = _make_api(monkeypatch, max_view_definition_length=50)
        api.edition = DremioEdition.COMMUNITY
        assert api._get_view_definition_select() == "SUBSTR(VIEW_DEFINITION, 1, 50)"


class TestGetViewDefinitions:
    def test_builds_map_keyed_by_full_path(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE

        rows = [
            {"FULL_TABLE_PATH": "space.view_a", "VIEW_DEFINITION": "SELECT 1"},
            {"FULL_TABLE_PATH": "space.view_b", "VIEW_DEFINITION": "SELECT 2"},
        ]
        api.execute_query_iter = Mock(return_value=iter(rows))

        result = api._get_view_definitions("", "")

        assert result == {"space.view_a": "SELECT 1", "space.view_b": "SELECT 2"}
        # SUBSTR must not appear when no truncation is configured.
        query_arg = api.execute_query_iter.call_args[1]["query"]
        assert "SUBSTR" not in query_arg
        assert "LIMIT 1000 OFFSET 0" in query_arg

    def test_paginates_across_chunks(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE
        api._chunk_size = 1

        chunk1 = [{"FULL_TABLE_PATH": "s.v1", "VIEW_DEFINITION": "d1"}]
        chunk2 = [{"FULL_TABLE_PATH": "s.v2", "VIEW_DEFINITION": "d2"}]
        empty: List[Dict] = []
        api.execute_query_iter = Mock(
            side_effect=[iter(chunk1), iter(chunk2), iter(empty)]
        )

        result = api._get_view_definitions("", "")

        assert result == {"s.v1": "d1", "s.v2": "d2"}
        assert api.execute_query_iter.call_count == 3

    def test_failure_is_reported_loudly(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE
        api.execute_query_iter = Mock(
            side_effect=DremioAPIException("SQL Error: OversizedAllocationException")
        )

        result = api._get_view_definitions("", "")

        # Partial (empty) map returned, but the run is marked failed rather
        # than silently dropping view definitions.
        assert result == {}
        api.report.failure.assert_called_once()


class TestViewDefinitionMerge:
    def test_enterprise_table_gets_definition_from_map(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE

        record = {
            "COLUMN_NAME": "col1",
            "FULL_TABLE_PATH": "space.my_view",
            "TABLE_NAME": "my_view",
            "TABLE_SCHEMA": "[space, my_view]",
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
        api.execute_query_iter = Mock(return_value=iter([record]))

        tables = list(
            api._get_all_tables_global_chunked(
                DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
                "",
                "",
                view_definitions={"space.my_view": "SELECT * FROM t"},
            )
        )

        assert len(tables) == 1
        assert tables[0]["VIEW_DEFINITION"] == "SELECT * FROM t"

    def test_table_without_definition_is_none(self, monkeypatch):
        api = _make_api(monkeypatch)
        api.edition = DremioEdition.ENTERPRISE

        record = {
            "COLUMN_NAME": "col1",
            "FULL_TABLE_PATH": "space.plain_table",
            "TABLE_NAME": "plain_table",
            "TABLE_SCHEMA": "[space, plain_table]",
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
        api.execute_query_iter = Mock(return_value=iter([record]))

        tables = list(
            api._get_all_tables_global_chunked(
                DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
                "",
                "",
                view_definitions={"space.some_view": "SELECT 1"},
            )
        )

        assert len(tables) == 1
        assert tables[0]["VIEW_DEFINITION"] is None

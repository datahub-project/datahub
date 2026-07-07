from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries


class TestDremioPartitioning:
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
            partition_datasets_by_container=True,
        )
        report = Mock(spec=DremioSourceReport)
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        api.edition = DremioEdition.ENTERPRISE
        api.allow_schema_pattern = [".*"]
        api.deny_schema_pattern = []
        return api

    def test_flag_routes_to_partitioned_fetch(self, dremio_api):
        dremio_api._get_view_definitions = Mock(return_value={})
        dremio_api._get_all_tables_partitioned_by_container = Mock(
            return_value=iter([{"TABLE_NAME": "t1"}])
        )
        dremio_api._get_all_tables_global_chunked = Mock(return_value=iter([]))

        tables = list(dremio_api.get_all_tables_and_columns())

        dremio_api._get_all_tables_partitioned_by_container.assert_called_once()
        dremio_api._get_all_tables_global_chunked.assert_not_called()
        assert [t["TABLE_NAME"] for t in tables] == ["t1"]

    def test_one_scoped_query_per_container(self, dremio_api):
        dremio_api._get_root_container_names = Mock(return_value=["src_a", "src_b"])
        dremio_api._get_all_tables_global_chunked = Mock(
            side_effect=[
                iter([{"TABLE_NAME": "a1"}]),
                iter([{"TABLE_NAME": "b1"}]),
            ]
        )

        tables = list(
            dremio_api._get_all_tables_partitioned_by_container(
                "SELECT 1 {schema_pattern} {deny_schema_pattern} "
                "{columns_schema_filter} {limit_clause}",
                "UPPER(TABLE_SCHEMA)",
                "TABLE_SCHEMA",
                "",
                "",
                view_definitions={},
            )
        )

        assert [t["TABLE_NAME"] for t in tables] == ["a1", "b1"]
        assert dremio_api._get_all_tables_global_chunked.call_count == 2

        calls = dremio_api._get_all_tables_global_chunked.call_args_list

        # Outer condition (bounds the system-table side + sort) scopes per container.
        outer = [call.args[1] for call in calls]
        assert "REGEXP_LIKE" in outer[0]
        assert "SRC_A" in outer[0]
        assert "SRC_B" in outer[1] and "SRC_B" not in outer[0]

        # Pushable COLUMNS-scan filter is passed per container and scoped correctly.
        pushable = [call.kwargs["columns_schema_filter"] for call in calls]
        assert "UPPER(TABLE_SCHEMA) = 'SRC_A'" in pushable[0]
        assert "UPPER(TABLE_SCHEMA) = 'SRC_B'" in pushable[1]
        assert "SRC_A" not in pushable[1]

    def test_falls_back_to_global_when_no_containers(self, dremio_api):
        dremio_api._get_root_container_names = Mock(return_value=[])
        dremio_api._get_all_tables_global_chunked = Mock(
            return_value=iter([{"TABLE_NAME": "t1"}])
        )

        tables = list(
            dremio_api._get_all_tables_partitioned_by_container(
                "SELECT 1 {schema_pattern} {deny_schema_pattern} "
                "{columns_schema_filter} {limit_clause}",
                "UPPER(TABLE_SCHEMA)",
                "TABLE_SCHEMA",
                "AND REGEXP_LIKE(x, 'Y')",
                "",
                view_definitions={},
            )
        )

        assert [t["TABLE_NAME"] for t in tables] == ["t1"]
        dremio_api._get_all_tables_global_chunked.assert_called_once()
        # Fallback keeps the original user schema condition, unscoped by container.
        assert (
            dremio_api._get_all_tables_global_chunked.call_args.args[1]
            == "AND REGEXP_LIKE(x, 'Y')"
        )

    def test_outer_condition_matches_root_and_nested_paths(self):
        condition = DremioSQLQueries.container_schema_condition(
            "my.source", "UPPER(TABLE_SCHEMA)"
        )
        # Dot in the name is regex-escaped; nested paths are matched via (\..*)?.
        assert "REGEXP_LIKE(UPPER(TABLE_SCHEMA)" in condition
        assert "MY\\.SOURCE" in condition
        assert "(\\..*)?" in condition

    def test_columns_filter_is_pushdown_shaped(self):
        # Dremio only pushes plain =/LIKE on a bare (UPPER-wrapped) TABLE_SCHEMA
        # into the info-schema scan; REGEXP_LIKE / CONCAT wrapping is not pushed.
        f = DremioSQLQueries.container_columns_filter("My_Source", "TABLE_SCHEMA")
        assert "REGEXP_LIKE" not in f
        assert "UPPER(TABLE_SCHEMA) = 'MY_SOURCE'" in f
        # LIKE captures nested schemas; the '_' wildcard in the name is escaped.
        assert "LIKE 'MY\\_SOURCE.%'" in f
        assert "ESCAPE '\\'" in f

    def test_columns_filter_qualifies_community_column(self):
        # Community joins COLUMNS directly, so the column must be qualified (C.).
        f = DremioSQLQueries.container_columns_filter("src", "C.TABLE_SCHEMA")
        assert "UPPER(C.TABLE_SCHEMA) = 'SRC'" in f

    def test_root_container_names_filters_and_maps(self, dremio_api):
        dremio_api.get = Mock(
            return_value={
                "data": [
                    {"containerType": "SOURCE", "path": ["src_a"]},
                    {"containerType": "SPACE", "path": ["space_b"]},
                    {"containerType": "HOME", "name": "@me"},
                    {"containerType": "FOLDER", "path": ["not_a_root"]},
                ]
            }
        )
        dremio_api.filter.should_include_container = Mock(
            side_effect=lambda path, name: name != "space_b"
        )

        names = dremio_api._get_root_container_names()

        # FOLDER is not a root; space_b is filtered out.
        assert names == ["src_a", "@me"]

    def test_get_root_container_names_empty_catalog(self, dremio_api):
        dremio_api.get = Mock(return_value={})
        assert dremio_api._get_root_container_names() == []

    @pytest.mark.parametrize(
        "template",
        [
            DremioSQLQueries.QUERY_DATASETS_CE_GLOBAL,
            DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL,
            DremioSQLQueries.QUERY_DATASETS_CLOUD_GLOBAL,
        ],
    )
    def test_columns_filter_lands_inside_columns_scan(self, template):
        # The predicate only pushes into the scan if it sits directly on the
        # INFORMATION_SCHEMA.COLUMNS read — not the outer post-join WHERE. Guard
        # its placement, since a wrong spot silently disables pushdown.
        marker = "AND UPPER(TABLE_SCHEMA) = 'MARKER'"
        query = template.format(
            schema_pattern="",
            deny_schema_pattern="",
            columns_schema_filter=marker,
            limit_clause="",
        )
        columns_idx = query.index("INFORMATION_SCHEMA.COLUMNS")
        marker_idx = query.index(marker)
        # Filter sits on the COLUMNS read (before the outer sort) with no JOIN
        # between them — i.e. directly on the scan, where pushdown can fire.
        assert columns_idx < marker_idx < query.index("ORDER BY")
        assert "JOIN" not in query[columns_idx:marker_idx]

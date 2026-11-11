import re
from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.utilities.file_backed_collections import FileBackedDict


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
        )
        report = Mock(spec=DremioSourceReport)
        api = DremioAPIOperations(config, report)
        api.session = mock_session
        api.edition = DremioEdition.ENTERPRISE
        api.allow_schema_pattern = [".*"]
        api.deny_schema_pattern = []
        return api

    def test_global_success_does_not_fall_back(self, dremio_api):
        dremio_api._get_view_definitions = Mock(return_value=FileBackedDict())
        dremio_api._get_all_tables_global_chunked = Mock(
            return_value=iter([{"TABLE_NAME": "t1"}])
        )
        dremio_api._get_all_tables_partitioned_by_container = Mock(
            return_value=iter([])
        )

        tables = list(dremio_api.get_all_tables_and_columns())

        assert [t["TABLE_NAME"] for t in tables] == ["t1"]
        dremio_api._get_all_tables_partitioned_by_container.assert_not_called()

    def test_first_chunk_failure_falls_back_to_partitioned(self, dremio_api):
        # Mocked at the query level so the real fallback path runs.
        dremio_api.report = DremioSourceReport()
        dremio_api._get_view_definitions = Mock(return_value=FileBackedDict())
        dremio_api.get = Mock(
            return_value={"data": [{"containerType": "SOURCE", "path": ["src_a"]}]}
        )
        row = {
            "FULL_TABLE_PATH": "src_a.fallback",
            "TABLE_NAME": "fallback",
            "TABLE_SCHEMA": "src_a",
            "COLUMN_NAME": "id",
            "IS_NULLABLE": "YES",
            "DATA_TYPE": "INTEGER",
            "COLUMN_SIZE": 4,
        }
        # Catalog-wide query fails, the per-container retry succeeds.
        dremio_api.execute_query_iter = Mock(
            side_effect=[DremioAPIException("timeout"), iter([row])]
        )

        tables = list(dremio_api.get_all_tables_and_columns())

        assert [t["TABLE_NAME"] for t in tables] == ["fallback"]
        # A successful fallback is a complete fetch.
        assert not dremio_api.report.dataset_fetch_incomplete
        assert not dremio_api.report.failures

    def test_dataset_without_column_rows_is_recorded(self, dremio_api):
        # LEFT JOIN on COLUMNS: a dataset whose columns can't be read (e.g.
        # its source is offline) comes back as one row with no COLUMN_NAME.
        dremio_api.report = DremioSourceReport()
        dremio_api.execute_query_iter = Mock(
            return_value=iter(
                [{"FULL_TABLE_PATH": "Src_A.Offline", "TABLE_NAME": "Offline"}]
            )
        )

        tables = list(
            dremio_api._get_all_tables_global_chunked(
                "SELECT 1 {schema_pattern} {deny_schema_pattern} "
                "{columns_schema_filter} {limit_clause}",
                "",
                "",
                view_definitions={},
            )
        )

        assert tables == []
        assert dremio_api.datasets_without_columns == {"src_a.offline"}
        assert list(dremio_api.report.datasets_without_columns) == ["src_a.offline"]

    def test_dataset_without_column_rows_is_recorded_community(self, dremio_api):
        dremio_api.report = DremioSourceReport()

        tables = dremio_api.community_get_formatted_tables(
            [{"FULL_TABLE_PATH": "Src_A.Offline", "TABLE_NAME": "Offline"}]
        )

        assert tables == []
        assert dremio_api.datasets_without_columns == {"src_a.offline"}

    def test_column_less_table_not_emitted_beside_one_with_columns_community(
        self, dremio_api
    ):
        dremio_api.report = DremioSourceReport()
        dremio_api.get_dataset_id = Mock(return_value="id")
        row = {"TABLE_SCHEMA": "src_a", "IS_NULLABLE": "YES", "COLUMN_SIZE": 1}

        tables = dremio_api.community_get_formatted_tables(
            [
                {
                    **row,
                    "FULL_TABLE_PATH": "src_a.ok",
                    "TABLE_NAME": "ok",
                    "COLUMN_NAME": "col_a",
                    "DATA_TYPE": "INTEGER",
                },
                {**row, "FULL_TABLE_PATH": "src_a.offline", "TABLE_NAME": "offline"},
            ]
        )

        assert [t["TABLE_NAME"] for t in tables] == ["ok"]
        assert dremio_api.datasets_without_columns == {"src_a.offline"}

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
        assert "src_a" in outer[0]
        assert "src_b" in outer[1] and "src_b" not in outer[0]

        # Pushable COLUMNS-scan filter is passed per container and scoped correctly.
        pushable = [call.kwargs["columns_schema_filter"] for call in calls]
        assert "UPPER(TABLE_SCHEMA) = 'SRC_A'" in pushable[0]
        assert "UPPER(TABLE_SCHEMA) = 'SRC_B'" in pushable[1]
        assert "SRC_A" not in pushable[1]

    def test_no_containers_reports_failure(self, dremio_api):
        dremio_api._get_root_container_names = Mock(return_value=[])
        dremio_api._get_all_tables_global_chunked = Mock()

        tables = list(
            dremio_api._get_all_tables_partitioned_by_container(
                "SELECT 1 {schema_pattern} {deny_schema_pattern} "
                "{columns_schema_filter} {limit_clause}",
                "UPPER(TABLE_SCHEMA)",
                "",
                "",
                view_definitions={},
            )
        )

        assert tables == []
        dremio_api._get_all_tables_global_chunked.assert_not_called()
        dremio_api.report.failure.assert_called_once()
        # `is True`: report is a Mock, so an unset attribute would be truthy.
        assert dremio_api.report.dataset_fetch_incomplete is True

    @pytest.mark.parametrize(
        "schema, matches",
        [
            ("MY.SOURCE", True),
            ("MY.SOURCE.X", True),
            # The dot in the name is literal, and a sibling sharing the prefix
            # is a different container.
            ("MYXSOURCE", False),
            ("MY.SOURCEX", False),
            ("MY.SOURCE_OLD.X", False),
        ],
    )
    def test_outer_condition_matches_root_and_nested_paths_only(
        self, schema: str, matches: bool
    ) -> None:
        condition = DremioSQLQueries.container_schema_condition(
            "my.source", "UPPER(TABLE_SCHEMA)"
        )
        match = re.fullmatch(
            r"AND REGEXP_LIKE\(UPPER\(TABLE_SCHEMA\), '(.*)'\)", condition
        )
        assert match
        # Dremio's REGEXP_LIKE is Java's Matcher.find().
        assert (re.search(match.group(1), schema) is not None) is matches

    def test_columns_filter_is_pushdown_shaped(self):
        # Dremio only pushes plain =/LIKE on a bare (UPPER-wrapped) TABLE_SCHEMA
        # into the info-schema scan; REGEXP_LIKE / CONCAT wrapping is not pushed.
        f = DremioSQLQueries.container_columns_filter("My_Source", "TABLE_SCHEMA")
        assert "REGEXP_LIKE" not in f
        assert "UPPER(TABLE_SCHEMA) = 'MY_SOURCE'" in f
        # LIKE captures nested schemas; the '_' wildcard in the name is escaped.
        assert "LIKE 'MY\\_SOURCE.%'" in f
        assert "ESCAPE '\\'" in f

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

        names = dremio_api._get_root_container_names()

        # FOLDER is not a root.
        assert names == ["src_a", "space_b", "@me"]

    def test_root_container_names_drop_system_roots(self, dremio_api):
        dremio_api.filter.config.include_system_tables = False
        dremio_api.get = Mock(
            return_value={
                "data": [
                    {"containerType": "SOURCE", "path": ["src_a"]},
                    {"containerType": "SOURCE", "path": ["SYS"]},
                    {"containerType": "SOURCE", "path": ["INFORMATION_SCHEMA"]},
                ]
            }
        )

        assert dremio_api._get_root_container_names() == ["src_a"]

    @pytest.mark.parametrize(
        "allow, expected",
        [
            (["^space\\.finance.*"], ["Space"]),
            (["^SPACE\\.FINANCE.*"], ["Space"]),
            (["^finance.*"], ["finance_src"]),
            # The s is optional, so `space` itself can match.
            (["^spaces?\\..*"], ["Space"]),
            # No literal prefix to rule a root out with: keep every root.
            (["^(space|other)\\..*"], ["Space", "finance_src", "@alice"]),
            (["^finance.*|space\\..*"], ["Space", "finance_src", "@alice"]),
        ],
    )
    def test_root_container_names_skip_roots_schema_pattern_cannot_match(
        self, dremio_api, allow, expected
    ):
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(allow=allow)
        dremio_api.get = Mock(
            return_value={
                "data": [
                    {"containerType": "SPACE", "path": ["Space"]},
                    {"containerType": "SOURCE", "path": ["finance_src"]},
                    {"containerType": "HOME", "name": "@alice"},
                ]
            }
        )

        assert dremio_api._get_root_container_names() == expected

    def test_root_container_names_ignore_schema_pattern(self, dremio_api):
        # Each per-container query applies schema_pattern itself; filtering
        # roots by it here skipped `space` for ^space\.finance.* and the
        # fallback then "succeeded" without that container.
        dremio_api.filter.config.schema_pattern = AllowDenyPattern(
            allow=["^space\\.finance.*"]
        )
        dremio_api.get = Mock(
            return_value={"data": [{"containerType": "SPACE", "path": ["space"]}]}
        )

        assert dremio_api._get_root_container_names() == ["space"]

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
        # An INNER JOIN on COLUMNS would drop datasets whose columns can't be
        # read. Comment lines are dropped first: the CE template mentions
        # LEFT JOIN in one.
        sql = "\n".join(
            line for line in query.splitlines() if not line.strip().startswith("--")
        )
        before_columns = sql[: sql.index("INFORMATION_SCHEMA.COLUMNS")]
        assert before_columns.rsplit("JOIN", 1)[0].split()[-1] == "LEFT"

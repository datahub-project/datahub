"""schema_pattern on Dremio Enterprise and Cloud is matched against the schema path.

The connector's real queries run against SQLite with the SYS and INFORMATION_SCHEMA
tables attached. Dremio's REGEXP_LIKE is Java's Matcher.find(), so re.search here.
"""

import re
import sqlite3
from typing import Any, Dict, Iterator, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport

_SOURCE_TABLES = [
    ["My_Source", "Sales", "Orders"],
    # Dremio file datasets often have a dot in their name.
    ["My_Source", "Sales", "weather.csv"],
    # A sibling source whose name starts with My_Source.
    ["My_SourceB", "Sales", "other"],
]
_SPACE_VIEW = ["Analytics", "orders_vw"]


def _sqlite_dremio_ee() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.create_function(
        "REGEXP_LIKE", 2, lambda value, pattern: re.search(pattern, value) is not None
    )
    # Built into SQLite only from 3.44.
    conn.create_function("CONCAT", -1, lambda *parts: "".join(parts))
    conn.execute("ATTACH ':memory:' AS SYS")
    conn.execute("ATTACH ':memory:' AS INFORMATION_SCHEMA")
    conn.executescript(
        """
        CREATE TABLE SYS.VIEWS (VIEW_ID, VIEW_NAME, PATH, SCHEMA_ID, SPACE_ID,
            OWNER_ID, CREATED, TYPE, SQL_DEFINITION);
        CREATE TABLE SYS."TABLES" (TABLE_ID, TABLE_NAME, PATH, SCHEMA_ID, SOURCE_ID,
            OWNER_ID, FORMAT_TYPE, CREATED, TYPE);
        CREATE TABLE SYS.USERS (USER_ID, USER_NAME);
        CREATE TABLE SYS.ROLES (ROLE_ID, ROLE_NAME);
        CREATE TABLE INFORMATION_SCHEMA.COLUMNS (TABLE_SCHEMA, TABLE_NAME,
            COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COLUMN_SIZE);
        """
    )
    for i, path in enumerate(_SOURCE_TABLES):
        conn.execute(
            """INSERT INTO SYS."TABLES" VALUES (?, ?, ?, 's1', 'src1', NULL,
            'PARQUET', NULL, 'TABLE')""",
            (f"t{i}", path[-1], f"[{', '.join(path)}]"),
        )
    conn.execute(
        """INSERT INTO SYS.VIEWS VALUES ('v1', ?, ?, '', 'sp1', NULL, NULL, 'VIEW',
        'SELECT * FROM My_Source.Sales.Orders')""",
        (_SPACE_VIEW[-1], f"[{', '.join(_SPACE_VIEW)}]"),
    )
    for *schema, name in [*_SOURCE_TABLES, _SPACE_VIEW]:
        conn.execute(
            """INSERT INTO INFORMATION_SCHEMA.COLUMNS
            VALUES (?, ?, 'id', 1, 'YES', 'INTEGER', 32)""",
            (".".join(schema), name),
        )
    return conn


@pytest.fixture(params=[DremioEdition.ENTERPRISE, DremioEdition.CLOUD], ids=str)
def sys_api(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> DremioAPIOperations:
    edition: DremioEdition = request.param
    monkeypatch.setattr("requests.Session", Mock(return_value=Mock()))
    monkeypatch.setattr(DremioAPIOperations, "get_dremio_edition", lambda self: edition)
    config = DremioSourceConfig(
        hostname="dummy-host",
        port=9047,
        tls=False,
        authentication_method="PAT",
    )
    api = DremioAPIOperations(config, DremioSourceReport())
    conn = _sqlite_dremio_ee()

    def execute_query_iter(query: str, timeout: int = 3600) -> Iterator[Dict[str, Any]]:
        # SQLite can't attach Cloud's three-part SYS.PROJECT/SYS.ORGANIZATION names.
        query = query.replace("SYS.PROJECT.", "SYS.").replace(
            "SYS.ORGANIZATION.", "SYS."
        )
        cursor = conn.execute(query)
        names = [d[0] for d in cursor.description]
        for row in cursor:
            yield dict(zip(names, row, strict=True))

    monkeypatch.setattr(api, "execute_query_iter", execute_query_iter)
    return api


def _fetched(api: DremioAPIOperations, allow: List[str], deny: List[str]) -> List[str]:
    api.allow_schema_pattern = allow
    api.deny_schema_pattern = deny
    return sorted(d["TABLE_NAME"] for d in api.get_all_tables_and_columns())


_IN_SALES = ["Orders", "weather.csv"]


@pytest.mark.parametrize(
    "allow, deny, expected",
    [
        # Anchored at the schema: the dataset name must not be part of the match.
        ([r"^My_Source(\.Sales)?$", r"^Analytics$"], [], [*_IN_SALES, "orders_vw"]),
        ([r"^My_Source($|\..*)"], [], _IN_SALES),
        ([r"^Other($|\..*)"], [], []),
        ([".*"], [r"^My_Source\.Sales$"], ["orders_vw", "other"]),
        # Allow and deny together: the deny carves the sibling out of the allow.
        ([r"^My_Source"], [r"^My_SourceB"], _IN_SALES),
        # A pattern that names the dataset is not a schema match.
        ([r"^Analytics\.orders_vw$"], [], []),
        # Breaking changes documented for this fix: "\..*" no longer reaches
        # datasets directly in the schema, and a dataset-name deny filters nothing.
        ([r"^My_Source\.Sales\..*"], [], []),
        ([".*"], [r".*_vw$"], [*_IN_SALES, "orders_vw", "other"]),
    ],
)
def test_schema_pattern_matches_schema_path(
    sys_api: DremioAPIOperations,
    allow: List[str],
    deny: List[str],
    expected: List[str],
) -> None:
    assert _fetched(sys_api, allow, deny) == sorted(expected)


def test_view_definition_survives_anchored_schema_pattern(
    sys_api: DremioAPIOperations,
) -> None:
    sys_api.allow_schema_pattern = [r"^Analytics$"]
    sys_api.deny_schema_pattern = []
    (view,) = sys_api.get_all_tables_and_columns()

    assert view["VIEW_DEFINITION"] == "SELECT * FROM My_Source.Sales.Orders"


def test_per_container_fallback_matches_schema_path(
    sys_api: DremioAPIOperations, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_query = sys_api.execute_query_iter

    def fail_catalog_wide(query: str, timeout: int = 3600) -> Iterator[Dict[str, Any]]:
        # Only the per-container dataset queries carry a TABLE_SCHEMA scan filter.
        if "TABLE_ID" in query and "UPPER(TABLE_SCHEMA) =" not in query:
            raise DremioAPIException("Query exceeded memory limit")
        return run_query(query, timeout)

    monkeypatch.setattr(sys_api, "execute_query_iter", fail_catalog_wide)
    monkeypatch.setattr(
        sys_api,
        "get",
        lambda url: {
            "data": [
                {"containerType": "SOURCE", "path": ["My_Source"]},
                {"containerType": "SOURCE", "path": ["My_SourceB"]},
                {"containerType": "SPACE", "path": ["Analytics"]},
            ]
        },
    )

    assert _fetched(sys_api, [r"^My_Source(\.Sales)?$"], []) == _IN_SALES
    # My_Source's query must not pick up My_SourceB's tables: its column scan
    # is exact, so they would come back without columns.
    assert _fetched(sys_api, [".*"], []) == sorted([*_IN_SALES, "orders_vw", "other"])
    assert not sys_api.datasets_without_columns

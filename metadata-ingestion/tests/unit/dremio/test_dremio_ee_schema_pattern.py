"""schema_pattern on Dremio Enterprise is matched against the schema path.

Enterprise reads datasets from SYS.VIEWS / SYS."TABLES", whose PATH includes the
dataset name. The pushed schema_pattern predicate must still see only the schema
path, as on Community Edition and in the Python-side container filter.

The connector's real Enterprise queries run against SQLite, with the SYS and
INFORMATION_SCHEMA tables they read attached. REGEXP_LIKE is Java's
Matcher.find() in Dremio, so it is re.search here.
"""

import re
import sqlite3
from typing import Any, Dict, Iterator, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport

_SOURCE_TABLE = ["My_Source", "Sales", "Orders"]
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
    *table_schema, table_name = _SOURCE_TABLE
    conn.execute(
        """INSERT INTO SYS."TABLES" VALUES ('t1', ?, ?, 's1', 'src1', NULL, 'PARQUET',
        NULL, 'TABLE')""",
        (table_name, f"[{', '.join(_SOURCE_TABLE)}]"),
    )
    *view_schema, view_name = _SPACE_VIEW
    conn.execute(
        """INSERT INTO SYS.VIEWS VALUES ('v1', ?, ?, '', 'sp1', NULL, NULL, 'VIEW',
        'SELECT * FROM My_Source.Sales.Orders')""",
        (view_name, f"[{', '.join(_SPACE_VIEW)}]"),
    )
    for schema, name in ((table_schema, table_name), (view_schema, view_name)):
        conn.execute(
            """INSERT INTO INFORMATION_SCHEMA.COLUMNS
            VALUES (?, ?, 'id', 1, 'YES', 'INTEGER', 32)""",
            (".".join(schema), name),
        )
    return conn


@pytest.fixture
def ee_api(monkeypatch: pytest.MonkeyPatch) -> DremioAPIOperations:
    monkeypatch.setattr("requests.Session", Mock(return_value=Mock()))
    monkeypatch.setattr(
        DremioAPIOperations,
        "get_dremio_edition",
        lambda self: DremioEdition.ENTERPRISE,
    )
    config = DremioSourceConfig(
        hostname="dummy-host",
        port=9047,
        tls=False,
        authentication_method="PAT",
    )
    api = DremioAPIOperations(config, DremioSourceReport())
    conn = _sqlite_dremio_ee()

    def execute_query_iter(query: str, timeout: int = 3600) -> Iterator[Dict[str, Any]]:
        cursor = conn.execute(query)
        names = [d[0] for d in cursor.description]
        for row in cursor:
            yield dict(zip(names, row, strict=True))

    monkeypatch.setattr(api, "execute_query_iter", execute_query_iter)
    return api


def _fetched(
    api: DremioAPIOperations, allow: List[str], deny: List[str]
) -> Dict[str, Any]:
    api.allow_schema_pattern = allow
    api.deny_schema_pattern = deny
    return {d["TABLE_NAME"]: d for d in api.get_all_tables_and_columns()}


@pytest.mark.parametrize(
    "allow, deny, expected",
    [
        # Anchored at the schema: the dataset name must not be part of the match.
        (
            [r"^My_Source(\.Sales)?$", r"^Analytics$"],
            [],
            {"Orders", "orders_vw"},
        ),
        ([r"^My_Source($|\..*)"], [], {"Orders"}),
        ([r"^Other($|\..*)"], [], set()),
        ([".*"], [r"^My_Source\.Sales$"], {"orders_vw"}),
        # A pattern that names the dataset is not a schema match.
        ([r"^Analytics\.orders_vw$"], [], set()),
    ],
)
def test_schema_pattern_matches_schema_path_on_enterprise(
    ee_api: DremioAPIOperations,
    allow: List[str],
    deny: List[str],
    expected: set,
) -> None:
    assert set(_fetched(ee_api, allow, deny)) == expected


def test_view_definition_survives_anchored_schema_pattern(
    ee_api: DremioAPIOperations,
) -> None:
    datasets = _fetched(ee_api, [r"^Analytics$"], [])

    assert datasets["orders_vw"]["VIEW_DEFINITION"] == (
        "SELECT * FROM My_Source.Sales.Orders"
    )

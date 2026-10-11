import importlib
import subprocess
import sys
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.sql.base import Executable

from datahub.ingestion.source.sql import _pyhive_compat
from datahub.ingestion.source.sql._pyhive_compat import (
    DatabricksDialectBase,
    HiveDialect,
    PrestoDialect,
    SparkSqlDialect,
    presto,
)
from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyClient


class StrictConnection:
    """Mimics an SA 2.0 connection: execute() rejects raw strings, and results are
    real SA 2.0 rows (tuple-like, name lookup only via ``row._mapping``)."""

    def __init__(self, sqlite_conn: Connection, responses: Dict[str, str]) -> None:
        self._sqlite_conn = sqlite_conn
        # Maps the SQL the dialect is expected to send to a SQLite SELECT producing
        # the rows the real engine would return.
        self._responses = responses
        self.executed: List[str] = []

    def execute(self, statement: Any, *args: Any, **kw: Any) -> CursorResult:
        if not isinstance(statement, Executable):
            raise exc.ObjectNotExecutableError(statement)
        sql = str(statement)
        self.executed.append(sql)
        return self._sqlite_conn.execute(text(self._responses[sql]))


@pytest.fixture
def sqlite_conn() -> Iterator[Connection]:
    with create_engine("sqlite://").connect() as conn:
        yield conn


def test_hive_schema_and_table_names(sqlite_conn: Connection) -> None:
    conn = StrictConnection(
        sqlite_conn,
        {
            "SHOW SCHEMAS": "SELECT 'db_a' UNION ALL SELECT 'db_b'",
            "SHOW TABLES IN `db_a`": "SELECT 't1' UNION ALL SELECT 't2'",
        },
    )
    dialect = HiveDialect()

    assert dialect.get_schema_names(conn) == ["db_a", "db_b"]
    assert dialect.get_table_names(conn, schema="db_a") == ["t1", "t2"]


def test_hive_get_columns_and_has_table(sqlite_conn: Connection) -> None:
    describe_rows = (
        "SELECT 'col_a' AS col_name, 'int' AS data_type, 'first' AS comment "
        "UNION ALL SELECT 'col_b', 'map<string,int>', NULL "
        "UNION ALL SELECT '# Partition Information', NULL, NULL "
        "UNION ALL SELECT 'col_p', 'string', NULL"
    )
    conn = StrictConnection(
        sqlite_conn,
        {
            "DESCRIBE `db`.`events`": describe_rows,
            "DESCRIBE `db`.`missing`": (
                "SELECT 'Table db.missing does not exist' AS col_name, "
                "NULL AS data_type, NULL AS comment"
            ),
        },
    )
    dialect = HiveDialect()

    columns = dialect.get_columns(conn, "events", schema="db")
    assert [(c["name"], c["full_type"]) for c in columns] == [
        ("col_a", "int"),
        ("col_b", "map<string,int>"),
    ]
    # SA 2.0's Inspector.has_table always passes info_cache.
    assert dialect.has_table(conn, "events", schema="db", info_cache={}) is True
    assert dialect.has_table(conn, "missing", schema="db", info_cache={}) is False


def test_presto_get_schema_names(sqlite_conn: Connection) -> None:
    conn = StrictConnection(sqlite_conn, {"SHOW SCHEMAS": "SELECT 's1' AS \"Schema\""})
    assert PrestoDialect().get_schema_names(conn) == ["s1"]


def test_presto_get_table_names(sqlite_conn: Connection) -> None:
    # Called directly: once presto.py is imported (as other tests do), the Presto
    # source's Trino-based override replaces this on the dialect.
    conn = StrictConnection(
        sqlite_conn,
        {'SHOW TABLES FROM "s1"': "SELECT 't1' AS \"Table\" UNION ALL SELECT 't2'"},
    )
    assert _pyhive_compat._presto_get_table_names(
        PrestoDialect(),
        conn,  # type: ignore[arg-type]
        schema="s1",
    ) == ["t1", "t2"]


@pytest.mark.parametrize(
    "columns_select",
    [
        # Partition info in the "Comment" column.
        "SELECT 'id' AS \"Column\", 'bigint' AS \"Type\", '' AS \"Comment\" "
        "UNION ALL SELECT 'ds', 'varchar', 'Partition Key'",
        # Partition info in the "Extra" column.
        "SELECT 'id' AS \"Column\", 'bigint' AS \"Type\", '' AS \"Extra\", "
        "'' AS \"Comment\" "
        "UNION ALL SELECT 'ds', 'varchar', 'partition key', ''",
        # Partition info in a boolean "Partition Key" column.
        'SELECT \'id\' AS "Column", \'bigint\' AS "Type", 0 AS "Partition Key", '
        "'' AS \"Comment\" "
        "UNION ALL SELECT 'ds', 'varchar', 1, ''",
    ],
)
def test_presto_get_indexes(sqlite_conn: Connection, columns_select: str) -> None:
    conn = StrictConnection(
        sqlite_conn, {'SHOW COLUMNS FROM "sch"."events"': columns_select}
    )

    assert PrestoDialect().get_indexes(conn, "events", schema="sch") == [
        {"name": "partition", "column_names": ["ds"], "unique": False}
    ]


def test_presto_missing_table_raises_no_such_table() -> None:
    # SA 2.0 wraps presto's error (raised while reading the cursor description) in
    # a DBAPIError; the table-not-found message must still be recognised.
    presto_error = presto.DatabaseError(
        {"message": "line 1:1: Table 'hive.sch.missing' does not exist"}
    )
    conn = MagicMock()
    conn.execute.side_effect = exc.DBAPIError.instance(
        "SHOW COLUMNS", None, presto_error, presto.Error
    )
    dialect = PrestoDialect()

    with pytest.raises(exc.NoSuchTableError):
        dialect._get_table_columns(conn, "missing", "sch")
    assert dialect.has_table(conn, "missing", schema="sch", info_cache={}) is False


def test_sparksql_table_names_and_columns(sqlite_conn: Connection) -> None:
    conn = StrictConnection(
        sqlite_conn,
        {
            "SHOW TABLES IN `db`": (
                "SELECT 'db', 't1', 0 UNION ALL SELECT 'db', 'tmp_view', 1"
            ),
            "SET spark.sql.debug.maxToStringFields=1000000": "SELECT 1",
            "DESCRIBE db.t1": "SELECT 'col_a' AS col_name, 'int', NULL",
        },
    )
    dialect = SparkSqlDialect()

    assert dialect.get_table_names(conn, schema="db") == ["t1"]
    assert [c["name"] for c in dialect.get_columns(conn, "t1", schema="db")] == [
        "col_a"
    ]


def test_databricks_get_table_names(sqlite_conn: Connection) -> None:
    from databricks_dbapi.sqlalchemy_dialects.hive import DatabricksPyhiveDialect

    assert DatabricksDialectBase is not None
    conn = StrictConnection(
        sqlite_conn,
        {"SHOW TABLES IN `db`": "SELECT 'db', 't1', 0 UNION ALL SELECT 'db', 't2', 0"},
    )

    assert DatabricksPyhiveDialect().get_table_names(conn, schema="db") == [
        "t1",
        "t2",
    ]


def test_reimport_does_not_repatch() -> None:
    before = HiveDialect.__dict__["get_schema_names"]

    importlib.reload(_pyhive_compat)

    assert HiveDialect.__dict__["get_schema_names"] is before


def test_sql_generic_can_create_pyhive_engines() -> None:
    # Run in a fresh interpreter so no other module has applied the shim first;
    # SQLAlchemy loads pyhive via entry points inside create_engine().
    script = (
        "import datahub.ingestion.source.sql.sql_generic\n"
        "import sqlalchemy\n"
        "for url in ['hive://localhost:10000/default', "
        "'presto://localhost:8080/hive/default']:\n"
        "    sqlalchemy.create_engine(url)\n"
        "from datahub.ingestion.source.sql import _pyhive_compat\n"
        "from pyhive.sqlalchemy_presto import PrestoDialect\n"
        "assert PrestoDialect.get_table_names is _pyhive_compat._presto_get_table_names\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_execute_query_closes_result_when_consumer_raises() -> None:
    client = SQLAlchemyClient(MagicMock())
    result = MagicMock()
    result.__iter__.return_value = iter([MagicMock(_mapping={"a": 1})] * 2)
    client._connection = MagicMock()
    client._connection.execute.return_value = result

    with pytest.raises(RuntimeError):
        for _row in client.execute_query("SELECT 1"):
            raise RuntimeError("consumer failed")

    result.close.assert_called_once()

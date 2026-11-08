import subprocess
import sys
from typing import Any, Dict, Iterator, List, cast
from unittest.mock import MagicMock, patch

import impala.dbapi
import impala.sasl_compat
import pytest
from sqlalchemy import create_engine, exc, text, types as sqlalchemy_types
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.sql.base import Executable

from datahub.ingestion.source.sql.hive.hive_dialect import (
    HiveDBAPIConnection,
    HiveDialect,
    HiveReflectedColumn,
    SparkSqlDialect,
    _SessionCursor,
    build_hive_connect_args,
)
from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyClient
from datahub.ingestion.source.sql.hive.sasl_client import PureSASLClient


class StrictConnection:
    """Mimics an SA 2.0 connection: execute() rejects raw strings, and results are
    real SA 2.0 rows (tuple-like, name lookup only via ``row._mapping``)."""

    def __init__(self, sqlite_conn: Connection, responses: Dict[str, str]) -> None:
        self._sqlite_conn = sqlite_conn
        # Maps the SQL the dialect is expected to send to a SQLite SELECT producing
        # the rows HiveServer2 would return.
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


def test_hive_schema_table_and_view_names(sqlite_conn: Connection) -> None:
    conn = cast(
        Connection,
        StrictConnection(
            sqlite_conn,
            {
                "SHOW SCHEMAS": "SELECT 'db_a' UNION ALL SELECT 'db_b'",
                "SHOW TABLES IN `db_a`": "SELECT 't1' UNION ALL SELECT 'v1'",
                "SHOW VIEWS IN `db_a`": "SELECT 'v1'",
            },
        ),
    )
    dialect = HiveDialect()

    assert dialect.get_schema_names(conn) == ["db_a", "db_b"]
    assert dialect.get_table_names(conn, schema="db_a") == ["t1", "v1"]
    assert dialect.get_view_names(conn, schema="db_a") == ["v1"]


def test_hive_view_definition_joins_rows(sqlite_conn: Connection) -> None:
    conn = cast(
        Connection,
        StrictConnection(
            sqlite_conn,
            {
                "SHOW CREATE TABLE `db`.`v1`": (
                    "SELECT 'CREATE VIEW `db`.`v1` AS SELECT' "
                    "UNION ALL SELECT 'col_a FROM db.t1'"
                )
            },
        ),
    )

    assert (
        HiveDialect().get_view_definition(conn, "v1", schema="db")
        == "CREATE VIEW `db`.`v1` AS SELECT\ncol_a FROM db.t1"
    )


def test_hive_get_columns_and_partitions() -> None:
    dialect = HiveDialect()
    describe_rows = [
        ("col1", "string", "comment1"),
        ("col2  ", " int ", "comment2"),
        ("", "", ""),
        ("# col_name", "data_type", "comment"),
        ("col3", "map<int,int>", None),
        ("col4", "decimal(10,2)", None),
        ("# Partition Information", None, None),
        ("# col_name", "data_type", "comment"),
        ("ds", "string", None),
    ]

    with patch.object(HiveDialect, "_get_table_columns", return_value=describe_rows):
        columns = cast(
            List[HiveReflectedColumn],
            dialect.get_columns(MagicMock(), "events", schema="db"),
        )
        indexes = dialect.get_indexes(MagicMock(), "events", schema="db")

    assert [(c["name"], c["full_type"], c["comment"]) for c in columns] == [
        ("col1", "string", "comment1"),
        ("col2", "int", "comment2"),
        ("col3", "map<int,int>", None),
        ("col4", "decimal(10,2)", None),
    ]
    assert isinstance(columns[2]["type"], sqlalchemy_types.NullType)
    assert indexes == [{"name": "partition", "column_names": ["ds"], "unique": False}]


def test_hive_get_columns_warns_on_unknown_type() -> None:
    with (
        patch.object(
            HiveDialect,
            "_get_table_columns",
            return_value=[("col1", "unknown_type_xyz", None)],
        ),
        patch("sqlalchemy.util.warn") as mock_warn,
    ):
        columns = HiveDialect().get_columns(MagicMock(), "events", schema="db")

    assert isinstance(columns[0]["type"], sqlalchemy_types.NullType)
    assert "unknown_type_xyz" in str(mock_warn.call_args)


def test_hive_table_comment_parses_detailed_information() -> None:
    describe_formatted = [
        ("col1", "string", None),
        ("", None, None),
        ("# Detailed Table Information", None, None),
        ("Database:           ", "db                  ", None),
        ("Location:           ", "hdfs://nn/warehouse/db.db/events", None),
        ("Table Parameters:", None, None),
        ("", "comment             ", "events table        "),
        ("", "numFiles            ", "3                   "),
    ]

    with patch.object(
        HiveDialect, "_get_table_columns", return_value=describe_formatted
    ):
        comment = HiveDialect().get_table_comment(MagicMock(), "events", schema="db")

    assert comment["text"] == "events table"
    assert comment["properties"]["Location:"] == "hdfs://nn/warehouse/db.db/events"
    assert comment["properties"]["Table Parameters: numFiles"] == "3"


def test_hive_missing_table_raises_no_such_table() -> None:
    hive_error = impala.dbapi.OperationalError(
        "Error while compiling statement: FAILED: SemanticException "
        "[Error 10001]: Table not found db.missing"
    )
    conn = MagicMock()
    conn.execute.side_effect = exc.DBAPIError.instance(
        "DESCRIBE", None, hive_error, impala.dbapi.Error
    )
    dialect = HiveDialect()

    with pytest.raises(exc.NoSuchTableError):
        dialect._get_table_columns(conn, "missing", "db")
    # SA 2.0's Inspector.has_table always passes info_cache.
    assert not dialect.has_table(conn, "missing", schema="db", info_cache={})


def test_sparksql_table_names_and_columns(sqlite_conn: Connection) -> None:
    conn = cast(
        Connection,
        StrictConnection(
            sqlite_conn,
            {
                "SHOW TABLES IN `db`": (
                    "SELECT 'db', 't1', 0 UNION ALL SELECT 'db', 'tmp_view', 1"
                ),
                "SET spark.sql.debug.maxToStringFields=1000000": "SELECT 1",
                "DESCRIBE `db`.`t1`": "SELECT 'col_a' AS col_name, 'int', NULL",
                "SHOW VIEWS IN `db`": (
                    "SELECT 'db', 'v1', 0 UNION ALL SELECT 'db', 'tmp_v', 1"
                ),
            },
        ),
    )
    dialect = SparkSqlDialect()

    assert dialect.get_table_names(conn, schema="db") == ["t1"]
    assert dialect.get_view_names(conn, schema="db") == ["v1"]
    assert [c["name"] for c in dialect.get_columns(conn, "t1", schema="db")] == [
        "col_a"
    ]


def test_sparksql_has_table_true_when_types_are_unreadable() -> None:
    # Spark found the table but could not decode a column type.
    with patch.object(
        SparkSqlDialect,
        "_get_table_columns",
        side_effect=exc.UnreflectableTableError("db.t1"),
    ):
        assert SparkSqlDialect().has_table(MagicMock(), "t1", schema="db")


@pytest.mark.parametrize(
    "params, transport, expected",
    [
        pytest.param(
            {"host": "h", "port": 10000, "username": "u"},
            None,
            {"auth_mechanism": "PLAIN", "user": "u", "port": 10000},
            id="binary-default-is-sasl-plain",
        ),
        pytest.param(
            {"host": "h", "auth": "NOSASL"},
            None,
            {"auth_mechanism": "NOSASL"},
            id="binary-nosasl",
        ),
        pytest.param(
            {"host": "h", "auth": "KERBEROS"},
            None,
            {"auth_mechanism": "GSSAPI", "kerberos_service_name": "hive"},
            id="kerberos-defaults-to-hive-service",
        ),
        pytest.param(
            {"host": "h", "auth": "LDAP", "username": "u", "password": "p"},
            None,
            {"auth_mechanism": "LDAP", "user": "u", "password": "p"},
            id="ldap",
        ),
        pytest.param(
            {"host": "h", "auth": "BASIC", "http_path": "/hive2"},
            "https",
            {
                "auth_mechanism": "PLAIN",
                "use_http_transport": True,
                "use_ssl": True,
                "http_path": "hive2",
                "verify_cert": False,
            },
            id="https-hdinsight",
        ),
        pytest.param(
            {"host": "h", "ssl_cert": "required"},
            "https",
            {"http_path": "cliservice", "verify_cert": True},
            id="https-verifying",
        ),
        pytest.param(
            {"host": "h", "auth": "KERBEROS", "auth_mechanism": "NOSASL"},
            None,
            {"auth_mechanism": "NOSASL"},
            id="impyla-argument-wins",
        ),
    ],
)
def test_build_hive_connect_args(
    params: Dict[str, Any], transport: str, expected: Dict[str, Any]
) -> None:
    args = build_hive_connect_args(params, transport)

    for key, value in expected.items():
        assert args.impyla_kwargs[key] == value
    assert "database" not in args.impyla_kwargs
    if args.impyla_kwargs["auth_mechanism"] != "GSSAPI":
        assert "kerberos_service_name" not in args.impyla_kwargs


@pytest.mark.parametrize(
    "params",
    [
        {"host": "h", "thrift_transport": object()},
        {"host": "h", "not_an_impyla_arg": 1},
        {"host": "h", "auth": "PAM"},
    ],
)
def test_build_hive_connect_args_rejects_unsupported(params: Dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        build_hive_connect_args(params)


def test_build_hive_connect_args_explains_missing_kerberos_package() -> None:
    with (
        patch("importlib.util.find_spec", return_value=None),
        pytest.raises(ValueError, match="pip install kerberos"),
    ):
        build_hive_connect_args({"host": "h", "auth": "KERBEROS"})


def test_connection_shares_one_session_across_cursors() -> None:
    owner = MagicMock()
    impyla_connection = MagicMock()
    impyla_connection.cursor.return_value = owner
    connect_args = build_hive_connect_args(
        {
            "host": "h",
            "username": "svc",
            "database": "my_db",
            "configuration": {"hive.exec.dynamic.partition": "true"},
        }
    )

    conn = HiveDBAPIConnection(impyla_connection, connect_args)
    first, second = conn.cursor(), conn.cursor()
    first.close()
    conn.close()

    impyla_connection.cursor.assert_called_once_with(
        user="svc", configuration={"hive.exec.dynamic.partition": "true"}
    )
    owner.execute.assert_called_once_with("USE `my_db`")
    assert isinstance(first, _SessionCursor)
    assert first.session is second.session is owner.session
    # Closing a statement cursor leaves the shared session open.
    owner.session.close.assert_not_called()
    owner.close.assert_called_once()
    impyla_connection.close.assert_called_once()


def test_impyla_uses_qop_correct_sasl_client() -> None:
    assert impala.sasl_compat.PureSASLClient is PureSASLClient


def test_sql_generic_registers_hive_dialects() -> None:
    # Run in a fresh interpreter, and compare names rather than importing
    # hive_dialect, so the registration can only come from sql_generic.
    script = (
        "import datahub.ingestion.source.sql.sql_generic\n"
        "import sqlalchemy\n"
        "module = 'datahub.ingestion.source.sql.hive.hive_dialect'\n"
        "for url, name in [\n"
        "    ('hive://localhost:10000/default', 'HiveDialect'),\n"
        "    ('hive+https://localhost:443/default', 'HiveHTTPSDialect'),\n"
        "    ('sparksql://localhost:10000/default', 'SparkSqlDialect'),\n"
        "]:\n"
        "    cls = type(sqlalchemy.create_engine(url).dialect)\n"
        "    assert (cls.__module__, cls.__name__) == (module, name), (url, cls)\n"
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

import os
import subprocess
import sys
from typing import Iterator, cast
from unittest.mock import MagicMock, patch

import prestodb.auth
import prestodb.exceptions
import pytest
import requests
from sqlalchemy import create_engine, exc, text, types as sqlalchemy_types
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sql.presto_dialect import (
    PrestoDialect,
    build_presto_connect_args,
)


class _TextConnection:
    """Answers each statement with a SQLite SELECT producing Presto's rows."""

    def __init__(self, sqlite_conn: Connection, select: str) -> None:
        self._sqlite_conn = sqlite_conn
        self._select = select

    def execute(self, statement: object, *args: object, **kw: object) -> object:
        return self._sqlite_conn.execute(text(self._select))


@pytest.fixture
def sqlite_conn() -> Iterator[Connection]:
    with create_engine("sqlite://").connect() as conn:
        yield conn


@pytest.mark.parametrize(
    "columns_select",
    [
        pytest.param(
            "SELECT 'id' AS \"Column\", 'bigint' AS \"Type\", '' AS \"Comment\" "
            "UNION ALL SELECT 'ds', 'varchar', 'Partition Key'",
            id="comment-column",
        ),
        pytest.param(
            "SELECT 'id' AS \"Column\", 'bigint' AS \"Type\", '' AS \"Extra\", "
            "'' AS \"Comment\" "
            "UNION ALL SELECT 'ds', 'varchar', 'partition key', ''",
            id="extra-column",
        ),
        pytest.param(
            'SELECT \'id\' AS "Column", \'bigint\' AS "Type", 0 AS "Partition Key", '
            "'' AS \"Comment\" "
            "UNION ALL SELECT 'ds', 'varchar', 1, ''",
            id="boolean-partition-key-column",
        ),
    ],
)
def test_presto_get_indexes(sqlite_conn: Connection, columns_select: str) -> None:
    conn = cast(Connection, _TextConnection(sqlite_conn, columns_select))

    assert PrestoDialect().get_indexes(conn, "events", schema="sch") == [
        {"name": "partition", "column_names": ["ds"], "unique": False}
    ]


def test_presto_missing_table_raises_no_such_table() -> None:
    presto_error = prestodb.exceptions.DatabaseError(
        'PrestoUserError(type=USER_ERROR, name=TABLE_NOT_FOUND, message="line 1:1: '
        "Table 'hive.sch.missing' does not exist\", query_id=q1)"
    )
    conn = MagicMock()
    conn.execute.side_effect = exc.DBAPIError.instance(
        "SHOW COLUMNS", None, presto_error, prestodb.exceptions.Error
    )
    dialect = PrestoDialect()

    with pytest.raises(exc.NoSuchTableError):
        dialect._get_table_columns(conn, "missing", "sch")
    assert not dialect.has_table(conn, "missing", schema="sch", info_cache={})


def test_presto_query_errors_become_dbapi_errors() -> None:
    # presto-python-client's query errors don't subclass the PEP 249 Error, so
    # SQLAlchemy would let them escape unwrapped.
    cursor = MagicMock()
    cursor.execute.side_effect = prestodb.exceptions.PrestoUserError(
        {"message": "Schema 'nope' does not exist", "errorType": "USER_ERROR"}
    )

    with pytest.raises(prestodb.exceptions.DatabaseError, match="does not exist"):
        PrestoDialect().do_execute(cursor, "SHOW TABLES FROM nope", ())
    cursor.execute.assert_called_once_with("SHOW TABLES FROM nope")


def test_build_presto_connect_args_translates_pyhive_args() -> None:
    kwargs = build_presto_connect_args(
        {
            "host": "presto.example.com",
            "port": 443,
            "username": "svc",
            "password": "test_password",
            "principal_username": "alice",
            "catalog": "hive",
            "protocol": "https",
            "session_props": {"query_max_run_time": "1h"},
            "requests_kwargs": {"verify": False, "timeout": 60},
        }
    )

    assert kwargs["user"] == "alice"
    assert kwargs["http_scheme"] == "https"
    assert kwargs["session_properties"] == {"query_max_run_time": "1h"}
    assert kwargs["request_timeout"] == 60
    assert kwargs["schema"] == "default"
    session = requests.Session()
    kwargs["auth"].set_http_session(session)
    assert session.verify is False
    assert isinstance(session.auth, requests.auth.HTTPBasicAuth)
    assert (session.auth.username, session.auth.password) == ("svc", "test_password")


def test_build_presto_connect_args_passes_prestodb_args_through() -> None:
    auth = prestodb.auth.BasicAuthentication("u", "p")

    kwargs = build_presto_connect_args(
        {"host": "h", "username": "u", "auth": auth, "http_scheme": "https"}
    )

    assert kwargs["auth"] is auth
    assert kwargs["http_scheme"] == "https"


def test_native_auth_keeps_requests_kwargs_tls_settings() -> None:
    native_auth = prestodb.auth.BasicAuthentication("u", "test_password")

    kwargs = build_presto_connect_args(
        {
            "host": "h",
            "http_scheme": "https",
            "auth": native_auth,
            "requests_kwargs": {"verify": "/etc/ssl/presto-ca.pem"},
        }
    )

    session = requests.Session()
    kwargs["auth"].set_http_session(session)
    assert session.verify == "/etc/ssl/presto-ca.pem"
    assert isinstance(session.auth, requests.auth.HTTPBasicAuth)


def test_presto_get_columns_maps_parameterized_types(sqlite_conn: Connection) -> None:
    conn = cast(
        Connection,
        _TextConnection(
            sqlite_conn,
            "SELECT 'price' AS \"Column\", 'decimal(10,2)' AS \"Type\" "
            "UNION ALL SELECT 'name', 'varchar(255)' "
            "UNION ALL SELECT 'code', 'char(5)' "
            "UNION ALL SELECT 'tags', 'array(varchar)'",
        ),
    )

    columns = PrestoDialect().get_columns(conn, "events", schema="sch")

    assert [type(c["type"]) for c in columns] == [
        sqlalchemy_types.DECIMAL,
        sqlalchemy_types.String,
        sqlalchemy_types.String,
        sqlalchemy_types.NullType,
    ]


def test_kerberos_without_requests_kerberos_explains_install() -> None:
    # A None entry in sys.modules makes the import raise ImportError.
    with (
        patch.dict(sys.modules, {"requests_kerberos": None}),
        pytest.raises(ImportError, match="pip install requests-kerberos"),
    ):
        build_presto_connect_args(
            {"host": "h", "http_scheme": "https", "KerberosRemoteServiceName": "presto"}
        )


def test_kerberos_args_configure_the_http_session() -> None:
    requests_kerberos = MagicMock(OPTIONAL=2)

    # The keytab path is exported to the environment; patch.dict restores it.
    with (
        patch.dict(sys.modules, {"requests_kerberos": requests_kerberos}),
        patch.dict(os.environ),
    ):
        kwargs = build_presto_connect_args(
            {
                "host": "presto.example.com",
                "protocol": "https",
                "KerberosRemoteServiceName": "presto",
                "KerberosPrincipal": "svc@EXAMPLE.COM",
                "KerberosKeytabPath": "/etc/security/svc.keytab",
                "KerberosUseCanonicalHostname": "false",
                "requests_kwargs": {"verify": "/etc/ssl/presto-ca.pem"},
            }
        )
        session = requests.Session()
        kwargs["auth"].set_http_session(session)
        keytab = os.environ.get("KRB5_CLIENT_KTNAME")

    requests_kerberos.HTTPKerberosAuth.assert_called_once()
    call = requests_kerberos.HTTPKerberosAuth.call_args.kwargs
    assert call["service"] == "presto"
    assert call["principal"] == "svc@EXAMPLE.COM"
    assert call["hostname_override"] == "presto.example.com"
    assert call["mutual_authentication"] == 2
    assert session.verify == "/etc/ssl/presto-ca.pem"
    assert keytab == "/etc/security/svc.keytab"
    assert kwargs["http_scheme"] == "https"


@pytest.mark.parametrize(
    "params",
    [
        {"host": "h", "requests_session": requests.Session()},
        {"host": "h", "requests_kwargs": {"proxies": {}}},
        {"host": "h", "password": "p", "requests_kwargs": {"auth": object()}},
        {
            "host": "h",
            "KerberosRemoteServiceName": "presto",
            "requests_kwargs": {"auth": object()},
        },
        {"host": "h", "not_a_presto_arg": 1},
        # presto-python-client refuses authentication over plain HTTP.
        {"host": "h", "requests_kwargs": {"auth": object()}},
        {"host": "h", "username": "u", "password": "p", "protocol": "http"},
    ],
)
def test_build_presto_connect_args_rejects_unsupported(params: dict) -> None:
    with pytest.raises(ValueError):
        build_presto_connect_args(params)


def test_tls_requests_kwargs_are_dropped_over_http() -> None:
    kwargs = build_presto_connect_args(
        {"host": "h", "requests_kwargs": {"verify": False, "cert": "/c.pem"}}
    )

    assert "auth" not in kwargs


def test_sql_generic_registers_presto_dialect() -> None:
    # Run in a fresh interpreter, and compare names rather than importing
    # presto_dialect, so the registration can only come from sql_generic.
    script = (
        "import datahub.ingestion.source.sql.sql_generic\n"
        "import sqlalchemy\n"
        "engine = sqlalchemy.create_engine('presto://localhost:8080/hive/default')\n"
        "cls = type(engine.dialect)\n"
        "assert (cls.__module__, cls.__name__) == (\n"
        "    'datahub.ingestion.source.sql.presto_dialect', 'PrestoDialect'\n"
        "), cls\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr

import datetime
import decimal
import importlib.util
import inspect
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Type

import impala.dbapi
import impala.sasl_compat
from dateutil.parser import parse as parse_datetime
from impala.hiveserver2 import HiveServer2Cursor
from sqlalchemy import exc, types, util
from sqlalchemy.dialects import mysql, registry
from sqlalchemy.engine import Connection, default, reflection
from sqlalchemy.engine.interfaces import (
    DBAPIConnection,
    DBAPICursor,
    Dialect,
    ReflectedColumn,
    ReflectedIndex,
    ReflectedPrimaryKeyConstraint,
    ReflectedTableComment,
)
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler, sqltypes, text

from datahub.ingestion.source.sql.hive.sasl_client import PureSASLClient

logger = logging.getLogger(__name__)

# impyla imports this name lazily for every SASL transport it builds, so rebinding it
# routes HiveServer2 Kerberos connections through the QOP-correct client.
impala.sasl_compat.PureSASLClient = PureSASLClient

HIVE_DEFAULT_PORT = 10000
HIVE_DEFAULT_DATABASE = "default"
HIVE_DEFAULT_KERBEROS_SERVICE_NAME = "hive"
HIVE_DEFAULT_HTTP_PATH = "cliservice"

_HTTP = "http"
_HTTPS = "https"

# `auth` values follow hive.server2.authentication; these were PyHive's connect_args,
# which existing recipes pass, so they are translated to impyla's auth_mechanism.
_BINARY_AUTH_MECHANISMS: Dict[str, str] = {
    "NONE": "PLAIN",
    "NOSASL": "NOSASL",
    "LDAP": "LDAP",
    "CUSTOM": "PLAIN",
    "KERBEROS": "GSSAPI",
}
# Over HTTP every mode except Kerberos authenticates with a Basic header.
_HTTP_AUTH_MECHANISMS: Dict[str, str] = {
    "NONE": "PLAIN",
    "NOSASL": "PLAIN",
    "BASIC": "PLAIN",
    "LDAP": "LDAP",
    "KERBEROS": "GSSAPI",
}
_VERIFYING_SSL_CERT_MODES = {"required", "optional"}
# pure-sasl's GSSAPI mechanism needs one of these; without it the mechanism is
# silently dropped and the SASL handshake fails with a generic error.
_KERBEROS_MODULES = ("kerberos", "winkerberos")

_IMPYLA_CONNECT_PARAMS: Set[str] = set(
    inspect.signature(impala.dbapi.connect).parameters
)

_TABLE_NOT_FOUND = re.compile(r"Table not found|Table .* does not exist")
_SPARK_TABLE_NOT_FOUND = re.compile(
    r"AnalysisException.*(Table or view not found|TABLE_OR_VIEW_NOT_FOUND)"
)
_SPARK_UNREADABLE_TYPE = re.compile(
    r"org\.apache\.spark\.SparkException: Cannot recognize hive type string"
)
_LEADING_WORD = re.compile(r"^\w+")


class _QuoteEverything(set[str]):
    def __contains__(self, item: object) -> bool:
        return True


class HiveStringTypeBase(types.TypeDecorator):
    """Read-only Hive type whose values may arrive as strings over Thrift."""

    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        raise NotImplementedError("Writing to Hive is not supported")


class HiveDate(HiveStringTypeBase):
    impl = types.DATE
    cache_ok = True

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Callable[[Any], Optional[datetime.date]]:
        def process(value: Any) -> Optional[datetime.date]:
            if isinstance(value, datetime.datetime):
                return value.date()
            if isinstance(value, datetime.date):
                return value
            if value is not None:
                return parse_datetime(value).date()
            return None

        return process


class HiveTimestamp(HiveStringTypeBase):
    impl = types.TIMESTAMP
    cache_ok = True

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Callable[[Any], Optional[datetime.datetime]]:
        def process(value: Any) -> Optional[datetime.datetime]:
            if isinstance(value, datetime.datetime):
                return value
            if value is not None:
                return parse_datetime(value)
            return None

        return process


class HiveDecimal(HiveStringTypeBase):
    impl = types.DECIMAL
    cache_ok = True

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Callable[[Any], Optional[decimal.Decimal]]:
        def process(value: Any) -> Optional[decimal.Decimal]:
            if isinstance(value, decimal.Decimal):
                return value
            if value is not None:
                return decimal.Decimal(value)
            return None

        return process


HIVE_TYPE_MAP: Dict[str, Type[types.TypeEngine]] = {
    "boolean": types.Boolean,
    "tinyint": mysql.TINYINT,
    "smallint": types.SmallInteger,
    "int": types.Integer,
    "bigint": types.BigInteger,
    "float": types.Float,
    "double": types.Float,
    "string": types.String,
    "varchar": types.String,
    "char": types.String,
    "date": HiveDate,
    "timestamp": HiveTimestamp,
    "binary": types.String,
    "array": sqltypes.NullType,
    "map": sqltypes.NullType,
    "struct": sqltypes.NullType,
    "uniontype": sqltypes.NullType,
    "decimal": HiveDecimal,
}


class HiveIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words: Set[str] = _QuoteEverything()

    def __init__(
        self,
        dialect: Dialect,
        initial_quote: str = "`",
        final_quote: Optional[str] = None,
        escape_quote: str = '"',
        quote_case_sensitive_collations: bool = True,
        omit_schema: bool = False,
    ) -> None:
        super().__init__(
            dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )


class HiveCompiler(compiler.SQLCompiler):
    def visit_concat_op_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        return (
            f"concat({self.process(binary.left, **kw)}, "
            f"{self.process(binary.right, **kw)})"
        )

    def visit_column(self, *args: Any, **kwargs: Any) -> str:
        result = super().visit_column(*args, **kwargs)
        # Hive rejects schema.table.column references, so drop the schema part.
        if result.count(".") == 2:
            result = result[result.index(".") + 1 :]
        return result

    def visit_char_length_func(self, fn: Any, **kw: Any) -> str:
        return f"length{self.function_argspec(fn, **kw)}"


class HiveTypeCompiler(compiler.GenericTypeCompiler):
    def visit_INTEGER(self, type_: Any, **kw: Any) -> str:
        return "INT"

    def visit_NUMERIC(self, type_: Any, **kw: Any) -> str:
        return "DECIMAL"

    def visit_CHAR(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    def visit_VARCHAR(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    def visit_NCHAR(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    def visit_TEXT(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    def visit_CLOB(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    def visit_BLOB(self, type_: Any, **kw: Any) -> str:
        return "BINARY"

    def visit_TIME(self, type_: Any, **kw: Any) -> str:
        return "TIMESTAMP"

    def visit_DATE(self, type_: Any, **kw: Any) -> str:
        return "TIMESTAMP"

    def visit_DATETIME(self, type_: Any, **kw: Any) -> str:
        return "TIMESTAMP"


class HiveExecutionContext(default.DefaultExecutionContext):
    @util.memoized_property
    def _preserve_raw_colnames(self) -> bool:
        return self.execution_options.get("hive_raw_colnames", False)

    def _translate_colname(self, colname: str) -> Tuple[str, Optional[str]]:
        # With hive.resultset.use.unique.column.names (the default) Hive labels result
        # columns "table.column".
        if not self._preserve_raw_colnames and "." in colname:
            return colname.split(".")[-1], colname
        return colname, None


class HiveReflectedColumn(ReflectedColumn, total=False):
    # The full Hive type ("map<string,int>"); `type` only reflects the base type.
    full_type: Optional[str]


class HiveReflectedTableComment(ReflectedTableComment):
    # The "DESCRIBE FORMATTED" detailed table information, surfaced as properties.
    properties: Dict[str, str]


@dataclass
class HiveConnectArgs:
    impyla_kwargs: Dict[str, Any]
    session_user: Optional[str]
    database: str
    configuration: Dict[str, str] = field(default_factory=dict)


def build_hive_connect_args(
    params: Dict[str, Any], transport_scheme: Optional[str] = None
) -> HiveConnectArgs:
    """Translate PyHive-style connect arguments into impyla's ``connect()`` kwargs.

    Recipes configure HiveServer2 through PyHive's argument names (``auth``,
    ``kerberos_service_name``, ``http_path``, ``ssl_cert``, ...), so those keep working.
    Any impyla ``connect()`` argument (``auth_mechanism``, ``use_ssl``, ``ca_cert``,
    ``timeout``, ...) is passed through unchanged and wins over the PyHive equivalent.
    """
    params = dict(params)
    if params.pop("thrift_transport", None) is not None:
        raise ValueError(
            "thrift_transport is not supported; configure the connection with "
            "auth, http_path, use_ssl, ca_cert and the other connect_args instead."
        )

    scheme = params.pop("scheme", None) or transport_scheme
    use_http = scheme in (_HTTP, _HTTPS)
    username = params.pop("username", None)
    database = params.pop("database", None) or HIVE_DEFAULT_DATABASE
    configuration = params.pop("configuration", None) or {}
    auth = str(params.pop("auth", None) or "NONE").upper()
    check_hostname = str(params.pop("check_hostname", "")).lower() == "true"
    ssl_cert = str(params.pop("ssl_cert", "") or "").lower()
    http_path = params.pop("http_path", None)

    unknown = sorted(set(params) - _IMPYLA_CONNECT_PARAMS)
    if unknown:
        raise ValueError(f"Unsupported Hive connect_args: {', '.join(unknown)}")

    kwargs: Dict[str, Any] = {"port": HIVE_DEFAULT_PORT, **params}
    if kwargs["port"] is None:
        kwargs["port"] = HIVE_DEFAULT_PORT
    kwargs.setdefault("user", username)

    if "auth_mechanism" not in kwargs:
        mechanisms = _HTTP_AUTH_MECHANISMS if use_http else _BINARY_AUTH_MECHANISMS
        if auth not in mechanisms:
            raise ValueError(
                f"Unsupported Hive auth {auth!r} for the "
                f"{'HTTP' if use_http else 'binary'} transport; "
                f"use one of {', '.join(sorted(mechanisms))}"
            )
        kwargs["auth_mechanism"] = mechanisms[auth]
    if str(kwargs["auth_mechanism"]).upper() == "GSSAPI":
        if not any(importlib.util.find_spec(m) for m in _KERBEROS_MODULES):
            raise ValueError(
                "Hive Kerberos authentication requires the kerberos package, which "
                "builds against the system Kerberos libraries (e.g. libkrb5-dev). "
                "Install it with: pip install kerberos"
            )
        if not kwargs.get("kerberos_service_name"):
            kwargs["kerberos_service_name"] = HIVE_DEFAULT_KERBEROS_SERVICE_NAME
    else:
        kwargs.pop("kerberos_service_name", None)

    if use_http:
        kwargs.setdefault("use_http_transport", True)
        # impyla inserts the "/" between port and path itself.
        kwargs.setdefault(
            "http_path", (http_path or HIVE_DEFAULT_HTTP_PATH).lstrip("/")
        )
        if scheme == _HTTPS:
            kwargs.setdefault("use_ssl", True)
    elif http_path is not None:
        kwargs.setdefault("http_path", http_path.lstrip("/"))

    if kwargs.get("use_ssl") and "verify_cert" not in kwargs:
        kwargs["verify_cert"] = check_hostname or ssl_cert in _VERIFYING_SSL_CERT_MODES

    return HiveConnectArgs(
        impyla_kwargs=kwargs,
        session_user=kwargs.get("user"),
        database=database,
        configuration={str(k): str(v) for k, v in configuration.items()},
    )


class _SessionCursor(HiveServer2Cursor):
    """A cursor that runs on a session it does not own.

    impyla opens and closes one HiveServer2 session per cursor. SQLAlchemy opens a
    cursor per statement, so that would mean a new Hive session (with its scratch
    directories and a ``USE`` statement) for every query. These cursors share the
    session held by :class:`HiveDBAPIConnection` and only close their own operation.
    """

    _closed: bool

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.close_operation()
        finally:
            self._closed = True

    def __del__(self) -> None:
        if not self._closed:
            try:
                self.close_operation()
            except Exception:
                pass


class HiveDBAPIConnection:
    """DB-API connection holding one HiveServer2 session, like PyHive's connection."""

    def __init__(self, impyla_connection: Any, connect_args: HiveConnectArgs) -> None:
        self._connection = impyla_connection
        self._session_owner: Optional[HiveServer2Cursor] = None
        try:
            self._session_owner = impyla_connection.cursor(
                user=connect_args.session_user,
                configuration=connect_args.configuration or None,
            )
            quoted_database = connect_args.database.replace("`", "``")
            self._session_owner.execute(f"USE `{quoted_database}`")
        except Exception:
            self.close()
            raise

    def cursor(self, *args: Any, **kwargs: Any) -> DBAPICursor:
        assert self._session_owner is not None
        # With close_finished_queries, impyla fetches the full operation log (GetLog)
        # after every query; SQLAlchemy closes each cursor, which closes the operation.
        return _SessionCursor(self._session_owner.session, close_finished_queries=False)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        try:
            if self._session_owner is not None:
                self._session_owner.close()
        finally:
            self._session_owner = None
            self._connection.close()

    def __getattr__(self, key: str) -> Any:
        # Delegate anything else the DB-API exposes, e.g. impyla's reconnect().
        if key.startswith("_"):
            raise AttributeError(key)
        return getattr(self._connection, key)


def _quote_table(
    dialect: default.DefaultDialect, table_name: str, schema: Optional[str]
) -> str:
    quote = dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(table_name)}" if schema else quote(table_name)


def _error_message(error: Exception) -> str:
    if isinstance(error, exc.DBAPIError) and error.orig is not None:
        return str(error.orig)
    return str(error)


def _strip_describe_rows(
    rows: Sequence[Any],
) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """Strip DESCRIBE output and drop blank rows and the "# col_name" header."""
    result = []
    for row in rows:
        col_name, col_type, comment = (col.strip() if col else None for col in row)
        if col_name and col_name != "# col_name":
            result.append((col_name, col_type, comment))
    return result


class HiveDialect(default.DefaultDialect):
    """SQLAlchemy dialect for HiveServer2 over impyla's DB-API driver."""

    name = "hive"
    driver = "impyla"
    transport_scheme: Optional[str] = None
    execution_ctx_cls: Type[default.DefaultExecutionContext] = HiveExecutionContext
    preparer = HiveIdentifierPreparer
    statement_compiler = HiveCompiler
    type_compiler_cls = HiveTypeCompiler
    supports_statement_cache = True
    supports_views = True
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_multivalues_insert = True
    supports_sane_rowcount = False
    default_paramstyle = "pyformat"
    info_rows_delimiters = [
        ("# Detailed Table Information", None, None),
        ("# Detailed Table Information", "", ""),
    ]
    partition_columns_names = ["# Partition Information"]

    @classmethod
    def import_dbapi(cls) -> Any:
        return impala.dbapi

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Any]]:
        kwargs: Dict[str, Any] = {
            "host": url.host,
            "port": url.port or HIVE_DEFAULT_PORT,
            "username": url.username or None,
            "password": url.password or None,
            "database": url.database or HIVE_DEFAULT_DATABASE,
        }
        kwargs.update(url.query)
        return [], kwargs

    def connect(self, *cargs: Any, **cparams: Any) -> DBAPIConnection:
        # connect_args from the recipe are merged into cparams after
        # create_connect_args, so the translation has to happen here.
        connect_args = build_hive_connect_args(cparams, self.transport_scheme)
        impyla_connection = self.loaded_dbapi.connect(
            *cargs, **connect_args.impyla_kwargs
        )
        return HiveDBAPIConnection(impyla_connection, connect_args)

    def do_rollback(self, dbapi_connection: Any) -> None:
        pass

    @reflection.cache
    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        return [row[0] for row in connection.execute(text("SHOW SCHEMAS"))]

    def _show_query(self, statement: str, schema: Optional[str]) -> str:
        if schema:
            return f"{statement} IN {self.identifier_preparer.quote_identifier(schema)}"
        return statement

    @reflection.cache
    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        # Hive's SHOW TABLES includes views.
        query = self._show_query("SHOW TABLES", schema)
        return [row[0] for row in connection.execute(text(query))]

    @reflection.cache
    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        query = self._show_query("SHOW VIEWS", schema)
        return [row[0] for row in connection.execute(text(query))]

    @reflection.cache
    def get_view_definition(
        self,
        connection: Connection,
        view_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> str:
        full_table = _quote_table(self, view_name, schema)
        # Hive returns multi-line view DDL as one row per line, with each line's
        # leading and trailing whitespace stripped.
        rows = connection.execute(text(f"SHOW CREATE TABLE {full_table}")).fetchall()
        return "\n".join(row[0] for row in rows)

    def _get_table_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str],
        extended: bool = False,
    ) -> Sequence[Any]:
        full_table = _quote_table(self, table_name, schema)
        formatted = " FORMATTED" if extended else ""
        try:
            rows = connection.execute(
                text(f"DESCRIBE{formatted} {full_table}")
            ).fetchall()
        except exc.DBAPIError as e:
            if _TABLE_NOT_FOUND.search(_error_message(e)):
                raise exc.NoSuchTableError(full_table) from e
            raise
        # Some Hive versions answer DESCRIBE of a missing table with a single row.
        if len(rows) == 1 and _TABLE_NOT_FOUND.match(rows[0][0] or ""):
            raise exc.NoSuchTableError(full_table)
        return rows

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> bool:
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[ReflectedColumn]:
        rows = _strip_describe_rows(
            self._get_table_columns(connection, table_name, schema)
        )
        result: List[ReflectedColumn] = []
        for col_name, full_col_type, comment in rows:
            if col_name in self.partition_columns_names:
                break
            # Reflect only the base type: 'map<int,int>' -> 'map', 'decimal(10,1)' ->
            # 'decimal'. The full type is kept in 'full_type'.
            match = _LEADING_WORD.search(full_col_type or "")
            col_type = match.group(0) if match else ""
            coltype = HIVE_TYPE_MAP.get(col_type.lower())
            if coltype is None:
                util.warn(f"Did not recognize type '{col_type}' of column '{col_name}'")
                coltype = sqltypes.NullType
            column: HiveReflectedColumn = {
                "name": col_name,
                "type": coltype(),
                "full_type": full_col_type,
                "nullable": True,
                "default": None,
                "comment": comment,
            }
            result.append(column)
        return result

    def get_foreign_keys(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Any]:
        return []

    def get_pk_constraint(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> ReflectedPrimaryKeyConstraint:
        return {"constrained_columns": [], "name": None}

    @reflection.cache
    def get_indexes(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[ReflectedIndex]:
        rows = _strip_describe_rows(
            self._get_table_columns(connection, table_name, schema)
        )
        partition_columns: List[Optional[str]] = []
        in_partition_section = False
        for col_name, _col_type, _comment in rows:
            if in_partition_section:
                partition_columns.append(col_name)
            elif col_name in self.partition_columns_names:
                in_partition_section = True
        if partition_columns:
            return [
                {
                    "name": "partition",
                    "column_names": partition_columns,
                    "unique": False,
                }
            ]
        return []

    @reflection.cache
    def get_table_comment(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> HiveReflectedTableComment:
        rows = [
            tuple(row)
            for row in self._get_table_columns(
                connection, table_name, schema, extended=True
            )
        ]
        start = next(
            (i for i, row in enumerate(rows) if row in self.info_rows_delimiters), None
        )
        if start is None:
            raise ValueError(
                f"DESCRIBE FORMATTED of {table_name} has no detailed table information"
            )

        properties: Dict[str, str] = {}
        active_heading: Optional[str] = None
        for col_name, data_type, value in rows[start:]:
            col_name = col_name.rstrip()
            if col_name.startswith("# "):
                continue
            if col_name == "" and data_type is None:
                active_heading = None
            elif col_name != "" and data_type is None:
                active_heading = col_name
            elif col_name != "":
                properties[col_name] = data_type.strip()
            else:
                properties[f"{active_heading} {data_type.rstrip()}"] = value.rstrip()

        return {
            "text": properties.get("Table Parameters: comment"),
            "properties": properties,
        }


class HiveHTTPDialect(HiveDialect):
    transport_scheme = _HTTP
    supports_statement_cache = True


class HiveHTTPSDialect(HiveDialect):
    transport_scheme = _HTTPS
    supports_statement_cache = True


class SparkSqlDialect(HiveDialect):
    """Spark Thrift Server, which speaks the HiveServer2 protocol."""

    # SQLAlchemy reads this from each dialect class's own __dict__.
    supports_statement_cache = True

    name = "sparksql"
    execution_ctx_cls = default.DefaultExecutionContext
    partition_columns_names = ["# Partition Information", "# Partitioning"]

    def _get_table_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str],
        extended: bool = False,
    ) -> Sequence[Any]:
        full_table = _quote_table(self, table_name, schema)
        formatted = " FORMATTED" if extended else ""
        try:
            # Stop Spark SQL truncating long column types such as structs.
            connection.execute(text("SET spark.sql.debug.maxToStringFields=1000000"))
            return connection.execute(
                text(f"DESCRIBE{formatted} {full_table}")
            ).fetchall()
        except exc.DBAPIError as e:
            message = _error_message(e)
            if _SPARK_TABLE_NOT_FOUND.search(message):
                raise exc.NoSuchTableError(full_table) from e
            if _SPARK_UNREADABLE_TYPE.search(message):
                raise exc.UnreflectableTableError(full_table) from e
            raise

    @reflection.cache
    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        query = self._show_query("SHOW TABLES", schema)
        # Rows are (database, tableName, isTemporary); temporary views are skipped.
        return [row[1] for row in connection.execute(text(query)) if not row[-1]]

    @reflection.cache
    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        query = self._show_query("SHOW VIEWS", schema)
        # Rows are (namespace, viewName, isTemporary), unlike Hive's single column.
        return [row[1] for row in connection.execute(text(query)) if not row[-1]]

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> bool:
        try:
            return super().has_table(connection, table_name, schema, **kw)
        except exc.UnreflectableTableError:
            # The table exists; Spark only failed to decode one of its column types.
            return True


# Registered programmatically (not through setup.py entry points) so these dialects
# take precedence over any other installed package that claims the same URL schemes.
registry.register("hive", __name__, "HiveDialect")
registry.register("hive.impyla", __name__, "HiveDialect")
registry.register("hive.http", __name__, "HiveHTTPDialect")
registry.register("hive.https", __name__, "HiveHTTPSDialect")
registry.register("sparksql", __name__, "SparkSqlDialect")

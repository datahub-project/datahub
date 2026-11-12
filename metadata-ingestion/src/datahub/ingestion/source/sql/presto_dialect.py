import getpass
import inspect
import logging
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type

import prestodb.auth
import prestodb.constants
import prestodb.dbapi
import prestodb.exceptions
from sqlalchemy import exc, types, util
from sqlalchemy.dialects import mysql, registry
from sqlalchemy.engine import Connection, default, reflection
from sqlalchemy.engine.interfaces import (
    DBAPIConnection,
    DBAPICursor,
    ReflectedColumn,
    ReflectedIndex,
    ReflectedPrimaryKeyConstraint,
)
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler, sqltypes, text

logger = logging.getLogger(__name__)

PRESTO_DEFAULT_PORT = 8080
PRESTO_DEFAULT_CATALOG = "hive"
PRESTO_DEFAULT_SCHEMA = "default"

_HTTPS = "https"
_PARTITION_KEY = "Partition Key"
_ALLOWED_REQUESTS_KWARGS = {"auth", "cert", "timeout", "verify"}
# PyHive connect_args with no presto-python-client equivalent; they are accepted so
# existing recipes keep working, and ignored.
_IGNORED_PYHIVE_ARGS = {"poll_interval"}

_PRESTODB_CONNECT_PARAMS: Set[str] = set(
    inspect.signature(prestodb.dbapi.Connection.__init__).parameters
) - {"self", "kwargs"}

_TABLE_DOES_NOT_EXIST = "Table '.*{}' does not exist"
_LEADING_WORD = re.compile(r"^\w+")


class PrestoDBAPI:
    """DB-API module facade for presto-python-client.

    ``prestodb.dbapi`` exports neither ``paramstyle`` nor the PEP 249 exception
    classes, both of which SQLAlchemy reads from the DB-API module.
    """

    apilevel = "2.0"
    threadsafety = 2
    # presto-python-client binds parameters through PREPARE / EXECUTE ... USING.
    paramstyle = "qmark"

    Warning = prestodb.exceptions.Warning
    Error = prestodb.exceptions.Error
    InterfaceError = prestodb.exceptions.InterfaceError
    DatabaseError = prestodb.exceptions.DatabaseError
    InternalError = prestodb.exceptions.InternalError
    OperationalError = prestodb.exceptions.OperationalError
    ProgrammingError = prestodb.exceptions.ProgrammingError
    IntegrityError = prestodb.exceptions.IntegrityError
    DataError = prestodb.exceptions.DataError
    NotSupportedError = prestodb.exceptions.NotSupportedError

    connect = staticmethod(prestodb.dbapi.connect)


class _HttpSessionAuthentication(prestodb.auth.Authentication):
    """Applies PyHive-style ``requests_kwargs`` to presto-python-client's session."""

    def __init__(
        self,
        inner: Optional[prestodb.auth.Authentication],
        requests_auth: Any,
        verify: Any,
        cert: Any,
    ) -> None:
        self._inner = inner
        self._requests_auth = requests_auth
        self._verify = verify
        self._cert = cert

    def set_http_session(self, http_session: Any) -> Any:
        if self._inner is not None:
            self._inner.set_http_session(http_session)
        if self._requests_auth is not None:
            http_session.auth = self._requests_auth
        if self._verify is not None:
            http_session.verify = self._verify
        if self._cert is not None:
            http_session.cert = self._cert
        return http_session

    def set_client_session(self, client_session: Any) -> None:
        if self._inner is not None:
            self._inner.set_client_session(client_session)

    def setup(self, presto_client: Any) -> None:
        self.set_client_session(presto_client.client_session)
        self.set_http_session(presto_client.http_session)

    def get_exceptions(self) -> Tuple[Type[BaseException], ...]:
        return self._inner.get_exceptions() if self._inner is not None else ()


def _kerberos_authentication(
    params: Dict[str, Any], host: Optional[str]
) -> prestodb.auth.KerberosAuthentication:
    # Imported here: requests-kerberos is an optional dependency (it needs the
    # system Kerberos libraries) that only Kerberos connections use.
    try:
        from requests_kerberos import OPTIONAL
    except ImportError as e:
        raise ImportError(
            "Presto Kerberos authentication requires requests-kerberos. "
            "Install it with: pip install requests-kerberos"
        ) from e

    # requests-kerberos reads the keytab and credential cache only from the
    # environment, as PyHive did; these are file locations, not secrets.
    keytab_path = params.pop("KerberosKeytabPath", None)
    if keytab_path is not None:
        os.environ["KRB5_CLIENT_KTNAME"] = keytab_path
    credential_cache_path = params.pop("KerberosCredentialCachePath", None)
    if credential_cache_path is not None:
        os.environ["KRB5CCNAME"] = credential_cache_path

    use_canonical_hostname = params.pop("KerberosUseCanonicalHostname", None)
    hostname_override = (
        host
        if use_canonical_hostname is not None
        and str(use_canonical_hostname).lower() == "false"
        else None
    )
    return prestodb.auth.KerberosAuthentication(
        config=params.pop("KerberosConfigPath", None),
        service_name=params.pop("KerberosRemoteServiceName"),
        principal=params.pop("KerberosPrincipal", None),
        hostname_override=hostname_override,
        mutual_authentication=OPTIONAL,
    )


def build_presto_connect_args(params: Dict[str, Any]) -> Dict[str, Any]:
    """Translate PyHive-style Presto connect arguments for presto-python-client.

    PyHive's argument names (``protocol``, ``requests_kwargs``, ``session_props``,
    ``principal_username``, ``Kerberos*``) keep working. Any
    ``prestodb.dbapi.connect()`` argument (``http_scheme``, ``auth``,
    ``http_headers``, ``request_timeout``, ...) is passed through and wins.
    """
    params = dict(params)
    if params.pop("requests_session", None) is not None:
        raise ValueError(
            "requests_session is not supported; use requests_kwargs or auth instead."
        )
    for ignored in _IGNORED_PYHIVE_ARGS & set(params):
        logger.debug(f"Ignoring unsupported Presto connect_arg {ignored}")
        params.pop(ignored)

    host = params.get("host")
    username = params.pop("username", None)
    password = params.pop("password", None)
    principal_username = params.pop("principal_username", None)
    protocol = params.pop("protocol", None)
    session_props = params.pop("session_props", None)
    requests_kwargs = dict(params.pop("requests_kwargs", None) or {})
    native_auth = params.pop("auth", None)

    use_kerberos = params.get("KerberosRemoteServiceName") is not None
    if "auth" in requests_kwargs and (use_kerberos or password is not None):
        raise ValueError(
            "requests_kwargs authentication cannot be combined with a password "
            "or Kerberos authentication"
        )
    auth: Optional[prestodb.auth.Authentication] = None
    if use_kerberos:
        auth = _kerberos_authentication(params, host)
    elif password is not None:
        auth = prestodb.auth.BasicAuthentication(username, password)
    if native_auth is not None:
        auth = native_auth

    unknown_requests_kwargs = sorted(set(requests_kwargs) - _ALLOWED_REQUESTS_KWARGS)
    if unknown_requests_kwargs:
        raise ValueError(
            "Unsupported Presto requests_kwargs: " + ", ".join(unknown_requests_kwargs)
        )
    unknown = sorted(set(params) - _PRESTODB_CONNECT_PARAMS)
    if unknown:
        raise ValueError(f"Unsupported Presto connect_args: {', '.join(unknown)}")

    kwargs: Dict[str, Any] = {
        "port": PRESTO_DEFAULT_PORT,
        "catalog": PRESTO_DEFAULT_CATALOG,
        "schema": PRESTO_DEFAULT_SCHEMA,
        **{k: v for k, v in params.items() if v is not None},
    }
    kwargs.setdefault("user", principal_username or username or getpass.getuser())
    if protocol is not None:
        kwargs.setdefault("http_scheme", protocol)
    if session_props is not None:
        kwargs.setdefault("session_properties", session_props)
    if "timeout" in requests_kwargs:
        kwargs.setdefault("request_timeout", requests_kwargs.pop("timeout"))

    if kwargs.get("http_scheme", prestodb.constants.HTTP) == prestodb.constants.HTTP:
        # TLS settings do nothing over plain HTTP, and presto-python-client
        # refuses any auth object there, so fail with the setting to change.
        requests_kwargs.pop("verify", None)
        requests_kwargs.pop("cert", None)
        if auth is not None or requests_kwargs:
            raise ValueError(
                "Presto authentication (password, Kerberos, auth or "
                "requests_kwargs auth) requires HTTPS; set protocol: https "
                "in connect_args."
            )

    if requests_kwargs:
        auth = _HttpSessionAuthentication(
            inner=auth,
            requests_auth=requests_kwargs.get("auth"),
            verify=requests_kwargs.get("verify"),
            cert=requests_kwargs.get("cert"),
        )
    if auth is not None:
        kwargs["auth"] = auth
    return kwargs


class _QuoteEverything(set[str]):
    def __contains__(self, item: object) -> bool:
        return True


class PrestoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words: Set[str] = _QuoteEverything()


PRESTO_TYPE_MAP: Dict[str, Type[types.TypeEngine]] = {
    "boolean": types.Boolean,
    "tinyint": mysql.TINYINT,
    "smallint": types.SmallInteger,
    "integer": types.Integer,
    "bigint": types.BigInteger,
    "real": types.Float,
    "double": types.Float,
    "varchar": types.String,
    "timestamp": types.TIMESTAMP,
    "date": types.DATE,
    "varbinary": types.VARBINARY,
    "char": types.String,
    "decimal": types.DECIMAL,
}


class PrestoCompiler(compiler.SQLCompiler):
    def visit_char_length_func(self, fn: Any, **kw: Any) -> str:
        return f"length{self.function_argspec(fn, **kw)}"


class PrestoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_CLOB(self, type_: Any, **kw: Any) -> str:
        raise ValueError("Presto does not support the CLOB column type.")

    def visit_NCLOB(self, type_: Any, **kw: Any) -> str:
        raise ValueError("Presto does not support the NCLOB column type.")

    def visit_DATETIME(self, type_: Any, **kw: Any) -> str:
        raise ValueError("Presto does not support the DATETIME column type.")

    def visit_FLOAT(self, type_: Any, **kw: Any) -> str:
        return "DOUBLE"

    def visit_TEXT(self, type_: Any, **kw: Any) -> str:
        return f"VARCHAR({type_.length:d})" if type_.length else "VARCHAR"


def _error_message(error: Exception) -> str:
    if isinstance(error, exc.DBAPIError) and error.orig is not None:
        return str(error.orig)
    return str(error)


class PrestoDialect(default.DefaultDialect):
    """SQLAlchemy dialect for PrestoDB over presto-python-client."""

    name = "presto"
    driver = "prestodb"
    preparer = PrestoIdentifierPreparer
    statement_compiler = PrestoCompiler
    type_compiler_cls = PrestoTypeCompiler
    supports_statement_cache = True
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def import_dbapi(cls) -> Any:
        return PrestoDBAPI

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Any]]:
        db_parts = (url.database or PRESTO_DEFAULT_CATALOG).split("/")
        if len(db_parts) > 2:
            raise ValueError(f"Unexpected database format {url.database}")
        kwargs: Dict[str, Any] = {
            "host": url.host,
            "port": url.port or PRESTO_DEFAULT_PORT,
            "username": url.username,
            "password": url.password,
            "catalog": db_parts[0],
        }
        if len(db_parts) == 2:
            kwargs["schema"] = db_parts[1]
        kwargs.update(url.query)
        return [], kwargs

    def connect(self, *cargs: Any, **cparams: Any) -> DBAPIConnection:
        # connect_args from the recipe are merged into cparams after
        # create_connect_args, so the translation has to happen here.
        return self.loaded_dbapi.connect(*cargs, **build_presto_connect_args(cparams))

    def _execute(
        self, cursor: DBAPICursor, statement: str, parameters: Optional[Any]
    ) -> None:
        try:
            if parameters:
                cursor.execute(statement, parameters)
            else:
                cursor.execute(statement)
        except (
            prestodb.exceptions.PrestoQueryError,
            prestodb.exceptions.HttpError,
        ) as e:
            # Query failures are not PEP 249 errors in presto-python-client; convert
            # them so SQLAlchemy wraps them like any other database error.
            raise prestodb.exceptions.DatabaseError(str(e)) from e

    def do_execute(
        self,
        cursor: DBAPICursor,
        statement: str,
        parameters: Optional[Any],
        context: Optional[Any] = None,
    ) -> None:
        self._execute(cursor, statement, parameters)

    def do_execute_no_params(
        self, cursor: DBAPICursor, statement: str, context: Optional[Any] = None
    ) -> None:
        self._execute(cursor, statement, None)

    def do_rollback(self, dbapi_connection: Any) -> None:
        pass

    @reflection.cache
    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        return [row[0] for row in connection.execute(text("SHOW SCHEMAS"))]

    @reflection.cache
    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        query = "SHOW TABLES"
        if schema:
            query += " FROM " + self.identifier_preparer.quote_identifier(schema)
        return [row[0] for row in connection.execute(text(query))]

    def _get_table_columns(
        self, connection: Connection, table_name: str, schema: Optional[str]
    ) -> Sequence[Any]:
        quote = self.identifier_preparer.quote_identifier
        full_table = (
            f"{quote(schema)}.{quote(table_name)}" if schema else quote(table_name)
        )
        try:
            return connection.execute(
                text(f"SHOW COLUMNS FROM {full_table}")
            ).fetchall()
        except exc.DBAPIError as e:
            pattern = _TABLE_DOES_NOT_EXIST.format(re.escape(table_name))
            if re.search(pattern, _error_message(e)):
                raise exc.NoSuchTableError(table_name) from e
            raise

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
        result: List[ReflectedColumn] = []
        for row in self._get_table_columns(connection, table_name, schema):
            mapping = row._mapping
            # Match on the base name: "decimal(10,2)" -> "decimal".
            base_type = _LEADING_WORD.match(mapping["Type"] or "")
            coltype = PRESTO_TYPE_MAP.get(
                base_type.group(0).lower() if base_type else ""
            )
            if coltype is None:
                util.warn(
                    f"Did not recognize type '{mapping['Type']}' of column "
                    f"'{mapping['Column']}'"
                )
                coltype = sqltypes.NullType
            result.append(
                {
                    "name": mapping["Column"],
                    "type": coltype(),
                    # Newer Presto versions no longer return this column.
                    "nullable": mapping.get("Null", True),
                    "default": None,
                }
            )
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
        col_names: List[Optional[str]] = []
        for row in self._get_table_columns(connection, table_name, schema):
            mapping = row._mapping
            # Depending on the Presto version, partition keys are flagged by a boolean
            # "Partition Key" column, the "Comment" column or the "Extra" column.
            is_partition_key = (
                mapping.get(_PARTITION_KEY)
                or (mapping.get("Comment") or "").startswith(_PARTITION_KEY)
                or "partition key" in (mapping.get("Extra") or "")
            )
            if is_partition_key:
                col_names.append(mapping["Column"])
        if col_names:
            return [{"name": "partition", "column_names": col_names, "unique": False}]
        return []


# Registered programmatically (not through setup.py entry points) so this dialect takes
# precedence over any other installed package that claims the presto:// scheme.
registry.register("presto", __name__, "PrestoDialect")
registry.register("presto.prestodb", __name__, "PrestoDialect")

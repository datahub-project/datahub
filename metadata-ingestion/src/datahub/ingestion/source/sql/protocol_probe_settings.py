"""Probe engine settings by wire protocol: what the probe's engine gets for the
dialect its URL names.

Each protocol is spoken by several connectors -- libpq by Postgres and the
dialects built on it, the MySQL protocol by MySQL and the servers compatible
with it, redshift_connector by Redshift -- and a recipe's sqlalchemy_uri can
point any SQL config at any of them (the generic `sqlalchemy` source always
does). So the settings follow the URL rather than the config:
SQLCommonConfig.probe_engine_settings defaults to probe_settings_for_url. Each
protocol names only connect arguments its drivers accept, since a driver
handed one it does not know refuses to connect.

SQL-family knowledge only: no connector module is imported here. Nor is
sql_config at runtime: it imports this module for ProbeEngineSettings. Nor is
the probe framework at module level, since every SQL source imports
sql_config.
"""

import logging
import os
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional

from sqlalchemy import event
from sqlalchemy.engine import Engine, make_url

from datahub.ingestion.source.sql.sqlalchemy_uri import url_dialect_and_driver

if TYPE_CHECKING:
    from datahub.ingestion.agent.sql_passthrough import QueryBudget
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

logger = logging.getLogger(__name__)

# Dialects whose default driver links libpq, which hands `-c setting` options
# to the server.
_LIBPQ_DIALECTS = frozenset({"postgresql", "postgres", "cockroachdb"})
# The drivers for those dialects that link libpq ("" is the default,
# psycopg2). The others (pg8000, asyncpg) are pure Python and refuse libpq's
# keywords, so a URL naming one gets no settings: no label, and no ceiling.
_LIBPQ_DRIVERS = frozenset({"", "psycopg2", "psycopg", "psycopg2cffi"})
# The one connect_arg libpq packs every `-c setting` into.
_LIBPQ_OPTIONS = "options"
# libpq waits as long as the OS does for a TCP handshake when no
# connect_timeout is given: about 75s for a blackholed host on macOS, longer
# on Linux, past the point an agent's tool call gives up. PyMySQL (10s) and
# pytds (15s) have defaults of their own; libpq has none.
_LIBPQ_CONNECT_TIMEOUT = "connect_timeout"
_LIBPQ_CONNECT_TIMEOUT_SECONDS = 10

# redshift_connector, this dialect's driver, is pure Python and rejects
# libpq's `options` keyword, so its ceiling is a statement instead.
_REDSHIFT_DIALECT = "redshift"

# MySQL bounds a statement with max_execution_time (milliseconds), MariaDB
# with max_statement_time (seconds), and each errors on the other's name. One
# URL reaches both, so which the server has is known only after connecting.
# Doris and StarRocks speak this protocol under dialects of their own; these
# statements are not checked against them, so they get the driver's label and
# no ceiling.
_MYSQL_DIALECTS = frozenset({"mysql", "mariadb"})
_MYSQL_TIMEOUT_STATEMENTS = (
    "SET SESSION max_execution_time={ms}",
    "SET SESSION max_statement_time={seconds}",
)


@dataclass(frozen=True)
class ProbeEngineSettings:
    """What the probe adds to the engine it builds from a recipe.

    Declared by SQLCommonConfig.probe_engine_settings: by default the URL's
    wire protocol's (probe_settings_for_url), plus a connector's own engine
    setup. A connect_arg the driver rejects stops the connection opening at
    all.
    """

    # Merged over the recipe's connect_args key by key; a setting that defers
    # to or extends the recipe's is composed by the config.
    connect_args: Mapping[str, Any] = field(default_factory=dict)
    # Run on the built engine, before the Inspector exists, for what
    # connect_args cannot carry: a statement on each new connection, a
    # credential listener, a replaced dialect.
    prepare: Optional[Callable[[Engine], None]] = None
    # Whether these bound every probe statement by the budget's timeout. When
    # False the probe reports no time ceiling rather than an unenforced one.
    timeout_applies: bool = False

    def followed_by(self, step: Callable[[Engine], None]) -> "ProbeEngineSettings":
        """These settings with `step` run on the engine after their own
        prepare: how a connector adds its setup to its protocol's."""
        first = self.prepare
        if first is None:
            return replace(self, prepare=step)

        def prepare(engine: Engine) -> None:
            first(engine)
            step(engine)

        return replace(self, prepare=prepare)


def _recipe_connect_args(config: "SQLCommonConfig") -> Mapping[str, Any]:
    """The connect_args the recipe passes create_engine, as ingestion does."""
    return config.options.get("connect_args") or {}


def probe_label_connect_arg(config: "SQLCommonConfig", kwarg: str) -> Dict[str, str]:
    """`{kwarg: PROBE_QUERY_LABEL}`, so probe traffic is told apart from
    ingestion's in the server's own logs, or nothing when the recipe already
    names its connection through `kwarg`, in connect_args or in the URL's
    query: that name is the recipe's choice, and a connect_arg would replace
    the URL's."""
    if kwarg in _recipe_connect_args(config) or kwarg in _probe_url_query(config):
        return {}
    # lazy: sql_config imports this module, and keeps the probe framework out
    # of every SQL source's import
    from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL

    return {kwarg: PROBE_QUERY_LABEL}


def probe_url(config: "SQLCommonConfig") -> str:
    """The URL the probe dials (SQLCommonConfig.probe_sql_alchemy_url)."""
    return str(config.probe_sql_alchemy_url())


def _probe_url_query(config: "SQLCommonConfig") -> Mapping[str, object]:
    """The probe URL's query arguments, which the dialect hands the driver
    as connect kwargs. create_engine lets connect_args override them, so a
    probe connect_arg with the same name replaces the recipe's value."""
    return make_url(probe_url(config)).query


def probe_settings_for_url(
    config: "SQLCommonConfig", budget: "QueryBudget"
) -> ProbeEngineSettings:
    """The settings of the protocol the config's probe URL names.

    Every dialect outside libpq's and Redshift's goes to the MySQL protocol's
    settings, which give a non-MySQL URL nothing unless it names the PyMySQL
    driver: program_name follows that driver whatever the dialect.
    """
    dialect, driver = url_dialect_and_driver(probe_url(config))
    if dialect in _LIBPQ_DIALECTS:
        if driver not in _LIBPQ_DRIVERS:
            return ProbeEngineSettings()
        return _libpq_settings(config, budget)
    if dialect == _REDSHIFT_DIALECT:
        return _redshift_settings(config, budget)
    return _mysql_protocol_settings(config, budget, dialect=dialect, driver=driver)


def _libpq_settings(
    config: "SQLCommonConfig", budget: "QueryBudget"
) -> ProbeEngineSettings:
    """The session's application_name, a connect_timeout, and
    statement_timeout in libpq's options string.

    The label is read from pg_stat_activity.application_name and `%a` in
    log_line_prefix. It is a parameter of its own rather than a second `-c`
    setting, so it can defer to a recipe's while the ceiling does not.

    The ceiling rides on the connection itself, so it bounds every statement
    the probe sends, and is reported as applying.
    """
    connect_args: Dict[str, Any] = dict(
        probe_label_connect_arg(config, "application_name")
    )
    seconds = budget.timeout_seconds
    connect_args.update(_libpq_connect_timeout(config, seconds))
    if seconds:
        ceiling = f"-c statement_timeout={seconds * 1000}"
        # Appended: the recipe's own settings (a search_path, say) share this
        # string, and the probe must connect with them as ingestion does.
        own = _recipe_libpq_options(config)
        connect_args[_LIBPQ_OPTIONS] = f"{own} {ceiling}" if own else ceiling
    return ProbeEngineSettings(connect_args=connect_args, timeout_applies=bool(seconds))


def _libpq_connect_timeout(
    config: "SQLCommonConfig", budget_seconds: Optional[int]
) -> Dict[str, int]:
    """`{connect_timeout: n}`, so an unreachable host fails within the
    probe's budget instead of the OS's TCP timeout, or nothing when the recipe
    sets its own, in connect_args or the URL's query, or PGCONNECT_TIMEOUT is
    set in the environment: ingestion connects with
    that value, and the probe should reach the source as ingestion does.

    Ten seconds, or the query budget's timeout when that is shorter: opening
    the connection should not cost more than a whole query may.
    """
    if (
        _LIBPQ_CONNECT_TIMEOUT in _recipe_connect_args(config)
        or _LIBPQ_CONNECT_TIMEOUT in _probe_url_query(config)
        # libpq reads this when no connect_timeout is passed, so an operator
        # who set it gets their value, as ingestion does.
        or os.environ.get("PGCONNECT_TIMEOUT")
    ):
        return {}
    timeout = _LIBPQ_CONNECT_TIMEOUT_SECONDS
    if budget_seconds:
        timeout = min(timeout, budget_seconds)
    return {_LIBPQ_CONNECT_TIMEOUT: timeout}


def _recipe_libpq_options(config: "SQLCommonConfig") -> object:
    """The libpq options ingestion's engine hands the driver: connect_args'
    when the recipe sets them there, else the probe URL's query string's, as
    create_engine lets connect_args override the URL."""
    own = _recipe_connect_args(config)
    if _LIBPQ_OPTIONS in own:
        return own[_LIBPQ_OPTIONS]
    return _probe_url_query(config).get(_LIBPQ_OPTIONS)


def set_redshift_statement_timeout(dbapi_connection: Any, seconds: int) -> None:
    """Bound every later statement on this Redshift connection.

    Raises whatever the driver raises. statement_timeout is Redshift's one
    spelling, so a server refusing it is an anomaly, and a ceiling that
    quietly does not apply reads as one that does: the connection fails.

    In autocommit, because a plain SET is transactional here and the rollback
    SQLAlchemy issues when a connection returns to its pool would undo it.
    Takes a DB-API connection, so a provider holding a bare redshift_connector
    connection applies the same ceiling.
    """
    prior = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    try:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET statement_timeout = {int(seconds) * 1000}")
        finally:
            cursor.close()
    finally:
        dbapi_connection.autocommit = prior


def _redshift_settings(
    config: "SQLCommonConfig", budget: "QueryBudget"
) -> ProbeEngineSettings:
    """application_name, and statement_timeout set on each new connection.

    The SET fails the connection when refused (see
    set_redshift_statement_timeout), so a connection that exists has the
    ceiling and it is reported as applying. Redshift's pg_stat_activity has no
    application_name column; the session has it (current_setting), and
    STL_CONNECTION_LOG records it.
    """
    seconds = budget.timeout_seconds
    return ProbeEngineSettings(
        connect_args=probe_label_connect_arg(config, "application_name"),
        prepare=(
            _on_each_connection(_redshift_statement_timeout(seconds))
            if seconds
            else None
        ),
        timeout_applies=bool(seconds),
    )


def _redshift_statement_timeout(seconds: int) -> Callable[[Any, Any], None]:
    def _set_timeout(dbapi_connection: Any, _record: Any) -> None:
        set_redshift_statement_timeout(dbapi_connection, seconds)

    return _set_timeout


def _mysql_protocol_settings(
    config: "SQLCommonConfig", budget: "QueryBudget", *, dialect: str, driver: str
) -> ProbeEngineSettings:
    """PyMySQL's program_name, and a best-effort ceiling that is not claimed.

    program_name is a PyMySQL connect argument, surfaced in
    performance_schema.session_connect_attrs, so it follows the driver: other
    drivers for this dialect reject it (mysqlconnector validates its keywords).

    The ceiling is tried after connecting, where a refusal is survivable, and
    timeout_applies stays False for two reasons. Whether either variable
    exists is known only after connecting; and where max_execution_time does
    apply it bounds read-only SELECTs, not the SHOW statements behind the
    Inspector's listings. Understating a protection is the safe direction.
    """
    seconds = budget.timeout_seconds
    return ProbeEngineSettings(
        connect_args=(
            probe_label_connect_arg(config, "program_name")
            if driver == "pymysql"
            else {}
        ),
        prepare=(
            _on_each_connection(_mysql_statement_timeout(seconds))
            if seconds and dialect in _MYSQL_DIALECTS
            else None
        ),
    )


def _mysql_statement_timeout(seconds: int) -> Callable[[Any, Any], None]:
    """A connect listener asking a MySQL-or-MariaDB server to bound each
    statement."""

    def _set_timeout(dbapi_connection: Any, _record: Any) -> None:
        for template in _MYSQL_TIMEOUT_STATEMENTS:
            statement = template.format(ms=seconds * 1000, seconds=seconds)
            try:
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute(statement)
                finally:
                    cursor.close()
                return
            except Exception:
                # Unknown system variable on this server; try the other spelling.
                continue
        # The budget already reports no ceiling here, so nothing is
        # misrepresented, but a query that then runs long has this reason.
        logger.debug(
            "neither %s applied; probe queries on this server are unbounded",
            " nor ".join(t.split("=")[0] for t in _MYSQL_TIMEOUT_STATEMENTS),
        )

    return _set_timeout


def _on_each_connection(
    listener: Callable[[Any, Any], None],
) -> Callable[[Engine], None]:
    """A prepare step running `listener` on each new DB-API connection."""

    def install(engine: Engine) -> None:
        # event.listen rather than the @event.listens_for decorator: the
        # decorator is untyped, so applying it would make the listener untyped.
        event.listen(engine, "connect", listener)

    return install

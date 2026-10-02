"""Probe engine settings for the client protocols several SQL configs share.

libpq and the MySQL protocol are each spoken by more than one connector:
Postgres and the dialects built on it, MySQL and the servers compatible with
it, Hive Metastore over either backend, and the generic `sqlalchemy` source
over any of them. Their settings live here rather than in one connector's
module, whose imports the other connectors' extras do not carry.

Each applies only when the config's URL names a dialect of its protocol. A
recipe's sqlalchemy_uri can point a config at another dialect, and a driver
handed a connect argument it does not know refuses to connect.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple

from sqlalchemy import event

from datahub.ingestion.source.redshift.probe_settings import (
    REDSHIFT_DIALECT,
    redshift_probe_settings,
)
from datahub.ingestion.source.sql.sql_config import (
    ProbeEngineSettings,
    SQLCommonConfig,
    probe_label_connect_arg,
    recipe_connect_args,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import url_dialect_and_driver

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from datahub.ingestion.agent.sql_passthrough import QueryBudget

logger = logging.getLogger(__name__)

# Dialects whose default driver links libpq, which hands `-c setting` options
# to the server.
_LIBPQ_DIALECTS = frozenset({"postgresql", "postgres", "cockroachdb"})
# The one connect_arg libpq packs every `-c setting` into.
_LIBPQ_OPTIONS = "options"

# MySQL bounds a statement with max_execution_time (milliseconds), MariaDB
# with max_statement_time (seconds), and each errors on the other's name. One
# URL reaches both, so which the server has is known only after connecting.
# Doris speaks this protocol under its own `doris` dialect; these statements
# are not checked against it, so it gets the driver's label and no ceiling.
_MYSQL_DIALECTS = frozenset({"mysql", "mariadb"})
_MYSQL_TIMEOUT_STATEMENTS = (
    "SET SESSION max_execution_time={ms}",
    "SET SESSION max_statement_time={seconds}",
)


def _dialect_and_driver(config: SQLCommonConfig) -> Tuple[str, str]:
    return url_dialect_and_driver(str(config.get_sql_alchemy_url()))


def speaks_libpq(config: SQLCommonConfig) -> bool:
    """Whether this config's URL connects through a libpq driver."""
    dialect, _ = _dialect_and_driver(config)
    return dialect in _LIBPQ_DIALECTS


def probe_settings_for_url(
    config: SQLCommonConfig, budget: "QueryBudget"
) -> ProbeEngineSettings:
    """The settings of whichever protocol the config's URL names, for a
    config whose dialect is the recipe's choice rather than its own.

    Every dialect outside libpq's and Redshift's goes to the MySQL protocol's
    settings, which give a non-MySQL URL nothing unless it names the PyMySQL
    driver: program_name follows that driver whatever the dialect.
    """
    dialect, _ = _dialect_and_driver(config)
    if dialect in _LIBPQ_DIALECTS:
        return libpq_probe_settings(config, budget)
    if dialect == REDSHIFT_DIALECT:
        return redshift_probe_settings(config, budget)
    return mysql_probe_settings(config, budget)


def libpq_probe_settings(
    config: SQLCommonConfig, budget: "QueryBudget"
) -> ProbeEngineSettings:
    """The session's application_name, and statement_timeout in libpq's
    options string.

    The label is read from pg_stat_activity.application_name and `%a` in
    log_line_prefix. It is a parameter of its own rather than a second `-c`
    setting, so it can defer to a recipe's while the ceiling does not.

    The ceiling rides on the connection itself, so it bounds every statement
    the probe sends, and is reported as applying.
    """
    if not speaks_libpq(config):
        return ProbeEngineSettings()
    connect_args: Dict[str, Any] = dict(
        probe_label_connect_arg(config, "application_name")
    )
    seconds = budget.timeout_seconds
    if seconds:
        ceiling = f"-c statement_timeout={seconds * 1000}"
        # Appended: the recipe's own settings (a search_path, say) share this
        # string, and the probe must connect with them as ingestion does.
        own = recipe_connect_args(config).get(_LIBPQ_OPTIONS)
        connect_args[_LIBPQ_OPTIONS] = f"{own} {ceiling}" if own else ceiling
    return ProbeEngineSettings(connect_args=connect_args, timeout_applies=bool(seconds))


def mysql_probe_settings(
    config: SQLCommonConfig, budget: "QueryBudget"
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
    dialect, driver = _dialect_and_driver(config)
    seconds = budget.timeout_seconds
    return ProbeEngineSettings(
        connect_args=(
            probe_label_connect_arg(config, "program_name")
            if driver == "pymysql"
            else {}
        ),
        prepare=(
            _mysql_statement_timeout(seconds)
            if seconds and dialect in _MYSQL_DIALECTS
            else None
        ),
    )


def _mysql_statement_timeout(seconds: int) -> Callable[["Engine"], None]:
    """A listener asking a MySQL-or-MariaDB server to bound each statement."""

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

    def install(engine: "Engine") -> None:
        # event.listen rather than the @event.listens_for decorator: the
        # decorator is untyped, so applying it would make _set_timeout untyped.
        event.listen(engine, "connect", _set_timeout)

    return install

"""Redshift's probe engine settings: its statement ceiling and client label.

Apart from redshift/config.py, whose imports (path specs, lineage settings)
need extras the generic `sqlalchemy` source does not carry, so that source can
apply the same ceiling to a Redshift URL.
"""

from typing import TYPE_CHECKING, Any, Callable

from sqlalchemy import event

from datahub.ingestion.source.sql.sql_config import (
    ProbeEngineSettings,
    SQLCommonConfig,
    probe_label_connect_arg,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import url_dialect_and_driver

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from datahub.ingestion.agent.sql_passthrough import QueryBudget

REDSHIFT_DIALECT = "redshift"


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


def _redshift_statement_timeout(seconds: int) -> Callable[["Engine"], None]:
    def _set_timeout(dbapi_connection: Any, _record: Any) -> None:
        set_redshift_statement_timeout(dbapi_connection, seconds)

    def install(engine: "Engine") -> None:
        event.listen(engine, "connect", _set_timeout)

    return install


def redshift_probe_settings(
    config: SQLCommonConfig, budget: "QueryBudget"
) -> ProbeEngineSettings:
    """application_name, and statement_timeout set on each new connection.

    Not as a libpq `-c` option: redshift_connector, this dialect's driver, is
    pure Python and rejects the `options` keyword. The SET fails the
    connection when refused (see set_redshift_statement_timeout), so a
    connection that exists has the ceiling and it is reported as applying.

    Redshift's pg_stat_activity has no application_name column; the session
    has it (current_setting), and STL_CONNECTION_LOG records it.

    Nothing when the config's URL names another dialect, whose driver may
    reject these.
    """
    dialect, _ = url_dialect_and_driver(str(config.get_sql_alchemy_url()))
    if dialect != REDSHIFT_DIALECT:
        return ProbeEngineSettings()
    seconds = budget.timeout_seconds
    return ProbeEngineSettings(
        connect_args=probe_label_connect_arg(config, "application_name"),
        prepare=_redshift_statement_timeout(seconds) if seconds else None,
        timeout_applies=bool(seconds),
    )

import logging
import sys
from types import SimpleNamespace
from typing import Any, Callable, Dict, Optional, Protocol, Type, cast

from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL, QueryBudget
from datahub.ingestion.agent.verdicts import ClassifyContext
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource

logger = logging.getLogger(__name__)

# Naming convention linking a config class to the Source class whose
# get_identifier() owns it -- see _source_class_for.
_CONFIG_CLASS_SUFFIX = "Config"
_SOURCE_CLASS_SUFFIX = "Source"


class _SqlAlchemyUrlConfig(Protocol):
    """The one method _shim_inspector needs -- every SQLCommonConfig subclass
    has it, but that base class isn't imported here to avoid pulling its own
    (heavier) dependency chain into this module just for a type hint."""

    def get_sql_alchemy_url(self) -> str: ...


# How each SQLAlchemy dialect family is asked to bound a statement server-side.
# Keyed by URL scheme prefix, because that is what a config's own
# get_sql_alchemy_url() hands us and it needs no driver import to read.
#
# Deliberately short. A wrong connect_arg does not degrade the probe -- it stops
# the connector opening a connection at all, which is a worse failure than an
# unbounded query -- so a dialect earns a row here only once its knob is known to
# be both server-side and safe to pass. Everything absent declares no ceiling
# rather than pretending to one.
def _postgres_timeout(seconds: int) -> Dict[str, Any]:
    # libpq passes -c settings straight through to the backend.
    return {"options": f"-c statement_timeout={seconds * 1000}"}


_TIMEOUT_CONNECT_ARGS: Dict[str, Any] = {
    "postgresql": _postgres_timeout,
    "postgres": _postgres_timeout,
    "cockroachdb": _postgres_timeout,
}

# Redshift is NOT in that table, though it speaks the Postgres dialect and was
# listed there until a live cluster refused every probe connection with
# `TypeError: connect() got an unexpected keyword argument 'options'`. The
# `-c setting` string is a libpq feature and Redshift's SQLAlchemy driver is
# redshift+redshift_connector, a pure-Python implementation that never links
# libpq -- so the row was the exact failure the comment above warns about, a
# knob passed before it was known to be safe.
#
# It still gets a ceiling, applied after connecting instead. Unlike the MySQL
# family below there is no ambiguity to survive here: `statement_timeout` is
# Redshift's one documented spelling, in milliseconds, so a server that refuses
# it is a genuine anomaly rather than an expected dialect difference. The
# statement is therefore left to fail loudly -- a safety control that quietly
# does not apply is the thing QueryBudget's docstring warns against, and
# failing closed on one is the right direction.
_REDSHIFT_SCHEME = "redshift"


def _install_redshift_statement_timeout(engine: Any, seconds: int) -> None:
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import event

    def _set_timeout(dbapi_connection: Any, _record: Any) -> None:
        # autocommit, and this is the whole reason the MySQL version above does
        # not need it: in the Postgres family a plain SET is transactional, so
        # the setting is undone by the rollback SQLAlchemy issues when the
        # connection goes back to the pool. Written without this, the listener
        # ran, raised nothing, and left the session at
        # `current_setting('statement_timeout') = 0` -- a ceiling that reads as
        # applied and is not, which is exactly what QueryBudget warns about and
        # is invisible to any test that only checks the statement was sent.
        prior = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"SET statement_timeout = {seconds * 1000}")
            finally:
                cursor.close()
        finally:
            dbapi_connection.autocommit = prior

    event.listen(engine, "connect", _set_timeout)


# The MySQL family cannot use connect_args at all. MySQL 5.7.8+ bounds a statement
# with max_execution_time (milliseconds); MariaDB uses max_statement_time (seconds)
# and errors on the MySQL name. Both share the mysql+pymysql scheme -- MariaDB's
# source is declared @config_class(MySQLConfig) -- so the URL cannot tell them
# apart, and an init_command naming the wrong variable does not degrade the probe:
# it runs on every connection and stops the connector connecting at all.
#
# So the statement is issued after connecting, where failure is survivable, and
# whichever variable the server has wins. A server with neither runs unbounded,
# which is the correct outcome for a dialect that offers no ceiling.
#
# Best-effort, and reported as such: max_execution_time bounds read-only SELECTs
# and not the SHOW statements the Inspector issues for the typed listings, so even
# a server that accepts it is only partly bounded. applies_statement_timeout
# therefore excludes this family -- see the reasoning there.
_MYSQL_SCHEMES = frozenset({"mysql", "mariadb"})

_MYSQL_TIMEOUT_STATEMENTS = (
    "SET SESSION max_execution_time={ms}",
    "SET SESSION max_statement_time={seconds}",
)


def _install_mysql_statement_timeout(engine: Any, seconds: int) -> None:
    """Ask a MySQL-or-MariaDB server to bound each statement, after connecting."""
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import event

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
        # Both spellings refused. The budget already reports no ceiling for this
        # family (see applies_statement_timeout), so nothing is being
        # misrepresented -- but a query that then runs long has a reason, and
        # this is the only place that knows it.
        logger.debug(
            "neither %s applied; probe queries on this server are unbounded",
            " nor ".join(t.split("=")[0] for t in _MYSQL_TIMEOUT_STATEMENTS),
        )

    # event.listen rather than the @event.listens_for decorator: the decorator is
    # untyped, so applying it would make _set_timeout untyped to mypy.
    event.listen(engine, "connect", _set_timeout)


# The connect_arg each dialect family names its client with, so probe traffic is
# tellable apart from ingestion's in the server's own logs. Value is the kwarg
# name; the label itself is the same everywhere.
#
# Deliberately disjoint from _TIMEOUT_CONNECT_ARGS above: nothing here may share
# a key with what that table emits, or one would silently overwrite the other in
# engine_options. Postgres is the one at risk -- its ceiling rides on the libpq
# `options` string -- so attribution uses the driver's own application_name
# parameter rather than a second `-c` setting inside that same string.
#
# Where a dialect offers nothing, it gets nothing. An unlabelled connection is
# honest; a label the server discards is not.
_ATTRIBUTION_CONNECT_ARGS: Dict[str, str] = {
    # pg_stat_activity.application_name, and %a in log_line_prefix.
    "postgresql": "application_name",
    "postgres": "application_name",
    "cockroachdb": "application_name",
    # Redshift takes the same parameter but surfaces it elsewhere: it forked
    # from Postgres 8.0, whose pg_stat_activity has no application_name column
    # yet. The session has it (current_setting), and STL_CONNECTION_LOG records
    # it per connection for anyone holding that grant.
    "redshift": "application_name",
    # MySQL has no application_name. PyMySQL sends program_name as a connection
    # attribute instead, which surfaces in
    # performance_schema.session_connect_attrs -- weaker, since it names the
    # session rather than each statement, but it is what this family offers.
    "mysql": "program_name",
    "mariadb": "program_name",
}


def _scheme_of(url: str) -> str:
    return url.split("://", 1)[0].split("+", 1)[0].lower()


def install_statement_timeout(engine: Any, url: str, seconds: Optional[int]) -> None:
    """Apply a statement ceiling that connect_args cannot carry, if this dialect
    needs one. A no-op for every dialect whose ceiling is already on the engine."""
    if seconds is None or seconds <= 0:
        return
    scheme = _scheme_of(url)
    if scheme in _MYSQL_SCHEMES:
        _install_mysql_statement_timeout(engine, seconds)
    elif scheme == _REDSHIFT_SCHEME:
        _install_redshift_statement_timeout(engine, seconds)


def _timeout_connect_args(url: str, seconds: Optional[int]) -> Dict[str, Any]:
    if seconds is None or seconds <= 0:
        return {}
    builder = _TIMEOUT_CONNECT_ARGS.get(_scheme_of(url))
    return builder(seconds) if builder else {}


def _attribution_connect_args(url: str) -> Dict[str, Any]:
    kwarg = _ATTRIBUTION_CONNECT_ARGS.get(_scheme_of(url))
    return {kwarg: PROBE_QUERY_LABEL} if kwarg else {}


def applies_statement_timeout(url: str, seconds: Optional[int]) -> bool:
    """Whether a server-side ceiling can be *shown* to bound every probe statement.

    The connect_args dialects qualify: the setting rides on the connection
    itself, so it is deterministic and covers whatever is then sent. Redshift
    qualifies too, on the same reasoning by a different route -- its ceiling is
    a post-connect SET, but of an unambiguous setting that is not allowed to
    fail quietly, so a connection that exists has it.

    The MySQL family is deliberately excluded even though install_statement_timeout
    still makes the attempt, for two independent reasons:

    - Whether either variable exists is not knowable from the URL. A server with
      neither leaves the listener with nothing to do, and it finds out after this
      function has already answered.
    - Where it does work, MySQL's max_execution_time bounds read-only SELECTs and
      nothing else, so the Inspector's SHOW-based listings (containers, tables,
      views) stay unbounded regardless. MariaDB's max_statement_time is broader,
      and the URL cannot tell us which server we have.

    So the attempt stays as best-effort defence and the ceiling is reported absent.
    Understating a protection is the safe direction; the alternative is the failure
    the QueryBudget docstring warns about -- a ceiling that reads as present and is
    not. Reporting it per command, true for `sql` and false for the listings, would
    need a budget attached to commands rather than to the provider.
    """
    if seconds is None or seconds <= 0:
        return False
    scheme = _scheme_of(url)
    return scheme in _TIMEOUT_CONNECT_ARGS or scheme == _REDSHIFT_SCHEME


def effective_budget(url: str, budget: QueryBudget) -> QueryBudget:
    """The budget as it will actually be enforced for this dialect.

    The declared default carries a timeout, but only some dialects have a knob to
    apply it through. Reporting the declared value on the rest would be the failure
    the QueryBudget docstring warns about -- a ceiling that reads as present and is
    not -- so the timeout is dropped where nothing applies it.
    """
    if applies_statement_timeout(url, budget.timeout_seconds):
        return budget
    return QueryBudget(timeout_seconds=None, max_bytes_billed=budget.max_bytes_billed)


def engine_options(
    config: object, budget: Optional[QueryBudget] = None
) -> Dict[str, Any]:
    # SQLAlchemy engine kwargs are heterogeneous (connect_args dict, pool ints,
    # bools, ...), so values are genuinely Any.
    #
    # `options`, and deliberately not get_options(). Every SQLAlchemy engine
    # ingestion builds passes `**config.options` -- sql_common.get_inspectors,
    # the generic and unity profilers, athena, oracle, clickhouse, mysql,
    # teradata, and unity's hive_metastore_proxy. Nothing in that path calls
    # get_options(); its callers are Snowflake's own connector and Fivetran's
    # destination readers, neither of which builds an engine through here.
    #
    # Preferring it therefore made the probe read a *different* dict from
    # ingestion on any config that has both -- unity-catalog is the one such
    # config in this family (get_options() returns extra_client_options, while
    # unity/source.py hands self.config.options to the metastore proxy). A probe
    # that connects with different options than ingestion is wrong about the one
    # thing it exists to be right about.
    options: Dict[str, Any] = {}
    plain = getattr(config, "options", None)
    if isinstance(plain, dict):
        options = dict(plain)

    if budget is None:
        return options

    # Additive: a connector's own connect_args (ssl, auth plugins) must survive,
    # so merge into a copy rather than replacing the dict it handed us.
    url_getter = getattr(config, "get_sql_alchemy_url", None)
    url = url_getter() if callable(url_getter) else ""
    connect_args = dict(options.get("connect_args") or {})
    unchanged = dict(connect_args)
    # The two merge differently, on purpose. A label defers to whatever the
    # recipe already set -- it is the user's connection to name, and a recipe
    # that names it has said what it wants called. The ceiling does not defer:
    # a safety control a recipe can switch off by naming the same key is not a
    # control.
    for key, value in _attribution_connect_args(str(url)).items():
        connect_args.setdefault(key, value)
    connect_args.update(_timeout_connect_args(str(url), budget.timeout_seconds))
    if connect_args != unchanged:
        options["connect_args"] = connect_args
    return options


def _source_class_for(config: object) -> Type[SQLAlchemySource]:
    """The Source class whose get_identifier() the probe should call for this
    config's Table level.

    Resolved by naming convention (FooConfig -> FooSource) from the config's
    own module, rather than a hardcoded per-connector table: every SQL
    connector that overrides get_identifier at the Source level (postgres,
    db2, vertica, starrocks, teradata, mssql) defines both classes in the same
    file, so no extra import is needed here -- that module is already loaded,
    since `config` is a live instance of a class it defines. Falls back to
    SQLAlchemySource itself when the convention doesn't resolve to a subclass
    (e.g. Hana's Source lives in a different module than its config); that
    default is correct there too, since Hana's Source doesn't override
    get_identifier, so it would resolve to the same base method anyway.

    Redshift and Unity Catalog reuse SQLCommonConfig's default probe (this
    module) for their Table level, but their real Source classes don't extend
    SQLAlchemySource at all -- their actual ingestion identifiers
    (`database.schema.table` / `catalog.schema.table`) are built ad hoc
    elsewhere, not via a get_identifier this shim can call. Those two declare
    their own answer instead, through SQLCommonConfig.probe_filter_target
    (checked in _identifier_target before this function ever runs) -- not by
    special-casing their source_type here, which would make this module the
    one place a new per-connector override had to be wired in by hand.
    """
    config_cls = type(config)
    name = config_cls.__name__
    if name.endswith(_CONFIG_CLASS_SUFFIX):
        module = sys.modules.get(config_cls.__module__)
        candidate = getattr(
            module, name[: -len(_CONFIG_CLASS_SUFFIX)] + _SOURCE_CLASS_SUFFIX, None
        )
        if isinstance(candidate, type) and issubclass(candidate, SQLAlchemySource):
            return candidate
    return SQLAlchemySource


def _shim_inspector(
    config: _SqlAlchemyUrlConfig, database: Optional[str] = None
) -> SimpleNamespace:
    """A stand-in Inspector exposing only what get_db_name reads --
    inspector.engine.url.database (sql_common.py:422-430).

    When `database` is known -- the Table level has a Database ancestor in its
    parent_path (postgres, mssql; see _identifier_target) -- the shim reports
    that name directly rather than parsing a connection string for it:
    get_db_name never reads anything else off the URL, and postgres/mssql's own
    get_sql_alchemy_url overrides disagree on the keyword for "this database"
    (database= vs current_db=), so resolving one here would mean special-casing
    per connector for a value we already have in hand.

    Otherwise parses the connector's own default SQLAlchemy URL instead of
    opening a connection, unlike the real Inspector that SqlAlchemyMetadataProbe
    builds to list tables/views/columns.
    """
    if database is not None:
        return SimpleNamespace(
            engine=SimpleNamespace(url=SimpleNamespace(database=database))
        )
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy.engine import make_url

    url = make_url(config.get_sql_alchemy_url())
    return SimpleNamespace(engine=SimpleNamespace(url=url))


def _identifier_target(ctx: ClassifyContext) -> str:
    """The exact string the connector's own get_identifier would use for this
    table/view node -- never a reimplementation of it (see _source_class_for).

    Checks SQLCommonConfig.probe_filter_target first: a connector whose real
    Source doesn't extend SQLAlchemySource (Redshift, Unity Catalog) declares
    its own identifier there instead of through get_identifier, since this
    module has no Source subclass to resolve for it. Every other SQL config
    inherits the default (returns None), so this is a no-op for them.

    Otherwise builds the resolved Source class via __new__ (bypassing
    __init__, which fires ingestion telemetry -- see
    SQLAlchemySource.__init__ -- and needs a PipelineContext a read-only probe
    doesn't have) so that overrides calling super() (e.g. Db2's uppercasing
    get_db_name) resolve exactly as they would on a real instance:
    isinstance(shim, source_cls) holds, since the shim IS an (uninitialized)
    instance of that class.

    Falls back to the node's plain fqn, recording the reason via ctx.warn,
    when an override reaches for source state normally set outside __init__
    that this shim doesn't carry (e.g. StarRocks's _current_catalog, primed
    below since __init__'s own value is known and cheap to reproduce; a case
    with no such known value would still land here).
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
    # A Database level above the container (postgres, mssql; see _build) makes
    # parent_path (database, schema) instead of (schema,) -- the extra element
    # is the real, connectable database this node lives under, distinct from
    # whatever config.database/initial_database would otherwise default to.
    database = ctx.parent_path[0] if len(ctx.parent_path) > 1 else None
    # getattr, not a direct call: every real SQLCommonConfig subclass declares
    # this (see sql_config.py), but some test doubles in this test suite are a
    # bare SimpleNamespace carrying only the few attributes their test needs.
    probe_filter_target = getattr(ctx.config, "probe_filter_target", None)
    override = (
        probe_filter_target(schema=schema, entity=ctx.name, warn=ctx.warn)
        if callable(probe_filter_target)
        else None
    )
    if override is not None:
        return override
    source_cls = _source_class_for(ctx.config)
    shim = source_cls.__new__(source_cls)
    shim.config = ctx.config
    # mssql reads this during ingestion (set per-database as it iterates, and
    # takes priority over config.database -- see SQLServerSource.get_identifier);
    # `database` reproduces that same per-database value here whenever a
    # Database level supplies one, rather than falling back to config.database
    # for every node regardless of which database it actually lives under. Not
    # every Source declares this attribute (only mssql's does), hence setattr
    # rather than a plain assignment mypy could check against a type that
    # doesn't have it.
    setattr(shim, "current_database", database)  # noqa: B010
    # StarRocksSource.get_identifier reads self._current_catalog, which is
    # only reassigned once real catalog enumeration starts
    # (StarRocksSource.get_inspectors sets it before every table); at
    # __init__ -- and thus on this shim -- it is None, and get_identifier's
    # own fallback for a None catalog is the literal "default_catalog",
    # StarRocks's name for its built-in internal catalog that most tables
    # actually live in. This mirrors current_database above: reproducing
    # __init__ state, not guessing at what a real run would set.
    setattr(shim, "_current_catalog", None)  # noqa: B010
    # Built outside the try: an AttributeError raised while resolving the
    # config's own SQLAlchemy URL (e.g. a typo'd config override, which
    # pydantic v2 itself raises as AttributeError) is not "get_identifier
    # needs source state the probe doesn't have" and must not be reported as
    # such.
    inspector = _shim_inspector(ctx.config, database=database)
    # lazy: sqlalchemy is only needed once a probe actually runs (see _engine)
    from sqlalchemy.engine.reflection import Inspector

    try:
        target = source_cls.get_identifier(
            shim,
            schema=schema,
            entity=ctx.name,
            # Cast, not Any: inspector only ever stands in for what
            # get_db_name reads (see _shim_inspector) -- it is deliberately
            # never a real Inspector, so isinstance would legitimately fail
            # here; get_identifier's signature still requires one.
            inspector=cast(Inspector, inspector),
        )
    except AttributeError as exc:
        # Message is connector-wide (source_cls + the missing attribute), not
        # per-node: ctx.warn dedupes on the message (see
        # check_filters' warn closure), so including ctx.fqn here
        # would defeat that dedupe and flood ProbeMethodResult.warnings with one
        # near-identical entry per table.
        ctx.warn(
            f"{source_cls.__name__}.get_identifier needs source state the "
            f"probe doesn't have ({exc}); using the plain fqn as the filter "
            "target instead"
        )
        return ctx.fqn
    assert isinstance(target, str)
    return target


# A per-database SQLAlchemy URL for the connector's own get_sql_alchemy_url --
# postgres names the keyword `database`, mssql names it `current_db` (see
# database_url= on _build), so this is never a single call signature shared
# across connectors; each connector supplies its own.
DatabaseUrl = Callable[[Any, str], str]

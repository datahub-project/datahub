"""The SQL family's table match target: the connector's own get_identifier,
called on an uninitialised Source with a stand-in Inspector, so `probe filter`
matches the string ingestion matches without a connection.

Declarations come first (probe_filter_target, then a Qualifier field); the
shim covers the rest, and degrades to the plain fqn with a warning when a
get_identifier needs state only ingestion sets.
"""

import sys
from dataclasses import dataclass
from typing import Optional, Protocol, Sequence, Type, cast

from sqlalchemy.engine import make_url
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.agent.declarations import declares_qualifier
from datahub.ingestion.agent.sql_passthrough import CatalogRows
from datahub.ingestion.agent.verdicts import ClassifyContext, parent_required
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_probe_verdicts import (
    qualified_table_target,
    qualifying_container,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport

# FooConfig -> FooSource (see _source_class_for).
_CONFIG_CLASS_SUFFIX = "Config"
_SOURCE_CLASS_SUFFIX = "Source"

# The stable part of the degrade warning. A contract test imports it to tell a
# degraded source from one whose identifier really is the bare name.
IDENTIFIER_DEGRADE_MARKER = "needs source state the probe doesn't have"


class DbApiCursor(Protocol):
    """The part of a DB-API 2.0 cursor execute_on_cursor uses. Positional-only,
    since drivers name these parameters differently (redshift_connector's
    fetchmany takes `num`)."""

    @property
    def description(self) -> Optional[Sequence[Sequence[object]]]: ...

    def execute(self, operation: str, /) -> object: ...

    def fetchmany(self, size: int, /) -> Sequence[Sequence[object]]: ...

    def close(self) -> object: ...


def execute_on_cursor(cursor: DbApiCursor, query: str, limit: int) -> CatalogRows:
    """Run a gate-cleared `sql` query on a raw DB-API cursor, then close it.

    No parameters at all: pytds and redshift_connector rewrite the statement
    for their paramstyle whenever a parameter set is passed (SQLAlchemy's
    exec_driver_sql always passes one), so a `%` in a LIKE literal would
    break. With none, the SQL reaches the server as written.
    """
    try:
        cursor.execute(query)
        columns = [str(d[0]) for d in cursor.description or []]
        rows = cursor.fetchmany(limit) if cursor.description else []
        return CatalogRows(columns=columns, rows=[list(row) for row in rows])
    finally:
        cursor.close()


def _source_class_for(config: object) -> Type[SQLAlchemySource]:
    """The Source class whose get_identifier() judges this config's tables.

    By naming convention (FooConfig -> FooSource) in the config's own module,
    which is loaded already: a connector overriding get_identifier defines
    both classes there. Otherwise SQLAlchemySource, whose get_identifier is
    what an unconventional pair inherits. A Source outside SQLAlchemySource
    declares its identifier instead (see _identifier_target).
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


def config_only_source(config: SQLCommonConfig) -> SQLAlchemySource:
    """This config's Source class (_source_class_for), built with __new__,
    since its __init__ opens connections and emits telemetry. It carries the
    config and a fresh report, and nothing else: state __init__ sets belongs
    in a class-level default.

    Both uses read only those two. `probe filter` calls its get_identifier
    (_identifier_target). `probe run views` calls its _get_view_names, the
    hook a connector overrides to list what ingestion judges by view_pattern
    (PostgresSource adds materialized views), and reads that listing's
    warnings back from the report.
    """
    source_cls = _source_class_for(config)
    shim = source_cls.__new__(source_cls)
    shim.config = config
    shim.report = SQLSourceReport()
    return shim


class _HasDatabase(Protocol):
    """The one URL attribute get_db_name reads."""

    @property
    def database(self) -> Optional[str]: ...


@dataclass(frozen=True)
class _StandInUrl:
    """A URL knowing only its database name, when the caller gave it."""

    database: Optional[str]


@dataclass(frozen=True)
class _StandInEngine:
    url: _HasDatabase


@dataclass(frozen=True)
class _StandInInspector:
    """What _shim_inspector hands to get_identifier: the one attribute path
    get_db_name reads, `inspector.engine.url.database`, declared so the
    contract is visible. Cast to Inspector at the call site."""

    engine: _StandInEngine


def _shim_inspector(
    config: SQLCommonConfig, database: Optional[str] = None
) -> _StandInInspector:
    """A stand-in Inspector exposing only `engine.url.database`: the given
    database (a Database ancestor), else the one in the connector's own URL,
    parsed without connecting."""
    if database is not None:
        return _StandInInspector(engine=_StandInEngine(url=_StandInUrl(database)))
    # A real SQLAlchemy URL already satisfies _HasDatabase.
    url = make_url(config.get_sql_alchemy_url())
    return _StandInInspector(engine=_StandInEngine(url=url))


def sql_table_match_target(
    config: SQLCommonConfig, ctx: ClassifyContext
) -> Optional[str]:
    """SQLCommonConfig.probe_match_target: the identifier ingestion matches a
    table or view against (_identifier_target), or None to judge the bare
    name."""
    if not _judged_on_identifier(config, ctx):
        return None
    return _complete_target(_identifier_target(ctx), ctx)


def _judged_on_identifier(config: SQLCommonConfig, ctx: ClassifyContext) -> bool:
    """Whether this node is matched on an identifier. Containers and
    top-level kinds keep the bare name (a qualified schema is
    probe_verdict_override's, which reports its own target)."""
    if ctx.kind in (
        DatasetContainerSubTypes.SCHEMA,
        DatasetContainerSubTypes.DATABASE,
    ):
        return False
    # () is a kind with no container; None one the chain does not describe,
    # so neither is a table or view whose identifier the shim could build.
    if not config.probe_ancestor_kinds(kind=ctx.kind):
        return False
    # Without the container the shim builds ".orders", which ingestion
    # never matches.
    return not parent_required(ctx)


def _complete_target(target: str, ctx: ClassifyContext) -> Optional[str]:
    """`target` when it is a whole identifier, else None after warning: a
    resolver can hand back nothing usable, or miss a component."""
    if not target:
        # Names no object, so it shows once.
        ctx.warn(
            "the connector's identifier resolver returned nothing usable, so "
            "these were judged on their bare names; the verdict may not be "
            "the one ingestion makes"
        )
        return None
    # A component is missing: an empty container, or nothing before the first
    # dot or after the last one that the name itself does not explain. Not
    # any `..`: a quoted name may hold dots, and ingestion matches the
    # identifier as built.
    if (
        any(not segment for segment in ctx.parent_path)
        or target.startswith(".")
        or (target.endswith(".") and not ctx.name.endswith("."))
    ):
        # Per object, so it names the identifier.
        ctx.warn(
            f"could not build a complete identifier for '{ctx.name}' (got "
            f"'{target}'); judged on its bare name instead"
        )
        return None
    return target


def _identifier_target(ctx: ClassifyContext) -> str:
    """The string the connector's own get_identifier builds for this table or
    view, never a reimplementation of it.

    A declaration wins (_declared_target): an overridden probe_filter_target,
    else a Qualifier field (`container.schema.entity`). Which provider a
    connector brings is never read as one. Otherwise the connector's
    get_identifier is called on config_only_source(config), so overrides
    calling super() resolve as on a real instance; state ingestion sets while
    walking belongs in probe_filter_target. Without either, the node degrades
    to its plain fqn with a warning. A hook's answer that is not a string
    comes back as "", which _complete_target refuses.
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
    # (database, schema) when a Database level is above the container.
    database = ctx.parent_path[-2] if len(ctx.parent_path) > 1 else None
    declared = _declared_target(ctx, database=database, schema=schema)
    if declared is not None:
        return declared
    return _get_identifier_target(ctx, database=database, schema=schema)


def _declared_target(
    ctx: ClassifyContext, database: Optional[str], schema: str
) -> Optional[str]:
    """The target the connector declares, or None to leave it to the
    get_identifier shim: an overridden probe_filter_target's answer, else a
    Qualifier field's `container.schema.entity`, else None."""
    if _overrides_filter_target(ctx.config):
        # getattr: test doubles may be a bare SimpleNamespace. An override
        # answering None reported its own degrade, so no Qualifier is tried.
        hook = getattr(ctx.config, "probe_filter_target", None)
        if not callable(hook):
            return None
        answer = hook(schema=schema, entity=ctx.name, warn=ctx.warn, database=database)
        if answer is None or isinstance(answer, str):
            return answer
        # Not a string: unusable, as an empty one is, which _complete_target
        # refuses with a warning rather than letting the shim overrule a
        # connector that did answer.
        return ""
    if not declares_qualifier(ctx.config):
        return None
    # Resolved as at the Schema level, so an authoritative Qualifier beats
    # --parent at both levels. An empty --parent names no database.
    container = qualifying_container(ctx.config, [database] if database else [])
    return qualified_table_target(container, schema, ctx.name, ctx.warn)


def _overrides_filter_target(config: object) -> bool:
    """Whether the config's class replaces SQLCommonConfig's
    probe_filter_target, which always leaves the target to the shim."""
    return (
        getattr(type(config), "probe_filter_target", None)
        is not SQLCommonConfig.probe_filter_target
    )


def _get_identifier_target(
    ctx: ClassifyContext, database: Optional[str], schema: str
) -> str:
    """The connector's get_identifier on config_only_source(config), or the
    plain fqn, with a warning, when it needs state only ingestion sets."""
    shim = config_only_source(ctx.config)
    source_cls = type(shim)
    # Outside the try: an AttributeError building the URL is not missing
    # source state.
    inspector = _shim_inspector(ctx.config, database=database)
    try:
        target = source_cls.get_identifier(
            shim,
            schema=schema,
            entity=ctx.name,
            # A declared stand-in, deliberately not a real Inspector.
            inspector=cast(Inspector, inspector),
        )
    except AttributeError as exc:
        # Connector-wide, so the warning dedupes to one; class and attribute
        # only, since the exception text can quote config or server values.
        missing = f" {exc.name!r}" if isinstance(exc.name, str) else ""
        ctx.warn(
            f"{source_cls.__name__}.get_identifier {IDENTIFIER_DEGRADE_MARKER} "
            f"({type(exc).__name__}{missing}); using the plain fqn as the filter target instead"
        )
        return ctx.fqn
    assert isinstance(target, str)
    return target

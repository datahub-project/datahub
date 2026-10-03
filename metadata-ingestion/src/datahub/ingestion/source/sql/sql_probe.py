"""The SQL family's table match target: the connector's own get_identifier,
called on an uninitialised Source with a stand-in Inspector, so `probe filter`
matches the string ingestion matches without a connection.

Declarations come first (probe_filter_target, then a Qualifier field); the
shim covers the rest, and degrades to the plain fqn with a warning when a
get_identifier needs state only ingestion sets.
"""

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Protocol, Type, cast

from datahub.ingestion.agent.verdicts import ClassifyContext
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_report import SQLSourceReport

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

# FooConfig -> FooSource (see _source_class_for).
_CONFIG_CLASS_SUFFIX = "Config"
_SOURCE_CLASS_SUFFIX = "Source"

# The stable part of the degrade warning. A contract test imports it to tell a
# degraded source from one whose identifier really is the bare name.
IDENTIFIER_DEGRADE_MARKER = "needs source state the probe doesn't have"


class _SqlAlchemyUrlConfig(Protocol):
    """The one method _shim_inspector needs, without importing SQLCommonConfig
    for a type hint."""

    def get_sql_alchemy_url(self) -> str: ...


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


def view_listing_source(config: "SQLCommonConfig") -> SQLAlchemySource:
    """This config's Source class, built with __new__ as _identifier_target
    builds it, carrying the config and a fresh report: what its
    _get_view_names reads. That is the hook a connector overrides to list
    view-like objects ingestion judges by view_pattern (PostgresSource adds
    materialized views), so `probe run views` calls it rather than restating
    which dialects have them. Its warnings land in the report.
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
    config: _SqlAlchemyUrlConfig, database: Optional[str] = None
) -> _StandInInspector:
    """A stand-in Inspector exposing only `engine.url.database`: the given
    database (a Database ancestor), else the one in the connector's own URL,
    parsed without connecting."""
    if database is not None:
        return _StandInInspector(engine=_StandInEngine(url=_StandInUrl(database)))
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy.engine import make_url

    # A real SQLAlchemy URL already satisfies _HasDatabase.
    url = make_url(config.get_sql_alchemy_url())
    return _StandInInspector(engine=_StandInEngine(url=url))


def _identifier_target(ctx: ClassifyContext) -> str:
    """The string the connector's own get_identifier builds for this table or
    view, never a reimplementation of it.

    A declaration wins: probe_filter_target, then a Qualifier field
    (`container.schema.entity`). Which provider a connector brings is never
    read as one. Otherwise the Source class is built with __new__ (its
    __init__ opens connections and emits telemetry), so overrides calling
    super() resolve as on a real instance. It carries the config and nothing
    else: state __init__ sets belongs in a class-level default, state
    ingestion sets while walking in probe_filter_target. Without either, the
    node degrades to its plain fqn with a warning.
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
    # (database, schema) when a Database level is above the container.
    database = ctx.parent_path[-2] if len(ctx.parent_path) > 1 else None
    # getattr: test doubles may be a bare SimpleNamespace.
    probe_filter_target = getattr(ctx.config, "probe_filter_target", None)
    override = (
        probe_filter_target(
            schema=schema, entity=ctx.name, warn=ctx.warn, database=database
        )
        if callable(probe_filter_target)
        else None
    )
    if override is not None:
        return override
    # lazy: keeps introspect and SQLCommonConfig off this module's import path.
    from datahub.ingestion.agent.introspect import (
        declared_qualifier,
        declares_qualifier,
    )
    from datahub.ingestion.source.sql.sql_config import (
        SQLCommonConfig,
        qualified_table_target,
    )

    # An override returning None reported its own degrade; do not warn twice.
    declared_own = (
        getattr(type(ctx.config), "probe_filter_target", None)
        is not SQLCommonConfig.probe_filter_target
    )
    if not declared_own and declares_qualifier(ctx.config):
        # The container is resolved as at the Schema level, so an
        # authoritative Qualifier beats --parent at both levels.
        declared, authoritative = declared_qualifier(ctx.config)
        container = declared if (authoritative and declared) else (database or declared)
        target = qualified_table_target(container, schema, ctx.name, ctx.warn)
        if target is not None:
            return target
    source_cls = _source_class_for(ctx.config)
    shim = source_cls.__new__(source_cls)
    shim.config = ctx.config
    # Outside the try: an AttributeError building the URL is not missing
    # source state.
    inspector = _shim_inspector(ctx.config, database=database)
    # lazy: sqlalchemy only once a probe runs
    from sqlalchemy.engine.reflection import Inspector

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

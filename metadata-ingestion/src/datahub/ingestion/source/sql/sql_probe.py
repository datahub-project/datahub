import sys
from dataclasses import dataclass
from typing import Optional, Protocol, Type, cast

from datahub.ingestion.agent.verdicts import ClassifyContext
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource

# Naming convention linking a config class to the Source class whose
# get_identifier() owns it -- see _source_class_for.
_CONFIG_CLASS_SUFFIX = "Config"
_SOURCE_CLASS_SUFFIX = "Source"

# The stable part of the get_identifier degrade warning.
#
# A build-time contract test tells this degrade apart from a connector that
# legitimately returns a bare name, and the only thing separating the two is
# this message -- both paths return ctx.fqn. That test matched a substring of
# the prose, so rewording the warning would have left `reason` None and
# silently skipped a genuinely degraded source. Named here and imported
# there, so the coupling is explicit and moves with the text.
IDENTIFIER_DEGRADE_MARKER = "needs source state the probe doesn't have"


class _SqlAlchemyUrlConfig(Protocol):
    """The one method _shim_inspector needs -- every SQLCommonConfig subclass
    has it, but that base class isn't imported here to avoid pulling its own
    (heavier) dependency chain into this module just for a type hint."""

    def get_sql_alchemy_url(self) -> str: ...


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


def _matches_a_qualified_name(config: object) -> bool:
    """Whether this connector's ingestion matches `container.schema.entity`.

    Declared, not inferred. Two ways to say it, and both are things the
    connector already says for other reasons:

      it brings its own probe provider -- Snowflake and BigQuery are not
      SQLAlchemy-backed at all, so there is no get_identifier to ask

      it marks a field with Qualifier -- Redshift's `database`, BigQuery's
      `project_ids`; a connector that names the container it qualifies with
      is telling you it qualifies

    Everything else goes through the shim, where SQLAlchemySource's own
    get_identifier (or a connector's override of it) is the authority.

    This replaced `_source_class_for(config) is not SQLAlchemySource`, which
    asked whether a class named FooSource sits in the same file as FooConfig
    -- a filename convention that already existed to pick which
    get_identifier to call, and was wrong for arity twice over:

      hana   HanaSource is in hana/hana.py, its config in
             hana/hana_config.py. The match failed, Hana was treated as
             fully qualified, and the probe told the caller to pass a
             database. Doing so produced MYDB.MYSCHEMA.T1 -- three parts
             HanaSource, which does not override get_identifier, never
             builds -- with no warning. A wrong answer reached by following
             the tool's own advice.

      redshift  The opposite. RedshiftSource is not in redshift/config.py
             either, so the match ALSO failed -- and there the failure gave
             the right answer, because Redshift genuinely is three-part.
             Reading the provider instead fixed Hana and broke Redshift,
             which is what a sweep of all 29 SQL sources caught: the
             provider says SqlAlchemyMetadataProbe for both.

    Neither question -- what file is the class in, which provider does it
    reuse -- is the arity question. This one is.
    """
    getter = getattr(type(config), "probe_provider_class", None)
    if callable(getter):
        # lazy: keeps sqlalchemy off the config import path
        from datahub.ingestion.source.sql.sqlalchemy_probe import (
            SqlAlchemyMetadataProbe,
        )

        try:
            if getter() is not SqlAlchemyMetadataProbe:
                return True
        except ImportError:
            # Only this. Every probe_provider_class is a lazy import of a
            # provider module, so an uninstalled extra is the one failure
            # that is about the environment rather than the connector, and
            # falling through to declares_qualifier is the right answer for
            # it.
            #
            # `except Exception: pass` also swallowed a provider that is
            # installed and broken, and the fallback is a WEAKER question --
            # declares_qualifier reads a marker, this reads the provider --
            # so a defect silently downgraded the arity answer with nothing
            # to show for it. This function returns a bool and has no warn
            # channel, so propagating is the only way it can be seen.
            pass
    # lazy: agent.introspect is only needed once a probe runs
    from datahub.ingestion.agent.introspect import declares_qualifier

    return declares_qualifier(config)


class _HasDatabase(Protocol):
    """The one URL attribute get_db_name reads."""

    @property
    def database(self) -> Optional[str]: ...


@dataclass(frozen=True)
class _StandInUrl:
    """A URL that only knows its database name, for the case where we
    already have it and never needed to parse a connection string."""

    database: Optional[str]


@dataclass(frozen=True)
class _StandInEngine:
    url: _HasDatabase


@dataclass(frozen=True)
class _StandInInspector:
    """What _shim_inspector hands to get_identifier.

    Typed rather than a nest of SimpleNamespace. The surface is one
    attribute path -- `inspector.engine.url.database` -- and writing it as
    three anonymous namespaces made that contract invisible: nothing said
    which attributes were load-bearing, and adding a fourth would have
    type-checked. Declaring it means the stand-in fails at the point it
    stops matching what get_db_name reads, rather than at the AttributeError
    _identifier_target has to catch downstream.

    Still cast to Inspector at the call site, because get_identifier's
    signature requires one and this is deliberately not a real Inspector.
    The cast is now over a declared shape instead of an anonymous one.
    """

    engine: _StandInEngine


def _shim_inspector(
    config: _SqlAlchemyUrlConfig, database: Optional[str] = None
) -> _StandInInspector:
    """A stand-in Inspector exposing only what get_db_name reads --
    inspector.engine.url.database.

    When `database` is known (the node has a Database ancestor), the shim
    reports that name directly: get_db_name reads nothing else off the URL,
    and building a per-database URL would mean calling each connector's
    get_sql_alchemy_url with its own keyword for the database.

    Otherwise parses the connector's own default SQLAlchemy URL instead of
    opening a connection, unlike the real Inspector that SqlAlchemyMetadataProbe
    builds to list tables/views/columns.
    """
    if database is not None:
        return _StandInInspector(engine=_StandInEngine(url=_StandInUrl(database)))
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy.engine import make_url

    # A real SQLAlchemy URL already satisfies _HasDatabase.
    url = make_url(config.get_sql_alchemy_url())
    return _StandInInspector(engine=_StandInEngine(url=url))


def _identifier_target(ctx: ClassifyContext) -> str:
    """The exact string the connector's own get_identifier would use for this
    table/view node -- never a reimplementation of it (see _source_class_for).

    Checks SQLCommonConfig.probe_filter_target first: a connector whose
    identifier this shim cannot build declares it there -- its real Source is
    not a SQLAlchemySource, or its get_identifier reads state ingestion sets
    while it walks. Every other SQL config inherits the default (returns
    None), so this is a no-op for them.

    Otherwise builds the resolved Source class via __new__ (bypassing
    __init__, which fires ingestion telemetry -- see
    SQLAlchemySource.__init__ -- and needs a PipelineContext a read-only probe
    doesn't have) so that overrides calling super() (e.g. Db2's uppercasing
    get_db_name) resolve exactly as they would on a real instance:
    isinstance(shim, source_cls) holds, since the shim IS an (uninitialized)
    instance of that class.

    The shim carries the config and nothing else. Source state a
    get_identifier also reads is the connector's to supply: a class-level
    default for state __init__ sets, or the config's probe_filter_target for
    state ingestion sets as it walks (the database being read). Without
    either, the node falls back to its plain fqn, with the reason recorded
    via ctx.warn.
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
    # A Database level above the container makes parent_path (database,
    # schema) instead of (schema,) -- the extra element is the real,
    # connectable database this node lives under, distinct from whatever
    # config.database/initial_database would otherwise default to.
    database = ctx.parent_path[-2] if len(ctx.parent_path) > 1 else None
    # getattr, not a direct call: every real SQLCommonConfig subclass declares
    # this (see sql_config.py), but some test doubles in this test suite are a
    # bare SimpleNamespace carrying only the few attributes their test needs.
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
    # Only when the connector declared nothing. Unity Catalog declares an
    # override that deliberately returns None when it cannot pin one catalog,
    # with its own warning explaining the degrade -- running the generic path
    # after it added a second warning saying the same thing.
    # lazy: this module keeps SQLCommonConfig off its import path (see
    # _SqlAlchemyUrlConfig) and needs it only for this identity check.
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    declared_own = (
        getattr(type(ctx.config), "probe_filter_target", None)
        is not SQLCommonConfig.probe_filter_target
    )
    if not declared_own and _matches_a_qualified_name(ctx.config):
        # No Source to ask, so the shim below would answer `schema.entity`
        # -- which drops the top level for every non-SQLAlchemy SQL source.
        # All four match `container.schema.entity`, so the framework builds
        # it rather than each connector declaring the same f-string.
        #
        # The container comes from the same resolution the Schema level
        # uses, not from parent_path alone: Qualifier(authoritative=True)
        # is how Redshift says its single configured database wins over a
        # --parent naming another, and the table level has to honour that
        # too or the two levels answer about different databases.
        # lazy: agent.introspect is only needed once a probe runs
        from datahub.ingestion.agent.introspect import declared_qualifier

        declared, authoritative = declared_qualifier(ctx.config)
        container = declared if (authoritative and declared) else (database or declared)
        if container:
            return f"{container}.{schema}.{ctx.name}"
        ctx.warn(
            "no parent container given, so these were judged on "
            "'schema.entity'; this source matches a fully qualified name, so "
            "pass the containing database/project to get the verdict "
            "ingestion actually makes"
        )
    source_cls = _source_class_for(ctx.config)
    shim = source_cls.__new__(source_cls)
    shim.config = ctx.config
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
        # Class and attribute name only: the exception text is not ours (a
        # pydantic or driver AttributeError can quote config or server values).
        missing = f" {exc.name!r}" if isinstance(exc.name, str) else ""
        ctx.warn(
            f"{source_cls.__name__}.get_identifier {IDENTIFIER_DEGRADE_MARKER} "
            f"({type(exc).__name__}{missing}); using the plain fqn as the filter target instead"
        )
        return ctx.fqn
    assert isinstance(target, str)
    return target

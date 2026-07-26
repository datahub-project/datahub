import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Protocol, Sequence, Type, cast

from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    ClassifyContext,
    ClientProbe,
    LevelSource,
    ProbeLevel,
    Verdict,
    pattern_verdict,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource

# Naming convention linking a config class to the Source class whose
# get_identifier() owns it -- see _source_class_for.
_CONFIG_CLASS_SUFFIX = "Config"
_SOURCE_CLASS_SUFFIX = "Source"


class _SqlAlchemyUrlConfig(Protocol):
    """The one method _shim_inspector needs -- every SQLCommonConfig subclass
    has it, but that base class isn't imported here to avoid pulling its own
    (heavier) dependency chain into this module just for a type hint."""

    def get_sql_alchemy_url(self) -> str: ...


def engine_options(config: object) -> Dict[str, Any]:
    # SQLAlchemy engine kwargs are heterogeneous (connect_args dict, pool ints,
    # bools, ...), so values are genuinely Any. Prefer get_options() when a config
    # defines it; otherwise fall back to the plain `options` dict. Mirrors the real
    # ingestion path, which passes these to create_engine (e.g. connect_args ssl).
    get_options = getattr(config, "get_options", None)
    if callable(get_options):
        opts = get_options()
        if isinstance(opts, dict):
            return dict(opts)
    options = getattr(config, "options", None)
    if isinstance(options, dict):
        return dict(options)
    return {}


def _engine(config: Any) -> Any:
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import create_engine

    return create_engine(config.get_sql_alchemy_url(), **engine_options(config))


def _inspector(engine: Any) -> Any:
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import inspect

    return inspect(engine)


def _containers(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # SQLAlchemy exposes both schemas and two-tier databases via get_schema_names().
    return _inspector(engine).get_schema_names()


def _tables(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return _inspector(engine).get_table_names(schema=parent_path[0])


def _views(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return _inspector(engine).get_view_names(schema=parent_path[0])


def _columns(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    cols = _inspector(engine).get_columns(parent_path[1], schema=parent_path[0])
    return [str(col["name"]) for col in cols]


def _classify_container(ctx: ClassifyContext) -> Verdict:
    # System catalogs the source drops before the user pattern is even applied.
    if ctx.name.lower() in {s.lower() for s in ctx.config.default_schemas()}:
        return Verdict(False, "default_schema")
    # Same shape as _identifier_target's probe_filter_target check: a config
    # (Redshift's match_fully_qualified_names) can declare its own answer for
    # "is this schema allowed" instead of the generic bare-name check below --
    # e.g. because ingestion matches schema_pattern against "database.schema"
    # once that flag is on (is_schema_allowed, configuration/pattern_utils.py;
    # see RedshiftConfig.probe_schema_verdict_override, the same predicate
    # bigquery_probe.py's _classify_dataset already calls). getattr, not a
    # direct call, for the same SimpleNamespace-test-double reason as there.
    schema_override = getattr(ctx.config, "probe_schema_verdict_override", None)
    verdict = schema_override(schema=ctx.name) if callable(schema_override) else None
    if verdict is not None:
        return Verdict.include() if verdict else Verdict(False, ctx.pattern_field)
    return pattern_verdict(ctx.config, ctx.pattern_field, ctx.name)


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


def _shim_inspector(config: _SqlAlchemyUrlConfig) -> SimpleNamespace:
    """A stand-in Inspector exposing only what get_db_name reads --
    inspector.engine.url.database (sql_common.py:422-430). Parses the
    connector's own SQLAlchemy URL instead of opening a connection, unlike the
    real _inspector() used to list tables/views/columns.
    """
    # lazy: sqlalchemy is only needed once a probe actually runs (see _engine)
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
    that this shim doesn't carry (e.g. mssql's current_database, StarRocks's
    _current_catalog -- both primed below instead, since __init__'s own value
    for each is known and cheap to reproduce; a case with no such known value
    would still land here).
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
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
    # mssql reads this during ingestion (set per-database as it iterates); the
    # probe has no equivalent, so its get_identifier falls back to
    # config.database. Documented as a known limit. Not every Source declares
    # this attribute (only mssql's does), hence setattr rather than a plain
    # assignment mypy could check against a type that doesn't have it.
    setattr(shim, "current_database", None)  # noqa: B010
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
    inspector = _shim_inspector(ctx.config)
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
        # ClientProbe.list_children's warn closure), so including ctx.fqn here
        # would defeat that dedupe and flood ProbeResult.warnings with one
        # near-identical entry per table.
        ctx.warn(
            f"{source_cls.__name__}.get_identifier needs source state the "
            f"probe doesn't have ({exc}); using the plain fqn as the filter "
            "target instead"
        )
        return ctx.fqn
    assert isinstance(target, str)
    return target


def _build(top_kind: ProbeNodeKind) -> ClientProbe:
    # The generic (schema-top) and two-tier (database-top) probes differ only in
    # what the top container is called; its filter (schema_pattern/database_pattern)
    # resolves by convention from top_kind, so it needs no explicit pattern_field.
    return ClientProbe(
        client_factory=_engine,
        close=lambda engine: engine.dispose(),
        levels=[
            ProbeLevel(
                top_kind,
                list_names=_containers,
                classify=_classify_container,
            ),
            ProbeLevel(
                DatasetSubTypes.TABLE,
                sources=[
                    LevelSource(_tables, DatasetSubTypes.TABLE),
                    LevelSource(_views, DatasetSubTypes.VIEW),
                ],
                parent=top_kind,
                filter_target=_identifier_target,
            ),
            ProbeLevel(
                ProbeLeafKind.COLUMN, list_names=_columns, parent=DatasetSubTypes.TABLE
            ),
        ],
    )


# Generic SQL sources are a 2-level namespace: schema -> table -> column.
SQL_PROBE = _build(DatasetContainerSubTypes.SCHEMA)
# Two-tier sources (MySQL, Hive, ...) have no schema layer: the database is the
# top container and is filtered by database_pattern.
TWO_TIER_PROBE = _build(DatasetContainerSubTypes.DATABASE)

SQL_PROBE_HIERARCHY: List[ProbeNodeKind] = SQL_PROBE.hierarchy()
TWO_TIER_PROBE_HIERARCHY: List[ProbeNodeKind] = TWO_TIER_PROBE.hierarchy()


def list_sql_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return SQL_PROBE.list_children(config, parent_path, limit)


def list_two_tier_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return TWO_TIER_PROBE.list_children(config, parent_path, limit)

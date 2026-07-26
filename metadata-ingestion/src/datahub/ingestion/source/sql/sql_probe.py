import contextvars
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence, Type

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
        return (False, "default_schema")
    return pattern_verdict(ctx.config, ctx.pattern_field, ctx.name)


# `_identifier_target` runs deep inside ClientProbe.list_children's per-node
# classification, which has no return channel back up to the ProbeResult it is
# building. A ContextVar (rather than a module-level list) keeps the fallback
# reasons scoped to a single list_children() call, so concurrent/nested probe
# calls cannot bleed warnings into each other.
_identifier_fallback_warnings: "contextvars.ContextVar[Optional[List[str]]]" = (
    contextvars.ContextVar("_identifier_fallback_warnings", default=None)
)


def _record_identifier_fallback(message: str) -> None:
    warnings = _identifier_fallback_warnings.get()
    if warnings is not None:
        warnings.append(message)


def _with_identifier_fallback_warnings(result: ProbeResult) -> ProbeResult:
    collected = _identifier_fallback_warnings.get()
    if collected:
        result.warnings = [*result.warnings, *collected]
    return result


def _source_class_for(config: Any) -> Type[SQLAlchemySource]:
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


def _shim_inspector(config: Any) -> Any:
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

    Falls back to the node's plain fqn, recording the reason as a probe
    warning (see _with_identifier_fallback_warnings), when an override
    reaches for source state this shim doesn't carry -- e.g. StarRocks's
    per-catalog `_current_catalog`, set only during real table enumeration.
    """
    schema = ctx.parent_path[-1] if ctx.parent_path else ""
    # getattr, not a direct call: every real SQLCommonConfig subclass declares
    # this (see sql_config.py), but some test doubles in this test suite are a
    # bare SimpleNamespace carrying only the few attributes their test needs.
    probe_filter_target = getattr(ctx.config, "probe_filter_target", None)
    override = (
        probe_filter_target(schema=schema, entity=ctx.name)
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
    try:
        target = source_cls.get_identifier(
            shim,
            schema=schema,
            entity=ctx.name,
            inspector=_shim_inspector(ctx.config),
        )
    except AttributeError as exc:
        _record_identifier_fallback(
            f"{ctx.fqn}: {source_cls.__name__}.get_identifier needs source state "
            f"the probe doesn't have ({exc}); using the plain fqn as the filter "
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
    token = _identifier_fallback_warnings.set([])
    try:
        result = SQL_PROBE.list_children(config, parent_path, limit)
        return _with_identifier_fallback_warnings(result)
    finally:
        _identifier_fallback_warnings.reset(token)


def list_two_tier_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    token = _identifier_fallback_warnings.set([])
    try:
        result = TWO_TIER_PROBE.list_children(config, parent_path, limit)
        return _with_identifier_fallback_warnings(result)
    finally:
        _identifier_fallback_warnings.reset(token)

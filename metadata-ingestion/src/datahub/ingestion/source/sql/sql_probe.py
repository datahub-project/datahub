from typing import Any, Dict, List, Sequence

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
            ),
            ProbeLevel(ProbeLeafKind.COLUMN, list_names=_columns),
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

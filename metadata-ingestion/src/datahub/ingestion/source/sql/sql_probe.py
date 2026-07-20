from typing import Any, Dict, List

from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    Verdict,
    column_nodes,
    container_nodes,
    table_nodes,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# Generic SQL sources are a 2-level namespace: schema -> table -> column.
SQL_PROBE_HIERARCHY: List[ProbeNodeKind] = [
    DatasetContainerSubTypes.SCHEMA,
    DatasetSubTypes.TABLE,
    ProbeLeafKind.COLUMN,
]

# Two-tier sources (MySQL, Hive, ...) have no schema layer: the database is the
# top container and is filtered by database_pattern. SQLAlchemy still exposes
# databases through get_schema_names(), so the listing mechanics are the same.
TWO_TIER_PROBE_HIERARCHY: List[ProbeNodeKind] = [
    DatasetContainerSubTypes.DATABASE,
    DatasetSubTypes.TABLE,
    ProbeLeafKind.COLUMN,
]


def _table_classifier(config: Any) -> Any:
    table_pattern = config.table_pattern
    view_pattern = config.view_pattern

    def classify_table(name: str, node_fqn: str, is_view: bool) -> Verdict:
        pattern = view_pattern if is_view else table_pattern
        field = "view_pattern" if is_view else "table_pattern"
        if not pattern.allowed(name):
            return (False, field)
        return (True, None)

    return classify_table


def _container_classifier(config: Any, pattern: Any, field: str) -> Any:
    # System catalogs the source drops before the user pattern is even applied.
    default_schemas = {s.lower() for s in config.default_schemas()}

    def classify_container(name: str, node_fqn: str) -> Verdict:
        if name.lower() in default_schemas:
            return (False, "default_schema")
        if not pattern.allowed(name):
            return (False, field)
        return (True, None)

    return classify_container


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


def _list_children(
    config: Any,
    parent_path: List[str],
    limit: int,
    top_kind: "DatasetContainerSubTypes",
    top_pattern_field: str,
) -> ProbeResult:
    # Shared by the generic (schema-top) and two-tier (database-top) paths — they
    # differ only in what the top container is called and which pattern filters it.
    # SQLAlchemy exposes both schemas and two-tier databases via get_schema_names().
    from sqlalchemy import create_engine, inspect

    top_pattern = getattr(config, top_pattern_field)
    classify_container = _container_classifier(config, top_pattern, top_pattern_field)
    classify_table = _table_classifier(config)

    engine = create_engine(config.get_sql_alchemy_url(), **engine_options(config))
    try:
        inspector = inspect(engine)
        if len(parent_path) == 0:
            nodes, truncated = container_nodes(
                inspector.get_schema_names(),
                limit,
                top_kind,
                top_pattern_field,
                classify=classify_container,
            )
        elif len(parent_path) == 1:
            schema = parent_path[0]
            nodes, truncated = table_nodes(
                inspector.get_table_names(schema=schema),
                inspector.get_view_names(schema=schema),
                limit,
                fqn_prefix=schema,
                classify=classify_table,
            )
        else:
            schema, table = parent_path[0], parent_path[1]
            nodes, truncated = column_nodes(
                inspector.get_columns(table, schema=schema),
                limit,
                fqn_prefix=f"{schema}.{table}",
            )
        return ProbeResult(
            source_type="",
            supported=True,
            parent_path=parent_path,
            nodes=nodes,
            truncated=truncated,
        )
    finally:
        engine.dispose()


def list_sql_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return _list_children(
        config,
        parent_path,
        limit,
        DatasetContainerSubTypes.SCHEMA,
        "schema_pattern",
    )


def list_two_tier_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return _list_children(
        config,
        parent_path,
        limit,
        DatasetContainerSubTypes.DATABASE,
        "database_pattern",
    )

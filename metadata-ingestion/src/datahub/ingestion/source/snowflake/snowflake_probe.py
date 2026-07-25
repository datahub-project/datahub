from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Sequence

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
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
from datahub.ingestion.source.snowflake.snowflake_utils import (
    _is_sys_table,
    is_snowflake_default_schema,
)


def _quote_identifier(name: str) -> str:
    # database comes from the agent-facing --database argument and is interpolated
    # into raw SHOW / USE statements (there's no bind-parameter form for
    # identifiers). Escape embedded double quotes so the value cannot break out of
    # the quoted identifier. Snowflake escapes " as "".
    return name.replace('"', '""')


def _engine(config: Any) -> Any:
    # lazy: sqlalchemy is only needed once a probe actually runs. get_options()
    # folds key-pair credentials into connect_args, so pass it to create_engine.
    from sqlalchemy import create_engine

    return create_engine(config.get_sql_alchemy_url(), **config.get_options())


def _show_names(engine: Any, statement: str) -> List[str]:
    # SQLAlchemy's get_schema_names() flattens schemas across all databases to bare
    # names, which can't express database_pattern or match_fully_qualified_names,
    # so enumerate via SHOW instead.
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import text

    with engine.connect() as conn:
        return [row._mapping["name"] for row in conn.execute(text(statement))]


@contextmanager
def _pinned_inspector(engine: Any, database: str) -> Iterator[Any]:
    """Yield an inspector on a connection with `database` current, so unqualified
    information_schema reflection resolves, and always return it to the pool.

    The recipe may set no default database, and the dialect's reflection queries an
    unqualified information_schema.
    """
    # lazy: sqlalchemy is only needed once a probe actually runs
    from sqlalchemy import inspect, text

    with engine.connect() as conn:
        conn.execute(text(f'USE DATABASE "{_quote_identifier(database)}"'))
        yield inspect(conn)


def _databases(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return _show_names(engine, "SHOW TERSE DATABASES")


def _schemas(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    database = _quote_identifier(parent_path[0])
    return _show_names(engine, f'SHOW TERSE SCHEMAS IN DATABASE "{database}"')


def _tables(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    with _pinned_inspector(engine, parent_path[0]) as inspector:
        return inspector.get_table_names(schema=parent_path[1])


def _views(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    with _pinned_inspector(engine, parent_path[0]) as inspector:
        return inspector.get_view_names(schema=parent_path[1])


def _columns(engine: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    with _pinned_inspector(engine, parent_path[0]) as inspector:
        cols = inspector.get_columns(parent_path[2], schema=parent_path[1])
        return [str(col["name"]) for col in cols]


def _classify_schema(
    config: Any, name: str, node_fqn: str, pattern_field: Optional[str]
) -> Verdict:
    # Snowflake auto-drops INFORMATION_SCHEMA regardless of patterns.
    if is_snowflake_default_schema(name):
        return (False, "default_schema")
    database = node_fqn.split(".")[0]
    if not is_schema_allowed(
        config.schema_pattern, name, database, config.match_fully_qualified_names
    ):
        return (False, "schema_pattern")
    return (True, None)


def _classify_table(
    config: Any, name: str, node_fqn: str, pattern_field: Optional[str]
) -> Verdict:
    # sys$… objects are dropped by ingestion irrespective of patterns.
    if _is_sys_table(name):
        return (False, "system_object")
    # Snowflake matches table/view patterns against DATABASE.SCHEMA.TABLE.
    return pattern_verdict(config, pattern_field, node_fqn)


# Snowflake is a 3-level namespace: database -> schema -> table -> column.
SNOWFLAKE_PROBE = ClientProbe(
    client_factory=_engine,
    close=lambda engine: engine.dispose(),
    levels=[
        ProbeLevel(DatasetContainerSubTypes.DATABASE, "database_pattern", _databases),
        ProbeLevel(
            DatasetContainerSubTypes.SCHEMA,
            "schema_pattern",
            _schemas,
            classify=_classify_schema,
        ),
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(_tables, DatasetSubTypes.TABLE, "table_pattern"),
                LevelSource(_views, DatasetSubTypes.VIEW, "view_pattern"),
            ],
            classify=_classify_table,
        ),
        ProbeLevel(ProbeLeafKind.COLUMN, list_names=_columns),
    ],
)

SNOWFLAKE_PROBE_HIERARCHY: List[ProbeNodeKind] = SNOWFLAKE_PROBE.hierarchy()


def list_snowflake_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return SNOWFLAKE_PROBE.list_children(config, parent_path, limit)

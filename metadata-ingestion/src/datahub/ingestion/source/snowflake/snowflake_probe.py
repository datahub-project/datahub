from typing import Any, List

from datahub.configuration.pattern_utils import is_schema_allowed
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
from datahub.ingestion.source.snowflake.snowflake_utils import (
    _is_sys_table,
    is_snowflake_default_schema,
)

# Snowflake is a 3-level namespace: database -> schema -> table -> column.
SNOWFLAKE_PROBE_HIERARCHY: List[ProbeNodeKind] = [
    DatasetContainerSubTypes.DATABASE,
    DatasetContainerSubTypes.SCHEMA,
    DatasetSubTypes.TABLE,
    ProbeLeafKind.COLUMN,
]


def _quote_identifier(name: str) -> str:
    # database comes from the agent-facing --database argument and is interpolated
    # into raw SHOW / USE statements (there's no bind-parameter form for
    # identifiers). Escape embedded double quotes so the value cannot break out of
    # the quoted identifier. Snowflake escapes " as "".
    return name.replace('"', '""')


def list_snowflake_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    # SQLAlchemy's get_schema_names() flattens schemas across all databases to bare
    # names, which can't express database_pattern or match_fully_qualified_names.
    # So enumerate databases/schemas via SHOW and qualify every fqn as
    # DATABASE[.SCHEMA[.TABLE]]. get_options() folds key-pair credentials into
    # connect_args, so pass it to create_engine.
    from sqlalchemy import create_engine, inspect, text

    engine = create_engine(config.get_sql_alchemy_url(), **config.get_options())
    try:
        if len(parent_path) == 0:

            def classify_database(name: str, node_fqn: str) -> Verdict:
                if not config.database_pattern.allowed(name):
                    return (False, "database_pattern")
                return (True, None)

            with engine.connect() as conn:
                rows = conn.execute(text("SHOW TERSE DATABASES"))
                names = [row._mapping["name"] for row in rows]
            nodes, truncated = container_nodes(
                names,
                limit,
                DatasetContainerSubTypes.DATABASE,
                "database_pattern",
                classify=classify_database,
            )
        elif len(parent_path) == 1:
            database = parent_path[0]

            def classify_schema(name: str, node_fqn: str) -> Verdict:
                # Snowflake auto-drops INFORMATION_SCHEMA regardless of patterns.
                if is_snowflake_default_schema(name):
                    return (False, "default_schema")
                if not is_schema_allowed(
                    config.schema_pattern,
                    name,
                    database,
                    config.match_fully_qualified_names,
                ):
                    return (False, "schema_pattern")
                return (True, None)

            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f'SHOW TERSE SCHEMAS IN DATABASE "{_quote_identifier(database)}"'
                    )
                )
                names = [row._mapping["name"] for row in rows]
            nodes, truncated = container_nodes(
                names,
                limit,
                DatasetContainerSubTypes.SCHEMA,
                "schema_pattern",
                fqn_prefix=database,
                classify=classify_schema,
            )
        elif len(parent_path) == 2:
            database, schema = parent_path[0], parent_path[1]

            def classify_table(name: str, node_fqn: str, is_view: bool) -> Verdict:
                # sys$… objects are dropped by ingestion irrespective of patterns.
                if _is_sys_table(name):
                    return (False, "system_object")
                pattern = config.view_pattern if is_view else config.table_pattern
                field = "view_pattern" if is_view else "table_pattern"
                # Snowflake matches table/view patterns against the fully-qualified
                # DATABASE.SCHEMA.TABLE name.
                if not pattern.allowed(node_fqn):
                    return (False, field)
                return (True, None)

            # The recipe may set no default database, and the dialect's column
            # reflection queries an unqualified information_schema (which needs a
            # current database). Pin the database on the connection and pass the
            # bare schema so both table and column reflection resolve.
            with engine.connect() as conn:
                conn.execute(text(f'USE DATABASE "{_quote_identifier(database)}"'))
                inspector = inspect(conn)
                nodes, truncated = table_nodes(
                    inspector.get_table_names(schema=schema),
                    inspector.get_view_names(schema=schema),
                    limit,
                    fqn_prefix=f"{database}.{schema}",
                    classify=classify_table,
                )
        else:
            database, schema, table = parent_path[0], parent_path[1], parent_path[2]
            with engine.connect() as conn:
                conn.execute(text(f'USE DATABASE "{_quote_identifier(database)}"'))
                inspector = inspect(conn)
                nodes, truncated = column_nodes(
                    inspector.get_columns(table, schema=schema),
                    limit,
                    fqn_prefix=f"{database}.{schema}.{table}",
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

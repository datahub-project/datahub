from typing import Optional, Tuple

import sqlglot

from datahub.sql_parsing.sql_parsing_common import QueryType, QueryTypeProps
from datahub.sql_parsing.sqlglot_utils import (
    DialectOrStr,
    get_dialect,
    is_dialect_instance,
)

# Databricks COPY may quote storage paths as Identifier; only known schemes are URIs.
_COPY_SOURCE_URI_SCHEMES = frozenset(
    {
        "s3",
        "s3a",
        "s3n",
        "gs",
        "gcs",
        "abfs",
        "abfss",
        "wasb",
        "wasbs",
        "adl",
        "dbfs",
        "hdfs",
        "file",
    }
)


def _is_temp_table(table: sqlglot.exp.Table, dialect: sqlglot.Dialect) -> bool:
    identifier: sqlglot.exp.Identifier = table.this

    return identifier.args.get("temporary") or (
        # These dialects use # as a prefix for temp tables.
        is_dialect_instance(
            dialect,
            [
                "redshift",
                "mssql",
                # sybase is another one, but we don't support that dialect yet.
            ],
        )
        and identifier.name.startswith("#")
    )


def is_snowflake_stage_table(table: sqlglot.exp.Table) -> bool:
    # sqlglot has no dedicated stage node. Snowflake stage SQL (`stg/p`, `~/p`,
    # `%t/p`, `db.schema.stg` with a leading at-sign) parses as Table(this=Var(
    # "@...")), and that shape is only produced by the `@`-gated branch of
    # Snowflake's table-parts parser. Real table names use Identifier (or
    # DynamicIdentifier for IDENTIFIER('db.s.t')); T-SQL table variables use Parameter.
    table_name = table.this
    return isinstance(table_name, sqlglot.exp.Var) and table_name.name.startswith("@")


def _scheme_from_quoted_storage_path(name: str) -> Optional[str]:
    # Databricks uses scheme:// for object stores and scheme:/ for dbfs and local paths.
    if "://" in name:
        return name.split("://", 1)[0].lower()
    if ":/" in name:
        return name.split(":/", 1)[0].lower()
    return None


def is_copy_source_uri_table(table: sqlglot.exp.Table) -> bool:
    # A URI parses as Table(this=Literal) in Snowflake COPY, and as a bare
    # Literal in Postgres/Redshift. Inside a COPY source sub-select, Databricks
    # parses 's3://...' as a quoted Identifier, not a Literal.
    table_name = table.this
    if isinstance(table_name, sqlglot.exp.Literal):
        return True
    if not (
        isinstance(table_name, sqlglot.exp.Identifier) and table_name.args.get("quoted")
    ):
        return False
    scheme = _scheme_from_quoted_storage_path(table_name.name)
    return scheme is not None and scheme in _COPY_SOURCE_URI_SCHEMES


def is_stage_or_uri_table(table: sqlglot.exp.Table) -> bool:
    # Stage refs delegate to is_snowflake_stage_table; file paths parse as
    # Table(this=Literal) because sqlglot has no URI node. Do not invert to
    # `not isinstance(..., Identifier)` — COPY INTO IDENTIFIER('db.s.t') is
    # Table(this=DynamicIdentifier) and is still a real load target.
    return is_snowflake_stage_table(table) or isinstance(
        table.this, sqlglot.exp.Literal
    )


def get_copy_load_target(
    expression: sqlglot.exp.Expr,
) -> Optional[sqlglot.exp.Table]:
    if not isinstance(expression, sqlglot.exp.Copy):
        return None
    if not expression.args.get("kind"):
        return None

    copy_target = expression.this
    if isinstance(copy_target, sqlglot.exp.Table):
        if is_stage_or_uri_table(copy_target):
            return None
        return copy_target
    if isinstance(copy_target, sqlglot.exp.Schema) and isinstance(
        copy_target.this, sqlglot.exp.Table
    ):
        inner = copy_target.this
        if is_stage_or_uri_table(inner):
            return None
        return inner
    return None


def is_copy_load(expression: sqlglot.exp.Expr) -> bool:
    return get_copy_load_target(expression) is not None


def _get_create_type_from_kind(kind: Optional[str]) -> QueryType:
    if kind and "TABLE" in kind:
        return QueryType.CREATE_TABLE_AS_SELECT
    elif kind and "VIEW" in kind:
        return QueryType.CREATE_VIEW
    else:
        return QueryType.CREATE_OTHER


def get_query_type_of_sql(
    expression: sqlglot.exp.Expr, dialect: DialectOrStr
) -> Tuple[QueryType, QueryTypeProps]:
    dialect = get_dialect(dialect)
    query_type_props: QueryTypeProps = {}

    # For creates, we need to look at the inner expression.
    if isinstance(expression, sqlglot.exp.Create):
        if is_create_table_ddl(expression):
            return QueryType.CREATE_DDL, query_type_props

        kind = expression.args.get("kind")
        if kind:
            kind = kind.upper()
            query_type_props["kind"] = kind

        target = expression.this
        if any(
            isinstance(prop, sqlglot.exp.TemporaryProperty)
            for prop in (expression.args.get("properties") or [])
        ) or _is_temp_table(target, dialect=dialect):
            query_type_props["temporary"] = True

        query_type = _get_create_type_from_kind(kind)
        return query_type, query_type_props

    if is_copy_load(expression):
        return QueryType.INSERT, query_type_props

    # UPGRADE: Once we use Python 3.10, replace this with a match expression.
    mapping = {
        sqlglot.exp.Select: QueryType.SELECT,
        sqlglot.exp.Insert: QueryType.INSERT,
        sqlglot.exp.Update: QueryType.UPDATE,
        sqlglot.exp.Delete: QueryType.DELETE,
        sqlglot.exp.Merge: QueryType.MERGE,
        sqlglot.exp.Query: QueryType.SELECT,  # unions, etc. are also selects
    }

    for cls, query_type in mapping.items():
        if isinstance(expression, cls):
            return query_type, query_type_props
    return QueryType.UNKNOWN, {}


def is_create_table_ddl(statement: sqlglot.exp.Expression) -> bool:
    return isinstance(statement, sqlglot.exp.Create) and isinstance(
        statement.this, sqlglot.exp.Schema
    )

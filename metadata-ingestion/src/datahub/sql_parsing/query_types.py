from typing import Optional, Tuple

import sqlglot

from datahub.sql_parsing.sql_parsing_common import QueryType, QueryTypeProps
from datahub.sql_parsing.sqlglot_utils import (
    DialectOrStr,
    get_dialect,
    is_dialect_instance,
)


def _is_temp_table(table: sqlglot.exp.Table, dialect: sqlglot.Dialect) -> bool:
    identifier: sqlglot.exp.Identifier = table.this

    return identifier.args.get("temporary") or (
        is_dialect_instance(dialect, "redshift") and identifier.name.startswith("#")
    )


def _get_create_type_from_kind(kind: Optional[str]) -> QueryType:
    if kind and "TABLE" in kind:
        return QueryType.CREATE_TABLE_AS_SELECT
    elif kind and "VIEW" in kind:
        return QueryType.CREATE_VIEW
    else:
        return QueryType.CREATE_OTHER


def get_query_type_of_sql(
    expression: sqlglot.exp.Expression, dialect: DialectOrStr
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

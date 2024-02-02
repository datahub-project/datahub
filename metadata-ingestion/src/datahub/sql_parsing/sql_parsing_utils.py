import hashlib
from typing import Iterable, Optional, Union

import sqlglot

DialectOrStr = Union[sqlglot.Dialect, str]


def _get_dialect_str(platform: str) -> str:
    # TODO: convert datahub platform names to sqlglot dialect
    if platform == "presto-on-hive":
        return "hive"
    elif platform == "mssql":
        return "tsql"
    elif platform == "athena":
        return "trino"
    elif platform == "mysql":
        # In sqlglot v20+, MySQL is now case-sensitive by default, which is the
        # default behavior on Linux. However, MySQL's default case sensitivity
        # actually depends on the underlying OS.
        # For us, it's simpler to just assume that it's case-insensitive, and
        # let the fuzzy resolution logic handle it.
        return "mysql, normalization_strategy = lowercase"
    else:
        return platform


def get_dialect(platform: DialectOrStr) -> sqlglot.Dialect:
    if isinstance(platform, sqlglot.Dialect):
        return platform
    return sqlglot.Dialect.get_or_raise(_get_dialect_str(platform))


def is_dialect_instance(
    dialect: sqlglot.Dialect, platforms: Union[str, Iterable[str]]
) -> bool:
    if isinstance(platforms, str):
        platforms = [platforms]
    else:
        platforms = list(platforms)

    dialects = [sqlglot.Dialect.get_or_raise(platform) for platform in platforms]

    if any(isinstance(dialect, dialect_class.__class__) for dialect_class in dialects):
        return True
    return False


def generalize_query(expression: sqlglot.exp.ExpOrStr, dialect: DialectOrStr) -> str:
    """
    Generalize a SQL query, to be usable for fingerprinting.

    The generalized query will strip comments and normalize things like
    whitespace and keyword casing. It will also replace things like date
    literals with placeholders so they don't affect the fingerprint.

    Args:
        expression: The SQL query to generalize.
        dialect: The SQL dialect to use.

    Returns:
        The generalized SQL query.
    """

    # Similar to sql-metadata's query normalization.
    #   Implementation: https://github.com/macbre/sql-metadata/blob/master/sql_metadata/generalizator.py
    #   Tests: https://github.com/macbre/sql-metadata/blob/master/test/test_normalization.py
    #
    # Note that this is somewhat different from SQLGlot's normalization
    # https://tobikodata.com/are_these_sql_queries_the_same.html
    # which is used to determine if queries are functionally equivalent.

    dialect = get_dialect(dialect)
    expression = sqlglot.maybe_parse(expression, dialect=dialect)

    def _simplify_node_expressions(node: sqlglot.exp.Expression) -> None:
        # Replace all literals in the expressions with a single placeholder.
        is_last_literal = True
        for i, expression in reversed(list(enumerate(node.expressions))):
            if isinstance(expression, sqlglot.exp.Literal):
                if is_last_literal:
                    node.expressions[i] = sqlglot.exp.Placeholder()
                    is_last_literal = False
                else:
                    node.expressions.pop(i)

            elif isinstance(expression, sqlglot.exp.Tuple):
                _simplify_node_expressions(expression)

    def _strip_expression(
        node: sqlglot.exp.Expression,
    ) -> Optional[sqlglot.exp.Expression]:
        node.comments = None

        if isinstance(node, (sqlglot.exp.In, sqlglot.exp.Values)):
            _simplify_node_expressions(node)
        elif isinstance(node, sqlglot.exp.Literal):
            return sqlglot.exp.Placeholder()

        return node

    return expression.transform(_strip_expression, copy=True).sql(dialect=dialect)


def get_query_fingerprint(
    expression: sqlglot.exp.ExpOrStr, dialect: DialectOrStr
) -> str:
    dialect = get_dialect(dialect)
    expression_sql = generalize_query(expression, dialect=dialect)

    # Once we move to Python 3.9+, we can set `usedforsecurity=False`.
    fingerprint = hashlib.sha256(expression_sql.encode("utf-8")).hexdigest()

    return fingerprint

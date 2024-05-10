import functools
import hashlib
import logging
from typing import Dict, Iterable, Optional, Tuple, Union

import sqlglot
import sqlglot.errors

logger = logging.getLogger(__name__)
DialectOrStr = Union[sqlglot.Dialect, str]
SQL_PARSE_CACHE_SIZE = 1000

FORMAT_QUERY_CACHE_SIZE = 1000


def _get_dialect_str(platform: str) -> str:
    if platform == "presto-on-hive":
        return "hive"
    elif platform == "mssql":
        return "tsql"
    elif platform == "athena":
        return "trino"
    # TODO: define SalesForce SOQL dialect
    # Temporary workaround is to treat SOQL as databricks dialect
    # At least it allows to parse simple SQL queries and built linage for them
    elif platform == "salesforce":
        return "databricks"
    elif platform in {"mysql", "mariadb"}:
        # In sqlglot v20+, MySQL is now case-sensitive by default, which is the
        # default behavior on Linux. However, MySQL's default case sensitivity
        # actually depends on the underlying OS.
        # For us, it's simpler to just assume that it's case-insensitive, and
        # let the fuzzy resolution logic handle it.
        # MariaDB is a fork of MySQL, so we reuse the same dialect.
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


@functools.lru_cache(maxsize=SQL_PARSE_CACHE_SIZE)
def _parse_statement(
    sql: sqlglot.exp.ExpOrStr, dialect: sqlglot.Dialect
) -> sqlglot.Expression:
    statement: sqlglot.Expression = sqlglot.maybe_parse(
        sql, dialect=dialect, error_level=sqlglot.ErrorLevel.RAISE
    )
    return statement


def parse_statement(
    sql: sqlglot.exp.ExpOrStr, dialect: sqlglot.Dialect
) -> sqlglot.Expression:
    # Parsing is significantly more expensive than copying the expression.
    # Because the expressions are mutable, we don't want to allow the caller
    # to modify the parsed expression that sits in the cache. We keep
    # the cached versions pristine by returning a copy on each call.
    return _parse_statement(sql, dialect).copy()


def parse_statements_and_pick(sql: str, platform: DialectOrStr) -> sqlglot.Expression:
    dialect = get_dialect(platform)
    statements = [
        expression for expression in sqlglot.parse(sql, dialect=dialect) if expression
    ]
    if not statements:
        raise ValueError(f"No statements found in query: {sql}")
    elif len(statements) == 1:
        return statements[0]
    else:
        # If we find multiple statements, we assume the last one is the main query.
        # Usually the prior queries are going to be things like `CREATE FUNCTION`
        # or `GRANT ...`, which we don't care about.
        logger.debug(
            "Found multiple statements in query, picking the last one: %s", sql
        )
        return statements[-1]


def _expression_to_string(
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr
) -> str:
    if isinstance(expression, str):
        return expression
    return expression.sql(dialect=get_dialect(platform))


def generalize_query(expression: sqlglot.exp.ExpOrStr, dialect: DialectOrStr) -> str:
    """
    Generalize/normalize a SQL query.

    The generalized query will strip comments and normalize things like
    whitespace and keyword casing. It will also replace things like date
    literals with placeholders so that the generalized query can be used
    for query fingerprinting.

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
    # Note that this is somewhat different from sqlglot's normalization
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


def generate_hash(text: str) -> str:
    # Once we move to Python 3.9+, we can set `usedforsecurity=False`.
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_query_fingerprint_debug(
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr
) -> Tuple[str, Optional[str]]:
    try:
        dialect = get_dialect(platform)
        expression_sql = generalize_query(expression, dialect=dialect)
    except (ValueError, sqlglot.errors.SqlglotError) as e:
        if not isinstance(expression, str):
            raise

        logger.debug("Failed to generalize query for fingerprinting: %s", e)
        expression_sql = None

    fingerprint = generate_hash(
        expression_sql
        if expression_sql is not None
        else _expression_to_string(expression, platform=platform)
    )
    return fingerprint, expression_sql


def get_query_fingerprint(
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr
) -> str:
    """Get a fingerprint for a SQL query.

    The fingerprint is a SHA-256 hash of the generalized query.

    If two queries have the same fingerprint, they should have the same table
    and column lineage. The only exception is if the query uses an indirection
    function like Snowflake's `IDENTIFIER`.

    Queries that are functionally equivalent equivalent may not have the same
    same fingerprint. For example, `SELECT 1+1` and `SELECT 2` have different
    fingerprints.

    Args:
        expression: The SQL query to fingerprint.
        platform: The SQL dialect to use.

    Returns:
        The fingerprint for the SQL query.
    """

    return get_query_fingerprint_debug(expression, platform)[0]


@functools.lru_cache(maxsize=FORMAT_QUERY_CACHE_SIZE)
def try_format_query(
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr, raises: bool = False
) -> str:
    """Format a SQL query.

    If the query cannot be formatted, the original query is returned unchanged.

    Args:
        expression: The SQL query to format.
        platform: The SQL dialect to use.
        raises: If True, raise an error if the query cannot be formatted.

    Returns:
        The formatted SQL query.
    """

    try:
        dialect = get_dialect(platform)
        expression = parse_statement(expression, dialect=dialect)
        return expression.sql(dialect=dialect, pretty=True)
    except Exception as e:
        if raises:
            raise
        logger.debug("Failed to format query: %s", e)
        return _expression_to_string(expression, platform=platform)


def detach_ctes(
    sql: sqlglot.exp.ExpOrStr, platform: str, cte_mapping: Dict[str, str]
) -> sqlglot.exp.Expression:
    """Replace CTE references with table references.

    For example, with cte_mapping = {"__cte_0": "_my_cte_table"}, the following SQL

    WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN __cte_0 ON table2.id = __cte_0.id

    is transformed into

    WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN _my_cte_table ON table2.id = _my_cte_table.id

    Note that the original __cte_0 definition remains in the query, but is simply not referenced.
    The query optimizer should be able to remove it.

    This method makes a major assumption: that no other table/column has the same name as a
    key in the cte_mapping.
    """

    dialect = get_dialect(platform)
    statement = parse_statement(sql, dialect=dialect)

    def replace_cte_refs(node: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        if (
            isinstance(node, sqlglot.exp.Identifier)
            and node.parent
            and not isinstance(node.parent.parent, sqlglot.exp.CTE)
            and node.name in cte_mapping
        ):
            full_new_name = cte_mapping[node.name]
            table_expr = sqlglot.maybe_parse(
                full_new_name, dialect=dialect, into=sqlglot.exp.Table
            )

            parent = node.parent

            # We expect node.parent to be a Table or Column, both of which support catalog/db/name.
            # However, we check the parent's arg_types to be safe.
            if "catalog" in parent.arg_types and table_expr.catalog:
                parent.set("catalog", table_expr.catalog)
            if "db" in parent.arg_types and table_expr.db:
                parent.set("db", table_expr.db)

            new_node = sqlglot.exp.Identifier(this=table_expr.name)

            return new_node
        else:
            return node

    statement = statement.copy()
    return statement.transform(replace_cte_refs, copy=False)

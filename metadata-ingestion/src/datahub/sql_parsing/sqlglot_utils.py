from datahub.sql_parsing._sqlglot_patch import SQLGLOT_PATCHED

import functools
import hashlib
import logging
import re
from typing import Dict, Iterable, Optional, Tuple, Union

import sqlglot
import sqlglot.errors
import sqlglot.optimizer.eliminate_ctes

assert SQLGLOT_PATCHED

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
        sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IMMEDIATE
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
    logger.debug("Parsing SQL query: %s", sql)

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


_BASIC_NORMALIZATION_RULES = {
    # Remove /* */ comments.
    re.compile(r"/\*.*?\*/", re.DOTALL): "",
    # Remove -- comments.
    re.compile(r"--.*$"): "",
    # Replace all runs of whitespace with a single space.
    re.compile(r"\s+"): " ",
    # Remove leading and trailing whitespace and trailing semicolons.
    re.compile(r"^\s+|[\s;]+$"): "",
    # Replace anything that looks like a number with a placeholder.
    re.compile(r"\b\d+\b"): "?",
    # Replace anything that looks like a string with a placeholder.
    re.compile(r"'[^']*'"): "?",
    # Replace sequences of IN/VALUES with a single placeholder.
    re.compile(r"\b(IN|VALUES)\s*\(\?(?:, \?)*\)", re.IGNORECASE): r"\1 (?)",
    # Normalize parenthesis spacing.
    re.compile(r"\( "): "(",
    re.compile(r" \)"): ")",
}
_TABLE_NAME_NORMALIZATION_RULES = {
    # Replace UUID-like strings with a placeholder (both - and _ variants).
    re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        re.IGNORECASE,
    ): "00000000-0000-0000-0000-000000000000",
    re.compile(
        r"[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}",
        re.IGNORECASE,
    ): "00000000_0000_0000_0000_000000000000",
    # GE temporary table names (prefix + 8 digits of a UUIDv4)
    re.compile(
        r"\b(ge_tmp_|ge_temp_|gx_temp_)[0-9a-f]{8}\b", re.IGNORECASE
    ): r"\1abcdefgh",
    # Date-suffixed table names (e.g. _20210101)
    re.compile(r"\b(\w+)(19|20)\d{4}\b"): r"\1YYYYMM",
    re.compile(r"\b(\w+)(19|20)\d{6}\b"): r"\1YYYYMMDD",
    re.compile(r"\b(\w+)(19|20)\d{8}\b"): r"\1YYYYMMDDHH",
    re.compile(r"\b(\w+)(19|20)\d{10}\b"): r"\1YYYYMMDDHHMM",
}


def generalize_query_fast(
    expression: sqlglot.exp.ExpOrStr,
    dialect: DialectOrStr,
    change_table_names: bool = False,
) -> str:
    """Variant of `generalize_query` that only does basic normalization.

    Args:
        expression: The SQL query to generalize.
        dialect: The SQL dialect to use.
        change_table_names: If True, replace table names with placeholders. Note
            that this should only be used for query filtering purposes, as it
            violates the general assumption that the queries with the same fingerprint
            have the same lineage/usage/etc.

    Returns:
        The generalized SQL query.
    """

    if isinstance(expression, sqlglot.exp.Expression):
        expression = expression.sql(dialect=get_dialect(dialect))
    query_text = expression

    REGEX_REPLACEMENTS = {
        **_BASIC_NORMALIZATION_RULES,
        **(_TABLE_NAME_NORMALIZATION_RULES if change_table_names else {}),
    }

    for pattern, replacement in REGEX_REPLACEMENTS.items():
        query_text = pattern.sub(replacement, query_text)
    return query_text


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
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr, fast: bool = False
) -> Tuple[str, Optional[str]]:
    try:
        if not fast:
            dialect = get_dialect(platform)
            expression_sql = generalize_query(expression, dialect=dialect)
        else:
            expression_sql = generalize_query_fast(expression, dialect=platform)
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
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr, fast: bool = False
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

    return get_query_fingerprint_debug(expression, platform, fast=fast)[0]


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
        parsed_expression = parse_statement(expression, dialect=dialect)
        return parsed_expression.sql(dialect=dialect, pretty=True)
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

    if not cte_mapping:
        return statement

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
    statement = statement.transform(replace_cte_refs, copy=False)

    # There's a bug in eliminate_ctes that causes it to not remove all unused CTEs
    # when there's a complex chain of dependent CTEs. As a workaround, we call the
    # method multiple times until it no longer eliminates any CTEs.
    max_eliminate_calls = 5
    for iteration in range(max_eliminate_calls):
        new_statement = sqlglot.optimizer.eliminate_ctes.eliminate_ctes(
            statement.copy()
        )
        if new_statement == statement:
            if iteration > 1:
                logger.debug(
                    f"Required {iteration+1} iterations to detach and eliminate all CTEs"
                )
            break
        statement = new_statement

    return statement

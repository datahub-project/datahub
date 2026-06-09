from datahub.sql_parsing._sqlglot_patch import SQLGLOT_PATCHED

import functools
import logging
import os
import re
from typing import Dict, Iterable, List, Optional, Tuple, Union

import sqlglot
import sqlglot.errors
import sqlglot.optimizer.eliminate_ctes

from datahub.configuration.env_vars import get_sql_parse_cache_size
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.sql_parsing.sql_parsing_common import get_dialect_str as _get_dialect_str

assert SQLGLOT_PATCHED

logger = logging.getLogger(__name__)
DialectOrStr = Union[sqlglot.Dialect, str]
SQL_PARSE_CACHE_SIZE = get_sql_parse_cache_size()
FORMAT_QUERY_CACHE_SIZE = get_sql_parse_cache_size()

# sqlglot's parser, optimizer, lineage traversal, and SQL rendering are all
# recursive over a statement's nesting structure. Pathologically nested SQL
# (e.g. dynamic SQL emitted by stored procedures) can recurse deep enough to
# overflow the stack. In the pure-Python build this is a catchable
# RecursionError, but in the mypyc-compiled sqlglot[c] build used in production
# it overflows the native C stack into an uncatchable SIGSEGV that kills the
# whole ingest process. We therefore refuse to parse statements past these
# limits, surfacing a catchable error instead. Native
# frames are smaller than CPython frames, so limits safely below the
# pure-Python recursion ceiling (~1000) also protect the compiled build, while
# sitting well above any realistic query (real SQL nests <~30 deep).
#
# Two limits guard two distinct failure points:
# - text nesting: bracket depth in the raw SQL, checked *before* parsing to
#   protect the parser itself from "paren bombs".
# - AST depth: node depth of the parsed tree, checked *after* parsing to
#   protect the optimizer/lineage/rendering, which recurse deeper than the
#   parser and choke on nesting (e.g. nested CASE) that has no brackets.
SQL_PARSE_MAX_TEXT_NESTING = int(
    os.environ.get("DATAHUB_SQL_PARSE_MAX_TEXT_NESTING", "500")
)
SQL_PARSE_MAX_AST_DEPTH = int(os.environ.get("DATAHUB_SQL_PARSE_MAX_AST_DEPTH", "600"))

# How much of an offending statement to surface at WARNING level. The full SQL
# is logged at DEBUG so it can be retrieved for debugging without flooding logs.
_SQL_COMPLEXITY_PREVIEW_CHARS = 2000


class StatementTooComplexError(sqlglot.errors.SqlglotError):
    """Raised when a SQL statement is nested deep enough that parsing it risks a
    native stack overflow. Subclasses SqlglotError so existing
    ``except SqlglotError`` handlers (e.g. fingerprinting) degrade gracefully."""


# Snowflake governance DDL syntax that sqlglot does not support. When sqlglot
# encounters any of these it silently falls back to parsing the whole statement
# as a Command node, causing DataHub to lose all lineage for that statement.
# None of these constructs carry table-reference information, so stripping them
# before parsing is safe for lineage purposes.
# Limitation: parenthesised content is matched with [^)]* and will break if a
# string literal inside the parens contains a literal ')'. This is not a
# realistic concern for policy names or column identifiers.
#
# TODO: If other dialects accumulate similar pre-parse fixups, consider
# extracting this into a dialect preprocessor registry:
#   Dict[str, Callable[[str], str]] mapping dialect name → sanitizer function,
# housed in a sql_parsing/dialect_preprocessors/ subpackage. For now a single
# dialect-specific block in parse_statement() is simpler and easier to follow.
_SNOWFLAKE_UNSUPPORTED_DDL_PATTERNS: List[re.Pattern] = [
    # col WITH TAG (schema.tag_name = 'value')
    re.compile(r"\s+WITH\s+TAG\s*\([^)]*\)", re.IGNORECASE),
    # col WITH MASKING POLICY db.schema.policy [USING (col1, col2)]
    re.compile(
        r"\s+WITH\s+MASKING\s+POLICY\s+[^\s,)(]+(?:\s+USING\s*\([^)]*\))?",
        re.IGNORECASE,
    ),
    # col WITH PROJECTION POLICY db.schema.policy
    re.compile(r"\s+WITH\s+PROJECTION\s+POLICY\s+[^\s,)(]+", re.IGNORECASE),
    # CREATE TABLE/VIEW ... WITH ROW ACCESS POLICY db.schema.policy ON (col_list)
    re.compile(
        r"\s+WITH\s+ROW\s+ACCESS\s+POLICY\s+[^\s,)(]+\s+ON\s*\([^)]*\)", re.IGNORECASE
    ),
]


def _sanitize_snowflake_ddl(sql: str) -> str:
    """Strip Snowflake governance DDL constructs unsupported by sqlglot (see patterns above)."""
    for pattern in _SNOWFLAKE_UNSUPPORTED_DDL_PATTERNS:
        sql = pattern.sub("", sql)
    return sql


# T-SQL temp tables whose name begins with a digit after the `#`/`##` prefix
# (e.g. `#63114Actual`, `##2025Totals`). sqlglot's tokenizer lexes the leading
# digit run as a NUMBER, so the parser sees `# NUMBER ...`, raises "Expected table
# name but got NUMBER", and the ENTIRE statement's lineage is lost (including the
# real FROM source). Wrapping the name in `[...]` (a T-SQL delimited identifier)
# makes sqlglot read it as one identifier; the normalized name still starts with
# `#`, so downstream temp-table detection (startswith("#")) is unaffected.
#
# The first alternative matches (and passes through unchanged) string literals and
# comments, so a `#<digit>` that is data inside a string/comment is never bracketed.
# Letter-leading names (`#temp`) already parse and are left alone.
_TSQL_DIGIT_TEMP_TABLE = re.compile(
    r"""
      (?P<skip>'(?:[^']|'')*' | --[^\n]* | /\*.*?\*/)   # string / line / block comment
    | (?<![\w$@#\[])(?P<temp>\#\#?\d[\w$@#]*)           # #<digit>... not mid-token / pre-bracketed
    """,
    re.VERBOSE | re.DOTALL,
)


def _sanitize_tsql_temp_tables(sql: str) -> str:
    """Bracket T-SQL `#`/`##` temp-table names that start with a digit (see pattern above)."""

    def _bracket(m: re.Match) -> str:
        temp = m.group("temp")
        return f"[{temp}]" if temp else m.group(0)

    return _TSQL_DIGIT_TEMP_TABLE.sub(_bracket, sql)


def get_dialect(platform: DialectOrStr) -> sqlglot.Dialect:
    if isinstance(platform, sqlglot.Dialect):
        return platform

    return sqlglot.Dialect.get_or_raise(_get_dialect_str(platform))


def is_dialect_instance(
    dialect: sqlglot.Dialect, platforms: Union[str, Iterable[str]]
) -> bool:
    platforms = [platforms] if isinstance(platforms, str) else list(platforms)

    dialects = [get_dialect(platform) for platform in platforms]

    if any(isinstance(dialect, dialect_class.__class__) for dialect_class in dialects):
        return True
    return False


@functools.lru_cache(maxsize=SQL_PARSE_CACHE_SIZE)
def _parse_statement(
    sql: sqlglot.exp.ExpOrStr, dialect: sqlglot.Dialect
) -> sqlglot.exp.Expression:
    statement = sqlglot.maybe_parse(
        sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IMMEDIATE
    )
    if not isinstance(statement, sqlglot.exp.Expression):
        raise TypeError(
            f"Expected Expression from maybe_parse(), got {type(statement)}"
        )

    # Handle Block statements from sqlglot v29+
    # Sqlglot parses SQL with double semicolons (e.g., "CREATE VIEW ...;\n;") as
    # Block([stmt1, None, ...]) where None represents empty statements between semicolons.
    # We only process the non None statement, if there is 1 and only 1.
    if isinstance(statement, sqlglot.exp.Block):
        if not statement.expressions:
            raise sqlglot.errors.ParseError(
                "Block statement must have at least one expression"
            )

        # Filter out None expressions (empty statements from double semicolons)
        non_none_expressions = [e for e in statement.expressions if e is not None]

        if not non_none_expressions:
            raise sqlglot.errors.ParseError(
                "Block contains only None expressions - no valid SQL statement found"
            )

        if len(non_none_expressions) > 1:
            # parse_statement expects a single statement, not multiple
            raise sqlglot.errors.ParseError(
                f"Block contains {len(non_none_expressions)} statements: "
                f"{[type(e).__name__ for e in non_none_expressions]}. "
                f"Use parse_statements_and_pick() for multi-statement SQL."
            )

        # Return the single non-None statement
        statement = non_none_expressions[0]

    return statement


def _max_text_nesting_depth(sql: str) -> int:
    """Return the maximum bracket-nesting depth of a SQL string.

    A cheap single-pass scan used as a pre-parse guard against "paren bombs".
    Brackets inside string literals are ignored (SQL escapes a quote by
    doubling it), so quoted text full of parens does not trigger a false
    positive.
    """
    depth = 0
    max_depth = 0
    quote: Optional[str] = None
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if quote is not None:
            if ch == quote:
                # A doubled quote ('') is an escaped quote, not a terminator.
                if i + 1 < n and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch in ("(", "["):
            depth += 1
            if depth > max_depth:
                max_depth = depth
        elif ch in (")", "]"):
            if depth > 0:
                depth -= 1
        i += 1
    return max_depth


def _log_too_complex(
    sql_text: Optional[str], *, depth: int, limit: int, kind: str
) -> None:
    length_note = f", length {len(sql_text)} chars" if sql_text is not None else ""
    if sql_text is None:
        preview = "<pre-parsed expression>"
    elif len(sql_text) > _SQL_COMPLEXITY_PREVIEW_CHARS:
        preview = sql_text[:_SQL_COMPLEXITY_PREVIEW_CHARS] + " …[truncated]"
    else:
        preview = sql_text
    logger.warning(
        "Skipping SQL too complex to parse safely (%s depth %d > limit %d%s); "
        "this avoids a stack overflow but means no lineage for this statement. "
        "SQL preview: %s",
        kind,
        depth,
        limit,
        length_note,
        preview,
    )
    # Log the full statement at DEBUG so it can be retrieved for debugging.
    if sql_text is not None and len(sql_text) > _SQL_COMPLEXITY_PREVIEW_CHARS:
        logger.debug(
            "Full SQL for too-complex statement (%s depth %d): %s",
            kind,
            depth,
            sql_text,
        )


def _raise_if_text_too_nested(sql: str) -> None:
    # Fast path: nesting depth can never exceed the number of opening brackets,
    # and str.count runs in C. This lets the ~all queries that are nowhere near
    # the limit skip the O(n) Python scan entirely — keeping the per-parse and
    # per-fingerprint overhead negligible. Only genuine outliers pay for the
    # precise, string-literal-aware scan below.
    if sql.count("(") + sql.count("[") <= SQL_PARSE_MAX_TEXT_NESTING:
        return
    depth = _max_text_nesting_depth(sql)
    if depth > SQL_PARSE_MAX_TEXT_NESTING:
        _log_too_complex(
            sql, depth=depth, limit=SQL_PARSE_MAX_TEXT_NESTING, kind="text nesting"
        )
        raise StatementTooComplexError(
            f"SQL nesting too deep to parse safely "
            f"(depth {depth} > limit {SQL_PARSE_MAX_TEXT_NESTING})"
        )


def _raise_if_statement_too_deep(
    statement: sqlglot.exp.Expression, sql_text: Optional[str]
) -> None:
    depth = get_expression_depth(statement)
    if depth > SQL_PARSE_MAX_AST_DEPTH:
        _log_too_complex(
            sql_text, depth=depth, limit=SQL_PARSE_MAX_AST_DEPTH, kind="parsed AST"
        )
        raise StatementTooComplexError(
            f"Parsed statement too deeply nested to process safely "
            f"(AST depth {depth} > limit {SQL_PARSE_MAX_AST_DEPTH})"
        )


def parse_statement(
    sql: sqlglot.exp.ExpOrStr, dialect: sqlglot.Dialect
) -> sqlglot.exp.Expression:
    # Parsing is significantly more expensive than copying the expression.
    # Because the expressions are mutable, we don't want to allow the caller
    # to modify the parsed expression that sits in the cache. We keep
    # the cached versions pristine by returning a copy on each call.
    sql_text = sql if isinstance(sql, str) else None
    if sql_text is not None:
        # Pre-parse guard: protect the recursive parser from paren bombs.
        _raise_if_text_too_nested(sql_text)
        if is_dialect_instance(dialect, "snowflake"):
            sanitized = _sanitize_snowflake_ddl(sql_text)
            if sanitized != sql_text:
                logger.debug("Sanitized Snowflake DDL: %s -> %s", sql_text, sanitized)
            sql = sanitized
        if is_dialect_instance(dialect, "tsql"):
            sanitized = _sanitize_tsql_temp_tables(sql_text)
            if sanitized != sql_text:
                logger.debug(
                    "Sanitized T-SQL temp tables: %s -> %s", sql_text, sanitized
                )
            sql = sanitized
    statement = _parse_statement(sql, dialect).copy()
    # Post-parse guard: protect the optimizer/lineage/rendering from deep ASTs
    # (e.g. nested CASE) that carry no brackets for the text guard to catch.
    _raise_if_statement_too_deep(statement, sql_text=sql_text)
    return statement


def parse_statements_and_pick(sql: str, platform: DialectOrStr) -> sqlglot.exp.Expr:
    # Note: does not go through parse_statement, so _sanitize_snowflake_ddl is
    # not applied here. This is intentional — callers (e.g. dbt) pass compiled
    # SELECT SQL, never raw DDL with governance metadata.
    logger.debug("Parsing SQL query: %s", sql)

    # Pre-parse guard: this path calls sqlglot.parse directly, so it needs the
    # same paren-bomb protection as parse_statement.
    _raise_if_text_too_nested(sql)

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


def get_expression_depth(expression: sqlglot.exp.Expression) -> int:
    """Return the maximum node depth of a sqlglot AST.

    Uses an explicit-stack traversal rather than recursion on purpose: a
    recursive walk over a pathologically nested statement would overflow the
    stack — the exact failure mode this function exists to detect. Callers use
    the depth to skip lineage parsing for statements deep enough to risk a
    native stack overflow in sqlglot's recursive parser/optimizer: a SIGSEGV in
    the compiled sqlglot[c] build is uncatchable and kills the whole ingest
    process.
    """
    max_depth = 0
    stack: List[Tuple[sqlglot.exp.Expression, int]] = [(expression, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > max_depth:
            max_depth = depth
        for value in node.args.values():
            if isinstance(value, sqlglot.exp.Expression):
                stack.append((value, depth + 1))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, sqlglot.exp.Expression):
                        stack.append((item, depth + 1))
    return max_depth


def _expression_to_string(
    expression: sqlglot.exp.ExpOrStr, platform: DialectOrStr
) -> str:
    if isinstance(expression, sqlglot.exp.Expr):
        return expression.sql(dialect=get_dialect(platform))
    return str(expression)


PLACEHOLDER_BACKWARD_FINGERPRINT_NORMALIZATION = re.compile(r"(%s|\$\d|\?)")

_BASIC_NORMALIZATION_RULES = {
    # Remove /* */ comments.
    re.compile(r"/\*.*?\*/", re.DOTALL): "",
    # Remove -- comments.
    re.compile(r"--.*$", re.MULTILINE): "",
    # Replace all runs of whitespace with a single space.
    re.compile(r"\s+"): " ",
    # Remove leading and trailing whitespace and trailing semicolons.
    re.compile(r"^\s+|[\s;]+$"): "",
    # Replace anything that looks like a number with a placeholder.
    re.compile(r"\b\d+\b"): "?",
    # Replace anything that looks like a string with a placeholder.
    re.compile(r"'[^']*'"): "?",
    # Replace sequences of IN/VALUES with a single placeholder.
    # The r" ?" makes it more robust to uneven spacing.
    re.compile(
        r"\b(IN|VALUES)\s*\( ?(?:%s|\$\d|\?)(?:, ?(?:%s|\$\d|\?))* ?\)", re.IGNORECASE
    ): r"\1 (?)",
    # Normalize parenthesis spacing.
    re.compile(r"\( "): "(",
    re.compile(r" \)"): ")",
    # Fix up spaces before commas in column lists.
    # e.g. "col1 , col2" -> "col1, col2"
    # e.g. "col1,col2" -> "col1, col2"
    re.compile(r"\b ,"): ",",
    re.compile(r"\b,\b"): ", ",
    # MAKE SURE THAT THIS IS AFTER THE ABOVE REPLACEMENT
    # Replace all versions of placeholders with generic ? placeholder.
    PLACEHOLDER_BACKWARD_FINGERPRINT_NORMALIZATION: "?",
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

    query_text: str
    if isinstance(expression, sqlglot.exp.Expr):
        query_text = expression.sql(dialect=get_dialect(dialect))
    else:
        query_text = str(expression)

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

    # This is a per-query hot path (fingerprinting), so we only apply the cheap
    # pre-parse text guard — its str.count fast path makes the common case
    # nearly free while still protecting the parser from paren bombs. We skip
    # the O(nodes) AST-depth walk here on purpose; callers
    # (get_query_fingerprint_debug) catch SqlglotError and fall back to a
    # string-based fingerprint, so this path degrades gracefully regardless.
    if isinstance(expression, str):
        _raise_if_text_too_nested(expression)
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

    transformed = expression.transform(_strip_expression, copy=True)
    assert transformed is not None
    return transformed.sql(dialect=dialect)


def get_query_fingerprint_debug(
    expression: sqlglot.exp.ExpOrStr,
    platform: DialectOrStr,
    fast: bool = False,
    secondary_id: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    try:
        if not fast:
            dialect = get_dialect(platform)
            expression_sql = generalize_query(expression, dialect=dialect)
            # Normalize placeholders for consistent fingerprinting -> this only needs to be backward compatible with earlier sqglot generated generalized queries where the placeholders were always ?
            expression_sql = PLACEHOLDER_BACKWARD_FINGERPRINT_NORMALIZATION.sub(
                "?", expression_sql
            )
        else:
            expression_sql = generalize_query_fast(expression, dialect=platform)
    except (ValueError, sqlglot.errors.SqlglotError) as e:
        if not isinstance(expression, str):
            raise

        logger.debug("Failed to generalize query for fingerprinting: %s", e)
        expression_sql = None

    text = expression_sql or _expression_to_string(expression, platform=platform)
    if secondary_id:
        text = text + " -- " + secondary_id
    fingerprint = generate_hash(text=text)
    return fingerprint, expression_sql


def get_query_fingerprint(
    expression: sqlglot.exp.ExpOrStr,
    platform: DialectOrStr,
    fast: bool = False,
    secondary_id: Optional[str] = None,
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
        secondary_id: An optional additional id string to included in the final fingerprint.

    Returns:
        The fingerprint for the SQL query.
    """

    return get_query_fingerprint_debug(
        expression=expression, platform=platform, fast=fast, secondary_id=secondary_id
    )[0]


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
                    f"Required {iteration + 1} iterations to detach and eliminate all CTEs"
                )
            break
        statement = new_statement

    return statement

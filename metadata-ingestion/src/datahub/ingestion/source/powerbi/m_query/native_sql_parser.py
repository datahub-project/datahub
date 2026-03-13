import logging
import re
from typing import List, Optional, Tuple

import sqlparse
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import Token, TokenType

from datahub.ingestion.api.common import PipelineContext
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
    create_lineage_sql_parsed_result,
)

# It is the PowerBI M-Query way to mentioned \n , \t
SPECIAL_CHARACTERS = {
    "#(lf)": "\n",
    "(lf)": "\n",
    "#(tab)": "\t",
}

ANSI_ESCAPE_CHARACTERS = r"\x1b\[[0-9;]*m"

logger = logging.getLogger(__name__)

_TSQL_DATA_TYPE_TOKENS: frozenset = frozenset(
    {
        TokenType.INT,
        TokenType.BIGINT,
        TokenType.SMALLINT,
        TokenType.TINYINT,
        TokenType.BIT,
        TokenType.DECIMAL,
        TokenType.MONEY,
        TokenType.SMALLMONEY,
        TokenType.FLOAT,
        TokenType.CHAR,
        TokenType.VARCHAR,
        TokenType.NCHAR,
        TokenType.NVARCHAR,
        TokenType.TEXT,
        TokenType.BINARY,
        TokenType.VARBINARY,
        TokenType.DATE,
        TokenType.TIME,
        TokenType.DATETIME,
        TokenType.DATETIME2,
        TokenType.SMALLDATETIME,
        TokenType.TIMESTAMP,
        TokenType.XML,
        TokenType.IMAGE,
        TokenType.UUID,
        TokenType.VARIANT,
        TokenType.TABLE,
    }
)

_STATEMENT_START_TOKENS: frozenset = frozenset(
    {
        TokenType.SELECT,
        TokenType.INSERT,
        TokenType.CREATE,
        TokenType.UPDATE,
        TokenType.DELETE,
        TokenType.WITH,
        TokenType.SET,
        TokenType.MERGE,
        TokenType.EXECUTE,
        TokenType.BEGIN,
        TokenType.DECLARE,
    }
)

_TABLE_HINT_KEYWORDS: frozenset = frozenset(
    {
        "NOLOCK",
        "NOWAIT",
        "READUNCOMMITTED",
        "READCOMMITTED",
        "REPEATABLEREAD",
        "SERIALIZABLE",
        "HOLDLOCK",
        "UPDLOCK",
        "TABLOCK",
        "TABLOCKX",
        "PAGLOCK",
        "ROWLOCK",
        "XLOCK",
        "READPAST",
    }
)


def remove_special_characters(native_query: str) -> str:
    for char in SPECIAL_CHARACTERS:
        native_query = native_query.replace(char, SPECIAL_CHARACTERS[char])

    ansi_escape_regx = re.compile(ANSI_ESCAPE_CHARACTERS)

    native_query = ansi_escape_regx.sub("", native_query)

    # Replace "" quotes by ". Sqlglot is not handling column name alias surrounded with two double quotes

    native_query = native_query.replace('""', '"')

    return native_query


def get_tables(native_query: str) -> List[str]:
    native_query = remove_special_characters(native_query)
    logger.debug(f"Processing native query = {native_query}")
    tables: List[str] = []
    parsed = sqlparse.parse(native_query)[0]
    tokens: List[sqlparse.sql.Token] = list(parsed.tokens)
    length: int = len(tokens)
    from_index: int = -1
    for index, token in enumerate(tokens):
        logger.debug(f"{token.value}={token.ttype}")
        if (
            token.value.lower().strip() == "from"
            and str(token.ttype) == "Token.Keyword"
        ):
            from_index = index + 1
            break

    # Collect all identifier after FROM clause till we reach to the end or WHERE clause encounter
    while (
        from_index < length
        and isinstance(tokens[from_index], sqlparse.sql.Where) is not True
    ):
        logger.debug(f"{tokens[from_index].value}={tokens[from_index].ttype}")
        logger.debug(f"Type={type(tokens[from_index])}")
        if isinstance(tokens[from_index], sqlparse.sql.Identifier):
            # Split on as keyword and collect the table name from 0th position. strip any spaces
            tables.append(tokens[from_index].value.split("as")[0].strip())
        from_index = from_index + 1

    return tables


def remove_drop_statement(query: str) -> str:
    # Certain PowerBI M-Queries contain a combination of DROP and SELECT statements within SQL, causing SQLParser to fail on these queries.
    # Therefore, these occurrences are being removed.

    patterns = [
        # Regular expression to match patterns like:
        #   "DROP TABLE IF EXISTS #<identifier>;"
        #   "DROP TABLE IF EXISTS #<identifier>, <identifier2>, ...;"
        #   "DROP TABLE IF EXISTS #<identifier>, <identifier2>, ...\n"
        #   "DROP TABLE IF EXISTS #<identifier>" (at end of string)
        r"DROP\s+TABLE\s+IF\s+EXISTS\s+(?:#?\w+(?:,\s*#?\w+)*)(?:[;\n]|$)",
    ]

    new_query = query

    for pattern in patterns:
        new_query = re.sub(pattern, "", new_query, flags=re.IGNORECASE)

    # Only normalize multiple consecutive spaces (but preserve newlines and tabs)
    # This fixes spacing issues caused by DROP statement removal without
    # collapsing the entire query into a single line
    new_query = re.sub(r"[ \t]+", " ", new_query)
    # Remove spaces at the start of lines (left by DROP statement removal)
    new_query = re.sub(r"\n[ \t]+", "\n", new_query)
    # Remove trailing spaces
    new_query = new_query.strip()

    return new_query


def _is_statement_start(token: Token) -> bool:
    """Check if a token starts a new SQL statement."""
    return token.token_type in _STATEMENT_START_TOKENS or (
        token.token_type == TokenType.VAR and token.text.upper() == "IF"
    )


def _skip_balanced_parens(tokens: List[Token], i: int) -> int:
    """Skip from L_PAREN to past matching R_PAREN, tracking nesting depth.

    Args:
        tokens: Full token list.
        i: Index of the L_PAREN token to start from.

    Returns:
        Index of the first token after the matching R_PAREN.

    Example::

        # Given tokens for "VARCHAR(100) = 5" with i pointing at "("
        # Returns the index of "=" (the token after ")")
    """
    n = len(tokens)
    depth = 1
    i += 1  # past L_PAREN
    while i < n and depth > 0:
        if tokens[i].token_type == TokenType.L_PAREN:
            depth += 1
        elif tokens[i].token_type == TokenType.R_PAREN:
            depth -= 1
        i += 1
    return i


def _find_declare_end(tokens: List[Token], start_idx: int) -> int:
    """Walk past a DECLARE statement and return index of the next statement's first token.

    Handles the full grammar of T-SQL DECLARE including:
    - Type precision parens: ``DECLARE @q VARCHAR(100) = 'hello'``
    - Parenthesized values: ``DECLARE @d DATETIME = (SELECT TOP 1 col FROM t1)``
    - Comma-separated vars: ``DECLARE @a INT = 1, @b VARCHAR(50) = 'test'``
    - TABLE type with column defs: ``DECLARE @t TABLE (id INT, name VARCHAR(50))``
    - No-value declarations: ``DECLARE @x INT``

    Args:
        tokens: Full token list from sqlglot's TSQL tokenizer.
        start_idx: Index of the DECLARE token.

    Returns:
        Index of the first token belonging to the next statement (e.g. SELECT),
        or ``len(tokens)`` if the DECLARE extends to the end of input.
    """
    n = len(tokens)
    i = start_idx + 1  # past DECLARE

    while i < n:
        # Expect @param + variable name
        if i < n and tokens[i].token_type == TokenType.PARAMETER:
            i += 1
        if i < n and tokens[i].token_type == TokenType.VAR:
            i += 1

        # Expect data type (known type token or VAR like CURSOR)
        if i < n and (
            tokens[i].token_type in _TSQL_DATA_TYPE_TOKENS
            or tokens[i].token_type == TokenType.VAR
        ):
            if tokens[i].token_type == TokenType.VAR and _is_statement_start(tokens[i]):
                break
            i += 1

            # Skip type precision parens: (100), (MAX), (10,2), or TABLE (col defs)
            if i < n and tokens[i].token_type == TokenType.L_PAREN:
                i = _skip_balanced_parens(tokens, i)

        # Check for assignment
        if i < n and tokens[i].token_type == TokenType.EQ:
            i += 1  # past =
            if i < n and tokens[i].token_type == TokenType.L_PAREN:
                i = _skip_balanced_parens(tokens, i)
            else:
                # Non-parenthesized value: walk to boundary
                while i < n:
                    tt = tokens[i].token_type
                    if tt in (
                        TokenType.SEMICOLON,
                        TokenType.COMMA,
                    ) or _is_statement_start(tokens[i]):
                        break
                    i += 1

        # Comma continues to next variable, semicolon/keyword ends
        if i < n and tokens[i].token_type == TokenType.COMMA:
            i += 1
            continue
        if i < n and tokens[i].token_type == TokenType.SEMICOLON:
            i += 1
        break

    return i


def _collect_hint_comma_insertions(
    tokens: List[Token], with_idx: int
) -> List[Tuple[int, str]]:
    """Insert commas between space-separated table hints inside WITH (...).

    sqlglot handles ``WITH (NOLOCK, NOWAIT)`` but fails on ``WITH (NOLOCK NOWAIT)``.
    Returns a list of (position, ", ") insertions, or empty if not a hint clause.
    """
    n = len(tokens)
    j = with_idx + 2  # past WITH and L_PAREN
    result: List[Tuple[int, str]] = []
    prev_was_hint = False
    while j < n and tokens[j].token_type != TokenType.R_PAREN:
        is_hint = (
            tokens[j].token_type == TokenType.VAR
            and tokens[j].text.upper() in _TABLE_HINT_KEYWORDS
        )
        if is_hint and prev_was_hint:
            result.append((tokens[j].start, ", "))
        prev_was_hint = is_hint
        if not is_hint and tokens[j].token_type != TokenType.COMMA:
            return []  # Not a hint clause
        j += 1
    return result


def preprocess_tsql(query: str) -> str:
    """Insert missing semicolons between T-SQL statements.

    PowerBI M-Query often extracts SQL without semicolons between statements.
    sqlglot's TSQL dialect handles DECLARE, WITH (NOLOCK), temp tables, etc.
    natively, but needs semicolons to split multi-statement SQL. This function
    tokenizes with sqlglot's TSQL tokenizer and inserts semicolons at statement
    boundaries where they're missing.

    Three patterns are handled:

    1. **DECLARE without trailing semicolon** — the DECLARE walker
       (``_find_declare_end``) skips past type parens and value expressions
       (including subqueries) to find where the next statement begins::

           # Input
           DECLARE @q VARCHAR(100) = 'hello' SELECT col FROM t1
           # Output
           DECLARE @q VARCHAR(100) = 'hello'; SELECT col FROM t1

    2. **SELECT INTO #temp without trailing semicolon** — detects
       ``INTO #<name>`` at top-level paren depth, then inserts ``;`` before
       the next statement-start keyword::

           # Input
           SELECT col INTO #tmp FROM t1 SELECT col2 FROM #tmp
           # Output
           SELECT col INTO #tmp FROM t1; SELECT col2 FROM #tmp

    3. **Space-separated table hints** — sqlglot handles comma-separated
       hints but fails on space-separated ones, so commas are inserted::

           # Input
           SELECT * FROM t1 WITH (NOLOCK NOWAIT)
           # Output
           SELECT * FROM t1 WITH (NOLOCK, NOWAIT)

    All patterns can appear together::

        # Input
        DECLARE @x INT = 1 SELECT col INTO #tmp FROM t1 WITH (NOLOCK NOWAIT) SELECT col FROM #tmp
        # Output
        DECLARE @x INT = 1; SELECT col INTO #tmp FROM t1 WITH (NOLOCK, NOWAIT); SELECT col FROM #tmp

    If no normalization is needed, the query is returned unchanged.
    If the tokenizer fails (e.g. non-T-SQL input), the query is returned as-is.
    """
    if not query or not query.strip():
        return query.strip()

    try:
        tokens = list(TSQL().tokenize(query))
    except Exception:
        return query

    if not tokens:
        return query

    n = len(tokens)
    insertions: List[Tuple[int, str]] = []

    paren_depth = 0
    saw_select_into_temp = False
    i = 0
    while i < n:
        tok = tokens[i]

        if tok.token_type == TokenType.L_PAREN:
            paren_depth += 1
        elif tok.token_type == TokenType.R_PAREN:
            paren_depth = max(0, paren_depth - 1)

        if paren_depth > 0:
            i += 1
            continue

        # DECLARE needs special walking because its value expressions
        # can contain subqueries — we can't just look for the next keyword.
        if tok.token_type == TokenType.DECLARE:
            end_idx = _find_declare_end(tokens, i)
            # Insert semicolon before DECLARE if it's not the first token
            if i > 0 and tokens[i - 1].token_type != TokenType.SEMICOLON:
                insertions.append((tok.start, "; "))
            # Insert semicolon after DECLARE if next token isn't one
            if end_idx < n and tokens[end_idx].token_type != TokenType.SEMICOLON:
                insertions.append((tokens[end_idx].start, "; "))
            i = end_idx
            continue

        # Normalize space-separated table hints: WITH (NOLOCK NOWAIT) → WITH (NOLOCK, NOWAIT)
        if (
            tok.token_type == TokenType.WITH
            and i + 1 < n
            and tokens[i + 1].token_type == TokenType.L_PAREN
        ):
            insertions.extend(_collect_hint_comma_insertions(tokens, i))

        # Detect INTO #temp_table pattern for semicolon insertion.
        # After HASH, the name may be TEMPORARY (for literal "temp") or VAR
        # (for any other name like #tmp, #OrgIDs, #loadinstances).
        if (
            not saw_select_into_temp
            and tok.token_type == TokenType.INTO
            and i + 2 < n
            and tokens[i + 1].token_type == TokenType.HASH
            and tokens[i + 2].token_type in (TokenType.TEMPORARY, TokenType.VAR)
        ):
            saw_select_into_temp = True
        elif saw_select_into_temp and _is_statement_start(tok):
            # Don't split on WITH (...) table hints — only on WITH as CTE start
            is_table_hint = tok.token_type == TokenType.WITH and (
                i + 1 < n and tokens[i + 1].token_type == TokenType.L_PAREN
            )
            if not is_table_hint:
                insertions.append((tok.start, "; "))
                saw_select_into_temp = False

        i += 1

    if not insertions:
        return query

    # Apply insertions in reverse order to preserve positions
    insertions.sort(key=lambda e: e[0], reverse=True)
    result = query
    for pos, text in insertions:
        result = result[:pos] + text + result[pos:]

    return result.strip()


def parse_custom_sql(
    ctx: PipelineContext,
    query: str,
    schema: Optional[str],
    database: Optional[str],
    platform: str,
    env: str,
    platform_instance: Optional[str],
) -> Optional["SqlParsingResult"]:
    logger.debug(f"Processing native query using DataHub Sql Parser = {query}")

    result = create_lineage_sql_parsed_result(
        query=query,
        default_schema=schema,
        default_db=database,
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        graph=ctx.graph,
    )

    if result.debug_info and result.debug_info.table_error:
        logger.debug(
            f"Single-statement parsing failed ({result.debug_info.table_error}), "
            "trying multi-statement parsing"
        )

        multi_result = create_lineage_from_sql_statements(
            queries=query,
            default_schema=schema,
            default_db=database,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=ctx.graph,
        )

        if multi_result.in_tables or multi_result.out_tables:
            return multi_result

    return result

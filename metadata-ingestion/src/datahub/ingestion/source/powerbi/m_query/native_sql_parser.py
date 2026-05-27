import logging
import re
from typing import List, Optional, Tuple

import sqlparse

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


def remove_tsql_control_statements(query: str) -> str:
    # Certain PowerBI M-Queries embed T-SQL control flow statements (USE, SET, GO, DROP)
    # that are not valid in standard SQL dialects and cause the SQL parser to fail.
    # Strip them before parsing so we can still extract lineage from the SELECT statements.

    patterns = [
        # DROP TABLE IF EXISTS #<temp> — temp table cleanup between statements
        r"DROP\s+TABLE\s+IF\s+EXISTS\s+(?:#?\w+(?:,\s*#?\w+)*)[;\n]",
        # USE <database> — T-SQL database context switch; \S+ handles both plain
        # identifiers (USE Reports) and bracketed ones (USE [Reports])
        r"^\s*USE\s+\S+\s*$",
        # SET <option> ON|OFF — T-SQL session-level options (NOCOUNT, QUOTED_IDENTIFIER, etc.)
        r"^\s*SET\s+\w+\s+(?:ON|OFF)\s*;?\s*$",
        # GO — T-SQL batch separator
        r"^\s*GO\s*$",
    ]

    new_query = query

    for pattern in patterns:
        new_query = re.sub(pattern, "", new_query, flags=re.IGNORECASE | re.MULTILINE)

    # SELECT … INTO #<temp> — strip only the INTO clause so FROM/WHERE lineage remains
    # parseable. Anchored to SELECT so INSERT INTO and MERGE INTO are never matched.
    # [^;\n] stops at semicolons and uses \n(?!\s*\n) to not cross blank lines,
    # preventing a SELECT in one statement from reaching an INSERT INTO in the next.
    new_query = re.sub(
        r"(SELECT\b(?:[^;\n]|\n(?!\s*\n))*?)\s+INTO\s+##?\w+",
        r"\1",
        new_query,
        flags=re.IGNORECASE,
    )

    # Leading semicolon before WITH — T-SQL defensive pattern (;WITH ...) used to ensure
    # the previous statement is terminated before a CTE. Strip only the semicolon,
    # preserving the WITH keyword so sqlglot can parse the CTE correctly.
    before = new_query
    new_query = re.sub(
        r"^\s*;(\s*WITH\b)", r"\1", new_query, flags=re.IGNORECASE | re.MULTILINE
    )
    if new_query != before:
        logger.debug("Stripped leading semicolon before WITH (CTE defensive pattern)")

    # Only normalize multiple consecutive spaces (but preserve newlines and tabs)
    # This fixes spacing issues caused by statement removal without
    # collapsing the entire query into a single line
    new_query = re.sub(r"[ \t]+", " ", new_query)
    # Remove spaces at the start of lines
    new_query = re.sub(r"\n[ \t]+", "\n", new_query)
    # Collapse 3+ consecutive blank lines down to one
    new_query = re.sub(r"\n{3,}", "\n\n", new_query)
    # Remove trailing spaces
    new_query = new_query.strip()

    return new_query


def remove_drop_statement(query: str) -> str:
    # Kept for backwards compatibility — delegates to the broader T-SQL cleanup function.
    return remove_tsql_control_statements(query)


def _has_real_semicolons(query: str) -> bool:
    """Return True if the query contains semicolons outside comments and string literals.

    Prevents comment content (e.g. '-- already done; continue') and string
    literals (e.g. SELECT 'status; pending') from incorrectly triggering the
    multi-statement parsing path.
    """
    no_line_comments = re.sub(r"--[^\n]*", "", query)
    no_comments = re.sub(r"/\*.*?\*/", "", no_line_comments, flags=re.DOTALL)
    # Strip single-quoted string literals; '' is the SQL escape for a literal quote.
    no_strings = re.sub(r"'(?:[^']|'')*'", "", no_comments, flags=re.DOTALL)
    return ";" in no_strings


def _scan_line_depth(
    line: str, paren_depth: int, in_block_comment: bool
) -> Tuple[int, bool]:
    """Return updated (paren_depth, in_block_comment) after scanning one SQL line.

    Skips characters inside line comments (--), block comments (/* ... */), and
    quoted strings so their parentheses are not counted.
    """
    in_line_comment = False
    in_string = False
    quote_char = ""
    idx = 0
    while idx < len(line):
        c = line[idx]
        if in_block_comment:
            if c == "*" and idx + 1 < len(line) and line[idx + 1] == "/":
                in_block_comment = False
                idx += 2
                continue
        elif in_line_comment:
            break
        elif in_string:
            if c == quote_char:
                in_string = False
        elif c == "/" and idx + 1 < len(line) and line[idx + 1] == "*":
            in_block_comment = True
            idx += 2
            continue
        elif c == "-" and idx + 1 < len(line) and line[idx + 1] == "-":
            in_line_comment = True
        elif c in ("'", '"'):
            in_string = True
            quote_char = c
        elif c == "(":
            paren_depth += 1
        elif c == ")":
            paren_depth = max(0, paren_depth - 1)
        idx += 1
    return paren_depth, in_block_comment


def _insert_statement_separators(query: str) -> str:
    """Insert semicolons before top-level SELECT statements preceded by blank lines.

    After remove_tsql_control_statements strips DDL between SELECT statements,
    they may be separated only by blank lines without any semicolons.  This
    function inserts the missing separators so the multi-statement parser can
    handle each statement independently.

    Unlike a simple regex, this function tracks parenthesis depth so it never
    inserts a separator inside a CTE body or subquery.  It also tracks whether
    a WITH clause is open at depth 0; the SELECT that closes a CTE belongs to
    the same statement and must not be separated from its WITH clause.
    """
    lines = query.split("\n")
    result: List[str] = []
    paren_depth = 0
    blank_count = 0
    in_cte_query = False  # True while a WITH clause is open at depth 0
    in_block_comment = False  # /* ... */ can span lines; persisted across iterations

    for line in lines:
        stripped = line.strip()

        if not stripped:
            blank_count += 1
            result.append(line)
            continue

        # Detect the opening of a CTE query (WITH keyword at depth 0).
        # Must be checked before the SELECT-separator logic so a WITH on the
        # same "paragraph" as preceding blank lines is recognised as a CTE
        # start rather than a potential new-statement boundary.
        if paren_depth == 0 and not in_cte_query:
            if re.match(r"WITH\b", stripped, re.IGNORECASE):
                in_cte_query = True

        # Consider inserting a statement separator before this line.
        if blank_count >= 1 and paren_depth == 0:
            if re.match(r"SELECT\b", stripped, re.IGNORECASE):
                if in_cte_query:
                    # This SELECT is the final query of the WITH clause.
                    # Inserting a separator here would detach it from the CTE
                    # definitions, causing sqlglot to treat CTE aliases as real
                    # tables and generate spurious upstream URNs.
                    in_cte_query = False
                else:
                    # Append to the last non-blank line for conventional SQL style.
                    for i in range(len(result) - 1, -1, -1):
                        if result[i].strip():
                            result[i] += ";"
                            break
        elif paren_depth == 0 and in_cte_query:
            # CTE closing SELECT with no preceding blank line — the blank_count
            # gate above never fires, so reset the flag here.  Without this,
            # the flag stays True and the next blank-line-separated SELECT is
            # incorrectly swallowed as the CTE closing SELECT.
            if re.match(r"SELECT\b", stripped, re.IGNORECASE):
                in_cte_query = False

        blank_count = 0
        result.append(line)

        paren_depth, in_block_comment = _scan_line_depth(
            line, paren_depth, in_block_comment
        )

    return "\n".join(result)


def parse_custom_sql(
    ctx: PipelineContext,
    query: str,
    schema: Optional[str],
    database: Optional[str],
    platform: str,
    env: str,
    platform_instance: Optional[str],
) -> Optional["SqlParsingResult"]:
    logger.debug("Using sqlglot_lineage to parse custom sql")
    logger.debug(f"Processing native query using DataHub Sql Parser = {query}")

    if _has_real_semicolons(query):
        # The query already has real statement separators — use multi-statement
        # parsing directly with the original query.  We avoid the blank-line
        # normalisation here because it can incorrectly insert semicolons inside
        # CTE bodies that happen to have blank lines before their SELECT clause.
        result = create_lineage_from_sql_statements(
            queries=query,
            default_schema=schema,
            default_db=database,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=ctx.graph,
        )
    else:
        # No real semicolons.  Blank-line-separated SELECTs can appear after
        # remove_tsql_control_statements strips the DDL between them.  Insert
        # separators only at depth-0 positions that are not the closing SELECT
        # of a CTE query, then decide which parser to use.
        normalized = _insert_statement_separators(query)
        if ";" in normalized:
            result = create_lineage_from_sql_statements(
                queries=normalized,
                default_schema=schema,
                default_db=database,
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                graph=ctx.graph,
            )
        else:
            result = create_lineage_sql_parsed_result(
                query=query,
                default_schema=schema,
                default_db=database,
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                graph=ctx.graph,
            )

    return result

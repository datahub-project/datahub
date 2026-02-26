import logging
import re
from typing import List, Optional

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

_DECLARE_PATTERN = re.compile(r"\bDECLARE\s+@", re.IGNORECASE)
_SELECT_INTO_TEMP_PATTERN = re.compile(r"\bINTO\s+#\w+", re.IGNORECASE)
_SELECT_KEYWORD_PATTERN = re.compile(r"\bSELECT\b", re.IGNORECASE)

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


def remove_table_hints(query: str) -> str:
    """Remove T-SQL table hints like WITH (NOLOCK), WITH (NOLOCK NOWAIT), etc.

    These hints are MSSQL-specific locking directives that are not relevant
    for lineage extraction and cause sqlglot parse failures.
    """
    hint_keywords = (
        r"NOLOCK|NOWAIT|READUNCOMMITTED|READCOMMITTED|REPEATABLEREAD|SERIALIZABLE"
        r"|HOLDLOCK|UPDLOCK|TABLOCK|TABLOCKX|PAGLOCK|ROWLOCK|XLOCK|READPAST"
    )
    return re.sub(
        rf"\bWITH\s*\(\s*(?:{hint_keywords})"
        rf"(?:\s+(?:{hint_keywords}))*\s*\)",
        "",
        query,
        flags=re.IGNORECASE,
    )


def remove_declare_statement(query: str) -> str:
    """Remove T-SQL DECLARE statements which are not relevant for lineage.

    Handles patterns like:
      DECLARE @var TYPE = value
      DECLARE @var TYPE = (SELECT ...)
    """
    result = query
    match = _DECLARE_PATTERN.search(result)
    while match:
        start = match.start()
        i = match.end()
        paren_depth = 0
        in_string = False
        # Walk forward to find the end of the DECLARE statement.
        # DECLARE ends at a top-level SELECT/INSERT/CREATE/WITH keyword,
        # or at a semicolon when not inside parentheses.
        while i < len(result):
            c = result[i]
            if in_string:
                if c == "'" and i + 1 < len(result) and result[i + 1] == "'":
                    i += 1  # escaped quote
                elif c == "'":
                    in_string = False
            elif c == "'":
                in_string = True
            elif c == "(":
                paren_depth += 1
            elif c == ")":
                if paren_depth > 0:
                    paren_depth -= 1
                    if paren_depth == 0:
                        # End of parenthesized expression in DECLARE.
                        # The DECLARE ends here; advance past the ')'.
                        i += 1
                        break
            elif paren_depth == 0 and c == ";":
                i += 1  # consume the semicolon
                break
            i += 1

        result = result[:start] + result[i:]
        match = _DECLARE_PATTERN.search(result, start)

    return result.strip()


def insert_semicolons_after_select_into(query: str) -> str:
    """Insert semicolons between T-SQL SELECT INTO #temp and subsequent statements.

    T-SQL scripts commonly use multiple statements without semicolons:
        SELECT ... INTO #temp FROM ... WHERE ...
        SELECT ... FROM #temp JOIN real_table ...

    This function detects SELECT INTO #temp_table patterns and inserts a
    semicolon before the next top-level SELECT to enable statement splitting.
    """
    if "#" not in query:
        return query

    result: List[str] = []
    i = 0
    paren_depth = 0
    in_string = False
    in_line_comment = False
    in_block_comment = False
    saw_into_hash = False

    while i < len(query):
        c = query[i]
        nc = query[i + 1] if i + 1 < len(query) else ""

        if in_string:
            result.append(c)
            if c == "'" and nc == "'":
                result.append(nc)
                i += 2
                continue
            elif c == "'":
                in_string = False
            i += 1
            continue

        if in_line_comment:
            result.append(c)
            if c == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            result.append(c)
            if c == "*" and nc == "/":
                result.append(nc)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if c == "'":
            in_string = True
            result.append(c)
            i += 1
            continue
        if c == "-" and nc == "-":
            in_line_comment = True
            result.append(c)
            i += 1
            continue
        if c == "/" and nc == "*":
            in_block_comment = True
            result.append(c)
            result.append(nc)
            i += 2
            continue

        if c == "(":
            paren_depth += 1
        elif c == ")":
            paren_depth = max(0, paren_depth - 1)

        if paren_depth == 0:
            if not saw_into_hash:
                m = _SELECT_INTO_TEMP_PATTERN.match(query, i)
                if m:
                    saw_into_hash = True
                    chunk = query[i : m.end()]
                    result.append(chunk)
                    i = m.end()
                    continue
            else:
                m = _SELECT_KEYWORD_PATTERN.match(query, i)
                if m:
                    result.append(";")
                    saw_into_hash = False

        result.append(c)
        i += 1

    return "".join(result)


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

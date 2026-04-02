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

    # Blank-line-separated SELECTs appear after DROP TABLE stripping removes the DDL
    # between statements, leaving no semicolons. Insert them so the multi-statement
    # check below treats them correctly.
    normalized = re.sub(r"\n{2,}(\s*SELECT\b)", r";\n\n\1", query, flags=re.IGNORECASE)

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

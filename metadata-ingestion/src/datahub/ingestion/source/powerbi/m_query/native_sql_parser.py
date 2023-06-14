import logging
from typing import List

import sqlparse
from sqllineage.runner import LineageRunner

SPECIAL_CHARACTERS = ["#(lf)", "(lf)"]
DEFAULT_SCHEMA_TEMPLATE = "<default>"

logger = logging.getLogger()


def remove_special_characters(native_query: str) -> str:
    for char in SPECIAL_CHARACTERS:
        native_query = native_query.replace(char, " ")

    return native_query


def _simple_sql_query_parsing(native_query: str, default_schema: str) -> List[str]:
    """
    It is for backward compatibility when we were using sqlparse library to find out table used in native_sql.
    """
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
            table: str = tokens[from_index].value.split("as")[0].strip()

            if len(table.split(".")) == 1:
                table = f"{default_schema}.{table}"

            tables.append(table)

        from_index = from_index + 1

    return tables


def _advance_sql_query_parsing(native_query: str, default_schema: str) -> List[str]:
    """
    This function uses sqllineage package to parse native sql.
    It supports normal as well as advance sql query construct.
    """
    tables: List[str] = []

    parser = LineageRunner(native_query)

    for table in parser.source_tables:
        tables.append(str(table).replace(DEFAULT_SCHEMA_TEMPLATE, default_schema))

    return tables


def get_tables(
    native_query: str,
    default_schema: str = "public",
    advance_sql_construct: bool = False,
) -> List[str]:
    native_query = remove_special_characters(native_query)
    logger.debug(f"Processing query = {native_query}")

    if advance_sql_construct:
        return _advance_sql_query_parsing(
            native_query=native_query,
            default_schema=default_schema,
        )

    return _simple_sql_query_parsing(
        native_query=native_query,
        default_schema=default_schema,
    )

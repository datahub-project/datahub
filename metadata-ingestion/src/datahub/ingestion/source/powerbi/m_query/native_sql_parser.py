import logging
from typing import List

import sqlparse

SPECIAL_CHARACTERS = ["#(lf)", "(lf)"]

LOGGER = logging.getLogger()


def remove_special_characters(native_query: str) -> str:
    for char in SPECIAL_CHARACTERS:
        native_query = native_query.replace(char, " ")

    return native_query


def get_tables(native_query: str) -> List[str]:
    native_query = remove_special_characters(native_query)
    LOGGER.debug("Processing query = %s", native_query)
    # As per current use-case, we are extracting only single table from "from"
    tables: List[str] = []
    parsed = sqlparse.parse(native_query)[0]

    tokens: List[sqlparse.sql.Token] = list(parsed.tokens)
    length: int = len(tokens)
    from_index: int = -1
    for index, token in enumerate(tokens):
        LOGGER.debug("%s=%s", token.value, token.ttype)
        if (
            token.value.lower().strip() == "from"
            and str(token.ttype) == "Token.Keyword"
        ):
            from_index = index + 1
            break

    table_name = None

    while from_index < length:
        LOGGER.debug("%s=%s", tokens[from_index].value, tokens[from_index].ttype)
        LOGGER.debug("Type=%s", type(tokens[from_index]))
        if isinstance(tokens[from_index], sqlparse.sql.Identifier):
            table_name = tokens[from_index].value
            break
        from_index = from_index + 1

    if table_name is not None:
        tables.append(table_name)

    return tables

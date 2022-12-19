import sqlparse
from typing import List


def get_tables(native_query: str) -> List[str]:
    # As per current use-case, we are extracting only single table from "from"
    tables: List[str] = []
    parsed = sqlparse.parse(native_query)[0]

    tokens: List[sqlparse.sql.Token] = list(parsed.tokens)
    length: int = len(tokens)
    from_index: int = -1
    for index, token in enumerate(tokens):
        if token.value.lower().strip() == "from" and str(token.ttype) == "Token.Keyword":
            from_index = index+1
            break

    table_name = None

    while from_index < length:
        if isinstance(tokens[from_index], sqlparse.sql.Identifier):
            table_name = tokens[from_index].value
            break
        from_index = from_index + 1

    if table_name is not None:
        tables.append(table_name)

    return tables

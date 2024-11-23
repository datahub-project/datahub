import re
from enum import Enum
from typing import Generator, List, Tuple

CONTROL_FLOW_KEYWORDS = [
    "GO",
    r"BEGIN\w+TRY",
    r"BEGIN\w+CATCH",
    "BEGIN",
    r"END\w+TRY",
    r"END\w+CATCH",
    "END",
]

# There's an exception to this rule, which is when the statement
# is preceeded by a CTE.
FORCE_NEW_STATEMENT_KEYWORDS = [
    # SELECT is used inside queries as well, so we can't include it here.
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
]


class ParserState(Enum):
    NORMAL = 1
    STRING = 2
    COMMENT = 3
    MULTILINE_COMMENT = 4


def _is_keyword_at_position(sql: str, pos: int, keyword: str) -> bool:
    """
    Check if a keyword exists at the given position using regex word boundaries.
    """
    if pos + len(keyword) > len(sql):
        return False

    # If we're not at a word boundary, we can't generate a keyword.
    if pos > 0 and not (
        bool(re.match(r"\w\W", sql[pos - 1 : pos + 1]))
        or bool(re.match(r"\W\w", sql[pos - 1 : pos + 1]))
    ):
        return False

    pattern = rf"^{re.escape(keyword)}\b"
    match = re.match(pattern, sql[pos:], re.IGNORECASE)
    return bool(match)


def _look_ahead_for_keywords(
    sql: str, pos: int, keywords: List[str]
) -> Tuple[bool, str, int]:
    """
    Look ahead for SQL keywords at the current position.
    """

    for keyword in keywords:
        if _is_keyword_at_position(sql, pos, keyword):
            return True, keyword, len(keyword)
    return False, "", 0


def split_statements(sql: str) -> Generator[str, None, None]:
    """
    Split T-SQL code into individual statements, handling various SQL constructs.
    """
    if not sql or not sql.strip():
        return

    current_statement: List[str] = []
    state = ParserState.NORMAL
    i = 0

    def yield_if_complete() -> Generator[str, None, None]:
        statement = "".join(current_statement).strip()
        if statement:
            yield statement
            current_statement.clear()

    prev_real_char = "\0"  # the most recent non-whitespace, non-comment character
    while i < len(sql):
        c = sql[i]
        next_char = sql[i + 1] if i < len(sql) - 1 else "\0"

        if state == ParserState.NORMAL:
            if c == "'":
                state = ParserState.STRING
                current_statement.append(c)
                prev_real_char = c
            elif c == "-" and next_char == "-":
                state = ParserState.COMMENT
                current_statement.append(c)
                current_statement.append(next_char)
                i += 1
            elif c == "/" and next_char == "*":
                state = ParserState.MULTILINE_COMMENT
                current_statement.append(c)
                current_statement.append(next_char)
                i += 1
            else:
                most_recent_real_char = prev_real_char
                if not c.isspace():
                    prev_real_char = c

                is_control_keyword, keyword, keyword_len = _look_ahead_for_keywords(
                    sql, i, keywords=CONTROL_FLOW_KEYWORDS
                )
                if is_control_keyword:
                    # Yield current statement if any
                    yield from yield_if_complete()
                    # Yield keyword as its own statement
                    yield keyword
                    i += keyword_len
                    continue

                (
                    is_force_new_statement_keyword,
                    keyword,
                    keyword_len,
                ) = _look_ahead_for_keywords(
                    sql, i, keywords=FORCE_NEW_STATEMENT_KEYWORDS
                )
                if (
                    is_force_new_statement_keyword and most_recent_real_char != ")"
                ):  # usually we'd have a close paren that closes a CTE
                    # Force termination of current statement
                    yield from yield_if_complete()

                    current_statement.append(keyword)
                    i += keyword_len
                    continue

                elif c == ";":
                    yield from yield_if_complete()
                else:
                    current_statement.append(c)

        elif state == ParserState.STRING:
            current_statement.append(c)
            if c == "'" and next_char == "'":
                current_statement.append(next_char)
                i += 1
            elif c == "'":
                state = ParserState.NORMAL

        elif state == ParserState.COMMENT:
            current_statement.append(c)
            if c == "\n":
                state = ParserState.NORMAL

        elif state == ParserState.MULTILINE_COMMENT:
            current_statement.append(c)
            if c == "*" and next_char == "/":
                current_statement.append(next_char)
                i += 1
                state = ParserState.NORMAL

        i += 1

    # Handle the last statement
    yield from yield_if_complete()

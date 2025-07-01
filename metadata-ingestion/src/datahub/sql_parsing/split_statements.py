import logging
import re
from enum import Enum
from typing import Iterator, List, Tuple

logger = logging.getLogger(__name__)
SELECT_KEYWORD = "SELECT"
CASE_KEYWORD = "CASE"
END_KEYWORD = "END"

CONTROL_FLOW_KEYWORDS = [
    "GO",
    r"BEGIN\s+TRY",
    r"BEGIN\s+CATCH",
    "BEGIN",
    r"END\s+TRY",
    r"END\s+CATCH",
    # This isn't strictly correct, but we assume that IF | (condition) | (block) should all be split up
    # This mainly ensures that IF statements don't get tacked onto the previous statement incorrectly
    "IF",
    # For things like CASE, END does not mean the end of a statement.
    # We have special handling for this.
    END_KEYWORD,
    # "ELSE",  # else is also valid in CASE, so we we can't use it here.
]

# There's an exception to this rule, which is when the statement
# is preceded by a CTE. For those, we have to check if the character
# before this is a ")".
NEW_STATEMENT_KEYWORDS = [
    # SELECT is used inside queries as well, so we can't include it here.
    "CREATE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
]
STRICT_NEW_STATEMENT_KEYWORDS = [
    # For these keywords, a SELECT following it does indicate a new statement.
    "DROP",
    "TRUNCATE",
]


class _AlreadyIncremented(Exception):
    # Using exceptions for control flow isn't great - but the code is clearer so it's fine.
    pass


class ParserState(Enum):
    NORMAL = 1
    STRING = 2
    COMMENT = 3
    MULTILINE_COMMENT = 4


class _StatementSplitter:
    def __init__(self, sql: str):
        self.sql = sql

        # Main parser state.
        self.i = 0
        self.state = ParserState.NORMAL
        self.current_statement: List[str] = []

        # Additional parser state.

        # If we see a SELECT, should we start a new statement?
        # If we previously saw a drop/truncate/etc, a SELECT does mean a new statement.
        # But if we're in a select/create/etc, a select could just be a subquery.
        self.does_select_mean_new_statement = False

        # The END keyword terminates CASE and BEGIN blocks.
        # We need to match the CASE statements with END blocks to determine
        # what a given END is closing.
        self.current_case_statements = 0

    def _is_keyword_at_position(self, pos: int, keyword: str) -> Tuple[bool, str]:
        """
        Check if a keyword exists at the given position using regex word boundaries.
        """
        sql = self.sql

        keyword_length = len(keyword.replace(r"\s+", " "))

        if pos + keyword_length > len(sql):
            return False, ""

        # If we're not at a word boundary, we can't generate a keyword.
        if pos > 0 and not (
            bool(re.match(r"\w\W", sql[pos - 1 : pos + 1]))
            or bool(re.match(r"\W\w", sql[pos - 1 : pos + 1]))
        ):
            return False, ""

        pattern = rf"^{keyword}\b"
        match = re.match(pattern, sql[pos:], re.IGNORECASE)
        is_match = bool(match)
        actual_match = (
            sql[pos:][match.start() : match.end()] if match is not None else ""
        )
        return is_match, actual_match

    def _look_ahead_for_keywords(self, keywords: List[str]) -> Tuple[bool, str, int]:
        """
        Look ahead for SQL keywords at the current position.
        """

        for keyword in keywords:
            is_match, keyword = self._is_keyword_at_position(self.i, keyword)
            if is_match:
                return True, keyword, len(keyword)
        return False, "", 0

    def _yield_if_complete(self) -> Iterator[str]:
        statement = "".join(self.current_statement).strip()
        if statement:
            # Subtle - to avoid losing full whitespace, they get merged into the next statement.
            yield statement
            self.current_statement.clear()

        # Reset current_statement-specific state.
        self.does_select_mean_new_statement = False
        if self.current_case_statements != 0:
            logger.warning(
                f"Unexpected END keyword. Current case statements: {self.current_case_statements}"
            )
        self.current_case_statements = 0

    def process(self) -> Iterator[str]:
        if not self.sql or not self.sql.strip():
            yield from ()

        prev_real_char = "\0"  # the most recent non-whitespace, non-comment character
        while self.i < len(self.sql):
            c = self.sql[self.i]
            next_char = self.sql[self.i + 1] if self.i < len(self.sql) - 1 else "\0"

            if self.state == ParserState.NORMAL:
                if c == "'":
                    self.state = ParserState.STRING
                    self.current_statement.append(c)
                    prev_real_char = c
                elif c == "-" and next_char == "-":
                    self.state = ParserState.COMMENT
                    self.current_statement.append(c)
                    self.current_statement.append(next_char)
                    self.i += 1
                elif c == "/" and next_char == "*":
                    self.state = ParserState.MULTILINE_COMMENT
                    self.current_statement.append(c)
                    self.current_statement.append(next_char)
                    self.i += 1
                else:
                    most_recent_real_char = prev_real_char
                    if not c.isspace():
                        prev_real_char = c

                    try:
                        yield from self._process_normal(
                            most_recent_real_char=most_recent_real_char
                        )
                    except _AlreadyIncremented:
                        # Skip the normal i += 1 step.
                        continue

            elif self.state == ParserState.STRING:
                self.current_statement.append(c)
                if c == "'" and next_char == "'":
                    self.current_statement.append(next_char)
                    self.i += 1
                elif c == "'":
                    self.state = ParserState.NORMAL

            elif self.state == ParserState.COMMENT:
                self.current_statement.append(c)
                if c == "\n":
                    self.state = ParserState.NORMAL

            elif self.state == ParserState.MULTILINE_COMMENT:
                self.current_statement.append(c)
                if c == "*" and next_char == "/":
                    self.current_statement.append(next_char)
                    self.i += 1
                    self.state = ParserState.NORMAL

            self.i += 1

        # Handle the last statement
        yield from self._yield_if_complete()

    def _process_normal(self, most_recent_real_char: str) -> Iterator[str]:
        c = self.sql[self.i]

        if self._is_keyword_at_position(self.i, CASE_KEYWORD)[0]:
            self.current_case_statements += 1

        is_control_keyword, keyword, keyword_len = self._look_ahead_for_keywords(
            keywords=CONTROL_FLOW_KEYWORDS
        )
        if (
            is_control_keyword
            and keyword == END_KEYWORD
            and self.current_case_statements > 0
        ):
            # If we're closing a CASE statement with END, we can just decrement the counter and continue.
            self.current_case_statements -= 1
        elif is_control_keyword:
            # Yield current statement if any
            yield from self._yield_if_complete()
            # Yield keyword as its own statement
            yield keyword
            self.i += keyword_len
            self.does_select_mean_new_statement = True
            raise _AlreadyIncremented()

        (
            is_strict_new_statement_keyword,
            keyword,
            keyword_len,
        ) = self._look_ahead_for_keywords(keywords=STRICT_NEW_STATEMENT_KEYWORDS)
        if is_strict_new_statement_keyword:
            yield from self._yield_if_complete()
            self.current_statement.append(keyword)
            self.i += keyword_len
            self.does_select_mean_new_statement = True
            raise _AlreadyIncremented()

        (
            is_force_new_statement_keyword,
            keyword,
            keyword_len,
        ) = self._look_ahead_for_keywords(
            keywords=(
                NEW_STATEMENT_KEYWORDS
                + ([SELECT_KEYWORD] if self.does_select_mean_new_statement else [])
            ),
        )
        if (
            is_force_new_statement_keyword
            and not self._has_preceding_cte(most_recent_real_char)
            and not self._is_part_of_merge_query()
        ):
            # Force termination of current statement
            yield from self._yield_if_complete()

            self.current_statement.append(keyword)
            self.i += keyword_len
            raise _AlreadyIncremented()

        if c == ";":
            yield from self._yield_if_complete()
        else:
            self.current_statement.append(c)

    def _has_preceding_cte(self, most_recent_real_char: str) -> bool:
        # usually we'd have a close paren that closes a CTE
        return most_recent_real_char == ")"

    def _is_part_of_merge_query(self) -> bool:
        # In merge statement we'd have `when matched then` or `when not matched then"
        return "".join(self.current_statement).strip().lower().endswith("then")


def split_statements(sql: str) -> Iterator[str]:
    """
    Split T-SQL code into individual statements, handling various SQL constructs.
    """

    splitter = _StatementSplitter(sql)
    yield from splitter.process()

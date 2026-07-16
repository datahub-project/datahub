import logging
import re
from typing import Final, Iterator, List, Optional, Tuple

from sqlglot import Dialect
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import Token, TokenType

logger = logging.getLogger(__name__)

# GO is a client batch separator (sqlcmd/SSMS), not a SQL statement: it appears alone on a
# line. We split on it before tokenizing because sqlglot's tsql tokenizer can otherwise
# swallow `GO\n<rest>` into a single STRING token. GO lines carry no lineage and are dropped.
_GO_BATCH_SEPARATOR = re.compile(r"(?im)^[ \t]*GO[ \t]*;?[ \t]*\r?$")

# Opening delimiter of a Postgres dollar-quoted string: $$ or $tag$. The matching close is the
# identical delimiter. Used to mask a GO line that appears inside a function body.
_DOLLAR_QUOTE_OPEN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


_OPEN: Final = {TokenType.L_PAREN, TokenType.L_BRACKET}
_CLOSE: Final = {TokenType.R_PAREN, TokenType.R_BRACKET}

_DML_STARTERS: Final = {
    TokenType.CREATE,
    TokenType.INSERT,
    TokenType.UPDATE,
    TokenType.DELETE,
    TokenType.MERGE,
    TokenType.DROP,
    TokenType.TRUNCATE,
    TokenType.ALTER,
}
_QUERY_STARTERS: Final = {TokenType.SELECT, TokenType.WITH}
_SET_OPS: Final = {TokenType.UNION, TokenType.EXCEPT, TokenType.INTERSECT}
_AS = TokenType.ALIAS  # sqlglot tokenizes `AS` as ALIAS
_CONTROL_FLOW_TEXT: Final = {
    "TRY",
    "CATCH",
    "IF",
    "WHILE",
    "LOOP",
    "GO",
    "ELSIF",
    "EXCEPTION",
}
_VARIABLE_STATEMENT_TEXT: Final = {"SET", "DECLARE"}

# A keyword immediately after one of these is a continuation (identifier / function / clause
# operand), NOT a statement boundary. Deliberately a blocklist: tokens NOT listed still permit
# a boundary, so we never miss a real statement start. PARAMETER/PLACEHOLDER are intentionally
# EXCLUDED (a statement can end with `@v` or `?`).
_CONTINUATION_TOKENS: Final = {
    TokenType.SELECT,
    TokenType.WITH,  # WITH is followed by a CTE name (identifier), never a new statement
    TokenType.UPDATE,  # UPDATE is followed by its target table (identifier)
    TokenType.FROM,
    TokenType.WHERE,
    TokenType.JOIN,
    # NOTE: ON and VALUES are intentionally NOT here -- a statement can end with them
    # (`SET NOCOUNT ON`, `INSERT ... DEFAULT VALUES`), so treating them as continuations
    # would merge a following real statement (lineage loss).
    TokenType.USING,
    TokenType.HAVING,
    TokenType.INTO,
    TokenType.SET,
    TokenType.QUALIFY,
    TokenType.LIMIT,
    TokenType.OFFSET,
    TokenType.OVER,
    TokenType.PARTITION,
    TokenType.ALIAS,
    # NOTE: ALL/DISTINCT are intentionally NOT here. `UNION ALL`/`UNION DISTINCT` continuation
    # is handled by set-op run tracking; and an identifier the tokenizer reports as the ALL/
    # DISTINCT keyword (e.g. a temp table `#1All` -> HASH+NUMBER+ALL) must still allow the next
    # statement to start, or it would merge (under-split / lineage loss).
    TokenType.UNION,
    TokenType.EXCEPT,
    TokenType.INTERSECT,
    TokenType.COMMA,
    TokenType.DOT,
    TokenType.EQ,
    TokenType.NEQ,
    TokenType.GT,
    TokenType.GTE,
    TokenType.LT,
    TokenType.LTE,
    TokenType.PLUS,
    TokenType.DASH,
    TokenType.STAR,
    TokenType.SLASH,
    TokenType.MOD,
    TokenType.AMP,
    TokenType.PIPE,
    TokenType.CARET,
    TokenType.AND,
    TokenType.OR,
    TokenType.NOT,
    TokenType.IN,
    TokenType.LIKE,
    TokenType.ILIKE,
    TokenType.BETWEEN,
    TokenType.IS,
    TokenType.WHEN,
    TokenType.THEN,
    TokenType.ELSE,
    TokenType.COLON,
    TokenType.DCOLON,
    TokenType.LR_ARROW,
    TokenType.HASH_ARROW,
    # Compound clause tokens (sqlglot emits these as single tokens in some dialects).
    TokenType.GROUP_BY,
    TokenType.ORDER_BY,
    TokenType.PARTITION_BY,
    TokenType.CLUSTER_BY,
    TokenType.GROUPING_SETS,
}
# Continuation keywords that don't have dedicated token types (tokenize as VAR/keyword text).
# TABLE/VIEW: a DDL object name follows (e.g. `DROP TABLE merge`), so it's an identifier.
_CONTINUATION_TEXT: Final = {
    "BY",
    "GROUP",
    "ORDER",
    "DO",
    "KEY",
    "FOR",
    "GRANT",
    "REVOKE",
    "TABLE",
    "VIEW",
}


def _masked_spans(sql: str) -> List[Tuple[int, int]]:
    """Inclusive (start, end) offsets of single-quoted strings, -- line comments,
    /* */ block comments, and Postgres $tag$ dollar-quoted bodies. Used to ignore GO lines
    that appear inside comments or string/quoted bodies."""
    spans: List[Tuple[int, int]] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "$":
            # Dollar-quoted string (Postgres): $$ ... $$ or $tag$ ... $tag$.
            m = _DOLLAR_QUOTE_OPEN.match(sql, i)
            if m:
                delim = m.group(0)
                close = sql.find(delim, m.end())
                end = n - 1 if close == -1 else close + len(delim) - 1
                spans.append((i, end))
                i = end + 1
                continue
            i += 1
        elif ch == "'":
            # Single-quoted string: scan until closing quote, honoring '' escape.
            start = i
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2  # escaped quote
                        continue
                    else:
                        spans.append((start, i))
                        i += 1
                        break
                i += 1
            else:
                spans.append((start, n - 1))
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            # Line comment: -- to end of line.
            start = i
            end = sql.find("\n", i + 2)
            if end == -1:
                end = n - 1
            spans.append((start, end))
            i = end + 1
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
            # Block comment: /* ... */
            start = i
            end = sql.find("*/", i + 2)
            if end == -1:
                end = n - 1
            else:
                end += 1  # include the '/'
            spans.append((start, end))
            i = end + 1
        else:
            i += 1
    return spans


def _split_go_batches(sql: str) -> List[str]:
    """Split on `GO` batch-separator lines, ignoring `GO` inside strings or comments."""
    spans = _masked_spans(sql)

    def in_masked(pos: int) -> bool:
        return any(a <= pos <= b for a, b in spans)

    batches: List[str] = []
    prev = 0
    for m in _GO_BATCH_SEPARATOR.finditer(sql):
        if in_masked(m.start()):
            continue
        batches.append(sql[prev : m.start()])
        prev = m.end()
    batches.append(sql[prev:])
    return batches


def _uses_go_batches(dialect: Optional[str]) -> bool:
    """`GO` is a client batch separator (sqlcmd/SSMS/isql) only in T-SQL/Sybase, not a SQL
    statement. Other dialects use `GO` as an ordinary identifier, so splitting on a bare `GO`
    line there would be a T-SQL bias. Gate the pre-split to T-SQL dialects only.

    Detected via the resolved dialect class so every alias that maps to T-SQL is covered
    (DataHub's `mssql` and `fabric-onelake` both resolve to sqlglot ``tsql``). Sybase has no
    sqlglot dialect (tokenizing it already fails and falls back), so it is intentionally not
    handled here."""
    try:
        return isinstance(Dialect.get_or_raise(dialect), TSQL)
    except Exception:
        return False


def _tokenize(sql: str, dialect: Optional[str]) -> List[Token]:
    """Lenient tokenize. Raises only if sqlglot itself raises (caller handles)."""
    return Dialect.get_or_raise(dialect).tokenizer_class().tokenize(sql)


class _TokenSplitter:
    """Finds top-level statement boundaries on a sqlglot token stream and yields
    statements as slices of the ORIGINAL source (verbatim text, comments preserved)."""

    def __init__(self, sql: str, tokens: List[Token]):
        self.sql = sql
        self.tokens = tokens
        self.depth = 0
        self.seg_start = 0  # start offset of the current (in-progress) statement
        self.last_end = -1  # .end offset of the last meaningful (depth-0) token seen
        self.stmt_first: Optional[TokenType] = (
            None  # first meaningful token type of current stmt
        )
        self.insert_active = (
            False  # inside INSERT/MERGE, source SELECT not yet consumed
        )
        self.setop_run = (
            False  # just saw a set operator (UNION/EXCEPT/INTERSECT [ALL|DISTINCT])
        )
        self.create_active = False  # inside a CREATE (for CREATE ... AS SELECT)
        self.just_as = False  # previous meaningful token was AS within a CREATE
        self.prev_type: Optional[TokenType] = (
            None  # previous meaningful depth-0 token type
        )
        self.prev_text: Optional[str] = (
            None  # previous meaningful depth-0 token text (upper)
        )
        self.after_then = (
            False  # previous meaningful depth-0 token was THEN (MERGE branch body)
        )
        self.case_depth = (
            0  # depth of open CASE expressions (their END is not a boundary)
        )

    def _prev_is_continuation(self) -> bool:
        return (
            self.prev_type in _CONTINUATION_TOKENS
            or self.prev_text in _CONTINUATION_TEXT
        )

    def _emit(self, end_exclusive: int, results: List[str]) -> None:
        # A segment is a statement only if it contained at least one token. The tokenizer
        # emits no tokens for comments/whitespace (any dialect: --, /* */, #), so
        # `last_end < seg_start` means nothing but comments/whitespace occurred here and the
        # slice is not emitted. This delegates comment detection to the tokenizer.
        if self.last_end >= self.seg_start:
            chunk = self.sql[self.seg_start : end_exclusive].strip()
            if chunk:
                results.append(chunk)

    def _start_new_statement(self, tok: Token, results: List[str]) -> None:
        # Close the previous statement at the end of its last meaningful token, so any
        # comments/whitespace between it and `tok` attach to the NEW statement (leading).
        if self.last_end >= 0:
            self._emit(self.last_end + 1, results)
            self.seg_start = self.last_end + 1
        self.stmt_first = tok.token_type
        self.insert_active = tok.token_type == TokenType.INSERT
        self.create_active = tok.token_type == TokenType.CREATE

    def _try_consume_structural(
        self,
        tt: TokenType,
        text_upper: str,
        i: int,
        tok: Token,
        results: List[str],
    ) -> bool:
        """Handle depth tracking, CASE/END depth, control-flow peeling, and SEMICOLON.

        Returns True if the token was fully consumed (caller should ``continue``).
        """
        # --- bracket depth ---
        if tt in _OPEN:
            self.depth += 1
            self.last_end = tok.end
            return True
        if tt in _CLOSE:
            self.depth = max(0, self.depth - 1)
            self.last_end = tok.end
            if self.depth == 0:
                # Record that we just closed a top-level paren/bracket; the CTE
                # continuation guard uses this to detect WITH ... (...) SELECT.
                self.prev_type = tt
            return True

        # --- depth != 0 short-circuit ---
        if self.depth != 0:
            if tt in _SET_OPS:
                self.setop_run = True
            self.last_end = tok.end
            return True

        # --- CASE/END depth ---
        if tt == TokenType.CASE:
            self.case_depth += 1
            self.stmt_first = self.stmt_first or tt
            self.prev_type = tt
            self.last_end = tok.end
            return True
        if tt == TokenType.END and self.case_depth > 0:
            self.case_depth -= 1
            self.prev_type = tt
            self.last_end = tok.end
            return True

        # --- control-flow peeling ---
        is_control_flow = (
            tt in (TokenType.BEGIN, TokenType.END) or text_upper in _CONTROL_FLOW_TEXT
        )
        if text_upper == "IF":
            nxt = self.tokens[i + 1].text.upper() if i + 1 < len(self.tokens) else ""
            nxt2 = self.tokens[i + 2].text.upper() if i + 2 < len(self.tokens) else ""
            if nxt == "EXISTS" or (nxt == "NOT" and nxt2 == "EXISTS"):
                is_control_flow = False  # DDL: IF [NOT] EXISTS
        # A control-flow keyword after a continuation token is an identifier, not a boundary.
        if is_control_flow and self._prev_is_continuation():
            is_control_flow = False
        if is_control_flow:
            if tok.start > self.seg_start:
                self._start_new_statement(tok, results)
            else:
                self.stmt_first = self.stmt_first or tt
            self.setop_run = False
            self.just_as = False
            self.after_then = False
            self.prev_type = tt
            self.last_end = tok.end
            return True

        # --- SEMICOLON ---
        if tt == TokenType.SEMICOLON:
            self._emit(tok.start, results)
            self.seg_start = tok.end + 1
            self.last_end = tok.end
            # Reset all per-statement flags symmetrically.
            # case_depth is intentionally NOT reset: a ';' cannot appear inside a CASE
            # expression at depth 0, so it is always 0 here; but structural depth
            # counters are owned by the tokenizer, not the statement boundary.
            self.stmt_first = None
            self.insert_active = self.create_active = False
            self.setop_run = self.just_as = False
            self.after_then = False
            self.prev_type = None
            self.prev_text = None
            return True

        return False

    def _classify_new_statement(
        self,
        tt: TokenType,
        text_upper: str,
        i: int,
        cte_main: bool,
    ) -> bool:
        """Return True if this token starts a new statement; update continuation flags."""
        # A keyword immediately after a continuation token is an identifier/operand, not a
        # new statement (e.g. a column named `end`, a table named `merge`, AS alias names,
        # GRANT/REVOKE privilege lists, FOR UPDATE locking clauses, etc.).
        if self._prev_is_continuation():
            return False
        if tt in _DML_STARTERS:
            return not self.after_then and not cte_main
        if (
            text_upper in _VARIABLE_STATEMENT_TEXT
            and i + 1 < len(self.tokens)
            and self.tokens[i + 1].token_type == TokenType.PARAMETER
        ):
            return True
        if tt in _QUERY_STARTERS:
            is_continuation = (
                self.setop_run or self.insert_active or self.just_as or cte_main
            )
            if is_continuation:
                self.insert_active = (
                    False  # the INSERT/MERGE source SELECT is now consumed
                )
                self.just_as = False
                return False
            return True
        return False

    def split(self) -> List[str]:
        results: List[str] = []
        for i, tok in enumerate(self.tokens):
            tt = tok.token_type
            text_upper = (tok.text or "").upper()

            if self._try_consume_structural(tt, text_upper, i, tok, results):
                continue

            cte_main = self.prev_type in _CLOSE and self.stmt_first == TokenType.WITH
            new_stmt = self._classify_new_statement(tt, text_upper, i, cte_main)

            if new_stmt and tok.start > self.seg_start:
                self._start_new_statement(tok, results)
            else:
                if self.stmt_first is None:
                    self.stmt_first = tt
                if tt == TokenType.INSERT:
                    self.insert_active = True
                if tt == TokenType.CREATE:
                    self.create_active = True

            # A VALUES clause means the INSERT has no source SELECT, so a later top-level
            # SELECT is a new statement, not the insert's source.
            if text_upper == "VALUES":
                self.insert_active = False

            if tt in _SET_OPS:
                self.setop_run = True
            elif text_upper not in ("ALL", "DISTINCT"):
                self.setop_run = False
            self.just_as = tt == _AS and self.create_active
            self.after_then = text_upper == "THEN"
            self.prev_type = tt
            self.prev_text = text_upper
            self.last_end = tok.end

        self._emit(len(self.sql), results)
        return results


def split_statements(sql: str, dialect: Optional[str] = None) -> Iterator[str]:
    """Split SQL into individual statements for per-statement lineage parsing.

    Boundary detection runs on a sqlglot token stream (dialect-aware lexing); each
    statement is returned as a verbatim slice of the original source. Used for stored
    procedures (MSSQL/Oracle/Postgres/MySQL/DB2) and Tableau Initial SQL, including
    semicolon-less T-SQL/PL-SQL batches.

    The `GO` batch-separator pre-split is applied only for T-SQL dialects (where `GO` is a
    client batch separator); for every other dialect `GO` is treated as an ordinary token.

    On tokenizer failure, yields the whole input unchanged (never raises, never loses SQL).
    """
    if not sql or not sql.strip():
        return
    batches = _split_go_batches(sql) if _uses_go_batches(dialect) else [sql]
    for batch in batches:
        if not batch or not batch.strip():
            continue
        try:
            tokens = _tokenize(batch, dialect)
            statements = _TokenSplitter(batch, tokens).split()
        except Exception as e:
            # Warn (not debug): the whole batch is yielded as one blob, which
            # parse_statement() will likely fail to parse, silently losing the
            # entire procedure/batch's lineage. Operators need a visible signal.
            logger.warning(
                f"split_statements failed for dialect={dialect!r} ({e!r}); "
                f"yielding whole batch as one statement. batch_prefix={batch[:120]!r}"
            )
            yield batch
            continue
        yield from statements

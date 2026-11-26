import re
from typing import Final, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel
from sqlglot import exp

from datahub.ingestion.source.kafka.stream_processing.constants import (
    FLINK_SQL_DIALECT,
)

# Flink window table-valued functions take their input as `TABLE <name>`, e.g.
# `FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE))`, which sqlglot
# cannot parse. The input table is the only table the call reads.
_WINDOW_TVF_RE: Final["re.Pattern[str]"] = re.compile(
    r"\bTABLE\s*\(\s*(?:TUMBLE|HOP|CUMULATE|SESSION)\s*\(", re.IGNORECASE
)
_TVF_TABLE_ARG_RE: Final["re.Pattern[str]"] = re.compile(
    r"\s*(?:DATA\s*=>\s*)?TABLE\s+((?:`[^`]*`|[\w-]+)(?:\s*\.\s*(?:`[^`]*`|[\w-]+))*)",
    re.IGNORECASE,
)
_STATEMENT_SET_RE: Final["re.Pattern[str]"] = re.compile(
    r"^\s*EXECUTE\s+STATEMENT\s+SET\s+BEGIN\b(?P<body>.*)\bEND\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# Flink accepts `EXECUTE INSERT INTO ...` as a synonym for `INSERT INTO ...`.
_EXECUTE_PREFIX_RE: Final["re.Pattern[str]"] = re.compile(
    r"^\s*EXECUTE\s+(?=INSERT\b)", re.IGNORECASE
)
# Keywords sqlglot only reads as an opaque Command; seeing one means data-moving SQL
# was not understood, not that the statement moves no data.
_UNREADABLE_DML_KEYWORDS: Final[frozenset] = frozenset(
    {"INSERT", "CREATE", "EXECUTE", "BEGIN", "END"}
)


class FlinkSqlParseError(Exception):
    pass


class FlinkTableRef(BaseModel):
    catalog: Optional[str] = None
    database: Optional[str] = None
    table: str


class FlinkDmlStatement(BaseModel):
    """One data-moving statement (INSERT INTO or CREATE TABLE ... AS SELECT)."""

    model_config = {"arbitrary_types_allowed": True}

    expression: exp.Expression
    output: exp.Table
    inputs: List[exp.Table]


class FlinkParsedStatement(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    dml: List[FlinkDmlStatement]
    # Views and temporary tables the statement creates. They are not Kafka topics, so
    # writes to them and reads of them elsewhere in the statement are not topic edges.
    created_non_topics: List[exp.Table]


def parse_flink_statement(sql: str) -> FlinkParsedStatement:
    """Parse a Flink SQL statement into its INSERT / CTAS parts.

    `dml` is empty for statements that move no data (SELECT, DDL without AS SELECT,
    SET). Raises FlinkSqlParseError when the SQL cannot be parsed, so the caller can
    skip the statement rather than guess its tables.
    """
    sql = _blank_comments(sql)
    statement_set = _STATEMENT_SET_RE.match(sql)
    body = statement_set.group("body") if statement_set else sql
    body = _unwrap_window_tvfs(_EXECUTE_PREFIX_RE.sub("", body))
    try:
        expressions = sqlglot.parse(body, read=FLINK_SQL_DIALECT)
    except sqlglot.errors.SqlglotError as e:
        # Flink DDL (computed columns, WATERMARK, DISTRIBUTED BY) is often beyond
        # sqlglot; it moves no data, so only SQL that can is an error.
        if not _may_move_data(body):
            return FlinkParsedStatement(dml=[], created_non_topics=[])
        raise FlinkSqlParseError(str(e)) from e

    statements: List[FlinkDmlStatement] = []
    created_non_topics: List[exp.Table] = []
    for expression in expressions:
        if not isinstance(expression, exp.Expression):
            continue
        if isinstance(expression, exp.Create) and _is_not_a_topic(expression):
            created = expression.this.find(exp.Table) if expression.this else None
            if created is not None:
                created_non_topics.append(created)
        if isinstance(expression, exp.Command):
            # sqlglot falls back to a Command for syntax it cannot read. Plain DDL
            # (CREATE TABLE without AS SELECT) moves no data, so it is not an error.
            if str(
                expression.this
            ).upper() not in _UNREADABLE_DML_KEYWORDS or not _may_move_data(
                expression.sql(dialect=FLINK_SQL_DIALECT)
            ):
                continue
            raise FlinkSqlParseError(
                f"unsupported syntax: {expression.sql(dialect=FLINK_SQL_DIALECT)[:100]}"
            )
        dml = _as_dml(expression)
        if dml is not None:
            statements.append(dml)
    return FlinkParsedStatement(dml=statements, created_non_topics=created_non_topics)


def _is_not_a_topic(create: exp.Create) -> bool:
    kind = str(create.args.get("kind", "")).upper()
    temporary = create.find(exp.TemporaryProperty) is not None
    return kind == "VIEW" or (kind == "TABLE" and temporary)


def table_ref(table: exp.Table) -> FlinkTableRef:
    return FlinkTableRef(
        catalog=table.catalog or None, database=table.db or None, table=table.name
    )


def _as_dml(expression: exp.Expression) -> Optional[FlinkDmlStatement]:
    if isinstance(expression, exp.Insert) or (
        isinstance(expression, exp.Create)
        and str(expression.args.get("kind", "")).upper() == "TABLE"
        and expression.expression is not None
    ):
        target = expression.this
        query = expression.expression
    else:
        return None

    output = target if isinstance(target, exp.Table) else target.find(exp.Table)
    if output is None or query is None:
        return None

    cte_names = {cte.alias_or_name for cte in expression.find_all(exp.CTE)}
    inputs = [
        table
        for table in expression.find_all(exp.Table)
        if table is not output
        and table.name
        and not (table.name in cte_names and not table.db and not table.catalog)
    ]
    return FlinkDmlStatement(expression=expression, output=output, inputs=inputs)


_WORD_RE: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_]+")


def _may_move_data(sql: str) -> bool:
    """Whether SQL that could not be parsed might be an INSERT or a CTAS.

    Only words outside literals, quoted names and parentheses count, so a computed
    column (`total AS (price * qty)`) or a WATERMARK inside a CREATE TABLE column list
    does not look like `CREATE TABLE ... AS SELECT`. A second statement's CREATE does
    not reach back past the first one's `;`, since each CREATE is checked on its own.
    """
    code = _code_mask(sql)
    depth = 0
    top_level: List[str] = []
    for i, char in enumerate(sql):
        if not code[i]:
            top_level.append(" ")
        elif char == "(":
            depth += 1
            top_level.append(" " if depth > 1 else "(")
        elif char == ")":
            depth -= 1
            top_level.append(" ")
        else:
            top_level.append(char if depth == 0 else " ")
    words = [word.upper() for word in _WORD_RE.findall("".join(top_level))]
    if "INSERT" in words:
        return True
    # Only CREATE [OR REPLACE] TABLE ... AS <query> writes a topic;
    # CREATE FUNCTION ... AS 'class' or CREATE VIEW ... AS SELECT do not.
    for i, word in enumerate(words):
        if word != "CREATE":
            continue
        rest = words[i + 1 :]
        while rest and rest[0] in ("OR", "REPLACE"):
            rest = rest[1:]
        # A TEMPORARY table lives in the session, not in a topic.
        if rest[:1] == ["TABLE"] and "AS" in rest:
            return True
    return False


def _unwrap_window_tvfs(sql: str) -> str:
    code = _code_mask(sql)
    out: List[str] = []
    position = 0
    for match in _WINDOW_TVF_RE.finditer(sql):
        if match.start() < position or not code[match.start()]:
            continue
        open_paren = sql.index("(", match.start())
        close_paren = _matching_paren(sql, code, open_paren)
        if close_paren is None:
            continue
        argument = _TVF_TABLE_ARG_RE.match(sql, match.end())
        if argument is None:
            continue
        out.append(sql[position : match.start()])
        out.append(argument.group(1))
        position = close_paren + 1
    out.append(sql[position:])
    return "".join(out)


def _blank_comments(sql: str) -> str:
    # Comments can sit anywhere, e.g. between `TUMBLE(` and its `TABLE` argument;
    # spaces keep the window-function matching simple and change nothing sqlglot reads.
    chars = list(sql)
    for start, end in _non_code_spans(sql):
        if sql.startswith("--", start) or sql.startswith("/*", start):
            for i in range(start, end):
                if chars[i] != "\n":
                    chars[i] = " "
    return "".join(chars)


def _code_mask(sql: str) -> List[bool]:
    mask = [True] * len(sql)
    for start, end in _non_code_spans(sql):
        for i in range(start, end):
            mask[i] = False
    return mask


def _non_code_spans(sql: str) -> List[Tuple[int, int]]:
    # Spans of string literals, quoted identifiers and comments.
    spans: List[Tuple[int, int]] = []
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            end = len(sql) if end == -1 else end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = len(sql) if end == -1 else end + 2
        elif sql[i] in "'`\"":
            quote = sql[i]
            end = i + 1
            while end < len(sql):
                if sql[end] == quote:
                    if end + 1 < len(sql) and sql[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
        else:
            i += 1
            continue
        spans.append((i, end))
        i = end
    return spans


def _matching_paren(sql: str, code: List[bool], open_paren: int) -> Optional[int]:
    depth = 0
    for i in range(open_paren, len(sql)):
        if not code[i]:
            continue
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return None

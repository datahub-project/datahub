import re
from typing import Final, List, Optional

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
    outputs: List[exp.Table]
    inputs: List[exp.Table]


def parse_flink_statement(sql: str) -> List[FlinkDmlStatement]:
    """Parse a Flink SQL statement into its INSERT / CTAS parts.

    Returns [] for statements that move no data (SELECT, DDL without AS SELECT, SET).
    Raises FlinkSqlParseError when the SQL cannot be parsed, so the caller can skip
    the statement rather than guess its tables.
    """
    statement_set = _STATEMENT_SET_RE.match(sql)
    body = statement_set.group("body") if statement_set else sql
    body = _unwrap_window_tvfs(body)
    try:
        expressions = sqlglot.parse(body, read=FLINK_SQL_DIALECT)
    except sqlglot.errors.SqlglotError as e:
        raise FlinkSqlParseError(str(e)) from e

    statements: List[FlinkDmlStatement] = []
    for expression in expressions:
        if not isinstance(expression, exp.Expression):
            continue
        if isinstance(expression, exp.Command):
            # sqlglot falls back to a Command for syntax it cannot read; only an
            # INSERT or CREATE could have moved data.
            if str(expression.this).upper() not in ("INSERT", "CREATE"):
                continue
            raise FlinkSqlParseError(
                f"unsupported syntax: {expression.sql(dialect=FLINK_SQL_DIALECT)[:100]}"
            )
        dml = _as_dml(expression)
        if dml is not None:
            statements.append(dml)
    return statements


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
    return FlinkDmlStatement(expression=expression, outputs=[output], inputs=inputs)


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


def _code_mask(sql: str) -> List[bool]:
    # True for characters outside string literals, quoted identifiers and comments.
    mask = [True] * len(sql)
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
        for j in range(i, end):
            mask[j] = False
        i = end
    return mask


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

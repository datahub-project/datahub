from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import sqlglot

from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_common import get_dialect_str
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement


@dataclass
class OutputColumns:
    parsed: bool
    is_insert: bool = False
    is_select: bool = False
    # Lowercased parser output name -> column of the written table. None when
    # the written columns are not known, such as an INSERT with no column list.
    by_output: Optional[Dict[str, str]] = None


def _parse(sql: str, platform: str) -> Optional[sqlglot.exp.Expression]:
    try:
        return parse_statement(sql, get_dialect(platform))
    except (sqlglot.errors.SqlglotError, ValueError, TypeError):
        return None


def statement_count(sql: str, platform: str) -> int:
    return sum(
        1
        for statement in split_statements(sql, dialect=get_dialect_str(platform))
        if statement.strip()
    )


def update_statements(sql: str, platform: str) -> List[str]:
    return [
        statement
        for statement in split_statements(sql, dialect=get_dialect_str(platform))
        if isinstance(_parse(statement, platform), sqlglot.exp.Update)
    ]


def _unique(pairs: List[Tuple[str, str]]) -> Dict[str, str]:
    by_output: Dict[str, str] = {}
    duplicates: Set[str] = set()
    for output, column in pairs:
        if not output:
            continue
        if output in by_output:
            duplicates.add(output)
        by_output[output] = column
    return {
        output: column
        for output, column in by_output.items()
        if output not in duplicates
    }


def output_columns(sql: str, platform: str) -> OutputColumns:
    statement = _parse(sql, platform)
    if statement is None:
        return OutputColumns(parsed=False)
    if isinstance(statement, sqlglot.exp.Select):
        # A SELECT loaded into a table keeps its output names. A star or an
        # unnamed expression has no name to carry over.
        if statement.is_star:
            return OutputColumns(parsed=True, is_select=True, by_output={})
        return OutputColumns(
            parsed=True,
            is_select=True,
            by_output=_unique(
                [
                    (projection.alias_or_name.lower(), projection.alias_or_name)
                    for projection in statement.selects
                ]
            ),
        )
    if not isinstance(statement, sqlglot.exp.Insert):
        return OutputColumns(parsed=True)
    if not isinstance(statement.this, sqlglot.exp.Schema):
        return OutputColumns(parsed=True, is_insert=True)
    # The SQL parser names an INSERT's target columns after the SELECT's output
    # names and ignores the INSERT's column list, so they are mapped back by
    # position here.
    columns = [column.name for column in statement.this.expressions]
    query = statement.expression
    if (
        not isinstance(query, sqlglot.exp.Select)
        or query.is_star
        or len(query.selects) != len(columns)
    ):
        return OutputColumns(parsed=True, is_insert=True, by_output={})
    return OutputColumns(
        parsed=True,
        is_insert=True,
        by_output=_unique(
            [
                (projection.alias_or_name.lower(), column)
                for projection, column in zip(query.selects, columns, strict=True)
            ]
        ),
    )

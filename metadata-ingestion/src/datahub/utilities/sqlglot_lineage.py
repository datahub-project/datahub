from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
import sqlglot.lineage


@dataclass(frozen=True)
class TableName:
    database: Optional[str]
    schema: Optional[str]
    table: str


@dataclass
class ColumnRef:
    table: TableName
    column: str


@dataclass
class UnresolvedColumnRef:
    table: Optional[TableName]
    column: str


@dataclass
class ColumnLineageInfo:
    downstream: UnresolvedColumnRef
    upstreams: List[ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str] = None


@dataclass
class SqlParsingResult:
    in_tables: List[TableName]
    out_tables: List[TableName]

    column_lineage: Optional[List[ColumnLineageInfo]]


def _raw_table_name(table: sqlglot.exp.Table, dialect: str) -> TableName:
    # TODO: Do we need dialect-specific quoting rules?
    return TableName(
        database=table.catalog,
        schema=table.db,
        table=table.this.name,
    )


def _parse_statement(sql: str, dialect: str) -> sqlglot.Expression:
    statement = sqlglot.parse_one(
        sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE
    )
    return statement


def _table_level_lineage(
    statement: sqlglot.Expression, dialect: str
) -> Tuple[Set[TableName], Set[TableName]]:
    # Generate table-level lineage.
    modified = {
        _raw_table_name(expr.this, dialect=dialect)
        for expr in statement.find_all(
            sqlglot.exp.Create,
            sqlglot.exp.Insert,
            sqlglot.exp.Update,
            sqlglot.exp.Delete,
            sqlglot.exp.Merge,
        )
        # In some cases like "MERGE ... then INSERT (col1, col2) VALUES (col1, col2)",
        # the `this` on the INSERT part isn't a table.
        # TODO: Maybe we should only consider top-level parts of the statement?
        if isinstance(expr.this, sqlglot.exp.Table)
    }

    tables = (
        {
            _raw_table_name(table, dialect=dialect)
            for table in statement.find_all(sqlglot.exp.Table)
        }
        # ignore references created in this query
        - modified
        # ignore CTEs created in this statement
        # TODO: add this back in
        # - {cte.alias_or_name for cte in statement.find_all(sqlglot.exp.CTE)}
    )
    # TODO: If a CTAS has "LIMIT 0", it's not really lineage, just copying the schema.

    return tables, modified


def _column_level_lineage(
    statement: sqlglot.Expression, dialect: str, input_tables
) -> List[ColumnLineageInfo]:
    pass


def sqlglot_tester(sql: str, dialect: str) -> SqlParsingResult:
    statement = _parse_statement(sql, dialect=dialect)
    # original_statement = statement.copy()

    # Generate table-level lineage.
    tables, modified = _table_level_lineage(statement, dialect=dialect)

    # Generate column-level lineage.
    downstream_table: Optional[TableName] = None
    if len(modified) == 1:
        downstream_table = next(iter(modified))
    if isinstance(statement, sqlglot.exp.Merge):
        # TODO Need to map column renames in the expressions part of the statement.
        statement = statement.args["using"]
        if isinstance(statement, sqlglot.exp.Table):
            # If we're querying a table directly, wrap it in a SELECT.
            statement = sqlglot.exp.Select().select("*").from_(statement)
    elif isinstance(statement, sqlglot.exp.Insert):
        statement = statement.expression
    elif isinstance(statement, sqlglot.exp.Create) and isinstance(
        statement.expression, sqlglot.exp.Select
    ):
        # TODO May need to map column renames.
        # TODO: Retain the original view name as the output table name.
        statement = statement.expression

    column_lineage: Optional[List[ColumnLineageInfo]] = None
    if isinstance(
        statement,
        (
            sqlglot.exp.Select,
            sqlglot.exp.Union,
            # For subqueries.
            sqlglot.exp.DerivedTable,
        ),
    ):
        # TODO: Loosen this requirement to support other types of statements.
        column_lineage = []

        try:
            # List output columns.
            output_columns = [
                select_col.alias_or_name for select_col in statement.selects
            ]
            # breakpoint()
            for output_col in output_columns:
                # print(f"output column: {output_col}")

                # Using a set here to deduplicate upstreams.
                output_col_upstreams = set()

                try:
                    lineage_node = sqlglot.lineage.lineage(output_col, statement)
                except ValueError as e:
                    if e.args[0].startswith("Could not find "):
                        print(f" failed to find col {output_col} -> {e}")
                        continue
                    else:
                        raise

                for node in lineage_node.walk():
                    if node.downstream:
                        # We only want the leaf nodes.
                        continue

                    if isinstance(node.expression, sqlglot.exp.Table):
                        table_ref = _raw_table_name(node.expression, dialect=dialect)
                        col = node.name
                        if "." in col:
                            # TODO: Not sure if this is enough, in case of a fully-qualified column name.
                            col = col.split(".", maxsplit=1)[1]
                        # print(f"-> depends on {table_ref} . {col}")

                        output_col_upstreams.add(ColumnRef(table_ref, col))
                    else:
                        # This branch doesn't matter as much. For example, a count() column would go here.
                        pass

            if output_col_upstreams:
                column_lineage.append(
                    ColumnLineageInfo(
                        downstream=UnresolvedColumnRef(downstream_table, output_col),
                        upstreams=list(output_col_upstreams),
                        logic=lineage_node.source.sql(pretty=True, dialect=dialect),
                    )
                )

            # x = str(lineage.to_html(dialect=dialect))
            # pathlib.Path("sqlglot.html").write_text(x)
            # breakpoint()
        except sqlglot.errors.OptimizeError as e:
            print(f" failed sqlglot optimization; probably requires schema info -> {e}")
    else:
        # Cannot generate column-level lineage for this statement type.
        print(
            f'Cannot generate column-level lineage for statement type "{type(statement)}"'
        )

    # TODO fallback to sqllineage / openlineage if sqlglot fails.

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    return SqlParsingResult(
        in_tables=list(tables),
        out_tables=list(modified),
        column_lineage=column_lineage,
    )

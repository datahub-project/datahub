import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer.qualify

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableName:
    database: Optional[str]
    schema: Optional[str]
    table: str

    def as_sqlglot_table(self) -> sqlglot.exp.Table:
        return sqlglot.exp.Table(
            catalog=self.database, schema=self.schema, name=self.table
        )

    @classmethod
    def from_sqlglot_table(
        cls,
        table: sqlglot.exp.Table,
        dialect: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "TableName":
        # TODO: Do we need dialect-specific quoting rules?
        return cls(
            database=table.catalog or default_db,
            schema=table.db or default_schema,
            table=table.this.name,
        )


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


def _parse_statement(sql: str, dialect: str) -> sqlglot.Expression:
    statement = sqlglot.parse_one(
        sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE
    )
    return statement


def _table_level_lineage(
    statement: sqlglot.Expression,
    dialect: str,
) -> Tuple[Set[TableName], Set[TableName]]:
    def _raw_table_name(table: sqlglot.exp.Table) -> TableName:
        return TableName.from_sqlglot_table(table, dialect=dialect)

    # Generate table-level lineage.
    modified = {
        _raw_table_name(expr.this)
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
        {_raw_table_name(table) for table in statement.find_all(sqlglot.exp.Table)}
        # ignore references created in this query
        - modified
        # ignore CTEs created in this statement
        # TODO: add this back in
        # - {cte.alias_or_name for cte in statement.find_all(sqlglot.exp.CTE)}
    )
    # TODO: If a CTAS has "LIMIT 0", it's not really lineage, just copying the schema.

    return tables, modified


# TODO generate a lightweight schema type to use instead of Any
SchemaInfo = Any


class SchemaResolver:
    def __init__(self, platform: str):
        self.platform = platform

        # TODO add a file-backed dict here
        # TODO add DataHubGraph

    def resolve_table(self, table: TableName) -> Tuple[str, Optional[SchemaInfo]]:
        # This must always generate an urn, and try to get the schema if possible.

        # TODO implement this

        return "", None


class UnsupportedStatementTypeError(Exception):
    pass


class SqlOptimizerError(Exception):
    # Usually hit when we need schema info for a given statement but don't have it.
    pass


def _column_level_lineage(
    statement: sqlglot.exp.Expression,
    dialect: str,
    input_tables: Dict[TableName, SchemaInfo],
    output_table: Optional[TableName],
) -> List[ColumnLineageInfo]:
    sqlglot_db_schema = sqlglot.MappingSchema()
    for table, schema in input_tables.items():
        sqlglot_db_schema.add_table(
            table.as_sqlglot_table(),
            column_mapping={
                # TODO fill this out
            },
        )

    if not isinstance(
        statement,
        (
            sqlglot.exp.Subqueryable,
            # Note that Select and Union inherit from Subqueryable.
            # For actual subqueries.
            sqlglot.exp.DerivedTable,
        ),
    ):
        # TODO: Loosen this requirement to support other types of statements.
        raise UnsupportedStatementTypeError(
            "Can only generate column-level lineage for select-like statements"
        )

    column_lineage = []

    try:
        # List output columns.
        output_columns = [select_col.alias_or_name for select_col in statement.selects]
        # breakpoint()
        for output_col in output_columns:
            # print(f"output column: {output_col}")

            # Using a set here to deduplicate upstreams.
            output_col_upstreams = set()

            try:
                # TODO inject input_tables schema info here
                lineage_node = sqlglot.lineage.lineage(
                    output_col, statement, schema=schema
                )
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
                    table_ref = TableName.from_sqlglot_table(
                        node.expression, dialect=dialect
                    )
                    col = node.name
                    if "." in col:
                        # TODO: Not sure if this is enough, in case of a fully-qualified column name.
                        col = col.split(".", maxsplit=1)[1]
                    # print(f"-> depends on {table_ref} . {col}")

                    output_col_upstreams.add(ColumnRef(table_ref, col))
                else:
                    # This branch doesn't matter. For example, a count(*) column would go here, and
                    # we don't get any column-level lineage for that.
                    pass

        if output_col_upstreams:
            column_lineage.append(
                ColumnLineageInfo(
                    downstream=UnresolvedColumnRef(output_table, output_col),
                    upstreams=list(output_col_upstreams),
                    logic=lineage_node.source.sql(pretty=True, dialect=dialect),
                )
            )

        # x = str(lineage.to_html(dialect=dialect))
        # pathlib.Path("sqlglot.html").write_text(x)
        # breakpoint()
    except sqlglot.errors.OptimizeError as e:
        raise SqlOptimizerError(
            f"sqlglot failed to optimize; likely missing table schema info: {e}"
        ) from e

    return column_lineage


def sqlglot_tester(
    sql: str,
    platform: str,
    schema_resolver: SchemaResolver,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> SqlParsingResult:
    # TODO: convert datahub platform names to sqlglot dialect
    dialect = platform

    statement = _parse_statement(sql, dialect=dialect)
    # original_statement = statement.copy()

    # Make sure the tables are resolved with the default db / schema.
    # sqlglot calls the db -> schema -> table hierarchy "catalog", "db", "table".
    statement = sqlglot.optimizer.qualify.qualify(
        statement, dialect=dialect, catalog=default_db, db=default_schema
    )

    # Generate table-level lineage.
    tables, modified = _table_level_lineage(statement, dialect=dialect)

    # Prep for generating column-level lineage.
    downstream_table: Optional[TableName] = None
    if len(modified) == 1:
        downstream_table = next(iter(modified))

    table_name_urn_mapping: Dict[TableName, str] = {}
    table_name_schema_mapping: Dict[TableName, SchemaInfo] = {}
    for table in tables:
        # table.database can be None for platforms that only have a two-level namespace hierarchy.
        assert table.schema

        urn, schema_info = schema_resolver.resolve_table(table)

        table_name_urn_mapping[table] = urn
        if schema_info:
            table_name_schema_mapping[table] = schema_info

    # Simplify the input statement for column-level lineage generation.
    # TODO [refactor] move this logic into the column-level lineage generation function.
    if isinstance(statement, sqlglot.exp.Merge):
        # TODO Need to map column renames in the expressions part of the statement.
        # Likely need to use the named_selects attr.
        statement = statement.args["using"]
        if isinstance(statement, sqlglot.exp.Table):
            # If we're querying a table directly, wrap it in a SELECT.
            statement = sqlglot.exp.Select().select("*").from_(statement)
    elif isinstance(statement, sqlglot.exp.Insert):
        # TODO Need to map column renames in the expressions part of the statement.
        statement = statement.expression
    elif isinstance(statement, sqlglot.exp.Create) and isinstance(
        statement.expression, sqlglot.exp.Select
    ):
        # TODO May need to map column renames.
        # TODO: Retain the original view name as the output table name.
        statement = statement.expression

    # Generate column-level lineage.
    column_lineage: Optional[List[ColumnLineageInfo]] = None
    try:
        column_lineage = _column_level_lineage(
            statement,
            dialect=dialect,
            input_tables=table_name_schema_mapping,
            output_table=downstream_table,
        )
    except UnsupportedStatementTypeError as e:
        print(
            f'  Cannot generate column-level lineage for statement type "{type(statement)}": {e}'
        )
        column_lineage = None
    except Exception as e:
        # Cannot generate column-level lineage for this statement type.
        print(f" Failed to generate column-level lineage: {e}")

    # TODO fallback to sqllineage / other tools if sqlglot fails.

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    return SqlParsingResult(
        in_tables=list(tables),
        out_tables=list(modified),
        column_lineage=column_lineage,
    )

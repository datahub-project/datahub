import logging
import pathlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer.qualify

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
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


@dataclass(frozen=True, order=True)
class ColumnRef:
    table: TableName
    column: str


@dataclass(frozen=True, order=True)
class DownstreamColumnRef:
    table: Optional[TableName]
    column: str


@dataclass(frozen=True, order=True)
class ColumnLineageInfo:
    downstream: DownstreamColumnRef
    upstreams: List[ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str] = None


@dataclass
class SqlParsingResult:
    in_tables: List[TableName]
    out_tables: List[TableName]

    column_lineage: Optional[List[ColumnLineageInfo]]

    # TODO add a statement type enum
    # TODO add a confidence score (per column?)


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
        if isinstance(expr.this, sqlglot.exp.Table)
    }

    tables = (
        {_raw_table_name(table) for table in statement.find_all(sqlglot.exp.Table)}
        # ignore references created in this query
        - modified
        # ignore CTEs created in this statement
        - {
            TableName(database=None, schema=None, table=cte.alias_or_name)
            for cte in statement.find_all(sqlglot.exp.CTE)
        }
    )
    # TODO: If a CTAS has "LIMIT 0", it's not really lineage, just copying the schema.

    return tables, modified


# TODO generate a lightweight schema type to use instead of Any
SchemaInfo = Dict[str, str]


class SchemaResolver:
    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        graph: Optional[DataHubGraph] = None,
        _cache_filename: Optional[pathlib.Path] = None,
    ):
        # TODO handle platforms when prefixed with urn:li:dataPlatform:
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env

        self.graph = graph

        # Init cache, potentially restoring from a previous run.
        shared_conn = None
        if _cache_filename:
            shared_conn = ConnectionWrapper(filename=_cache_filename)
        self._schema_cache: FileBackedDict[Optional[SchemaInfo]] = FileBackedDict(
            shared_connection=shared_conn,
            # TODO: tweak settings to maintain a fairly large in-memory cache
        )

    def get_urn_for_table(self, table: TableName, lower: bool = False) -> str:
        # TODO: Validate that this is the correct 2/3 layer hierarchy for the platform.

        table_name = ".".join(filter(None, [table.database, table.schema, table.table]))
        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance,
            env=self.env,
            name=table_name,
        )
        return urn

    def resolve_table(self, table: TableName) -> Tuple[str, Optional[SchemaInfo]]:
        urn = self.get_urn_for_table(table)

        schema_info = self._resolve_schema_info(urn)
        if schema_info:
            return urn, schema_info

        urn_lower = self.get_urn_for_table(table, lower=True)
        if urn_lower != urn:
            schema_info = self._resolve_schema_info(urn_lower)
            if schema_info:
                return urn_lower, schema_info

        return urn_lower, None

    def _resolve_schema_info(self, urn: str) -> Optional[SchemaInfo]:
        if urn in self._schema_cache:
            return self._schema_cache[urn]

        if self.graph:
            schema_info = self._fetch_schema_info(self.graph, urn)
            if schema_info:
                self._save_to_cache(urn, schema_info)
                return schema_info

        self._save_to_cache(urn, None)
        return None

    def add_schema_metadata(
        self, urn: str, schema_metadata: SchemaMetadataClass
    ) -> None:
        schema_info = self._convert_schema_aspect_to_info(schema_metadata)
        self._save_to_cache(urn, schema_info)

    def _save_to_cache(self, urn: str, schema_info: Optional[SchemaInfo]) -> None:
        self._schema_cache[urn] = schema_info

    def _fetch_schema_info(self, graph: DataHubGraph, urn: str) -> Optional[SchemaInfo]:
        aspect = graph.get_aspect(urn, SchemaMetadataClass)
        if not aspect:
            return None

        return self._convert_schema_aspect_to_info(aspect)

    @classmethod
    def _convert_schema_aspect_to_info(
        cls, schema_metadata: SchemaMetadataClass
    ) -> SchemaInfo:
        return {
            col.fieldPath: col.nativeDataType or "str" for col in schema_metadata.fields
        }

    # TODO add a method to load all from graphql


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
    for table, table_schema in input_tables.items():
        sqlglot_db_schema.add_table(
            table.as_sqlglot_table(),
            column_mapping=table_schema,
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
        logger.debug("output columns: %s", output_columns)
        for output_col in output_columns:
            # print(f"output column: {output_col}")

            lineage_node = sqlglot.lineage.lineage(
                output_col, statement, schema=sqlglot_db_schema
            )
            # pathlib.Path("sqlglot.html").write_text(
            #     str(lineage_node.to_html(dialect=dialect))
            # )

            # Generate SELECT lineage.
            # Using a set here to deduplicate upstreams.
            direct_col_upstreams = set()
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

                    direct_col_upstreams.add(ColumnRef(table_ref, col))
                else:
                    # This branch doesn't matter. For example, a count(*) column would go here, and
                    # we don't get any column-level lineage for that.
                    pass

            # TODO: Generate non-SELECT lineage.
            column_logic = lineage_node.source

            if not direct_col_upstreams:
                logger.debug(f'  "{output_col}" has no upstreams')
            if INCLUDE_COLUMN_LOGIC or direct_col_upstreams:
                column_lineage.append(
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(output_table, output_col),
                        upstreams=sorted(direct_col_upstreams),
                        logic=column_logic.sql(pretty=True, dialect=dialect)
                        if INCLUDE_COLUMN_LOGIC
                        else None,
                    )
                )

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

    original_statement = statement.copy()
    logger.debug(
        "Got sql statement: %s", original_statement.sql(pretty=True, dialect=dialect)
    )

    # Make sure the tables are resolved with the default db / schema.
    # sqlglot calls the db -> schema -> table hierarchy "catalog", "db", "table".
    statement = sqlglot.optimizer.qualify.qualify(
        statement,
        dialect=dialect,
        catalog=default_db,
        db=default_schema,
        # At this stage we only want to qualify the table names. The columns will be dealt with later.
        qualify_columns=False,
        validate_qualify_columns=False,
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
        urn, schema_info = schema_resolver.resolve_table(table)

        table_name_urn_mapping[table] = urn
        if schema_info:
            table_name_schema_mapping[table] = schema_info
        # TODO decrease confidence if schema info is missing

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
    except SqlOptimizerError as e:
        # Cannot generate column-level lineage for this statement type.
        print(f" Failed to generate column-level lineage: {e}")
        # TODO: Add a message to the result.

    # TODO fallback to sqllineage / other tools if sqlglot fails.

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    # TODO convert TableNames to urns

    return SqlParsingResult(
        in_tables=sorted(tables),
        out_tables=sorted(modified),
        column_lineage=column_lineage,
    )

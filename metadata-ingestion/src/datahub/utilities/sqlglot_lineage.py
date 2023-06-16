from collections import defaultdict
import contextlib
import enum
import functools
import logging
import pathlib
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from pydantic import BaseModel
import pydantic
import pydantic.dataclasses

import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer.qualify
from sqlglot.dialects.dialect import RESOLVES_IDENTIFIERS_AS_UPPERCASE

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)

Urn = str

# A lightweight table schema: column -> type mapping.
SchemaInfo = Dict[str, str]


class QueryType(enum.Enum):
    CREATE = "CREATE"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"

    UNKNOWN = "UNKNOWN"


def get_query_type_of_sql(expression: sqlglot.exp.Expression) -> QueryType:
    mapping = {
        sqlglot.exp.Create: QueryType.CREATE,
        sqlglot.exp.Select: QueryType.SELECT,
        sqlglot.exp.Insert: QueryType.INSERT,
        sqlglot.exp.Update: QueryType.UPDATE,
        sqlglot.exp.Delete: QueryType.DELETE,
        sqlglot.exp.Merge: QueryType.MERGE,
    }

    for cls, query_type in mapping.items():
        if isinstance(expression, cls):
            return query_type
    return QueryType.UNKNOWN


@functools.total_ordering
class FrozenModel(BaseModel, frozen=True):
    def __lt__(self, other: "FrozenModel") -> bool:
        for field in self.__fields__:
            self_v = getattr(self, field)
            other_v = getattr(other, field)
            if self_v != other_v:
                return self_v < other_v

        return False


class TableName(FrozenModel):
    database: Optional[str]
    db_schema: Optional[str]
    table: str

    def as_sqlglot_table(self) -> sqlglot.exp.Table:
        return sqlglot.exp.Table(
            catalog=self.database, db=self.db_schema, this=self.table
        )

    def qualified(
        self,
        dialect: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> str:
        database = self.database or default_db
        db_schema = self.db_schema or default_schema

        return TableName(
            database=database,
            db_schema=db_schema,
            table=self.table,
        )

    @classmethod
    def from_sqlglot_table(
        cls,
        table: sqlglot.exp.Table,
        dialect: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "TableName":
        return cls(
            database=table.catalog or default_db,
            db_schema=table.db or default_schema,
            table=table.this.name,
        )


class _ColumnRef(FrozenModel):
    table: TableName
    column: str


class ColumnRef(BaseModel):
    table: Urn
    column: str


class _DownstreamColumnRef(BaseModel):
    table: Optional[TableName]
    column: str


class DownstreamColumnRef(BaseModel):
    table: Optional[Urn]
    column: str


class _ColumnLineageInfo(BaseModel):
    downstream: _DownstreamColumnRef
    upstreams: List[_ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str]


class ColumnLineageInfo(BaseModel):
    downstream: DownstreamColumnRef
    upstreams: List[ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str] = pydantic.Field(exclude=True)


class SqlParsingDebugInfo(BaseModel, arbitrary_types_allowed=True):
    confidence: float

    tables_discovered: int
    table_schemas_resolved: int

    column_error: Optional[Exception]
    # TODO add debug info w/ error messages, info about how many tables were resolved, etc.


class SqlParsingResult(BaseModel):
    query_type: QueryType = QueryType.UNKNOWN

    in_tables: List[Urn]
    out_tables: List[Urn]

    column_lineage: Optional[List[ColumnLineageInfo]]

    # TODO formatted original sql logic

    debug_info: SqlParsingDebugInfo = pydantic.Field(default=None, exclude=True)


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
        )

    def get_urn_for_table(self, table: TableName, lower: bool = False) -> str:
        # TODO: Validate that this is the correct 2/3 layer hierarchy for the platform.

        table_name = ".".join(
            filter(None, [table.database, table.db_schema, table.table])
        )
        if lower:
            table_name = table_name.lower()

        if self.platform == "bigquery":
            # Normalize shard numbers and other BigQuery weirdness.
            # TODO check that this is the right way to do it
            with contextlib.suppress(IndexError):
                table_name = BigqueryTableIdentifier.from_string_name(
                    table_name
                ).get_table_name()

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

        # TODO: For bigquery partitioned tables, add the pseudo-column _PARTITIONTIME
        # or _PARTITIONDATE where appropriate.

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

    def add_raw_schema_info(self, urn: str, schema_info: SchemaInfo) -> None:
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
            DatasetUrn._get_simple_field_path_from_v2_field_path(col.fieldPath): (
                # The actual types are more of a "nice to have".
                col.nativeDataType
                or "str"
            )
            for col in schema_metadata.fields
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "."
            not in DatasetUrn._get_simple_field_path_from_v2_field_path(col.fieldPath)
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
) -> List[_ColumnLineageInfo]:
    use_case_insensitive_cols = dialect in {
        # Column identifiers are case-insensitive in BigQuery, so we need to
        # do a normalization step beforehand to make sure it's resolved correctly.
        "bigquery",
        # Our snowflake source lowercases column identifiers, so we are forced
        # to do fuzzy (case-insensitive) resolution instead of exact resolution.
        "snowflake",
    }

    sqlglot_db_schema = sqlglot.MappingSchema(
        dialect=dialect,
        # We do our own normalization, so don't let sqlglot do it.
        normalize=False,
    )
    table_schema_normalized_mapping: Dict[TableName, Dict[str, str]] = defaultdict(dict)
    for table, table_schema in input_tables.items():
        normalized_table_schema: SchemaInfo = {}
        for col, col_type in table_schema.items():
            if use_case_insensitive_cols:
                col_normalized = (
                    # This is required to match Sqlglot's behavior.
                    col.upper()
                    if dialect in RESOLVES_IDENTIFIERS_AS_UPPERCASE
                    else col.lower()
                )
            else:
                col_normalized = col

            table_schema_normalized_mapping[table][col_normalized] = col
            normalized_table_schema[col_normalized] = col_type

        sqlglot_db_schema.add_table(
            table.as_sqlglot_table(),
            column_mapping=normalized_table_schema,
        )
        # TODO undo this translation when we're done

    # TODO: Longer term solution: We'll also need to monkey-patch the column qualifier to use a
    # case-insensitive fallback, but not do it if there's a real match available.

    if use_case_insensitive_cols:

        def _sqlglot_force_column_normalizer(
            node: sqlglot.exp.Expression, dialect: "sqlglot.DialectType" = None
        ) -> sqlglot.exp.Expression:
            if isinstance(node, sqlglot.exp.Column):
                node.this.set("quoted", False)

            return node

        logger.debug(
            "Prior to case normalization sql %s",
            statement.sql(pretty=True, dialect=dialect),
        )
        statement = statement.transform(
            _sqlglot_force_column_normalizer, dialect, copy=False
        )

    # Optimize the statement + qualify column references.
    logger.debug(
        "Prior to qualification sql %s", statement.sql(pretty=True, dialect=dialect)
    )
    try:
        statement = sqlglot.optimizer.qualify.qualify(
            statement,
            dialect=dialect,
            schema=sqlglot_db_schema,
            validate_qualify_columns=False,
            identify=True,
        )
    except sqlglot.errors.OptimizeError as e:
        # TODO replace the `raise OptimizeError(f"Unknown column: {column_name}")`
        # line in sqlglot with a pass
        raise SqlOptimizerError(
            f"sqlglot failed to map columns to their source tables; likely missing/outdated table schema info: {e}"
        ) from e
    logger.debug("Qualified sql %s", statement.sql(pretty=True, dialect=dialect))

    if not isinstance(
        statement,
        (
            # Note that Select and Union inherit from Subqueryable.
            sqlglot.exp.Subqueryable,
            # For actual subqueries, the statement type might also be DerivedTable.
            sqlglot.exp.DerivedTable,
        ),
    ):
        raise UnsupportedStatementTypeError(
            f"Can only generate column-level lineage for select-like inner statements, not {type(statement)}"
        )

    column_lineage = []

    try:
        # List output columns.
        output_columns = [
            (select_col.alias_or_name, select_col) for select_col in statement.selects
        ]
        logger.debug("output columns: %s", [col[0] for col in output_columns])
        output_col: str
        for output_col, original_col_expression in output_columns:
            # print(f"output column: {output_col}")
            if output_col == "*":
                # If schema information is available, the * will be expanded to the actual columns.
                # Otherwise, we can't process it.
                continue

            if dialect == "bigquery" and output_col.lower() in {
                "_partitiontime",
                "_partitiondate",
            }:
                # These are not real columns, just a way to filter by partition.
                # TODO: We should add these columns to the schema info instead.
                # Once that's done, we should actually generate lineage for these
                # if they appear in the output.
                continue

            lineage_node = sqlglot.lineage.lineage(
                output_col,
                statement,
                dialect=dialect,
                schema=sqlglot_db_schema,
            )
            # pathlib.Path("sqlglot.html").write_text(
            #     str(lineage_node.to_html(dialect=dialect))
            # )

            # Generate SELECT lineage.
            # Using a set here to deduplicate upstreams.
            direct_col_upstreams: Set[_ColumnRef] = set()
            for node in lineage_node.walk():
                if node.downstream:
                    # We only want the leaf nodes.
                    pass

                elif isinstance(node.expression, sqlglot.exp.Table):
                    table_ref = TableName.from_sqlglot_table(
                        node.expression, dialect=dialect
                    )

                    # Parse the column name out of the node name.
                    # Sqlglot calls .sql(), so we have to do the inverse.
                    normalized_col = sqlglot.parse_one(node.name).this.name
                    if node.subfield:
                        normalized_col = f"{normalized_col}.{node.subfield}"
                    col = table_schema_normalized_mapping[table_ref].get(
                        normalized_col, normalized_col
                    )

                    direct_col_upstreams.add(_ColumnRef(table=table_ref, column=col))
                else:
                    # This branch doesn't matter. For example, a count(*) column would go here, and
                    # we don't get any column-level lineage for that.
                    pass

            # TODO: Generate non-SELECT lineage.
            column_logic = lineage_node.source

            if output_col.startswith("_col_"):
                # This is the format sqlglot uses for unnamed columns e.g. 'count(id)' -> 'count(id) AS _col_0'
                # This is a bit jank since we're relying on sqlglot internals, but it seems to be
                # the best way to do it.
                output_col = original_col_expression.this.sql(dialect=dialect)
            if not direct_col_upstreams:
                logger.debug(f'  "{output_col}" has no upstreams')
            column_lineage.append(
                _ColumnLineageInfo(
                    downstream=_DownstreamColumnRef(
                        table=output_table, column=output_col
                    ),
                    upstreams=sorted(direct_col_upstreams),
                    # logic=column_logic.sql(pretty=True, dialect=dialect),
                )
            )

    except sqlglot.errors.OptimizeError as e:
        raise SqlOptimizerError(f"sqlglot failed to compute some lineage: {e}") from e

    return column_lineage


def _extract_select_from_create(statement: sqlglot.exp.Create) -> sqlglot.exp.Select:
    # TODO: Validate that this properly includes WITH clauses in all dialects.
    inner = statement.expression

    if inner:
        return inner
    else:
        return statement


def _try_extract_select(
    statement: sqlglot.exp.Expression,
) -> Optional[sqlglot.exp.Select]:
    # Try to extract the core select logic from a more complex statement.

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
    elif isinstance(statement, sqlglot.exp.Create):
        # TODO May need to map column renames.
        # Assumption: the output table is already captured in the modified tables list.
        statement = _extract_select_from_create(statement)

    return statement


def _translate_internal_column_lineage(
    table_name_urn_mapping: Dict[TableName, str],
    raw_column_lineage: _ColumnLineageInfo,
) -> ColumnLineageInfo:
    return ColumnLineageInfo(
        downstream=DownstreamColumnRef(
            table=table_name_urn_mapping.get(raw_column_lineage.downstream.table),
            column=raw_column_lineage.downstream.column,
        ),
        upstreams=[
            ColumnRef(
                table=table_name_urn_mapping[upstream.table],
                column=upstream.column,
            )
            for upstream in raw_column_lineage.upstreams
        ],
    )


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
    # This only works for Unionable statements. For other types of statements,
    # we have to do it manually afterwards, but that's slightly lower accuracy
    # because of CTEs.
    statement = sqlglot.optimizer.qualify.qualify(
        statement,
        dialect=dialect,
        # sqlglot calls the db -> schema -> table hierarchy "catalog", "db", "table".
        catalog=default_db,
        db=default_schema,
        # At this stage we only want to qualify the table names. The columns will be dealt with later.
        qualify_columns=False,
        validate_qualify_columns=False,
        # Only insert quotes where necessary.
        identify=False,
    )

    # Generate table-level lineage.
    tables, modified = _table_level_lineage(statement, dialect=dialect)

    # Prep for generating column-level lineage.
    downstream_table: Optional[TableName] = None
    if len(modified) == 1:
        downstream_table = next(iter(modified))

    # Fetch schema info for the relevant tables.
    table_name_urn_mapping: Dict[TableName, str] = {}
    table_name_schema_mapping: Dict[TableName, SchemaInfo] = {}
    for table in [*tables, *modified]:
        # For select statements, qualification will be a no-op. For other statements, this
        # is where the qualification actually happens.
        qualified_table = table.qualified(
            dialect=dialect, default_db=default_db, default_schema=default_schema
        )

        urn, schema_info = schema_resolver.resolve_table(qualified_table)

        table_name_urn_mapping[table] = urn
        if schema_info:
            table_name_schema_mapping[table] = schema_info

    debug_info = SqlParsingDebugInfo(
        confidence=0.9 if len(tables) == len(table_name_schema_mapping)
        # If we're missing any schema info, our confidence will be in the 0.2-0.5 range depending
        # on how many tables we were able to resolve.
        else 0.2 + 0.3 * len(table_name_schema_mapping) / len(tables),
        tables_discovered=len(tables),
        table_schemas_resolved=len(table_name_schema_mapping),
    )
    logger.debug(
        f"Resolved {len(table_name_schema_mapping)} of {len(tables)} table schemas"
    )

    # Simplify the input statement for column-level lineage generation.
    select_statement = _try_extract_select(statement)

    # Generate column-level lineage.
    column_lineage: Optional[List[_ColumnLineageInfo]] = None
    try:
        column_lineage = _column_level_lineage(
            select_statement,
            dialect=dialect,
            input_tables=table_name_schema_mapping,
            output_table=downstream_table,
        )
    except UnsupportedStatementTypeError as e:
        # Wrap with details about the outer statement type too.
        e.args[0] += f" (outer statement type: {type(statement)})"
        debug_info.column_error = e
        logger.debug(debug_info.column_error)
    except SqlOptimizerError as e:
        logger.debug(f"Failed to generate column-level lineage: {e}", exc_info=True)
        debug_info.column_error = e

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    # TODO convert TableNames to urns

    # Convert TableName to urns.
    in_urns = [table_name_urn_mapping[table] for table in sorted(tables)]
    out_urns = [table_name_urn_mapping[table] for table in sorted(modified)]
    column_lineage_urns = None
    if column_lineage:
        column_lineage_urns = [
            _translate_internal_column_lineage(
                table_name_urn_mapping, internal_col_lineage
            )
            for internal_col_lineage in column_lineage
        ]

    return SqlParsingResult(
        query_type=get_query_type_of_sql(original_statement),
        in_tables=in_urns,
        out_tables=out_urns,
        column_lineage=column_lineage_urns,
        debug_info=debug_info,
    )

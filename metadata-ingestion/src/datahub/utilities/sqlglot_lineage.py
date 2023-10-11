import contextlib
import enum
import functools
import itertools
import logging
import pathlib
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple, Union

import pydantic.dataclasses
import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer.qualify
import sqlglot.optimizer.qualify_columns
from pydantic import BaseModel
from typing_extensions import TypedDict

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.metadata.schema_classes import OperationTypeClass, SchemaMetadataClass
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)

Urn = str

# A lightweight table schema: column -> type mapping.
SchemaInfo = Dict[str, str]

SQL_PARSE_RESULT_CACHE_SIZE = 1000


class GraphQLSchemaField(TypedDict):
    fieldPath: str
    nativeDataType: str


class GraphQLSchemaMetadata(TypedDict):
    fields: List[GraphQLSchemaField]


class QueryType(enum.Enum):
    CREATE = "CREATE"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"

    UNKNOWN = "UNKNOWN"

    def to_operation_type(self) -> Optional[str]:
        if self == QueryType.CREATE:
            return OperationTypeClass.CREATE
        elif self == QueryType.INSERT:
            return OperationTypeClass.INSERT
        elif self == QueryType.UPDATE:
            return OperationTypeClass.UPDATE
        elif self == QueryType.DELETE:
            return OperationTypeClass.DELETE
        elif self == QueryType.MERGE:
            return OperationTypeClass.UPDATE
        elif self == QueryType.SELECT:
            return None
        else:
            return OperationTypeClass.UNKNOWN


def get_query_type_of_sql(expression: sqlglot.exp.Expression) -> QueryType:
    # UPGRADE: Once we use Python 3.10, replace this with a match expression.
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
class _FrozenModel(BaseModel, frozen=True):
    def __lt__(self, other: "_FrozenModel") -> bool:
        for field in self.__fields__:
            self_v = getattr(self, field)
            other_v = getattr(other, field)
            if self_v != other_v:
                return self_v < other_v

        return False


class _TableName(_FrozenModel):
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
    ) -> "_TableName":
        database = self.database or default_db
        db_schema = self.db_schema or default_schema

        return _TableName(
            database=database,
            db_schema=db_schema,
            table=self.table,
        )

    @classmethod
    def from_sqlglot_table(
        cls,
        table: sqlglot.exp.Table,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "_TableName":
        return cls(
            database=table.catalog or default_db,
            db_schema=table.db or default_schema,
            table=table.this.name,
        )


class _ColumnRef(_FrozenModel):
    table: _TableName
    column: str


class ColumnRef(BaseModel):
    table: Urn
    column: str


class _DownstreamColumnRef(BaseModel):
    table: Optional[_TableName]
    column: str


class DownstreamColumnRef(BaseModel):
    table: Optional[Urn]
    column: str


class _ColumnLineageInfo(BaseModel):
    downstream: _DownstreamColumnRef
    upstreams: List[_ColumnRef]

    logic: Optional[str]


class ColumnLineageInfo(BaseModel):
    downstream: DownstreamColumnRef
    upstreams: List[ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str] = pydantic.Field(default=None, exclude=True)


class SqlParsingDebugInfo(BaseModel, arbitrary_types_allowed=True):
    confidence: float = 0.0

    tables_discovered: int = 0
    table_schemas_resolved: int = 0

    table_error: Optional[Exception] = None
    column_error: Optional[Exception] = None

    @property
    def error(self) -> Optional[Exception]:
        return self.table_error or self.column_error


class SqlParsingResult(BaseModel):
    query_type: QueryType = QueryType.UNKNOWN

    in_tables: List[Urn]
    out_tables: List[Urn]

    column_lineage: Optional[List[ColumnLineageInfo]]

    # TODO include formatted original sql logic
    # TODO include list of referenced columns

    debug_info: SqlParsingDebugInfo = pydantic.Field(
        default_factory=lambda: SqlParsingDebugInfo(),
        exclude=True,
    )


def _parse_statement(sql: str, dialect: str) -> sqlglot.Expression:
    statement = sqlglot.parse_one(
        sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE
    )
    return statement


def _table_level_lineage(
    statement: sqlglot.Expression,
    dialect: str,
) -> Tuple[Set[_TableName], Set[_TableName]]:
    # Generate table-level lineage.
    modified = {
        _TableName.from_sqlglot_table(expr.this)
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
    } | {
        # For CREATE DDL statements, the table name is nested inside
        # a Schema object.
        _TableName.from_sqlglot_table(expr.this.this)
        for expr in statement.find_all(sqlglot.exp.Create)
        if isinstance(expr.this, sqlglot.exp.Schema)
        and isinstance(expr.this.this, sqlglot.exp.Table)
    }

    tables = (
        {
            _TableName.from_sqlglot_table(table)
            for table in statement.find_all(sqlglot.exp.Table)
        }
        # ignore references created in this query
        - modified
        # ignore CTEs created in this statement
        - {
            _TableName(database=None, db_schema=None, table=cte.alias_or_name)
            for cte in statement.find_all(sqlglot.exp.CTE)
        }
    )
    # TODO: If a CTAS has "LIMIT 0", it's not really lineage, just copying the schema.

    return tables, modified


class SchemaResolver(Closeable):
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

    def get_urns(self) -> Set[str]:
        return set(self._schema_cache.keys())

    def get_urn_for_table(self, table: _TableName, lower: bool = False) -> str:
        # TODO: Validate that this is the correct 2/3 layer hierarchy for the platform.

        table_name = ".".join(
            filter(None, [table.database, table.db_schema, table.table])
        )
        if lower:
            table_name = table_name.lower()

        if self.platform == "bigquery":
            # Normalize shard numbers and other BigQuery weirdness.
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

    def resolve_table(self, table: _TableName) -> Tuple[str, Optional[SchemaInfo]]:
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

    def add_graphql_schema_metadata(
        self, urn: str, schema_metadata: GraphQLSchemaMetadata
    ) -> None:
        schema_info = self.convert_graphql_schema_metadata_to_info(schema_metadata)
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
            DatasetUrn.get_simple_field_path_from_v2_field_path(col.fieldPath): (
                # The actual types are more of a "nice to have".
                col.nativeDataType
                or "str"
            )
            for col in schema_metadata.fields
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "."
            not in DatasetUrn.get_simple_field_path_from_v2_field_path(col.fieldPath)
        }

    @classmethod
    def convert_graphql_schema_metadata_to_info(
        cls, schema: GraphQLSchemaMetadata
    ) -> SchemaInfo:
        return {
            DatasetUrn.get_simple_field_path_from_v2_field_path(field["fieldPath"]): (
                # The actual types are more of a "nice to have".
                field["nativeDataType"]
                or "str"
            )
            for field in schema["fields"]
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "."
            not in DatasetUrn.get_simple_field_path_from_v2_field_path(
                field["fieldPath"]
            )
        }

    def close(self) -> None:
        self._schema_cache.close()


# TODO: Once PEP 604 is supported (Python 3.10), we can unify these into a
# single type. See https://peps.python.org/pep-0604/#isinstance-and-issubclass.
_SupportedColumnLineageTypes = Union[
    # Note that Select and Union inherit from Subqueryable.
    sqlglot.exp.Subqueryable,
    # For actual subqueries, the statement type might also be DerivedTable.
    sqlglot.exp.DerivedTable,
]
_SupportedColumnLineageTypesTuple = (sqlglot.exp.Subqueryable, sqlglot.exp.DerivedTable)


class UnsupportedStatementTypeError(TypeError):
    pass


class SqlUnderstandingError(Exception):
    # Usually hit when we need schema info for a given statement but don't have it.
    pass


# TODO: Break this up into smaller functions.
def _column_level_lineage(  # noqa: C901
    statement: sqlglot.exp.Expression,
    dialect: str,
    input_tables: Dict[_TableName, SchemaInfo],
    output_table: Optional[_TableName],
    default_db: Optional[str],
    default_schema: Optional[str],
) -> List[_ColumnLineageInfo]:
    if not isinstance(
        statement,
        _SupportedColumnLineageTypesTuple,
    ):
        raise UnsupportedStatementTypeError(
            f"Can only generate column-level lineage for select-like inner statements, not {type(statement)}"
        )

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
    table_schema_normalized_mapping: Dict[_TableName, Dict[str, str]] = defaultdict(
        dict
    )
    for table, table_schema in input_tables.items():
        normalized_table_schema: SchemaInfo = {}
        for col, col_type in table_schema.items():
            if use_case_insensitive_cols:
                col_normalized = (
                    # This is required to match Sqlglot's behavior.
                    col.upper()
                    if dialect in {"snowflake"}
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

    if use_case_insensitive_cols:

        def _sqlglot_force_column_normalizer(
            node: sqlglot.exp.Expression, dialect: "sqlglot.DialectType" = None
        ) -> sqlglot.exp.Expression:
            if isinstance(node, sqlglot.exp.Column):
                node.this.set("quoted", False)

            return node

        # logger.debug(
        #     "Prior to case normalization sql %s",
        #     statement.sql(pretty=True, dialect=dialect),
        # )
        statement = statement.transform(
            _sqlglot_force_column_normalizer, dialect, copy=False
        )
        # logger.debug(
        #     "Sql after casing normalization %s",
        #     statement.sql(pretty=True, dialect=dialect),
        # )

    def _schema_aware_fuzzy_column_resolve(
        table: Optional[_TableName], sqlglot_column: str
    ) -> str:
        default_col_name = (
            sqlglot_column.lower() if use_case_insensitive_cols else sqlglot_column
        )
        if table:
            return table_schema_normalized_mapping[table].get(
                sqlglot_column, default_col_name
            )
        else:
            return default_col_name

    # Optimize the statement + qualify column references.
    logger.debug(
        "Prior to qualification sql %s", statement.sql(pretty=True, dialect=dialect)
    )
    try:
        # Second time running qualify, this time with:
        # - the select instead of the full outer statement
        # - schema info
        # - column qualification enabled

        # logger.debug("Schema: %s", sqlglot_db_schema.mapping)
        statement = sqlglot.optimizer.qualify.qualify(
            statement,
            dialect=dialect,
            schema=sqlglot_db_schema,
            validate_qualify_columns=False,
            identify=True,
            # sqlglot calls the db -> schema -> table hierarchy "catalog", "db", "table".
            catalog=default_db,
            db=default_schema,
        )
    except (sqlglot.errors.OptimizeError, ValueError) as e:
        raise SqlUnderstandingError(
            f"sqlglot failed to map columns to their source tables; likely missing/outdated table schema info: {e}"
        ) from e
    logger.debug("Qualified sql %s", statement.sql(pretty=True, dialect=dialect))

    column_lineage = []

    try:
        assert isinstance(statement, _SupportedColumnLineageTypesTuple)

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
                    table_ref = _TableName.from_sqlglot_table(node.expression)

                    # Parse the column name out of the node name.
                    # Sqlglot calls .sql(), so we have to do the inverse.
                    normalized_col = sqlglot.parse_one(node.name).this.name
                    if node.subfield:
                        normalized_col = f"{normalized_col}.{node.subfield}"

                    col = _schema_aware_fuzzy_column_resolve(table_ref, normalized_col)
                    direct_col_upstreams.add(_ColumnRef(table=table_ref, column=col))
                else:
                    # This branch doesn't matter. For example, a count(*) column would go here, and
                    # we don't get any column-level lineage for that.
                    pass

            # column_logic = lineage_node.source

            if output_col.startswith("_col_"):
                # This is the format sqlglot uses for unnamed columns e.g. 'count(id)' -> 'count(id) AS _col_0'
                # This is a bit jank since we're relying on sqlglot internals, but it seems to be
                # the best way to do it.
                output_col = original_col_expression.this.sql(dialect=dialect)

            output_col = _schema_aware_fuzzy_column_resolve(output_table, output_col)

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

        # TODO: Also extract referenced columns (e.g. non-SELECT lineage)
    except (sqlglot.errors.OptimizeError, ValueError) as e:
        raise SqlUnderstandingError(
            f"sqlglot failed to compute some lineage: {e}"
        ) from e

    return column_lineage


def _extract_select_from_create(
    statement: sqlglot.exp.Create,
) -> sqlglot.exp.Expression:
    # TODO: Validate that this properly includes WITH clauses in all dialects.
    inner = statement.expression

    if inner:
        return inner
    else:
        return statement


def _try_extract_select(
    statement: sqlglot.exp.Expression,
) -> sqlglot.exp.Expression:
    # Try to extract the core select logic from a more complex statement.
    # If it fails, just return the original statement.

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

    if isinstance(statement, sqlglot.exp.Subquery):
        statement = statement.unnest()

    return statement


def _translate_internal_column_lineage(
    table_name_urn_mapping: Dict[_TableName, str],
    raw_column_lineage: _ColumnLineageInfo,
) -> ColumnLineageInfo:
    downstream_urn = None
    if raw_column_lineage.downstream.table:
        downstream_urn = table_name_urn_mapping[raw_column_lineage.downstream.table]
    return ColumnLineageInfo(
        downstream=DownstreamColumnRef(
            table=downstream_urn,
            column=raw_column_lineage.downstream.column,
        ),
        upstreams=[
            ColumnRef(
                table=table_name_urn_mapping[upstream.table],
                column=upstream.column,
            )
            for upstream in raw_column_lineage.upstreams
        ],
        logic=raw_column_lineage.logic,
    )


def _get_dialect(platform: str) -> str:
    # TODO: convert datahub platform names to sqlglot dialect
    if platform == "presto-on-hive":
        return "hive"
    else:
        return platform


def _sqlglot_lineage_inner(
    sql: str,
    schema_resolver: SchemaResolver,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> SqlParsingResult:
    dialect = _get_dialect(schema_resolver.platform)
    if dialect == "snowflake":
        # in snowflake, table identifiers must be uppercased to match sqlglot's behavior.
        if default_db:
            default_db = default_db.upper()
        if default_schema:
            default_schema = default_schema.upper()

    logger.debug("Parsing lineage from sql statement: %s", sql)
    statement = _parse_statement(sql, dialect=dialect)

    original_statement = statement.copy()
    # logger.debug(
    #     "Formatted sql statement: %s",
    #     original_statement.sql(pretty=True, dialect=dialect),
    # )

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
    downstream_table: Optional[_TableName] = None
    if len(modified) == 1:
        downstream_table = next(iter(modified))

    # Fetch schema info for the relevant tables.
    table_name_urn_mapping: Dict[_TableName, str] = {}
    table_name_schema_mapping: Dict[_TableName, SchemaInfo] = {}
    for table in itertools.chain(tables, modified):
        # For select statements, qualification will be a no-op. For other statements, this
        # is where the qualification actually happens.
        qualified_table = table.qualified(
            dialect=dialect, default_db=default_db, default_schema=default_schema
        )

        urn, schema_info = schema_resolver.resolve_table(qualified_table)

        table_name_urn_mapping[qualified_table] = urn
        if schema_info:
            table_name_schema_mapping[qualified_table] = schema_info

        # Also include the original, non-qualified table name in the urn mapping.
        table_name_urn_mapping[table] = urn

    total_tables_discovered = len(tables) + len(modified)
    total_schemas_resolved = len(table_name_schema_mapping)
    debug_info = SqlParsingDebugInfo(
        confidence=0.9 if total_tables_discovered == total_schemas_resolved
        # If we're missing any schema info, our confidence will be in the 0.2-0.5 range depending
        # on how many tables we were able to resolve.
        else 0.2 + 0.3 * total_schemas_resolved / total_tables_discovered,
        tables_discovered=total_tables_discovered,
        table_schemas_resolved=total_schemas_resolved,
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
            default_db=default_db,
            default_schema=default_schema,
        )
    except UnsupportedStatementTypeError as e:
        # Inject details about the outer statement type too.
        e.args = (f"{e.args[0]} (outer statement type: {type(statement)})",)
        debug_info.column_error = e
        logger.debug(debug_info.column_error)
    except SqlUnderstandingError as e:
        logger.debug(f"Failed to generate column-level lineage: {e}", exc_info=True)
        debug_info.column_error = e

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    # Convert TableName to urns.
    in_urns = sorted(set(table_name_urn_mapping[table] for table in tables))
    out_urns = sorted(set(table_name_urn_mapping[table] for table in modified))
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


@functools.lru_cache(maxsize=SQL_PARSE_RESULT_CACHE_SIZE)
def sqlglot_lineage(
    sql: str,
    schema_resolver: SchemaResolver,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> SqlParsingResult:
    """Parse a SQL statement and generate lineage information.

    This is a schema-aware lineage generator, meaning that it will use the
    schema information for the tables involved to generate lineage information
    for the columns involved. The schema_resolver is responsible for providing
    the table schema information. In most cases, the DataHubGraph can be used
    to construct a schema_resolver that will fetch schemas from DataHub.

    The parser supports most types of DML statements (SELECT, INSERT, UPDATE,
    DELETE, MERGE) as well as CREATE TABLE AS SELECT (CTAS) statements. It
    does not support DDL statements (CREATE TABLE, ALTER TABLE, etc.).

    The table-level lineage tends to be fairly reliable, while column-level
    can be brittle with respect to missing schema information and complex
    SQL logic like UNNESTs.

    The SQL dialect is inferred from the schema_resolver's platform. The
    set of supported dialects is the same as sqlglot's. See their
    `documentation <https://sqlglot.com/sqlglot/dialects/dialect.html#Dialects>`_
    for the full list.

    The default_db and default_schema parameters are used to resolve unqualified
    table names. For example, the statement "SELECT * FROM my_table" would be
    converted to "SELECT * FROM default_db.default_schema.my_table".

    Args:
        sql: The SQL statement to parse. This should be a single statement, not
            a multi-statement string.
        schema_resolver: The schema resolver to use for resolving table schemas.
        default_db: The default database to use for unqualified table names.
        default_schema: The default schema to use for unqualified table names.

    Returns:
        A SqlParsingResult object containing the parsed lineage information.

        The in_tables and out_tables fields contain the input and output tables
        for the statement, respectively. These are represented as urns.
        The out_tables field will be empty for SELECT statements.

        The column_lineage field contains the column-level lineage information
        for the statement. This is a list of ColumnLineageInfo objects, each
        representing the lineage for a single output column. The downstream
        field contains the output column, and the upstreams field contains the
        (urn, column) pairs for the input columns.

        The debug_info field contains debug information about the parsing. If
        table_error or column_error are set, then the parsing failed and the
        other fields may be incomplete.
    """
    try:
        return _sqlglot_lineage_inner(
            sql=sql,
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )
    except Exception as e:
        return SqlParsingResult(
            in_tables=[],
            out_tables=[],
            column_lineage=None,
            debug_info=SqlParsingDebugInfo(
                table_error=e,
            ),
        )


def create_lineage_sql_parsed_result(
    query: str,
    database: Optional[str],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    schema: Optional[str] = None,
    graph: Optional[DataHubGraph] = None,
) -> SqlParsingResult:
    needs_close = False
    try:
        if graph:
            schema_resolver = graph._make_schema_resolver(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
            )
        else:
            needs_close = True
            schema_resolver = SchemaResolver(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                graph=None,
            )

        return sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=database,
            default_schema=schema,
        )
    except Exception as e:
        return SqlParsingResult(
            in_tables=[],
            out_tables=[],
            column_lineage=None,
            debug_info=SqlParsingDebugInfo(
                table_error=e,
            ),
        )
    finally:
        if needs_close:
            schema_resolver.close()

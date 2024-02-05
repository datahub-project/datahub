import contextlib
import enum
import functools
import itertools
import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic.dataclasses
import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer.annotate_types
import sqlglot.optimizer.optimizer
import sqlglot.optimizer.qualify
from pydantic import BaseModel
from typing_extensions import TypedDict

from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    OperationTypeClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

logger = logging.getLogger(__name__)

Urn = str

# A lightweight table schema: column -> type mapping.
SchemaInfo = Dict[str, str]

SQL_PARSE_RESULT_CACHE_SIZE = 1000


RULES_BEFORE_TYPE_ANNOTATION: tuple = tuple(
    filter(
        lambda func: func.__name__
        not in {
            # Skip pushdown_predicates because it sometimes throws exceptions, and we
            # don't actually need it for anything.
            "pushdown_predicates",
            # Skip normalize because it can sometimes be expensive.
            "normalize",
        },
        itertools.takewhile(
            lambda func: func != sqlglot.optimizer.annotate_types.annotate_types,
            sqlglot.optimizer.optimizer.RULES,
        ),
    )
)
# Quick check that the rules were loaded correctly.
assert 0 < len(RULES_BEFORE_TYPE_ANNOTATION) < len(sqlglot.optimizer.optimizer.RULES)


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
        sqlglot.exp.Subqueryable: QueryType.SELECT,  # unions, etc. are also selects
    }

    for cls, query_type in mapping.items():
        if isinstance(expression, cls):
            return query_type
    return QueryType.UNKNOWN


class _ParserBaseModel(
    BaseModel,
    arbitrary_types_allowed=True,
    json_encoders={
        SchemaFieldDataTypeClass: lambda v: v.to_obj(),
    },
):
    def json(self, *args: Any, **kwargs: Any) -> str:
        if PYDANTIC_VERSION_2:
            return super().model_dump_json(*args, **kwargs)  # type: ignore
        else:
            return super().json(*args, **kwargs)


@functools.total_ordering
class _FrozenModel(_ParserBaseModel, frozen=True):
    def __lt__(self, other: "_FrozenModel") -> bool:
        # TODO: The __fields__ attribute is deprecated in Pydantic v2.
        for field in self.__fields__:
            self_v = getattr(self, field)
            other_v = getattr(other, field)
            if self_v != other_v:
                return self_v < other_v

        return False


class _TableName(_FrozenModel):
    database: Optional[str] = None
    db_schema: Optional[str] = None
    table: str

    def as_sqlglot_table(self) -> sqlglot.exp.Table:
        return sqlglot.exp.Table(
            catalog=sqlglot.exp.Identifier(this=self.database)
            if self.database
            else None,
            db=sqlglot.exp.Identifier(this=self.db_schema) if self.db_schema else None,
            this=sqlglot.exp.Identifier(this=self.table),
        )

    def qualified(
        self,
        dialect: sqlglot.Dialect,
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


class ColumnRef(_FrozenModel):
    table: Urn
    column: str


class _DownstreamColumnRef(_ParserBaseModel):
    table: Optional[_TableName] = None
    column: str
    column_type: Optional[sqlglot.exp.DataType] = None


class DownstreamColumnRef(_ParserBaseModel):
    table: Optional[Urn] = None
    column: str
    column_type: Optional[SchemaFieldDataTypeClass] = None
    native_column_type: Optional[str] = None

    @pydantic.validator("column_type", pre=True)
    def _load_column_type(
        cls, v: Optional[Union[dict, SchemaFieldDataTypeClass]]
    ) -> Optional[SchemaFieldDataTypeClass]:
        if v is None:
            return None
        if isinstance(v, SchemaFieldDataTypeClass):
            return v
        return SchemaFieldDataTypeClass.from_obj(v)


class _ColumnLineageInfo(_ParserBaseModel):
    downstream: _DownstreamColumnRef
    upstreams: List[_ColumnRef]

    logic: Optional[str] = None


class ColumnLineageInfo(_ParserBaseModel):
    downstream: DownstreamColumnRef
    upstreams: List[ColumnRef]

    # Logic for this column, as a SQL expression.
    logic: Optional[str] = pydantic.Field(default=None, exclude=True)


class SqlParsingDebugInfo(_ParserBaseModel):
    confidence: float = 0.0

    tables_discovered: int = 0
    table_schemas_resolved: int = 0

    table_error: Optional[Exception] = None
    column_error: Optional[Exception] = None

    @property
    def error(self) -> Optional[Exception]:
        return self.table_error or self.column_error


class SqlParsingResult(_ParserBaseModel):
    query_type: QueryType = QueryType.UNKNOWN

    in_tables: List[Urn]
    out_tables: List[Urn]

    column_lineage: Optional[List[ColumnLineageInfo]] = None

    # TODO include formatted original sql logic
    # TODO include list of referenced columns

    debug_info: SqlParsingDebugInfo = pydantic.Field(
        default_factory=lambda: SqlParsingDebugInfo(),
        exclude=True,
    )

    @classmethod
    def make_from_error(cls, error: Exception) -> "SqlParsingResult":
        return cls(
            in_tables=[],
            out_tables=[],
            debug_info=SqlParsingDebugInfo(
                table_error=error,
            ),
        )


def _parse_statement(
    sql: sqlglot.exp.ExpOrStr, dialect: sqlglot.Dialect
) -> sqlglot.Expression:
    statement: sqlglot.Expression = sqlglot.maybe_parse(
        sql, dialect=dialect, error_level=sqlglot.ErrorLevel.RAISE
    )
    return statement


def _table_level_lineage(
    statement: sqlglot.Expression, dialect: sqlglot.Dialect
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

    # Update statements implicitly read from the table being updated, so add those back in.
    if isinstance(statement, sqlglot.exp.Update):
        tables = tables | modified

    return tables, modified


TABLE_CASE_SENSITIVE_PLATFORMS = {"bigquery"}


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

        platform_instance = self.platform_instance

        if lower:
            table_name = table_name.lower()
            platform_instance = platform_instance.lower() if platform_instance else None

        if self.platform == "bigquery":
            # Normalize shard numbers and other BigQuery weirdness.
            with contextlib.suppress(IndexError):
                table_name = BigqueryTableIdentifier.from_string_name(
                    table_name
                ).get_table_name()

        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=platform_instance,
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

        if self.platform in TABLE_CASE_SENSITIVE_PLATFORMS:
            return urn, None
        else:
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
            get_simple_field_path_from_v2_field_path(col.fieldPath): (
                # The actual types are more of a "nice to have".
                col.nativeDataType
                or "str"
            )
            for col in schema_metadata.fields
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "." not in get_simple_field_path_from_v2_field_path(col.fieldPath)
        }

    @classmethod
    def convert_graphql_schema_metadata_to_info(
        cls, schema: GraphQLSchemaMetadata
    ) -> SchemaInfo:
        return {
            get_simple_field_path_from_v2_field_path(field["fieldPath"]): (
                # The actual types are more of a "nice to have".
                field["nativeDataType"]
                or "str"
            )
            for field in schema["fields"]
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "." not in get_simple_field_path_from_v2_field_path(field["fieldPath"])
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

DIALECTS_WITH_CASE_INSENSITIVE_COLS = {
    # Column identifiers are case-insensitive in BigQuery, so we need to
    # do a normalization step beforehand to make sure it's resolved correctly.
    "bigquery",
    # Our snowflake source lowercases column identifiers, so we are forced
    # to do fuzzy (case-insensitive) resolution instead of exact resolution.
    "snowflake",
    # Teradata column names are case-insensitive.
    # A name, even when enclosed in double quotation marks, is not case sensitive. For example, CUSTOMER and Customer are the same.
    # See more below:
    # https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/n0ejgx4895bofnn14rlguktfx5r3.htm
    "teradata",
}
DIALECTS_WITH_DEFAULT_UPPERCASE_COLS = {
    # In some dialects, column identifiers are effectively case insensitive
    # because they are automatically converted to uppercase. Most other systems
    # automatically lowercase unquoted identifiers.
    "snowflake",
}


class UnsupportedStatementTypeError(TypeError):
    pass


class SqlUnderstandingError(Exception):
    # Usually hit when we need schema info for a given statement but don't have it.
    pass


# TODO: Break this up into smaller functions.
def _column_level_lineage(  # noqa: C901
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
    table_schemas: Dict[_TableName, SchemaInfo],
    output_table: Optional[_TableName],
    default_db: Optional[str],
    default_schema: Optional[str],
) -> List[_ColumnLineageInfo]:
    is_create_ddl = _is_create_table_ddl(statement)
    if (
        not isinstance(
            statement,
            _SupportedColumnLineageTypesTuple,
        )
        and not is_create_ddl
    ):
        raise UnsupportedStatementTypeError(
            f"Can only generate column-level lineage for select-like inner statements, not {type(statement)}"
        )

    column_lineage: List[_ColumnLineageInfo] = []

    use_case_insensitive_cols = _is_dialect_instance(
        dialect, DIALECTS_WITH_CASE_INSENSITIVE_COLS
    )

    sqlglot_db_schema = sqlglot.MappingSchema(
        dialect=dialect,
        # We do our own normalization, so don't let sqlglot do it.
        normalize=False,
    )
    table_schema_normalized_mapping: Dict[_TableName, Dict[str, str]] = defaultdict(
        dict
    )
    for table, table_schema in table_schemas.items():
        normalized_table_schema: SchemaInfo = {}
        for col, col_type in table_schema.items():
            if use_case_insensitive_cols:
                col_normalized = (
                    # This is required to match Sqlglot's behavior.
                    col.upper()
                    if _is_dialect_instance(
                        dialect, DIALECTS_WITH_DEFAULT_UPPERCASE_COLS
                    )
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
            node: sqlglot.exp.Expression,
        ) -> sqlglot.exp.Expression:
            if isinstance(node, sqlglot.exp.Column):
                node.this.set("quoted", False)

            return node

        # logger.debug(
        #     "Prior to case normalization sql %s",
        #     statement.sql(pretty=True, dialect=dialect),
        # )
        statement = statement.transform(_sqlglot_force_column_normalizer, copy=False)
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
        "Prior to column qualification sql %s",
        statement.sql(pretty=True, dialect=dialect),
    )
    try:
        # Second time running qualify, this time with:
        # - the select instead of the full outer statement
        # - schema info
        # - column qualification enabled
        # - running the full pre-type annotation optimizer

        # logger.debug("Schema: %s", sqlglot_db_schema.mapping)
        statement = sqlglot.optimizer.optimizer.optimize(
            statement,
            dialect=dialect,
            schema=sqlglot_db_schema,
            qualify_columns=True,
            validate_qualify_columns=False,
            identify=True,
            # sqlglot calls the db -> schema -> table hierarchy "catalog", "db", "table".
            catalog=default_db,
            db=default_schema,
            rules=RULES_BEFORE_TYPE_ANNOTATION,
        )
    except (sqlglot.errors.OptimizeError, ValueError) as e:
        raise SqlUnderstandingError(
            f"sqlglot failed to map columns to their source tables; likely missing/outdated table schema info: {e}"
        ) from e
    logger.debug("Qualified sql %s", statement.sql(pretty=True, dialect=dialect))

    # Handle the create DDL case.
    if is_create_ddl:
        assert (
            output_table is not None
        ), "output_table must be set for create DDL statements"

        create_schema: sqlglot.exp.Schema = statement.this
        sqlglot_columns = create_schema.expressions

        for column_def in sqlglot_columns:
            if not isinstance(column_def, sqlglot.exp.ColumnDef):
                # Ignore things like constraints.
                continue

            output_col = _schema_aware_fuzzy_column_resolve(
                output_table, column_def.name
            )
            output_col_type = column_def.args.get("kind")

            column_lineage.append(
                _ColumnLineageInfo(
                    downstream=_DownstreamColumnRef(
                        table=output_table,
                        column=output_col,
                        column_type=output_col_type,
                    ),
                    upstreams=[],
                )
            )

        return column_lineage

    # Try to figure out the types of the output columns.
    try:
        statement = sqlglot.optimizer.annotate_types.annotate_types(
            statement, schema=sqlglot_db_schema
        )
    except (sqlglot.errors.OptimizeError, sqlglot.errors.ParseError) as e:
        # This is not a fatal error, so we can continue.
        logger.debug("sqlglot failed to annotate or parse types: %s", e)

    try:
        assert isinstance(statement, _SupportedColumnLineageTypesTuple)

        # List output columns.
        output_columns = [
            (select_col.alias_or_name, select_col) for select_col in statement.selects
        ]
        logger.debug("output columns: %s", [col[0] for col in output_columns])
        for output_col, original_col_expression in output_columns:
            if output_col == "*":
                # If schema information is available, the * will be expanded to the actual columns.
                # Otherwise, we can't process it.
                continue

            if _is_dialect_instance(dialect, "bigquery") and output_col.lower() in {
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
            direct_raw_col_upstreams: Set[_ColumnRef] = set()
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

                    direct_raw_col_upstreams.add(
                        _ColumnRef(table=table_ref, column=normalized_col)
                    )
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

            # Guess the output column type.
            output_col_type = None
            if original_col_expression.type:
                output_col_type = original_col_expression.type

            # Fuzzy resolve upstream columns.
            direct_resolved_col_upstreams = {
                _ColumnRef(
                    table=edge.table,
                    column=_schema_aware_fuzzy_column_resolve(edge.table, edge.column),
                )
                for edge in direct_raw_col_upstreams
            }

            if not direct_resolved_col_upstreams:
                logger.debug(f'  "{output_col}" has no upstreams')
            column_lineage.append(
                _ColumnLineageInfo(
                    downstream=_DownstreamColumnRef(
                        table=output_table,
                        column=output_col,
                        column_type=output_col_type,
                    ),
                    upstreams=sorted(direct_resolved_col_upstreams),
                    # logic=column_logic.sql(pretty=True, dialect=dialect),
                )
            )

        # TODO: Also extract referenced columns (aka auxillary / non-SELECT lineage)
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


_UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT: Set[str] = set(
    sqlglot.exp.Update.arg_types.keys()
) - set(sqlglot.exp.Select.arg_types.keys())
_UPDATE_FROM_TABLE_ARGS_TO_MOVE = {"joins", "laterals", "pivot"}


def _extract_select_from_update(
    statement: sqlglot.exp.Update,
) -> sqlglot.exp.Select:
    statement = statement.copy()

    # The "SET" expressions need to be converted.
    # For the update command, it'll be a list of EQ expressions, but the select
    # should contain aliased columns.
    new_expressions = []
    for expr in statement.expressions:
        if isinstance(expr, sqlglot.exp.EQ) and isinstance(
            expr.left, sqlglot.exp.Column
        ):
            new_expressions.append(
                sqlglot.exp.Alias(
                    this=expr.right,
                    alias=expr.left.this,
                )
            )
        else:
            # If we don't know how to convert it, just leave it as-is. If this causes issues,
            # they'll get caught later.
            new_expressions.append(expr)

    # Special translation for the `from` clause.
    extra_args = {}
    original_from = statement.args.get("from")
    if original_from and isinstance(original_from.this, sqlglot.exp.Table):
        # Move joins, laterals, and pivots from the Update->From->Table->field
        # to the top-level Select->field.

        for k in _UPDATE_FROM_TABLE_ARGS_TO_MOVE:
            if k in original_from.this.args:
                # Mutate the from table clause in-place.
                extra_args[k] = original_from.this.args.pop(k)

    select_statement = sqlglot.exp.Select(
        **{
            **{
                k: v
                for k, v in statement.args.items()
                if k not in _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT
            },
            **extra_args,
            "expressions": new_expressions,
        }
    )

    # Update statements always implicitly have the updated table in context.
    # TODO: Retain table name alias, if one was present.
    if select_statement.args.get("from"):
        select_statement = select_statement.join(
            statement.this, append=True, join_kind="cross"
        )
    else:
        select_statement = select_statement.from_(statement.this)

    return select_statement


def _is_create_table_ddl(statement: sqlglot.exp.Expression) -> bool:
    return isinstance(statement, sqlglot.exp.Create) and isinstance(
        statement.this, sqlglot.exp.Schema
    )


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
    elif isinstance(statement, sqlglot.exp.Update):
        # Assumption: the output table is already captured in the modified tables list.
        statement = _extract_select_from_update(statement)
    elif isinstance(statement, sqlglot.exp.Create):
        # TODO May need to map column renames.
        # Assumption: the output table is already captured in the modified tables list.
        statement = _extract_select_from_create(statement)

    if isinstance(statement, sqlglot.exp.Subquery):
        statement = statement.unnest()

    return statement


def _translate_sqlglot_type(
    sqlglot_type: sqlglot.exp.DataType.Type,
) -> Optional[SchemaFieldDataTypeClass]:
    TypeClass: Any
    if sqlglot_type in sqlglot.exp.DataType.TEXT_TYPES:
        TypeClass = StringTypeClass
    elif sqlglot_type in sqlglot.exp.DataType.NUMERIC_TYPES or sqlglot_type in {
        sqlglot.exp.DataType.Type.DECIMAL,
    }:
        TypeClass = NumberTypeClass
    elif sqlglot_type in {
        sqlglot.exp.DataType.Type.BOOLEAN,
        sqlglot.exp.DataType.Type.BIT,
    }:
        TypeClass = BooleanTypeClass
    elif sqlglot_type in {
        sqlglot.exp.DataType.Type.DATE,
    }:
        TypeClass = DateTypeClass
    elif sqlglot_type in sqlglot.exp.DataType.TEMPORAL_TYPES:
        TypeClass = TimeTypeClass
    elif sqlglot_type in {
        sqlglot.exp.DataType.Type.ARRAY,
    }:
        TypeClass = ArrayTypeClass
    elif sqlglot_type in {
        sqlglot.exp.DataType.Type.UNKNOWN,
        sqlglot.exp.DataType.Type.NULL,
    }:
        return None
    else:
        logger.debug("Unknown sqlglot type: %s", sqlglot_type)
        return None

    return SchemaFieldDataTypeClass(type=TypeClass())


def _translate_internal_column_lineage(
    table_name_urn_mapping: Dict[_TableName, str],
    raw_column_lineage: _ColumnLineageInfo,
    dialect: sqlglot.Dialect,
) -> ColumnLineageInfo:
    downstream_urn = None
    if raw_column_lineage.downstream.table:
        downstream_urn = table_name_urn_mapping[raw_column_lineage.downstream.table]
    return ColumnLineageInfo(
        downstream=DownstreamColumnRef(
            table=downstream_urn,
            column=raw_column_lineage.downstream.column,
            column_type=_translate_sqlglot_type(
                raw_column_lineage.downstream.column_type.this
            )
            if raw_column_lineage.downstream.column_type
            else None,
            native_column_type=raw_column_lineage.downstream.column_type.sql(
                dialect=dialect
            )
            if raw_column_lineage.downstream.column_type
            and raw_column_lineage.downstream.column_type.this
            != sqlglot.exp.DataType.Type.UNKNOWN
            else None,
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


def _get_dialect_str(platform: str) -> str:
    # TODO: convert datahub platform names to sqlglot dialect
    if platform == "presto-on-hive":
        return "hive"
    elif platform == "mssql":
        return "tsql"
    elif platform == "athena":
        return "trino"
    elif platform == "mysql":
        # In sqlglot v20+, MySQL is now case-sensitive by default, which is the
        # default behavior on Linux. However, MySQL's default case sensitivity
        # actually depends on the underlying OS.
        # For us, it's simpler to just assume that it's case-insensitive, and
        # let the fuzzy resolution logic handle it.
        return "mysql, normalization_strategy = lowercase"
    else:
        return platform


def _get_dialect(platform: str) -> sqlglot.Dialect:
    return sqlglot.Dialect.get_or_raise(_get_dialect_str(platform))


def _is_dialect_instance(
    dialect: sqlglot.Dialect, platforms: Union[str, Iterable[str]]
) -> bool:
    if isinstance(platforms, str):
        platforms = [platforms]
    else:
        platforms = list(platforms)

    dialects = [sqlglot.Dialect.get_or_raise(platform) for platform in platforms]

    if any(isinstance(dialect, dialect_class.__class__) for dialect_class in dialects):
        return True
    return False


def _sqlglot_lineage_inner(
    sql: sqlglot.exp.ExpOrStr,
    schema_resolver: SchemaResolver,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> SqlParsingResult:
    dialect = _get_dialect(schema_resolver.platform)
    if _is_dialect_instance(dialect, "snowflake"):
        # in snowflake, table identifiers must be uppercased to match sqlglot's behavior.
        if default_db:
            default_db = default_db.upper()
        if default_schema:
            default_schema = default_schema.upper()
    if _is_dialect_instance(dialect, "redshift") and not default_schema:
        # On Redshift, there's no "USE SCHEMA <schema>" command. The default schema
        # is public, and "current schema" is the one at the front of the search path.
        # See https://docs.aws.amazon.com/redshift/latest/dg/r_search_path.html
        # and https://stackoverflow.com/questions/9067335/how-does-the-search-path-influence-identifier-resolution-and-the-current-schema?noredirect=1&lq=1
        # default_schema = "public"
        # TODO: Re-enable this.
        pass

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
    for table in tables | modified:
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

    total_tables_discovered = len(tables | modified)
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
        f"Resolved {total_schemas_resolved} of {total_tables_discovered} table schemas"
    )

    # Simplify the input statement for column-level lineage generation.
    try:
        select_statement = _try_extract_select(statement)
    except Exception as e:
        logger.debug(f"Failed to extract select from statement: {e}", exc_info=True)
        debug_info.column_error = e
        select_statement = None

    # Generate column-level lineage.
    column_lineage: Optional[List[_ColumnLineageInfo]] = None
    try:
        if select_statement is not None:
            column_lineage = _column_level_lineage(
                select_statement,
                dialect=dialect,
                table_schemas=table_name_schema_mapping,
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
                table_name_urn_mapping, internal_col_lineage, dialect=dialect
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
        return SqlParsingResult.make_from_error(e)


def detach_ctes(
    sql: sqlglot.exp.ExpOrStr, platform: str, cte_mapping: Dict[str, str]
) -> sqlglot.exp.Expression:
    """Replace CTE references with table references.

    For example, with cte_mapping = {"__cte_0": "_my_cte_table"}, the following SQL

    WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN __cte_0 ON table2.id = __cte_0.id

    is transformed into

    WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN _my_cte_table ON table2.id = _my_cte_table.id

    Note that the original __cte_0 definition remains in the query, but is simply not referenced.
    The query optimizer should be able to remove it.

    This method makes a major assumption: that no other table/column has the same name as a
    key in the cte_mapping.
    """

    dialect = _get_dialect(platform)
    statement = _parse_statement(sql, dialect=dialect)

    def replace_cte_refs(node: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        if (
            isinstance(node, sqlglot.exp.Identifier)
            and node.parent
            and not isinstance(node.parent.parent, sqlglot.exp.CTE)
            and node.name in cte_mapping
        ):
            full_new_name = cte_mapping[node.name]
            table_expr = sqlglot.maybe_parse(
                full_new_name, dialect=dialect, into=sqlglot.exp.Table
            )

            parent = node.parent

            # We expect node.parent to be a Table or Column, both of which support catalog/db/name.
            # However, we check the parent's arg_types to be safe.
            if "catalog" in parent.arg_types and table_expr.catalog:
                parent.set("catalog", table_expr.catalog)
            if "db" in parent.arg_types and table_expr.db:
                parent.set("db", table_expr.db)

            new_node = sqlglot.exp.Identifier(this=table_expr.name)

            return new_node
        else:
            return node

    return statement.transform(replace_cte_refs, copy=False)


def create_lineage_sql_parsed_result(
    query: str,
    default_db: Optional[str],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    default_schema: Optional[str] = None,
    graph: Optional[DataHubGraph] = None,
) -> SqlParsingResult:
    if graph:
        needs_close = False
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

    try:
        return sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )
    except Exception as e:
        return SqlParsingResult.make_from_error(e)
    finally:
        if needs_close:
            schema_resolver.close()


def view_definition_lineage_helper(
    result: SqlParsingResult, view_urn: str
) -> SqlParsingResult:
    if result.query_type is QueryType.SELECT:
        # Some platforms (e.g. postgres) store only <select statement> from view definition
        # `create view V as <select statement>` . For such view definitions, `result.out_tables` and
        # `result.column_lineage[].downstream` are empty in `sqlglot_lineage` response, whereas upstream
        # details and downstream column details are extracted correctly.
        # Here, we inject view V's urn in `result.out_tables` and `result.column_lineage[].downstream`
        # to get complete lineage result.
        result.out_tables = [view_urn]
        if result.column_lineage:
            for col_result in result.column_lineage:
                col_result.downstream.table = view_urn
    return result

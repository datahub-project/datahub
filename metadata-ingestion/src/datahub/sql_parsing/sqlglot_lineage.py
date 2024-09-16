import dataclasses
import functools
import itertools
import logging
import traceback
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pydantic.dataclasses
import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer
import sqlglot.optimizer.annotate_types
import sqlglot.optimizer.optimizer
import sqlglot.optimizer.qualify

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.sql_parsing._models import _FrozenModel, _ParserBaseModel, _TableName
from datahub.sql_parsing.query_types import get_query_type_of_sql, is_create_table_ddl
from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    SchemaResolverInterface,
)
from datahub.sql_parsing.sql_parsing_common import (
    DIALECTS_WITH_CASE_INSENSITIVE_COLS,
    DIALECTS_WITH_DEFAULT_UPPERCASE_COLS,
    QueryType,
    QueryTypeProps,
)
from datahub.sql_parsing.sqlglot_utils import (
    get_dialect,
    get_query_fingerprint_debug,
    is_dialect_instance,
    parse_statement,
)
from datahub.utilities.cooperative_timeout import (
    CooperativeTimeoutError,
    cooperative_timeout,
)

logger = logging.getLogger(__name__)

Urn = str

SQL_PARSE_RESULT_CACHE_SIZE = 1000
SQL_LINEAGE_TIMEOUT_ENABLED = get_boolean_env_variable(
    "SQL_LINEAGE_TIMEOUT_ENABLED", True
)
SQL_LINEAGE_TIMEOUT_SECONDS = 10


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

    tables_discovered: int = pydantic.Field(0, exclude=True)
    table_schemas_resolved: int = pydantic.Field(0, exclude=True)

    generalized_statement: Optional[str] = None

    table_error: Optional[Exception] = pydantic.Field(default=None, exclude=True)
    column_error: Optional[Exception] = pydantic.Field(default=None, exclude=True)

    @property
    def error(self) -> Optional[Exception]:
        return self.table_error or self.column_error

    @pydantic.validator("table_error", "column_error")
    def remove_variables_from_error(cls, v: Optional[Exception]) -> Optional[Exception]:
        if v and v.__traceback__:
            # Remove local variables from the traceback to avoid memory leaks.
            # See https://docs.python.org/3/library/traceback.html#traceback.clear_frames
            # and https://docs.python.org/3/reference/datamodel.html#frame.clear
            traceback.clear_frames(v.__traceback__)

        return v


class SqlParsingResult(_ParserBaseModel):
    query_type: QueryType = QueryType.UNKNOWN
    query_type_props: QueryTypeProps = {}
    query_fingerprint: Optional[str] = None

    in_tables: List[Urn]
    out_tables: List[Urn]

    column_lineage: Optional[List[ColumnLineageInfo]] = None

    # TODO include formatted original sql logic
    # TODO include list of referenced columns

    debug_info: SqlParsingDebugInfo = pydantic.Field(
        default_factory=lambda: SqlParsingDebugInfo()
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


def _table_level_lineage(
    statement: sqlglot.Expression, dialect: sqlglot.Dialect
) -> Tuple[Set[_TableName], Set[_TableName]]:
    # Generate table-level lineage.
    modified = (
        {
            _TableName.from_sqlglot_table(expr.this)
            for expr in statement.find_all(
                sqlglot.exp.Create,
                sqlglot.exp.Insert,
                sqlglot.exp.Update,
                sqlglot.exp.Delete,
                sqlglot.exp.Merge,
                sqlglot.exp.Alter,
            )
            # In some cases like "MERGE ... then INSERT (col1, col2) VALUES (col1, col2)",
            # the `this` on the INSERT part isn't a table.
            if isinstance(expr.this, sqlglot.exp.Table)
        }
        | {
            # For statements that include a column list, like
            # CREATE DDL statements and `INSERT INTO table (col1, col2) SELECT ...`
            # the table name is nested inside a Schema object.
            _TableName.from_sqlglot_table(expr.this.this)
            for expr in statement.find_all(
                sqlglot.exp.Create,
                sqlglot.exp.Insert,
            )
            if isinstance(expr.this, sqlglot.exp.Schema)
            and isinstance(expr.this.this, sqlglot.exp.Table)
        }
        | {
            # For drop statements, we only want it if a table/view is being dropped.
            # Other "kinds" will not have table.name populated.
            _TableName.from_sqlglot_table(expr.this)
            for expr in ([statement] if isinstance(statement, sqlglot.exp.Drop) else [])
            if isinstance(expr.this, sqlglot.exp.Table)
            and expr.this.this
            and expr.this.name
        }
    )

    tables = (
        {
            _TableName.from_sqlglot_table(table)
            for table in statement.find_all(sqlglot.exp.Table)
            if not isinstance(table.parent, sqlglot.exp.Drop)
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


# TODO: Once PEP 604 is supported (Python 3.10), we can unify these into a
# single type. See https://peps.python.org/pep-0604/#isinstance-and-issubclass.
_SupportedColumnLineageTypes = Union[
    # Note that Select and Union inherit from Query.
    sqlglot.exp.Query,
    # For actual subqueries, the statement type might also be DerivedTable.
    sqlglot.exp.DerivedTable,
]
_SupportedColumnLineageTypesTuple = (sqlglot.exp.Query, sqlglot.exp.DerivedTable)


class UnsupportedStatementTypeError(TypeError):
    pass


class SqlUnderstandingError(Exception):
    # Usually hit when we need schema info for a given statement but don't have it.
    pass


@dataclasses.dataclass
class _ColumnResolver:
    sqlglot_db_schema: sqlglot.MappingSchema
    table_schema_normalized_mapping: Dict[_TableName, Dict[str, str]]
    use_case_insensitive_cols: bool

    def schema_aware_fuzzy_column_resolve(
        self, table: Optional[_TableName], sqlglot_column: str
    ) -> str:
        default_col_name = (
            sqlglot_column.lower() if self.use_case_insensitive_cols else sqlglot_column
        )
        if table:
            return self.table_schema_normalized_mapping[table].get(
                sqlglot_column, default_col_name
            )
        else:
            return default_col_name


def _prepare_query_columns(
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
    table_schemas: Dict[_TableName, SchemaInfo],
    default_db: Optional[str],
    default_schema: Optional[str],
) -> Tuple[sqlglot.exp.Expression, "_ColumnResolver"]:
    is_create_ddl = is_create_table_ddl(statement)
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

    use_case_insensitive_cols = is_dialect_instance(
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
                    if is_dialect_instance(
                        dialect, DIALECTS_WITH_DEFAULT_UPPERCASE_COLS
                    )
                    else col.lower()
                )
            else:
                col_normalized = col

            table_schema_normalized_mapping[table][col_normalized] = col
            normalized_table_schema[col_normalized] = col_type or "UNKNOWN"

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

    if not is_create_ddl:
        # Optimize the statement + qualify column references.
        if logger.isEnabledFor(logging.DEBUG):
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Qualified sql %s", statement.sql(pretty=True, dialect=dialect)
            )

        # Try to figure out the types of the output columns.
        try:
            statement = sqlglot.optimizer.annotate_types.annotate_types(
                statement, schema=sqlglot_db_schema
            )
        except (sqlglot.errors.OptimizeError, sqlglot.errors.ParseError) as e:
            # This is not a fatal error, so we can continue.
            logger.debug("sqlglot failed to annotate or parse types: %s", e)

    return statement, _ColumnResolver(
        sqlglot_db_schema=sqlglot_db_schema,
        table_schema_normalized_mapping=table_schema_normalized_mapping,
        use_case_insensitive_cols=use_case_insensitive_cols,
    )


def _create_table_ddl_cll(
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
    column_resolver: _ColumnResolver,
    output_table: Optional[_TableName],
) -> List[_ColumnLineageInfo]:
    column_lineage: List[_ColumnLineageInfo] = []

    assert (
        output_table is not None
    ), "output_table must be set for create DDL statements"

    create_schema: sqlglot.exp.Schema = statement.this
    sqlglot_columns = create_schema.expressions

    for column_def in sqlglot_columns:
        if not isinstance(column_def, sqlglot.exp.ColumnDef):
            # Ignore things like constraints.
            continue

        output_col = column_resolver.schema_aware_fuzzy_column_resolve(
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


def _select_statement_cll(  # noqa: C901
    statement: _SupportedColumnLineageTypes,
    dialect: sqlglot.Dialect,
    root_scope: sqlglot.optimizer.Scope,
    column_resolver: _ColumnResolver,
    output_table: Optional[_TableName],
) -> List[_ColumnLineageInfo]:
    column_lineage: List[_ColumnLineageInfo] = []

    try:
        # List output columns.
        output_columns = [
            (select_col.alias_or_name, select_col) for select_col in statement.selects
        ]
        logger.debug("output columns: %s", [col[0] for col in output_columns])
        for output_col, original_col_expression in output_columns:
            if not output_col or output_col == "*":
                # If schema information is available, the * will be expanded to the actual columns.
                # Otherwise, we can't process it.
                continue

            if is_dialect_instance(dialect, "bigquery") and output_col.lower() in {
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
                scope=root_scope,
                trim_selects=False,
                # We don't need to pass the schema in here, since we've already qualified the columns.
            )
            # import pathlib
            # pathlib.Path("sqlglot.html").write_text(
            #     str(lineage_node.to_html(dialect=dialect))
            # )

            # Generate SELECT lineage.
            direct_raw_col_upstreams = _get_direct_raw_col_upstreams(lineage_node)

            # column_logic = lineage_node.source

            # Fuzzy resolve the output column.
            original_col_expression = lineage_node.expression
            if output_col.startswith("_col_"):
                # This is the format sqlglot uses for unnamed columns e.g. 'count(id)' -> 'count(id) AS _col_0'
                # This is a bit jank since we're relying on sqlglot internals, but it seems to be
                # the best way to do it.
                output_col = original_col_expression.this.sql(dialect=dialect)

            output_col = column_resolver.schema_aware_fuzzy_column_resolve(
                output_table, output_col
            )

            # Guess the output column type.
            output_col_type = None
            if original_col_expression.type:
                output_col_type = original_col_expression.type

            # Fuzzy resolve upstream columns.
            direct_resolved_col_upstreams = {
                _ColumnRef(
                    table=edge.table,
                    column=column_resolver.schema_aware_fuzzy_column_resolve(
                        edge.table, edge.column
                    ),
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
    except (sqlglot.errors.OptimizeError, ValueError, IndexError) as e:
        raise SqlUnderstandingError(
            f"sqlglot failed to compute some lineage: {e}"
        ) from e

    return column_lineage


class _ColumnLineageWithDebugInfo(_ParserBaseModel):
    column_lineage: List[_ColumnLineageInfo]

    select_statement: Optional[sqlglot.exp.Expression] = None
    # TODO: Add column exceptions here.


def _column_level_lineage(
    statement: sqlglot.exp.Expression,
    dialect: sqlglot.Dialect,
    downstream_table: Optional[_TableName],
    table_name_schema_mapping: Dict[_TableName, SchemaInfo],
    default_db: Optional[str],
    default_schema: Optional[str],
) -> _ColumnLineageWithDebugInfo:
    # Simplify the input statement for column-level lineage generation.
    try:
        select_statement = _try_extract_select(statement)
    except Exception as e:
        raise SqlUnderstandingError(
            f"Failed to extract select from statement: {e}"
        ) from e

    try:
        assert select_statement is not None
        (select_statement, column_resolver) = _prepare_query_columns(
            select_statement,
            dialect=dialect,
            table_schemas=table_name_schema_mapping,
            default_db=default_db,
            default_schema=default_schema,
        )
    except UnsupportedStatementTypeError as e:
        # Inject details about the outer statement type too.
        e.args = (f"{e.args[0]} (outer statement type: {type(statement)})",)
        logger.debug(e)
        raise e

    # Handle the create table DDL case separately.
    if is_create_table_ddl(select_statement):
        column_lineage = _create_table_ddl_cll(
            select_statement,
            dialect=dialect,
            column_resolver=column_resolver,
            output_table=downstream_table,
        )
        return _ColumnLineageWithDebugInfo(
            column_lineage=column_lineage,
            select_statement=select_statement,
        )

    assert isinstance(select_statement, _SupportedColumnLineageTypesTuple)
    try:
        root_scope = sqlglot.optimizer.build_scope(select_statement)
        if root_scope is None:
            raise SqlUnderstandingError(
                f"Failed to build scope for statement - scope was empty: {statement}"
            )
    except (sqlglot.errors.OptimizeError, ValueError, IndexError) as e:
        raise SqlUnderstandingError(
            f"sqlglot failed to preprocess statement: {e}"
        ) from e

    # Generate column-level lineage.
    column_lineage = _select_statement_cll(
        select_statement,
        dialect=dialect,
        root_scope=root_scope,
        column_resolver=column_resolver,
        output_table=downstream_table,
    )

    return _ColumnLineageWithDebugInfo(
        column_lineage=column_lineage,
        select_statement=select_statement,
    )


def _get_direct_raw_col_upstreams(
    lineage_node: sqlglot.lineage.Node,
) -> Set[_ColumnRef]:
    # Using a set here to deduplicate upstreams.
    direct_raw_col_upstreams: Set[_ColumnRef] = set()

    for node in lineage_node.walk():
        if node.downstream:
            # We only want the leaf nodes.
            pass

        elif isinstance(node.expression, sqlglot.exp.Table):
            table_ref = _TableName.from_sqlglot_table(node.expression)

            if node.name == "*":
                # This will happen if we couldn't expand the * to actual columns e.g. if
                # we don't have schema info for the table. In this case, we can't generate
                # column-level lineage, so we skip it.
                continue

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

    return direct_raw_col_upstreams


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
    extra_args: dict = {}
    original_from = statement.args.get("from")
    if original_from and isinstance(original_from.this, sqlglot.exp.Table):
        # Move joins, laterals, and pivots from the Update->From->Table->field
        # to the top-level Select->field.

        for k in _UPDATE_FROM_TABLE_ARGS_TO_MOVE:
            if k in original_from.this.args:
                # Mutate the from table clause in-place.
                extra_args[k] = original_from.this.args.get(k)
                original_from.this.set(k, None)

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
            column_type=(
                _translate_sqlglot_type(raw_column_lineage.downstream.column_type.this)
                if raw_column_lineage.downstream.column_type
                else None
            ),
            native_column_type=(
                raw_column_lineage.downstream.column_type.sql(dialect=dialect)
                if raw_column_lineage.downstream.column_type
                and raw_column_lineage.downstream.column_type.this
                not in {
                    sqlglot.exp.DataType.Type.UNKNOWN,
                    sqlglot.exp.DataType.Type.NULL,
                }
                else None
            ),
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


def _sqlglot_lineage_inner(
    sql: sqlglot.exp.ExpOrStr,
    schema_resolver: SchemaResolverInterface,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    default_dialect: Optional[str] = None,
) -> SqlParsingResult:

    if not default_dialect:
        dialect = get_dialect(schema_resolver.platform)
    else:
        dialect = get_dialect(default_dialect)

    if is_dialect_instance(dialect, "snowflake"):
        # in snowflake, table identifiers must be uppercased to match sqlglot's behavior.
        if default_db:
            default_db = default_db.upper()
        if default_schema:
            default_schema = default_schema.upper()
    if is_dialect_instance(dialect, "redshift") and not default_schema:
        # On Redshift, there's no "USE SCHEMA <schema>" command. The default schema
        # is public, and "current schema" is the one at the front of the search path.
        # See https://docs.aws.amazon.com/redshift/latest/dg/r_search_path.html
        # and https://stackoverflow.com/questions/9067335/how-does-the-search-path-influence-identifier-resolution-and-the-current-schema?noredirect=1&lq=1
        # default_schema = "public"
        # TODO: Re-enable this.
        pass

    logger.debug("Parsing lineage from sql statement: %s", sql)
    statement = parse_statement(sql, dialect=dialect)

    original_statement, statement = statement, statement.copy()
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
        confidence=(
            0.9
            if total_tables_discovered == total_schemas_resolved
            # If we're missing any schema info, our confidence will be in the 0.2-0.5 range depending
            # on how many tables we were able to resolve.
            else 0.2 + 0.3 * total_schemas_resolved / total_tables_discovered
        ),
        tables_discovered=total_tables_discovered,
        table_schemas_resolved=total_schemas_resolved,
    )
    logger.debug(
        f"Resolved {total_schemas_resolved} of {total_tables_discovered} table schemas"
    )

    column_lineage: Optional[List[_ColumnLineageInfo]] = None
    try:
        with cooperative_timeout(
            timeout=(
                SQL_LINEAGE_TIMEOUT_SECONDS if SQL_LINEAGE_TIMEOUT_ENABLED else None
            )
        ):
            column_lineage_debug_info = _column_level_lineage(
                statement,
                dialect=dialect,
                downstream_table=downstream_table,
                table_name_schema_mapping=table_name_schema_mapping,
                default_db=default_db,
                default_schema=default_schema,
            )
            column_lineage = column_lineage_debug_info.column_lineage
    except CooperativeTimeoutError as e:
        logger.debug(f"Timed out while generating column-level lineage: {e}")
        debug_info.column_error = e
    except UnsupportedStatementTypeError as e:
        # For this known exception type, we assume the error is logged at the point of failure.
        debug_info.column_error = e
    except Exception as e:
        logger.debug(f"Failed to generate column-level lineage: {e}", exc_info=True)
        debug_info.column_error = e

    # TODO: Can we generate a common JOIN tables / keys section?
    # TODO: Can we generate a common WHERE clauses section?

    # Convert TableName to urns.
    in_urns = sorted({table_name_urn_mapping[table] for table in tables})
    out_urns = sorted({table_name_urn_mapping[table] for table in modified})
    column_lineage_urns = None
    if column_lineage:
        try:
            column_lineage_urns = [
                _translate_internal_column_lineage(
                    table_name_urn_mapping, internal_col_lineage, dialect=dialect
                )
                for internal_col_lineage in column_lineage
            ]
        except KeyError as e:
            # When this happens, it's usually because of things like PIVOT where we can't
            # really go up the scope chain.
            logger.debug(
                f"Failed to translate column lineage to urns: {e}", exc_info=True
            )
            debug_info.column_error = e

    query_type, query_type_props = get_query_type_of_sql(
        original_statement, dialect=dialect
    )
    query_fingerprint, debug_info.generalized_statement = get_query_fingerprint_debug(
        original_statement, dialect
    )
    return SqlParsingResult(
        query_type=query_type,
        query_type_props=query_type_props,
        query_fingerprint=query_fingerprint,
        in_tables=in_urns,
        out_tables=out_urns,
        column_lineage=column_lineage_urns,
        debug_info=debug_info,
    )


@functools.lru_cache(maxsize=SQL_PARSE_RESULT_CACHE_SIZE)
def sqlglot_lineage(
    sql: str,
    schema_resolver: SchemaResolverInterface,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    default_dialect: Optional[str] = None,
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

    The SQL dialect can be given as an argument called default_dialect or it can
    be inferred from the schema_resolver's platform.
    The set of supported dialects is the same as sqlglot's. See their
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
        default_dialect: A default dialect to override the dialect provided by 'schema_resolver'.

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
            default_dialect=default_dialect,
        )
    except Exception as e:
        return SqlParsingResult.make_from_error(e)


def create_lineage_sql_parsed_result(
    query: str,
    default_db: Optional[str],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    default_schema: Optional[str] = None,
    graph: Optional[DataHubGraph] = None,
    schema_aware: bool = True,
) -> SqlParsingResult:
    if graph and schema_aware:
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


def infer_output_schema(result: SqlParsingResult) -> Optional[List[SchemaFieldClass]]:
    if result.column_lineage is None:
        return None

    output_schema = []
    for column_info in result.column_lineage:
        output_schema.append(
            SchemaFieldClass(
                fieldPath=column_info.downstream.column,
                type=(
                    column_info.downstream.column_type
                    or SchemaFieldDataTypeClass(type=NullTypeClass())
                ),
                nativeDataType=column_info.downstream.native_column_type or "",
            )
        )
    return output_schema


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

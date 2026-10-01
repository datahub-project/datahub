import functools
import logging
import re
import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Type, cast

from sqlalchemy import text
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.dialects.mysql.reflection import ReflectedState
from sqlalchemy.engine import Connection, reflection
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedTableComment
from sqlalchemy.exc import SAWarning, SQLAlchemyError
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeDecorator, TypeEngine

logger = logging.getLogger(__name__)


class HLL(sqltypes.LargeBinary):
    __visit_name__ = "HLL"


class BITMAP(sqltypes.LargeBinary):
    __visit_name__ = "BITMAP"


class QUANTILE_STATE(sqltypes.LargeBinary):
    __visit_name__ = "QUANTILE_STATE"


class AGG_STATE(sqltypes.LargeBinary):
    __visit_name__ = "AGG_STATE"


class DORIS_ARRAY(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "ARRAY"


class DORIS_MAP(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "MAP"


class DORIS_STRUCT(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "STRUCT"


class DORIS_JSONB(sqltypes.JSON):
    __visit_name__ = "JSONB"


class LARGEINT(sqltypes.Numeric):
    __visit_name__ = "LARGEINT"


class VARIANT(sqltypes.JSON):
    __visit_name__ = "VARIANT"


class IPV4(sqltypes.String):
    __visit_name__ = "IPV4"


class IPV6(sqltypes.String):
    __visit_name__ = "IPV6"


_doris_type_map: Dict[str, Type[TypeEngine]] = {
    "hll": HLL,
    "bitmap": BITMAP,
    "quantile_state": QUANTILE_STATE,
    "agg_state": AGG_STATE,
    "array": DORIS_ARRAY,
    "map": DORIS_MAP,
    "struct": DORIS_STRUCT,
    "jsonb": DORIS_JSONB,
    "largeint": LARGEINT,
    "variant": VARIANT,
    "ipv4": IPV4,
    "ipv6": IPV6,
}

# Doris names for types MySQL already models. Registering them keeps SQLAlchemy's
# MySQL DDL parser from falling back to NullType(*args), which raises TypeError
# because NullType takes no arguments.
_doris_alias_type_map: Dict[str, Type[TypeEngine]] = {
    "string": sqltypes.TEXT,
    "datev2": sqltypes.DATE,
    "datetimev2": sqltypes.DATETIME,
    "decimalv2": sqltypes.DECIMAL,
    "decimalv3": sqltypes.DECIMAL,
}


_TYPE_NAME_PATTERN = re.compile(r"\w+")

# Doris' two known refusals to hand back a table in DDL form: async materialized
# views, and the TypeError the MySQL DDL parser raises building NullType(*args) for a
# type it cannot model. Anything else that reaches the fallback is a different problem
# — a missing grant, a dropped connection — and must not be filed under the same
# benign heading. The second branch is anchored to NullType so an unrelated
# "takes no arguments" TypeError is not waved through as expected.
_EXPECTED_DDL_REFUSAL_PATTERN = re.compile(
    r"not support async materialized view|NullType\(\).*takes no arguments",
    re.IGNORECASE,
)

# DESCRIBE returns Field, Type, Null, Key, Default, Extra.
_DESCRIBE_NULLABLE_INDEX = 2
_DESCRIBE_DEFAULT_INDEX = 4

# SHOW CREATE TABLE writes every Doris comment as COMMENT "..." while SQLAlchemy's
# MySQL DDL parser only reads COMMENT '...', and aggregate-model columns
# (`hll HLL_UNION`) and complex types (`array<int>`) defeat its column regex
# outright, so the parse yields no comments at all. information_schema serves the
# same comments unescaped; one query per database covers every table in it.
_COLUMN_COMMENTS_SQL = (
    "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = :schema"
)
_TABLE_COMMENTS_SQL = (
    "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES "
    "WHERE TABLE_SCHEMA = :schema"
)


@dataclass(frozen=True)
class ReflectionFallback:
    error: str
    # Whether `error` matched a known Doris refusal. Drives which warning the source
    # raises, so an unexpected failure is not reported as routine degradation.
    expected: bool


@dataclass(frozen=True)
class DescribeRow:
    name: str
    type_str: str
    nullable: bool
    default: Optional[str]


def _parse_describe_row(row: Sequence[Any]) -> DescribeRow:
    # Trailing columns are read defensively because Doris external catalogs (Iceberg,
    # Hive) do not always return the full six-column shape the internal catalog does.
    default = (
        row[_DESCRIBE_DEFAULT_INDEX] if len(row) > _DESCRIBE_DEFAULT_INDEX else None
    )
    return DescribeRow(
        name=str(row[0]),
        type_str=str(row[1]),
        nullable=len(row) <= _DESCRIBE_NULLABLE_INDEX
        or str(row[_DESCRIBE_NULLABLE_INDEX]).upper() != "NO",
        # SQLAlchemy reflects a column default as its DDL text, so match the driver's
        # value to the annotation rather than passing whatever type it handed back.
        default=None if default is None else str(default),
    )


def _non_empty_comment(value: Any) -> Optional[str]:
    # Doris reports "no comment" as an empty string, which must not become an empty
    # description in DataHub.
    if value is None:
        return None
    comment = str(value)
    return comment if comment.strip() else None


@functools.lru_cache(maxsize=None)
def _warn_type_not_instantiable(type_name: str) -> None:
    # lru_cache keeps this to one line per type name: a registered type that cannot be
    # built without arguments is a bug in the type map, not a per-column event.
    logger.warning(
        f"Type {type_name!r} is registered but cannot be built without arguments. "
        f"Falling back to MySQL type reflection."
    )


def _parse_doris_type(
    type_str: str, known_types: Mapping[str, Type[TypeEngine]]
) -> TypeEngine:
    # Precision and length arguments are dropped: full_type carries the exact Doris
    # type string for display, and DataHub only maps the type class.
    match = _TYPE_NAME_PATTERN.match(type_str.strip().lower())
    if not match:
        logger.debug(
            f"Failed to parse type string {type_str!r} (expected alphanumeric type name). "
            f"Using MySQL type reflection."
        )
        return sqltypes.NULLTYPE

    type_name = match.group()
    type_class = known_types.get(type_name)
    if type_class is None:
        logger.debug(f"No SQLAlchemy type registered for {type_name!r}.")
        return sqltypes.NULLTYPE
    try:
        return type_class()
    except Exception:
        _warn_type_not_instantiable(type_name)
        return sqltypes.NULLTYPE


class DorisDialect(MySQLDialect_pymysql):
    name = "doris"
    supports_statement_cache = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Rebind rather than update: ischema_names is a class attribute on
        # MySQLDialect, shared by every MySQL-family dialect in the process, so
        # mutating it in place teaches MySQL/MariaDB/TiDB reflection about Doris types.
        self.ischema_names = {
            **self.ischema_names,
            **_doris_type_map,
            **_doris_alias_type_map,
        }
        # Tables reflected from DESCRIBE instead of SHOW CREATE TABLE, and tables whose
        # DESCRIBE type overlay failed, both keyed by quoted full name. The dialect
        # cannot reach the ingestion report, so DorisSource drains these into report
        # warnings once a database is done.
        self.reflection_fallbacks: Dict[str, ReflectionFallback] = {}
        self.type_overlay_failures: Dict[str, str] = {}
        # Databases whose information_schema comment lookup failed, keyed by
        # database name, drained the same way.
        self.comment_lookup_failures: Dict[str, str] = {}

    def pop_reflection_fallbacks(self) -> Dict[str, ReflectionFallback]:
        fallbacks = self.reflection_fallbacks
        self.reflection_fallbacks = {}
        return fallbacks

    def pop_type_overlay_failures(self) -> Dict[str, str]:
        failures = self.type_overlay_failures
        self.type_overlay_failures = {}
        return failures

    def pop_comment_lookup_failures(self) -> Dict[str, str]:
        failures = self.comment_lookup_failures
        self.comment_lookup_failures = {}
        return failures

    @reflection.cache  # type: ignore[call-arg]
    def _setup_parser(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> ReflectedState
        """
        Parse SHOW CREATE TABLE, falling back to DESCRIBE when Doris rejects it.

        Every MySQL reflection method (get_columns, get_pk_constraint,
        get_foreign_keys, get_table_comment, get_indexes) reads the state this
        produces, so guarding it here keeps a table that Doris will not describe in
        DDL form from being dropped by whichever method happens to run first.

        Type hints are in comment form because @reflection.cache doesn't support
        modern Python type annotations in function signatures.
        """
        try:
            # Suppress expected warnings from Doris-specific DDL syntax that SQLAlchemy's
            # MySQL parser doesn't recognize: AGGREGATE KEY, DUPLICATE KEY, DISTRIBUTED BY,
            # PROPERTIES, array<T> column definitions, etc.
            # These warnings are non-actionable and would otherwise appear in every ingestion run.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"Unknown schema content.*",
                    category=SAWarning,
                )
                warnings.filterwarnings(
                    "ignore",
                    message=r"Incomplete reflection of column definition.*",
                    category=SAWarning,
                )
                # Private in SQLAlchemy and absent from its type stubs, but it is the
                # single point every MySQL reflection method funnels through.
                return super()._setup_parser(  # type: ignore[misc]
                    connection, table_name, schema, **kw
                )
        except (SQLAlchemyError, TypeError) as e:
            # Doris rejects SHOW CREATE TABLE for async materialized views, and the
            # MySQL DDL parser raises TypeError on Doris types it cannot model.
            # DESCRIBE answers for both, but carries no keys or constraints, so
            # those degrade to empty rather than taking the whole table
            # down. Errors outside those two families stay fatal rather than turning a
            # bug in this path into missing metadata.
            full_name = self._full_name(connection, table_name, schema)
            if full_name is None:
                raise
            logger.info(
                f"SHOW CREATE TABLE reflection failed for {full_name}: {e}. "
                f"Falling back to DESCRIBE; keys and foreign keys will be missing."
            )
            state = ReflectedState()
            state.table_name = table_name
            # Record the fallback only once DESCRIBE has actually produced columns. If
            # it fails too (the same missing grant that killed SHOW CREATE TABLE, a
            # dropped connection) the exception propagates and the caller drops the
            # table, which must not then also be reported as successfully reflected.
            state.columns = cast(
                List[ReflectedColumn], self._describe_columns(connection, full_name)
            )
            self.reflection_fallbacks[full_name] = ReflectionFallback(
                error=str(e),
                expected=_EXPECTED_DDL_REFUSAL_PATTERN.search(str(e)) is not None,
            )
            return state

    @reflection.cache  # type: ignore[call-arg]
    def get_columns(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> List[ReflectedColumn]
        """
        Override to preserve Doris-specific types via DESCRIBE queries.

        Handles: HLL, BITMAP, QUANTILE_STATE, AGG_STATE, ARRAY, MAP, STRUCT, JSONB.

        Type hints are in comment form because @reflection.cache doesn't support
        modern Python type annotations in function signatures.
        """
        columns = super().get_columns(connection, table_name, schema, **kw)

        full_name = self._full_name(connection, table_name, schema)
        if full_name is None:
            return columns

        # _describe_columns already built fallback tables from DESCRIBE, so the type
        # overlay would only repeat that round-trip to compute identical types.
        if full_name not in self.reflection_fallbacks:
            self._overlay_doris_types(connection, full_name, columns)

        comment_schema = self._comment_schema(connection, schema)
        if comment_schema is not None:
            table_comments = self._column_comments(
                connection, comment_schema, **kw
            ).get(table_name, {})
            for col in columns:
                # information_schema wins: it has the comment unescaped, and the DDL
                # parse only ever recovers the rare single-quoted one.
                comment = table_comments.get(col["name"])
                if comment is not None:
                    col["comment"] = comment

        return columns

    @reflection.cache  # type: ignore[call-arg]
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> ReflectedTableComment
        """
        Read the table comment from information_schema, which Doris fills in for
        tables, views and DESCRIBE-fallback tables alike.

        Type hints are in comment form because @reflection.cache doesn't support
        modern Python type annotations in function signatures.
        """
        table_comment = super().get_table_comment(connection, table_name, schema, **kw)

        comment_schema = self._comment_schema(connection, schema)
        if comment_schema is None:
            return table_comment

        comment = self._table_comments(connection, comment_schema, **kw).get(table_name)
        if comment is None:
            return table_comment
        return ReflectedTableComment(text=comment)

    @reflection.cache  # type: ignore[call-arg]
    def _column_comments(self, connection, schema, **kw):
        # type: (Connection, str, Any) -> Dict[str, Dict[str, str]]
        comments: Dict[str, Dict[str, str]] = {}
        for row in self._query_comments(connection, _COLUMN_COMMENTS_SQL, schema):
            comment = _non_empty_comment(row[2])
            if comment is not None:
                comments.setdefault(str(row[0]), {})[str(row[1])] = comment
        return comments

    @reflection.cache  # type: ignore[call-arg]
    def _table_comments(self, connection, schema, **kw):
        # type: (Connection, str, Any) -> Dict[str, str]
        comments: Dict[str, str] = {}
        for row in self._query_comments(connection, _TABLE_COMMENTS_SQL, schema):
            comment = _non_empty_comment(row[1])
            if comment is not None:
                comments[str(row[0])] = comment
        return comments

    def _query_comments(
        self, connection: Connection, sql: str, schema: str
    ) -> List[Sequence[Any]]:
        try:
            return list(connection.execute(text(sql), {"schema": schema}))
        except SQLAlchemyError as e:
            # Comments are lost but the tables are not. The empty result is what the
            # reflection cache memoises, so a database whose lookup fails costs one
            # failed query rather than one per table.
            self.comment_lookup_failures.setdefault(schema, str(e))
            logger.info(
                f"Reading comments from information_schema failed for {schema}: {e}. "
                f"Its tables and columns will have no descriptions."
            )
            return []

    def _comment_schema(self, connection, schema):
        # type: (Connection, Optional[str]) -> Optional[str]
        # An external-catalog connection's URL database is `catalog.database`, but
        # information_schema keys rows by the bare database name. Doris database
        # names cannot contain a dot, so the last segment is always the database.
        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return None
        return current_schema.rsplit(".", 1)[-1]

    def _overlay_doris_types(
        self, connection: Connection, full_name: str, columns: List[ReflectedColumn]
    ) -> None:
        try:
            type_map = {
                row.name: row.type_str
                for row in self._describe_rows(connection, full_name)
            }

            for col in columns:
                if col["name"] in type_map:
                    doris_type_str = type_map[col["name"]]
                    # "full_type" is a non-standard key consumed by SQLAlchemySource
                    # to preserve the raw Doris type string; it is not part of the
                    # ReflectedColumn TypedDict.
                    col["full_type"] = doris_type_str  # type: ignore[typeddict-unknown-key]

                    # Only the Doris-only map here: MySQL reflection already resolved
                    # the standard types correctly, so this overlay exists purely to
                    # replace them where Doris has a type MySQL cannot express.
                    parsed_type = _parse_doris_type(doris_type_str, _doris_type_map)
                    if parsed_type is not sqltypes.NULLTYPE:
                        col["type"] = parsed_type

        except (SQLAlchemyError, IndexError, TypeError) as e:
            # Columns survive with MySQL's types; only the Doris-specific ones are
            # lost. Recorded rather than logged so the source can report it, since a
            # silently generic HLL or BITMAP column looks like correct output.
            self.type_overlay_failures[full_name] = str(e)
            logger.info(
                f"DESCRIBE failed for {full_name}: {e}. "
                f"Falling back to MySQL type reflection."
            )

    def _full_name(self, connection, table_name, schema):
        # type: (Connection, str, Optional[str]) -> Optional[str]
        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return None
        quote = self.identifier_preparer.quote_identifier
        return f"{quote(current_schema)}.{quote(table_name)}"

    def _describe_rows(
        self, connection: Connection, full_name: str
    ) -> List[DescribeRow]:
        return [
            _parse_describe_row(row)
            for row in connection.execute(text(f"DESCRIBE {full_name}"))
        ]

    def _describe_columns(
        self, connection: Connection, full_name: str
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        for row in self._describe_rows(connection, full_name):
            # The full map, not just the Doris-only one: DESCRIBE is the sole type
            # information available here, so standard MySQL types have to resolve too.
            # ischema_names is a superset of _doris_type_map, so one lookup suffices.
            column_type = _parse_doris_type(row.type_str, self.ischema_names)
            columns.append(
                {
                    "name": row.name,
                    "type": column_type,
                    "full_type": row.type_str,
                    "nullable": row.nullable,
                    "default": row.default,
                    "comment": None,
                }
            )
        return columns

    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        result = connection.execute(text("SHOW SCHEMAS"))
        return [row[0] for row in result]

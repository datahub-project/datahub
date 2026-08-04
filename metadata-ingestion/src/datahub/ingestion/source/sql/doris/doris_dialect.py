import functools
import logging
import re
import warnings
from typing import Any, Dict, List, Mapping, Optional, Type

from sqlalchemy import text
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.dialects.mysql.reflection import ReflectedState
from sqlalchemy.engine import Connection, reflection
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


@functools.lru_cache(maxsize=None)
def _warn_type_not_instantiable(type_name: str) -> None:
    # lru_cache keeps this to one line per type name: a registered type that cannot be
    # built without arguments is a bug in the type map, not a per-column event.
    logger.warning(
        f"Type {type_name!r} is registered but cannot be built without arguments. "
        f"Falling back to MySQL type reflection."
    )


def _parse_doris_type(
    type_str: str, type_map: Optional[Mapping[str, Type[TypeEngine]]] = None
) -> TypeEngine:
    # Precision and length arguments are dropped: full_type carries the exact Doris
    # type string for display, and DataHub only maps the type class.
    known_types = _doris_type_map if type_map is None else type_map
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
        self.ischema_names.update(_doris_type_map)
        self.ischema_names.update(_doris_alias_type_map)
        # Tables reflected from DESCRIBE instead of SHOW CREATE TABLE, keyed by
        # quoted full name. The dialect cannot reach the ingestion report, so
        # DorisSource drains this into report warnings once a database is done.
        self.reflection_fallbacks: Dict[str, str] = {}

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
            # DESCRIBE answers for both, but carries no keys, constraints or table
            # comment, so those degrade to empty rather than taking the whole table
            # down. Errors outside those two families stay fatal rather than turning a
            # bug in this path into missing metadata.
            full_name = self._full_name(connection, table_name, schema)
            if full_name is None:
                raise
            self.reflection_fallbacks[full_name] = str(e)
            logger.info(
                f"SHOW CREATE TABLE reflection failed for {full_name}: {e}. "
                f"Falling back to DESCRIBE; keys, foreign keys and the table comment "
                f"will be missing."
            )
            state = ReflectedState()
            state.table_name = table_name
            state.columns = self._describe_columns(connection, full_name)
            return state

    @reflection.cache  # type: ignore[call-arg]
    def get_columns(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> List[Dict[str, Any]]
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

        if full_name in self.reflection_fallbacks:
            # _describe_columns already built these from DESCRIBE, so the overlay below
            # would only repeat that round-trip to compute identical types.
            return columns

        try:
            type_map = {
                row[0]: row[1] for row in self._describe_rows(connection, full_name)
            }

            for col in columns:
                if col["name"] in type_map:
                    doris_type_str = type_map[col["name"]]
                    col["full_type"] = doris_type_str

                    parsed_type = _parse_doris_type(doris_type_str)
                    if parsed_type is not sqltypes.NULLTYPE:
                        col["type"] = parsed_type

        except SQLAlchemyError as e:
            logger.debug(
                f"DESCRIBE failed for {full_name}: {e}. "
                f"Falling back to MySQL type reflection."
            )
        except Exception as e:
            logger.warning(
                f"Unexpected error in DESCRIBE for {full_name}: {e}. "
                f"Falling back to MySQL type reflection."
            )

        return columns

    def _full_name(self, connection, table_name, schema):
        # type: (Connection, str, Optional[str]) -> Optional[str]
        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return None
        quote = self.identifier_preparer.quote_identifier
        return f"{quote(current_schema)}.{quote(table_name)}"

    def _describe_rows(self, connection: Connection, full_name: str) -> List[Any]:
        return list(connection.execute(text(f"DESCRIBE {full_name}")))

    def _describe_columns(
        self, connection: Connection, full_name: str
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        for row in self._describe_rows(connection, full_name):
            type_str = str(row[1])
            # Fall back to the full type map: DESCRIBE is the only type information
            # available here, so standard MySQL types have to resolve too.
            column_type = _parse_doris_type(type_str)
            if column_type is sqltypes.NULLTYPE:
                column_type = _parse_doris_type(type_str, self.ischema_names)
            columns.append(
                {
                    "name": row[0],
                    "type": column_type,
                    "full_type": type_str,
                    # DESCRIBE reports nullability as YES/NO in its third column.
                    "nullable": len(row) < 3 or str(row[2]).upper() != "NO",
                    "default": row[4] if len(row) > 4 else None,
                    "comment": None,
                }
            )
        return columns

    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        result = connection.execute(text("SHOW SCHEMAS"))
        return [row[0] for row in result]

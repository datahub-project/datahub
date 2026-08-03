import logging
import re
import warnings
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
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


_doris_type_map = {
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
_doris_alias_type_map = {
    "string": sqltypes.TEXT,
    "datev2": sqltypes.DATE,
    "datetimev2": sqltypes.DATETIME,
    "decimalv2": sqltypes.DECIMAL,
    "decimalv3": sqltypes.DECIMAL,
}


def _parse_doris_type(type_str: str) -> TypeEngine:
    type_str = type_str.strip().lower()
    match = re.match(r"^(?P<type>\w+)", type_str)
    if not match:
        logger.debug(
            f"Failed to parse type string {type_str!r} (expected alphanumeric type name). "
            f"Using MySQL type reflection."
        )
        return sqltypes.NULLTYPE

    type_name = match.group("type")
    if type_name in _doris_type_map:
        return _doris_type_map[type_name]()

    logger.debug(
        f"Type {type_name!r} not in Doris custom type map "
        f"(known: {', '.join(_doris_type_map.keys())}). Using MySQL type reflection."
    )
    return sqltypes.NULLTYPE


class DorisDialect(MySQLDialect_pymysql):
    name = "doris"
    supports_statement_cache = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ischema_names.update(_doris_type_map)
        self.ischema_names.update(_doris_alias_type_map)

    @reflection.cache  # type: ignore[call-arg]
    def get_columns(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> List[Dict[str, Any]]
        """
        Override to preserve Doris-specific types via DESCRIBE queries.

        Handles: HLL, BITMAP, QUANTILE_STATE, AGG_STATE, ARRAY, MAP, STRUCT, JSONB.

        Type hints are in comment form because @reflection.cache doesn't support
        modern Python type annotations in function signatures.
        """
        current_schema = schema or connection.engine.url.database
        full_name: Optional[str] = None
        if current_schema:
            quote = self.identifier_preparer.quote_identifier
            full_name = f"{quote(current_schema)}.{quote(table_name)}"

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
                columns = super().get_columns(connection, table_name, schema, **kw)
        except Exception as e:
            # MySQL reflection reads SHOW CREATE TABLE, which Doris rejects for async
            # materialized views, and its DDL parser raises on Doris types it cannot
            # model. DESCRIBE answers for both, so rebuild the columns from it rather
            # than losing the whole table.
            if full_name is None:
                raise
            logger.info(
                f"MySQL reflection failed for {current_schema}.{table_name}: {e}. "
                f"Rebuilding columns from DESCRIBE."
            )
            return self._describe_columns(connection, full_name)

        if full_name is None:
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
                f"DESCRIBE failed for {current_schema}.{table_name}: {e}. "
                f"Falling back to MySQL type reflection."
            )
        except Exception as e:
            logger.warning(
                f"Unexpected error in DESCRIBE for {current_schema}.{table_name}: {e}. "
                f"Falling back to MySQL type reflection."
            )

        return columns

    def _describe_rows(self, connection: Connection, full_name: str) -> List[Any]:
        return list(connection.execute(text(f"DESCRIBE {full_name}")))

    def _describe_columns(
        self, connection: Connection, full_name: str
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        for row in self._describe_rows(connection, full_name):
            type_str = str(row[1])
            column_type = _parse_doris_type(type_str)
            if column_type is sqltypes.NULLTYPE:
                column_type = self._alias_type(type_str)
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

    def _alias_type(self, type_str: str) -> TypeEngine:
        match = re.match(r"^(?P<type>\w+)", type_str.strip().lower())
        if not match:
            return sqltypes.NULLTYPE
        type_class = self.ischema_names.get(match.group("type"))
        if type_class is None:
            return sqltypes.NULLTYPE
        # Precision/length arguments are dropped here; full_type keeps the exact
        # Doris type string for display, and DataHub only maps the type class.
        try:
            return type_class()
        except TypeError:
            return sqltypes.NULLTYPE

    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        result = connection.execute(text("SHOW SCHEMAS"))
        return [row[0] for row in result]

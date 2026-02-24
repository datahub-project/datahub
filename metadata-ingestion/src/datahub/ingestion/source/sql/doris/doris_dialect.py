import logging
import re
import warnings
from typing import Any, Dict, List, Optional  # noqa: F401 (used in type comments)

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


_doris_type_map = {
    "hll": HLL,
    "bitmap": BITMAP,
    "quantile_state": QUANTILE_STATE,
    "agg_state": AGG_STATE,
    "array": DORIS_ARRAY,
    "map": DORIS_MAP,
    "struct": DORIS_STRUCT,
    "jsonb": DORIS_JSONB,
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

    @reflection.cache  # type: ignore[call-arg]
    def get_columns(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> List[Dict[str, Any]]
        """
        Override to preserve Doris-specific types via DESCRIBE queries.

        Handles: HLL, BITMAP, QUANTILE_STATE, AGG_STATE, ARRAY, MAP, STRUCT, JSONB.

        Type hints are in comment form because @reflection.cache doesn't support
        modern Python type annotations in function signatures.
        """
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

        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return columns

        try:
            quote = self.identifier_preparer.quote_identifier
            full_name = f"{quote(current_schema)}.{quote(table_name)}"
            result = connection.execute(text(f"DESCRIBE {full_name}"))
            type_map = {row[0]: row[1] for row in result}

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

    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        result = connection.execute(text("SHOW SCHEMAS"))
        return [row[0] for row in result]

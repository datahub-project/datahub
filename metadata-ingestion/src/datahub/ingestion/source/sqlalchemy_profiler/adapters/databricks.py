"""Databricks-specific profiling adapter."""

import logging
import re
from typing import Any, Dict, List, Optional, Type

import sqlalchemy as sa
from databricks.sqlalchemy import DatabricksDialect
from databricks.sqlalchemy.dialect import (
    DatabricksDate,
    DatabricksDecimal,
    DatabricksTimestamp,
)
from sqlalchemy.engine import Connection
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)

logger = logging.getLogger(__name__)

# Vendor get_columns _type_map has no VARIANT; KeyError aborts the whole table.
_DATABRICKS_COLUMN_TYPE_MAP: Dict[str, Type[sqltypes.TypeEngine]] = {
    "boolean": sqltypes.Boolean,
    "smallint": sqltypes.SmallInteger,
    "int": sqltypes.Integer,
    "bigint": sqltypes.BigInteger,
    "float": sqltypes.Float,
    "double": sqltypes.Float,
    "string": sqltypes.String,
    "varchar": sqltypes.String,
    "char": sqltypes.String,
    "binary": sqltypes.String,
    "array": sqltypes.String,
    "map": sqltypes.String,
    "struct": sqltypes.String,
    "uniontype": sqltypes.String,
    "decimal": DatabricksDecimal,
    "timestamp": DatabricksTimestamp,
    "date": DatabricksDate,
    "variant": sqltypes.NullType,
}


def map_databricks_column_type(type_name: str) -> Type[sqltypes.TypeEngine]:
    match = re.search(r"^\w+", type_name or "")
    if not match:
        return sqltypes.NullType
    return _DATABRICKS_COLUMN_TYPE_MAP.get(match.group(0).lower(), sqltypes.NullType)


def _get_columns_unknown_types_as_null(
    self: DatabricksDialect,
    connection: Any,
    table_name: str,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    with self.get_connection_cursor(connection) as cur:
        resp = cur.columns(
            catalog_name=self.catalog,
            schema_name=schema or self.schema,
            table_name=table_name,
        ).fetchall()

    columns: List[Dict[str, Any]] = []
    for col in resp:
        columns.append(
            {
                "name": col.COLUMN_NAME,
                "type": map_databricks_column_type(col.TYPE_NAME),
                "nullable": bool(col.NULLABLE),
                "default": col.COLUMN_DEF,
                "autoincrement": col.IS_AUTO_INCREMENT != "NO",
            }
        )
    return columns


def _patch_databricks_get_columns() -> None:
    if getattr(DatabricksDialect.get_columns, "_datahub_unknown_types_patched", False):
        return

    original_get_columns = DatabricksDialect.get_columns

    def get_columns(
        self: DatabricksDialect,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        try:
            return original_get_columns(
                self, connection, table_name, schema=schema, **kwargs
            )
        except KeyError as exc:
            missing = exc.args[0] if exc.args else None
            if not isinstance(missing, str):
                raise
            logger.debug(
                "Databricks dialect has no mapping for column type %r; "
                "reflecting with NullType fallback",
                missing,
            )
            return _get_columns_unknown_types_as_null(
                self, connection, table_name, schema=schema, **kwargs
            )

    get_columns._datahub_unknown_types_patched = True  # type: ignore[attr-defined]
    DatabricksDialect.get_columns = get_columns  # type: ignore[method-assign]


_patch_databricks_get_columns()


class DatabricksAdapter(PlatformAdapter):
    """
    Databricks-specific profiling adapter.

    Databricks optimizations:
    1. approx_count_distinct for fast unique counts
    2. approx_percentile for median calculation

    Note: Databricks uses lowercase function names (approx_count_distinct, approx_percentile)
    unlike some other platforms.

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Databricks uses approx_count_distinct for fast unique counts.

        This matches GE profiler behavior (ge_data_profiler.py:233-239).
        Note: Databricks uses lowercase function name.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_count_distinct
        """
        return sa.func.approx_count_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Databricks uses approx_percentile for median.

        This matches GE profiler behavior (ge_data_profiler.py:684-693).
        approx_percentile(column, 0.5) computes the approximate median.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_percentile(column, 0.5)
        """
        return sa.func.approx_percentile(sa.column(column), 0.5)

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using Databricks' approx_percentile.

        Databricks: approx_percentile(col, array(0.05, 0.25, ...)) returns an array.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            quantiles: List of quantile values (default: DEFAULT_QUANTILES)

        Returns:
            List of quantile values (None for unavailable quantiles)
        """
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        quoted_column = self.quote_identifier(column)
        # Databricks: Similar to Athena/Trino but uses array() syntax
        array_str = f"array({', '.join(str(q) for q in quantiles)})"
        databricks_expr = sa.literal_column(
            f"approx_percentile({quoted_column}, {array_str})"
        ).label("quantiles")
        query = sa.select([databricks_expr]).select_from(table)
        result = conn.execute(query).scalar()
        logger.debug(
            f"Databricks quantiles for {column}: result type={type(result)}, "
            f"value={result}, expected_length={len(quantiles)}"
        )
        # Result is an array, convert to list
        if isinstance(result, list):
            if len(result) != len(quantiles):
                logger.warning(
                    f"Quantile result length mismatch: got {len(result)}, expected {len(quantiles)}"
                )
            return [float(v) if v is not None else None for v in result]
        logger.warning(
            f"Quantile result is not a list: type={type(result)}, value={result}"
        )
        return [None] * len(quantiles)

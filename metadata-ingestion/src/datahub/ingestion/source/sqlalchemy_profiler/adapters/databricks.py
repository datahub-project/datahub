"""Databricks-specific profiling adapter."""

import logging
import re
from typing import Any, Dict, List, Optional, Type

import databricks.sqlalchemy.base as databricks_dialect_base
import sqlalchemy as sa
from databricks.sqlalchemy._parse import (
    GET_COLUMNS_TYPE_MAP,
    parse_column_info_from_tgetcolumnsresponse,
)
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.elements import ColumnElement, Label
from sqlalchemy.sql.type_api import TypeEngine

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
    ProfilingConnection,
)

logger = logging.getLogger(__name__)

# databricks-sqlalchemy reflects columns with a bare GET_COLUMNS_TYPE_MAP[...] lookup
# (and a regex .group(0) that assumes TYPE_NAME is non-empty), so one unmapped type --
# e.g. TIMESTAMP_LTZ -- raises and aborts reflection for the whole table. These
# overrides take precedence over the vendor map. VARIANT is reflected as NULL so the
# profiler skips it rather than issuing aggregates it cannot compute.
_DATABRICKS_COLUMN_TYPE_OVERRIDES: Dict[str, Type[TypeEngine]] = {
    "variant": sqltypes.NullType,
    "timestamp_ltz": GET_COLUMNS_TYPE_MAP["timestamp"],
}


def _base_type_name(type_name: Optional[str]) -> Optional[str]:
    match = re.search(r"^\w+", type_name or "")
    return match.group(0).lower() if match else None


def map_databricks_column_type(type_name: Optional[str]) -> Type[TypeEngine]:
    base = _base_type_name(type_name)
    if base is None:
        logger.info(
            "Databricks returned an unparseable column type %r; reflecting it as NULL, "
            "so this column will be skipped for profiling.",
            type_name,
        )
        return sqltypes.NullType
    mapped = _DATABRICKS_COLUMN_TYPE_OVERRIDES.get(base) or GET_COLUMNS_TYPE_MAP.get(
        base
    )
    if mapped is None:
        logger.info(
            "No SQLAlchemy type mapping for Databricks type %r; reflecting it as NULL, "
            "so this column will be skipped for profiling. If Databricks has added a "
            "new type, add it to _DATABRICKS_COLUMN_TYPE_OVERRIDES.",
            base,
        )
        return sqltypes.NullType
    return mapped


def _tolerant_parse_column_info(thrift_resp_row: Any) -> Dict[str, Any]:
    base = _base_type_name(thrift_resp_row.TYPE_NAME)
    if base in GET_COLUMNS_TYPE_MAP and base not in _DATABRICKS_COLUMN_TYPE_OVERRIDES:
        # Vendor path keeps DECIMAL precision/scale and column comments.
        return dict(parse_column_info_from_tgetcolumnsresponse(thrift_resp_row))
    return {
        "name": thrift_resp_row.COLUMN_NAME,
        "type": map_databricks_column_type(thrift_resp_row.TYPE_NAME),
        "nullable": bool(thrift_resp_row.NULLABLE),
        "default": thrift_resp_row.COLUMN_DEF,
        "comment": getattr(thrift_resp_row, "REMARKS", None) or None,
    }


# DatabricksDialect.get_columns resolves the parser through this module global.
_parser_attr = "parse_column_info_from_tgetcolumnsresponse"
setattr(databricks_dialect_base, _parser_attr, _tolerant_parse_column_info)


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
        conn: ProfilingConnection,
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
        databricks_expr: Label = sa.literal_column(
            f"approx_percentile({quoted_column}, {array_str})"
        ).label("quantiles")
        query = sa.select(databricks_expr).select_from(table)
        result = conn.execute_rows(query).scalar()
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

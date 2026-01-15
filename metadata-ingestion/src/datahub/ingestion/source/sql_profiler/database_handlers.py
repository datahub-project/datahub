"""Database-specific SQL generation for optimized profiling."""

import logging
from typing import Any, List, Optional, Union

import sqlalchemy as sa
from sqlalchemy.engine import Connection

logger: logging.Logger = logging.getLogger(__name__)


class DatabaseHandlers:
    """Database-specific SQL generation."""

    @staticmethod
    def get_approx_unique_count_expr(
        platform: str, column: str
    ) -> Union[sa.sql.elements.ClauseElement, str]:
        """Get database-specific approximate unique count expression."""

        platform_lower = platform.lower()

        if platform_lower in ("bigquery", "snowflake"):
            # BigQuery and Snowflake support APPROX_COUNT_DISTINCT
            return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

        elif platform_lower == "redshift":
            # Redshift uses special APPROXIMATE syntax
            # Note: This is raw SQL because SQLAlchemy doesn't have a func for it
            return sa.text(f'APPROXIMATE count(distinct "{column}")')

        elif platform_lower in ("athena", "trino"):
            # Athena/Trino use approx_distinct
            return sa.func.approx_distinct(sa.column(column))

        elif platform_lower == "databricks":
            # Databricks uses lowercase
            return sa.func.approx_count_distinct(sa.column(column))

        else:
            # Fallback: exact count (slower but works everywhere)
            return sa.func.count(sa.func.distinct(sa.column(column)))

    @staticmethod
    def get_median_expr(platform: str, column: str) -> Any:
        """Get database-specific median expression."""

        platform_lower = platform.lower()

        if platform_lower == "snowflake":
            # Snowflake has native MEDIAN function
            return sa.func.median(sa.column(column))

        elif platform_lower == "bigquery":
            # BigQuery uses approx_quantiles and extracts middle value
            return sa.text(f"approx_quantiles({column}, 2)[OFFSET(1)]")

        elif platform_lower in ("athena", "trino", "databricks"):
            # Use APPROX_PERCENTILE(col, 0.5)
            return sa.func.approx_percentile(sa.column(column), 0.5)

        elif platform_lower == "redshift":
            # Redshift uses MEDIAN function
            return sa.func.median(sa.column(column))

        else:
            # Fallback: Use PERCENTILE_CONT if supported
            try:
                return sa.text(f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column})")
            except Exception:
                return None

    @staticmethod
    def get_quantiles(
        conn: Connection,
        platform: str,
        table: sa.Table,
        column: str,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
        """Get quantile values for a column (approximate where possible)."""

        platform_lower = platform.lower()

        if platform_lower == "bigquery":
            # BigQuery: approx_quantiles(col, 100) returns 101 values
            indices = [int(q * 100) for q in quantiles]
            selects = [
                sa.literal_column(
                    f"approx_quantiles({column}, 100)[OFFSET({idx})]"
                ).label(f"q_{int(q * 100)}")
                for q, idx in zip(quantiles, indices)
            ]
            query = sa.select(selects).select_from(table)
            result = conn.execute(query).fetchone()
            if result is None:
                return [None] * len(quantiles)
            return [float(v) if v is not None else None for v in result]

        elif platform_lower == "snowflake":
            # Snowflake: APPROX_PERCENTILE(col, quantile)
            results = []
            for q in quantiles:
                snowflake_expr = sa.func.approx_percentile(sa.column(column), q)
                query = sa.select([snowflake_expr]).select_from(table)
                results.append(conn.execute(query).scalar())
            return [float(v) if v is not None else None for v in results]

        elif platform_lower in ("athena", "trino"):
            # Athena/Trino: approx_percentile(col, ARRAY[0.05, 0.25, ...])
            array_str = f"ARRAY[{', '.join(str(q) for q in quantiles)}]"
            athena_expr: Any = sa.literal_column(
                f"approx_percentile({column}, {array_str})"
            )
            query = sa.select([athena_expr]).select_from(table)
            result = conn.execute(query).scalar()
            # Result is an array, convert to list
            if isinstance(result, list):
                return [float(v) if v is not None else None for v in result]
            return [None] * len(quantiles)

        elif platform_lower == "databricks":
            # Databricks: Similar to Athena/Trino
            array_str = f"array({', '.join(str(q) for q in quantiles)})"
            databricks_expr: Any = sa.literal_column(
                f"approx_percentile({column}, {array_str})"
            )
            query = sa.select([databricks_expr]).select_from(table)
            result = conn.execute(query).scalar()
            # Result is an array, convert to list
            if isinstance(result, list):
                return [float(v) if v is not None else None for v in result]
            return [None] * len(quantiles)

        else:
            # Fallback: Use exact percentile_cont if supported
            results = []
            for q in quantiles:
                try:
                    percentile_expr: Any = sa.text(
                        f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {column})"
                    )
                    query = sa.select([percentile_expr]).select_from(table)
                    result = conn.execute(query).scalar()
                    results.append(float(result) if result is not None else None)
                except Exception:
                    results.append(None)
            return results

    @staticmethod
    def get_sample_clause(
        platform: str, sample_size: Optional[int] = None
    ) -> Optional[str]:
        """Get SQL suffix for sampling (if supported)."""

        platform_lower = platform.lower()

        if platform_lower == "bigquery" and sample_size:
            return f"TABLESAMPLE SYSTEM ({sample_size} ROWS)"

        return None

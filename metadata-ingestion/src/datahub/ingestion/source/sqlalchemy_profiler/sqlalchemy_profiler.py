"""Custom SQLAlchemy-based profiler to replace Great Expectations."""

import collections
import concurrent.futures
import dataclasses
import json
import logging
import re
import traceback
from datetime import datetime
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy as sa
import sqlalchemy.sql.compiler
import sqlalchemy.types as sa_types
from dateutil import parser as date_parser
from sqlalchemy.engine import Connection, Engine

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner_runner import (
    FutureResult,
    QueryCombinerRunner,
)
from datahub.ingestion.source.sqlalchemy_profiler.type_mapping import (
    NORMALIZE_TYPE_PATTERN,
    ProfilerDataType,
    _get_column_types_to_ignore,
    get_column_profiler_type,
    resolve_profiler_type_with_fallback,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import (
    PartitionTypeClass,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    PartitionSpecClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlalchemy_query_combiner import (
    IS_SQLALCHEMY_1_4,
    SQLAlchemyQueryCombiner,
)


def _is_single_row_query_method(query: Any) -> bool:
    """
    Determine if a query method returns a single row.

    This is used by SQLAlchemyQueryCombiner to optimize query batching.
    For the custom profiler, we assume all our stat methods return single rows
    except for histogram and value frequencies which return multiple rows.

    Why this is still needed despite FutureResult making batching explicit:
    FutureResult marks methods as batchable at the API level, but the query combiner
    intercepts at the SQLAlchemy connection level and needs runtime checking for safety.

    NOTE: Code duplication with ge_data_profiler.py
    This function is duplicated from the GE profiler implementation. We maintain
    separate implementations because:
    1. The GE profiler has additional complexity (filename checks, GE-specific methods)
       that doesn't apply to our direct SQLAlchemy implementation
    2. The GE profiler will eventually be removed entirely, at which point this
       duplication will be resolved naturally

    FUTURE: Query tagging approach (not implemented yet)
    A cleaner approach would be to use SQLAlchemy 1.4's execution_options() to
    tag queries directly:
        query.execution_options(datahub_single_row=True)

    We're not implementing this yet because:
    1. SQLAlchemyQueryCombiner currently requires a callable (is_single_row_query_method)
       that inspects the call stack
    2. Migrating to query tagging would require modifying ~20 query generation sites
       in stats_calculator.py
    3. We'd need to update query_combiner.py to check execution_options first, then
       fall back to the callable for GE profiler compatibility
    4. Once GE profiler is removed, we can migrate both the query combiner and this
       profiler to use query tagging exclusively

    For now, traceback inspection is battle-tested (used by GE profiler in production)
    and the performance overhead (~1-5 microseconds) is negligible in the profiling context.
    """
    # Methods that return single rows (scalar results)
    SINGLE_ROW_QUERY_METHODS = {
        "get_row_count",
        "get_column_min",
        "get_column_max",
        "get_column_mean",
        "get_column_stdev",
        "get_column_unique_count",
        "get_column_non_null_count",
        "get_column_median",
    }

    # Methods that return multiple rows
    MULTI_ROW_QUERY_METHODS = {
        "get_column_histogram",
        "get_column_value_frequencies",
        "get_column_distinct_value_frequencies",
        "get_column_quantiles",  # May return multiple values but as a single row array
    }

    # Check the call stack to see which method is calling
    stack = traceback.extract_stack()
    for frame in reversed(stack):
        if frame.name in MULTI_ROW_QUERY_METHODS:
            return False
        if frame.name in SINGLE_ROW_QUERY_METHODS:
            return True

    # Default: assume single row for optimization
    return True


if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import ProfilerRequest

logger: logging.Logger = logging.getLogger(__name__)

BIGQUERY = "bigquery"
ATHENA = "athena"
TRINO = "trino"


def _format_datetime_value(value: Any) -> str:
    """
    Format datetime value as string, matching GE profiler behavior.

    GE uses ISO format with 'T' separator for datetime values.
    Examples:
    - datetime(2000, 1, 1, 10, 30, 0) -> "2000-01-01T10:30:00"
    - date(2000, 1, 1) -> "2000-01-01"
    - timestamp with timezone -> "2000-01-01T10:30:00+00:00"

    Uses Python's datetime parsing instead of regex for better format handling.

    NOTE: Much of the complexity here is arbitrary decisions to maintain exact
    compatibility with GE profiler output formats. Once GE profiler is removed,
    these formatting functions could be significantly simplified.
    """
    if value is None:
        return ""

    # Check if it's a datetime-like object (datetime, date, time)
    if hasattr(value, "isoformat"):
        # Use isoformat() which produces ISO 8601 format with 'T' separator
        return value.isoformat()

    # For string values, try to parse as datetime
    if isinstance(value, str):
        # Check if it's a date-only string (YYYY-MM-DD) - preserve as-is
        if len(value) == 10 and value.count("-") == 2:
            try:
                # Validate it's a valid date format
                datetime.strptime(value, "%Y-%m-%d")
                return value  # Return date-only string as-is
            except ValueError:
                pass  # Not a valid date, continue to parsing

        # Try parsing with dateutil (handles many formats automatically)
        try:
            parsed_dt = date_parser.parse(value)
            return parsed_dt.isoformat()
        except (ValueError, TypeError, AttributeError):
            # Fallback: try Python's fromisoformat for common ISO-like formats
            try:
                # Handle space separator by replacing with T
                iso_str = value.replace(" ", "T", 1)
                parsed = datetime.fromisoformat(iso_str)
                return parsed.isoformat()
            except (ValueError, AttributeError):
                # Final fallback: simple space-to-T replacement for common formats
                if " " in value and len(value) >= 10:
                    # Replace first space with T (handles "YYYY-MM-DD HH:MM:SS")
                    return value.replace(" ", "T", 1)
                # For date-only values (YYYY-MM-DD), return as-is
                return value

    # For other types, convert to string
    return str(value)


def _format_mean_value(value: Any) -> str:
    """
    Format mean value as string, matching GE profiler behavior.

    Mean values from AVG are always float/DECIMAL, so they should be formatted
    as floats. If it's a whole number, format as "100000.0" not "100000" to
    match GE's behavior.

    Examples:
    - Decimal('100000') -> "100000.0"
    - Decimal('100000.5') -> "100000.5"
    - 100000.0 -> "100000.0"
    - 100000 -> "100000.0"

    NOTE: Much of the complexity here is arbitrary decisions to maintain exact
    compatibility with GE profiler output formats. Once GE profiler is removed,
    these formatting functions could be significantly simplified.
    """
    if value is None:
        return ""

    # Convert to float to ensure proper formatting
    if isinstance(value, (Decimal, int, float)):
        float_val = float(value)
    else:
        # For other types, try to convert to float
        try:
            float_val = float(value)
        except (ValueError, TypeError):
            return str(value)

    # Format as float string, preserving precision
    # If it's a whole number, ensure it shows as "100000.0"
    if float_val.is_integer():
        return f"{float_val:.1f}"
    else:
        return str(float_val)


def _format_median_value(value: Any, platform: str, col_type: ProfilerDataType) -> str:
    """
    Format median value as string, matching GE profiler behavior.

    GE profiler uses str() directly on the median value, preserving whatever
    the database returns. This means:
    - If database returns float 1.0, format as "1.0"
    - If database returns int 1, format as "1"
    - If database returns float 39.0, format as "39.0"
    - Preserves database-native type formatting exactly as GE does

    Examples:
    - Redshift MEDIAN returns 1.0 -> "1.0" (preserves float format)
    - Redshift MEDIAN returns 1 -> "1" (preserves int format)
    - Redshift MEDIAN returns 39.0 -> "39.0" (preserves float format)
    - PostgreSQL MEDIAN returns 1 -> "1"

    NOTE: Much of the complexity here is arbitrary decisions to maintain exact
    compatibility with GE profiler output formats. Once GE profiler is removed,
    these formatting functions could be significantly simplified.
    """
    if value is None:
        return ""

    # GE uses str() directly, so we do the same to preserve database-native format
    # This matches GE's behavior: str(self.dataset.get_column_median(column))
    return str(value)


def _format_numeric_value(value: Any, col_type: ProfilerDataType) -> str:
    """
    Format numeric value as string, matching GE profiler behavior.

    GE profiler uses str() directly on the value. For INT columns, if BigQuery
    returns integer values as floats (e.g., 0.0, 200000.0), we format them as
    integers. For FLOAT columns, we preserve the float format (e.g., "0.0").

    Examples:
    - INT column with value 0.0 -> "0" (not "0.0")
    - INT column with value 200000.0 -> "200000" (not "200000.0")
    - FLOAT column with value 0.0 -> "0.0" (preserve float format)
    - FLOAT column with value 3.14 -> "3.14"
    - FLOAT column with Decimal(0) -> "0.0" (format as float)

    NOTE: Much of the complexity here is arbitrary decisions to maintain exact
    compatibility with GE profiler output formats. Once GE profiler is removed,
    these formatting functions could be significantly simplified.
    """
    if value is None:
        return ""

    # Handle Decimal types (common in BigQuery for NUMERIC columns)
    if isinstance(value, Decimal):
        # For FLOAT columns, format Decimal integers as "0.0" to match GE
        if col_type == ProfilerDataType.FLOAT:
            # Check if it's an integer value
            if value == value.to_integral_value():
                # Format as float: "0.0" instead of "0"
                return f"{float(value):.1f}"
            # For non-integer decimals, use string representation
            return str(value)
        elif col_type == ProfilerDataType.INT:
            # For INT columns, format as integer
            return str(int(value))

    # Convert to string first to see what we're working with
    str_value = str(value)

    if isinstance(value, (int, float)):
        # For INT columns, format integer-like floats as integers
        if col_type == ProfilerDataType.INT:
            # Check if it's a float that's actually an integer
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            # If it's already an integer, just convert to string
            return str(int(value))
        elif col_type == ProfilerDataType.FLOAT:
            # For FLOAT columns, preserve the float format (GE keeps "0.0" as "0.0")
            # Just use str() directly to preserve the database's native format
            return str_value

    # For other types, convert to string
    return str_value


@dataclasses.dataclass
class SQLAlchemyProfiler:
    """Custom SQLAlchemy-based profiler replacing Great Expectations."""

    report: SQLSourceReport
    config: ProfilingConfig
    times_taken: List[float]
    total_row_count: int

    base_engine: Engine
    platform: str  # passed from parent source config
    env: str

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: ProfilingConfig,
        platform: str,
        env: str = "PROD",
    ):
        self.report = report
        self.config = config
        self.times_taken = []
        self.total_row_count = 0

        self.env = env

        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DB-API object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly. As such, we need to make sure we've
        # got an engine here.
        self.base_engine = conn.engine

        if IS_SQLALCHEMY_1_4:
            # SQLAlchemy 1.4 added a statement "linter", which issues warnings about cartesian products in SELECT statements.
            # Changelog: https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4737.
            # Code: https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/sql/compiler.py#L549
            #
            # The query combiner does indeed produce queries with cartesian products, but they are
            # safe because each "FROM" clause only returns one row, so the cartesian product
            # is also always a single row. As such, we disable the linter here.

            # Modified from https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/engine/create.py#L612
            self.base_engine.dialect.compiler_linting &= (  # type: ignore[attr-defined]
                ~sqlalchemy.sql.compiler.COLLECT_CARTESIAN_PRODUCTS  # type: ignore[attr-defined]
            )

        self.platform = platform.lower()

    def _get_columns_to_profile(self, table: sa.Table, dataset_name: str) -> List[str]:
        """Get list of columns to profile based on config and patterns."""
        if not self.config.any_field_level_metrics_enabled():
            return []

        columns_to_profile: List[str] = []
        ignored_columns_by_pattern: List[str] = []
        ignored_columns_by_type: List[str] = []

        for column in table.columns:
            col_name = column.name

            # Check allow/deny patterns (format: <table>.<column>)
            if not self.config._allow_deny_patterns.allowed(
                f"{dataset_name}.{col_name}"
            ):
                ignored_columns_by_pattern.append(col_name)
                continue

            # Check nested fields
            if not self.config.profile_nested_fields and "." in col_name:
                ignored_columns_by_pattern.append(col_name)
                continue

            # Check column type
            if self._should_ignore_column(column.type, column.name):
                ignored_columns_by_type.append(col_name)
                continue

            columns_to_profile.append(col_name)

        # Report ignored columns
        if ignored_columns_by_pattern:
            self.report.report_dropped(
                f"The profile of columns by pattern {dataset_name}({', '.join(sorted(ignored_columns_by_pattern))})"
            )
        if ignored_columns_by_type:
            self.report.report_dropped(
                f"The profile of columns by type {dataset_name}({', '.join(sorted(ignored_columns_by_type))})"
            )

        # Apply max_number_of_fields_to_profile limit
        if self.config.max_number_of_fields_to_profile is not None:
            if len(columns_to_profile) > self.config.max_number_of_fields_to_profile:
                columns_being_dropped = columns_to_profile[
                    self.config.max_number_of_fields_to_profile :
                ]
                columns_to_profile = columns_to_profile[
                    : self.config.max_number_of_fields_to_profile
                ]
                if self.config.report_dropped_profiles:
                    self.report.report_dropped(
                        f"The max_number_of_fields_to_profile={self.config.max_number_of_fields_to_profile} "
                        f"reached. Profile of columns {dataset_name}({', '.join(sorted(columns_being_dropped))})"
                    )

        return columns_to_profile

    def _should_ignore_column(
        self, sqlalchemy_type: sa_types.TypeEngine, column_name: str
    ) -> bool:
        """Check if column should be ignored based on type."""
        if str(sqlalchemy_type) == "NULL":
            return True

        # Get type name - prefer class name over string representation
        # Class name is more reliable (e.g., "GEOGRAPHY" vs "geography")
        type_name = type(sqlalchemy_type).__name__

        # Normalize the type string from str() representation
        sql_type = str(sqlalchemy_type)
        match = re.match(NORMALIZE_TYPE_PATTERN, sql_type)
        if match:
            sql_type = match.group(1)

        # Check both type class name and string representation (case-insensitive)
        # Some dialects return lowercase (e.g., BigQuery GEOGRAPHY returns "geography")
        types_to_ignore = _get_column_types_to_ignore(self.platform)
        return (
            type_name.upper() in types_to_ignore or sql_type.upper() in types_to_ignore
        )

    def _maybe_add_distinct_value_frequencies(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        cardinality: Optional["Cardinality"],
        allowed_cardinalities: set,
    ) -> None:
        """
        Add distinct value frequencies if configured and cardinality is in allowed set.

        This helper method reduces code duplication for adding distinct value frequencies
        across different column types (numeric, string, datetime, other).

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            cardinality: Column cardinality (ONE, TWO, VERY_FEW, FEW, etc.)
            allowed_cardinalities: Set of cardinalities for which to add frequencies
        """
        if (
            self.config.include_field_distinct_value_frequencies
            and cardinality
            and cardinality in allowed_cardinalities
        ):
            frequencies = runner.get_column_distinct_value_frequencies(
                sql_table, col_name
            )
            column_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=str(value), frequency=freq)
                for value, freq in frequencies
            ]

    def _add_sample_values(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        non_null_count: int,
        row_count: Optional[int],
    ) -> None:
        """
        Add sample values to column profile.

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            non_null_count: Number of non-null values
            row_count: Total row count in table
        """
        sample_values = runner.get_column_sample_values(
            sql_table,
            col_name,
            limit=self.config.field_sample_values_limit,
        )
        # Convert to strings (GE does: str(v) for v in partial_unexpected_list)
        sample_list = [str(value) for value in sample_values if value is not None]
        # Only set sampleValues if there are actual values (match GE behavior)
        if sample_list:
            column_profile.sampleValues = sample_list
        # For null-only columns (rows exist but all null), set empty list to match GE behavior
        # But don't set it for empty tables (row_count == 0) - GE doesn't set it in that case
        elif non_null_count == 0 and row_count is not None and row_count > 0:
            column_profile.sampleValues = []

    def _process_numeric_column_stats(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        col_type: "ProfilerDataType",
        cardinality: Optional["Cardinality"],
        numeric_stats_futures: Dict[str, Dict[str, "FutureResult"]],
        pretty_name: str,
        platform: str,
    ) -> None:
        """
        Process numeric column statistics (int/float).

        Extracts batched numeric stats results and executes non-batchable complex queries.

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            col_type: Column data type (INT or FLOAT)
            cardinality: Column cardinality
            numeric_stats_futures: Dictionary of scheduled futures for numeric stats
            pretty_name: Pretty name for error reporting
            platform: Platform name for formatting
        """
        # Extract batched numeric stats results
        if col_name in numeric_stats_futures:
            futures = numeric_stats_futures[col_name]

            # Match GE behavior: catch exceptions and log debug messages
            # GE always sets these fields, even when None (for null-only columns)
            # We need to set them even when non_null_count == 0 to match GE behavior

            # Process all numeric stats with unified error handling
            if "min" in futures:
                try:
                    min_val = futures["min"].result()
                    column_profile.min = (
                        _format_numeric_value(min_val, col_type)
                        if min_val is not None
                        else None
                    )
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Min",
                        message="The min for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

            if "max" in futures:
                try:
                    max_val = futures["max"].result()
                    column_profile.max = (
                        _format_numeric_value(max_val, col_type)
                        if max_val is not None
                        else None
                    )
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Max",
                        message="The max for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

            if "mean" in futures:
                try:
                    mean_val = futures["mean"].result()
                    column_profile.mean = (
                        _format_mean_value(mean_val) if mean_val is not None else None
                    )
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Mean",
                        message="The mean for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

            if "stdev" in futures:
                try:
                    stdev_val = futures["stdev"].result()
                    column_profile.stdev = (
                        str(stdev_val) if stdev_val is not None else None
                    )
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Standard Deviation",
                        message="The stdev for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

            if "median" in futures:
                try:
                    median_val = futures["median"].result()
                    column_profile.median = (
                        _format_median_value(median_val, platform, col_type)
                        if median_val is not None
                        else None
                    )
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Median",
                        message="The median for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

        # Non-batchable complex queries for numeric columns
        if self.config.include_field_quantiles:
            quantiles = runner.get_column_quantiles(
                sql_table,
                col_name,
                [0.05, 0.25, 0.5, 0.75, 0.95],
            )
            logger.debug(
                f"Quantiles for {col_name}: type={type(quantiles)}, "
                f"len={len(quantiles) if quantiles else 0}, value={quantiles}"
            )
            column_profile.quantiles = [
                QuantileClass(quantile=str(q), value=str(v))
                for q, v in zip(
                    [0.05, 0.25, 0.5, 0.75, 0.95],
                    quantiles,
                    strict=False,
                )
                if v is not None
            ]

        # Add histogram for numeric columns with high cardinality
        if (
            self.config.include_field_histogram
            and cardinality
            and cardinality
            in {Cardinality.FEW, Cardinality.MANY, Cardinality.VERY_MANY}
        ):
            histogram = runner.get_column_histogram(sql_table, col_name)
            if histogram:
                # Convert to HistogramClass format
                # boundaries: bucket boundaries (k+1 values for k buckets)
                # heights: counts per bucket (k values)
                boundaries = [str(start) for start, _, _ in histogram]
                # Add the last bucket end as final boundary
                if histogram:
                    boundaries.append(str(histogram[-1][1]))
                heights = [float(count) for _, _, count in histogram]
                column_profile.histogram = HistogramClass(
                    boundaries=boundaries, heights=heights
                )

        # Add distinct value frequencies for low cardinality numeric columns
        self._maybe_add_distinct_value_frequencies(
            runner,
            sql_table,
            col_name,
            column_profile,
            cardinality,
            {
                Cardinality.ONE,
                Cardinality.TWO,
                Cardinality.VERY_FEW,
            },
        )

    def _process_string_column_stats(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        cardinality: Optional["Cardinality"],
    ) -> None:
        """
        Process string column statistics.

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            cardinality: Column cardinality
        """
        # For string columns, add distinct value frequencies for low cardinality
        self._maybe_add_distinct_value_frequencies(
            runner,
            sql_table,
            col_name,
            column_profile,
            cardinality,
            {
                Cardinality.ONE,
                Cardinality.TWO,
                Cardinality.VERY_FEW,
                Cardinality.FEW,
            },
        )

    def _process_datetime_column_stats(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        cardinality: Optional["Cardinality"],
        numeric_stats_futures: Dict[str, Dict[str, "FutureResult"]],
        pretty_name: str,
    ) -> None:
        """
        Process datetime column statistics.

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            cardinality: Column cardinality
            numeric_stats_futures: Dictionary of scheduled futures for datetime min/max
            pretty_name: Pretty name for error reporting
        """
        # Extract batched min/max results for datetime columns
        if col_name in numeric_stats_futures:
            futures = numeric_stats_futures[col_name]

            # Match GE behavior: catch exceptions and log debug messages
            if "min" in futures:
                try:
                    min_val = futures["min"].result()
                    if min_val is not None:
                        # Format datetime values to match GE's ISO format
                        column_profile.min = _format_datetime_value(min_val)
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Min",
                        message="The min for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

            if "max" in futures:
                try:
                    max_val = futures["max"].result()
                    if max_val is not None:
                        # Format datetime values to match GE's ISO format
                        column_profile.max = _format_datetime_value(max_val)
                except Exception as e:
                    self.report.warning(
                        title="Profiling: Unable to Calculate Max",
                        message="The max for the column will not be accessible",
                        context=f"{pretty_name}.{col_name}",
                        exc=e,
                    )

        # Add distinct value frequencies for low cardinality datetime columns
        self._maybe_add_distinct_value_frequencies(
            runner,
            sql_table,
            col_name,
            column_profile,
            cardinality,
            {
                Cardinality.ONE,
                Cardinality.TWO,
                Cardinality.VERY_FEW,
                Cardinality.FEW,
            },
        )

    def _process_other_column_stats(
        self,
        runner: "QueryCombinerRunner",
        sql_table: "sa.Table",
        col_name: str,
        column_profile: DatasetFieldProfileClass,
        cardinality: Optional["Cardinality"],
    ) -> None:
        """
        Process statistics for other column types (boolean, binary, etc.).

        Args:
            runner: Query combiner runner for executing queries
            sql_table: SQLAlchemy table object
            col_name: Column name
            column_profile: Profile object to update
            cardinality: Column cardinality
        """
        # For other types, add distinct value frequencies for low cardinality
        self._maybe_add_distinct_value_frequencies(
            runner,
            sql_table,
            col_name,
            column_profile,
            cardinality,
            {
                Cardinality.ONE,
                Cardinality.TWO,
                Cardinality.VERY_FEW,
                Cardinality.FEW,
            },
        )

    def generate_profiles(
        self,
        requests: List["ProfilerRequest"],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple["ProfilerRequest", Optional[DatasetProfileClass]]]:
        """
        Generate dataset profiles for a list of requests.

        This is the main entry point, matching DatahubGEProfiler.generate_profiles() signature.
        """
        max_workers = min(max_workers, len(requests)) if requests else max_workers
        if max_workers <= 0:
            max_workers = 1
        logger.info(
            f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
        )

        with (
            PerfTimer() as timer,
            concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as async_executor,
            SQLAlchemyQueryCombiner(
                enabled=self.config.query_combiner_enabled,
                catch_exceptions=self.config.catch_exceptions,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            # Submit the profiling requests to the thread pool executor.
            async_profiles = collections.deque(
                async_executor.submit(
                    self._generate_profile_from_request,
                    query_combiner,
                    request,
                    platform=platform,
                    profiler_args=profiler_args,
                )
                for request in requests
            )

            # Avoid using as_completed so that the results are yielded in the
            # same order as the requests.
            while len(async_profiles) > 0:
                async_profile = async_profiles.popleft()
                yield async_profile.result()

        total_time_taken = timer.elapsed_seconds()
        logger.info(
            f"Profiling {len(requests)} table(s) finished in {total_time_taken:.3f} seconds"
        )

        time_percentiles: Dict[str, float] = {}

        if len(self.times_taken) > 0:
            percentiles = [50, 75, 95, 99]
            percentile_values = stats.calculate_percentiles(
                self.times_taken, percentiles
            )

            time_percentiles = {
                f"table_time_taken_p{percentile}": stats.discretize(
                    percentile_values[percentile]
                )
                for percentile in percentiles
            }

        telemetry.telemetry_instance.ping(
            "sql_profiling_summary",
            {
                "total_time_taken": stats.discretize(total_time_taken),
                "count": stats.discretize(len(self.times_taken)),
                "total_row_count": stats.discretize(self.total_row_count),
                "platform": self.platform,
                **time_percentiles,
            },
        )

        self.report.report_from_query_combiner(query_combiner.report)

    def _generate_profile_from_request(
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        request: "ProfilerRequest",
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Tuple["ProfilerRequest", Optional[DatasetProfileClass]]:
        return request, self._generate_single_profile(
            query_combiner=query_combiner,
            pretty_name=request.pretty_name,
            platform=platform,
            profiler_args=profiler_args,
            **request.batch_kwargs,
        )

    def _generate_single_profile(  # noqa: C901
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        pretty_name: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        partition: Optional[str] = None,
        custom_sql: Optional[str] = None,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
        **kwargs: Any,
    ) -> Optional[DatasetProfileClass]:
        """Generate a single dataset profile."""
        logger.debug(
            f"Received single profile request for {pretty_name} for {schema}, {table}, {custom_sql}"
        )
        platform = platform or self.platform

        # Create profiling context to track state
        context = ProfilingContext(
            schema=schema,
            table=table,
            custom_sql=custom_sql,
            pretty_name=pretty_name,
            partition=partition,
        )

        # Get platform-specific adapter
        adapter = get_adapter(platform, self.config, self.report, self.base_engine)

        with PerfTimer() as timer:
            try:
                logger.info(f"Profiling {pretty_name}")
                with self.base_engine.connect() as conn:
                    # Setup profiling using platform adapter
                    # This handles temp tables, sampling, and creates sql_table
                    try:
                        context = adapter.setup_profiling(context, conn)
                    except Exception as e:
                        self.report.warning(
                            title="Profiling setup failed",
                            message="Failed to setup profiling using platform adapter",
                            context=f"{pretty_name}: {type(e).__name__}: {str(e)}",
                        )
                        if not self.config.catch_exceptions:
                            raise
                        return None

                    # Validate that we have a sql_table to profile
                    if context.sql_table is None:
                        self.report.warning(
                            title="Profiling setup incomplete",
                            message="No table available for profiling after setup",
                            context=pretty_name,
                        )
                        return None

                    sql_table = context.sql_table

                    # Initialize query combiner runner with adapter
                    runner = QueryCombinerRunner(
                        conn=conn,
                        platform=platform,
                        adapter=adapter,
                        query_combiner=query_combiner,
                    )

                    # Create profile
                    profile = DatasetProfileClass(timestampMillis=get_sys_time())
                    profile.columnCount = len(sql_table.columns)

                    # Handle partition spec
                    if partition:
                        profile.partitionSpec = PartitionSpecClass(partition=partition)
                    elif self.config.limit:
                        profile.partitionSpec = PartitionSpecClass(
                            type=PartitionTypeClass.QUERY,
                            partition=json.dumps(
                                dict(limit=self.config.limit, offset=self.config.offset)
                            ),
                        )
                    elif custom_sql:
                        profile.partitionSpec = PartitionSpecClass(
                            type=PartitionTypeClass.QUERY, partition="SAMPLE"
                        )

                    # ================================================================
                    # 3-STAGE QUERY BATCHING PATTERN
                    # ================================================================
                    # To maximize query batching efficiency, we use 3 strategic flush points:
                    #
                    # STAGE 1: Row Count
                    #   - Schedule: 1 row count query
                    #   - Flush: Execute row count
                    #   - Use: Need row count before deciding on sampling
                    #
                    # STAGE 2: Column Cardinality
                    #   - Schedule: non-null + unique count for ALL columns
                    #   - Flush: Batch all cardinality queries into ONE SQL statement
                    #   - Use: Need cardinality to decide which columns get numeric stats
                    #
                    # STAGE 3: Numeric Stats + Complex Queries
                    #   - Schedule: min/max/mean/stdev/median for numeric columns
                    #   - Flush: Batch all numeric queries into ONE SQL statement
                    #   - Execute: Non-batchable queries (histograms, frequencies) run separately
                    #
                    # Performance: Reduces 50-100+ queries down to 3-5 queries per table!
                    #
                    # Pattern (using FutureResult[T]):
                    #   futures = {col: runner.get_stat(table, col) for col in cols}  # Schedule
                    #   query_combiner.flush()                                          # Batch execute
                    #   results = {col: future.result() for col, future in futures}    # Extract
                    # ================================================================

                    # ----------------------------------------------------------------
                    # STAGE 1: ROW COUNT
                    # ----------------------------------------------------------------
                    use_estimation = (
                        self.config.profile_table_row_count_estimate_only
                        and platform in ("postgresql", "mysql")
                    )
                    logger.debug(
                        f"Getting row count for {pretty_name}: "
                        f"use_estimation={use_estimation}, platform={platform}"
                    )

                    # Schedule row count query (returns FutureResult)
                    row_count_future = runner.get_row_count(
                        sql_table, use_estimation=use_estimation
                    )

                    # Flush Stage 1: Execute row count query
                    logger.debug(
                        f"profiling {pretty_name}: flushing stage 1 (row count)"
                    )
                    query_combiner.flush()

                    # Extract row count result
                    profile.rowCount = row_count_future.result()
                    row_count = profile.rowCount
                    logger.debug(
                        f"Row count result for {pretty_name}: {row_count}, type: {type(row_count)}"
                    )
                    self.total_row_count += row_count if row_count is not None else 0

                    # Update partition spec if sampling was applied by adapter
                    if context.is_sampled:
                        if (
                            profile.partitionSpec
                            and profile.partitionSpec.type
                            == PartitionTypeClass.FULL_TABLE
                        ):
                            profile.partitionSpec = PartitionSpecClass(
                                type=PartitionTypeClass.QUERY, partition="SAMPLE"
                            )
                        elif (
                            profile.partitionSpec
                            and profile.partitionSpec.type
                            == PartitionTypeClass.PARTITION
                        ):
                            profile.partitionSpec.partition += " SAMPLE"

                        if profile.partitionSpec and row_count is not None:
                            profile.partitionSpec.partition += (
                                f" (sample rows {row_count})"
                            )

                    # Get columns to profile
                    columns_to_profile = self._get_columns_to_profile(
                        sql_table, pretty_name
                    )

                    # Check for tags to ignore sampling
                    from datahub.ingestion.source.sqlalchemy_profiler.utils import (
                        _get_columns_to_ignore_sampling,
                    )

                    (
                        ignore_table_sampling,
                        columns_list_to_ignore_sampling,
                    ) = _get_columns_to_ignore_sampling(
                        pretty_name,
                        self.config.tags_to_ignore_sampling,
                        platform,
                        self.env,
                    )

                    # Profile columns
                    # Match GE profiler behavior: if there are columns to profile,
                    # create fieldProfiles for ALL columns, but only calculate stats
                    # for columns in columns_to_profile
                    # When profile_table_level_only is True, columns_to_profile is empty,
                    # so no fieldProfiles are created (matching GE behavior)
                    all_columns = [col.name for col in sql_table.columns]
                    columns_to_profile_set = set(columns_to_profile)

                    field_profiles = []
                    # Only create fieldProfiles if there are columns to profile (like GE does)
                    if columns_to_profile_set:
                        # First pass: create fieldProfiles for all columns (like GE does)
                        for col_name in all_columns:
                            field_profile = DatasetFieldProfileClass(fieldPath=col_name)
                            field_profiles.append(field_profile)

                    # ----------------------------------------------------------------
                    # STAGE 2: COLUMN CARDINALITY (Schedule all queries)
                    # ----------------------------------------------------------------
                    # Schedule non-null + unique count queries for ALL columns at once
                    # This allows query combiner to batch them into ONE SQL statement
                    cardinality_futures = {}
                    for column in sql_table.columns:
                        col_name = column.name

                        if col_name not in columns_to_profile_set:
                            continue

                        # Schedule non-null count (returns FutureResult)
                        cardinality_futures[col_name] = {
                            "non_null": runner.get_column_non_null_count(
                                sql_table, col_name
                            )
                        }

                        # Schedule unique count if needed (returns FutureResult)
                        if self.config.include_field_distinct_count:
                            cardinality_futures[col_name]["unique"] = (
                                runner.get_column_unique_count(sql_table, col_name)
                            )

                    # Flush Stage 2: Execute ALL cardinality queries in ONE batch
                    logger.debug(
                        f"profiling {pretty_name}: flushing stage 2 "
                        f"({len(cardinality_futures)} columns - cardinality)"
                    )
                    query_combiner.flush()

                    # ----------------------------------------------------------------
                    # STAGE 2: COLUMN CARDINALITY (Extract results and prepare for Stage 3)
                    # ----------------------------------------------------------------
                    # Extract cardinality results and collect column metadata for Stage 3
                    columns_with_types = {}  # col_name -> (column_profile, col_type, cardinality, non_null_count)

                    for column in sql_table.columns:
                        col_name = column.name

                        if col_name not in columns_to_profile_set:
                            continue

                        # Find the corresponding column_profile we created
                        column_profile: Optional[DatasetFieldProfileClass] = next(
                            (p for p in field_profiles if p.fieldPath == col_name), None
                        )
                        if column_profile is None:
                            continue

                        # Get column type
                        col_type = get_column_profiler_type(column.type, platform)
                        if col_type == ProfilerDataType.UNKNOWN:
                            col_type = resolve_profiler_type_with_fallback(
                                column.type, platform, str(column.type)
                            )

                        # Extract non-null count from FutureResult
                        non_null_count = cardinality_futures[col_name][
                            "non_null"
                        ].result()

                        # Calculate null_count: use row_count variable (set from profile.rowCount)
                        # This matches GE profiler behavior which uses row_count directly
                        effective_row_count = row_count
                        if effective_row_count is None:
                            try:
                                effective_row_count = profile.rowCount
                            except (AttributeError, TypeError):
                                effective_row_count = None
                        null_count = (
                            max(0, effective_row_count - non_null_count)
                            if effective_row_count is not None
                            and non_null_count is not None
                            else None
                        )
                        if self.config.include_field_null_count:
                            column_profile.nullCount = null_count
                            if (
                                row_count is not None
                                and row_count > 0
                                and null_count is not None
                            ):
                                column_profile.nullProportion = min(
                                    1, null_count / row_count
                                )

                        # Extract unique count from FutureResult if we scheduled it
                        unique_count = None
                        cardinality = None
                        if self.config.include_field_distinct_count:
                            unique_count = cardinality_futures[col_name][
                                "unique"
                            ].result()
                            column_profile.uniqueCount = unique_count
                            if (
                                non_null_count is not None
                                and non_null_count > 0
                                and unique_count is not None
                            ):
                                unique_proportion = min(
                                    1, unique_count / non_null_count
                                )
                                column_profile.uniqueProportion = unique_proportion
                            cardinality = convert_to_cardinality(
                                unique_count,
                                float(unique_count) / non_null_count
                                if non_null_count is not None
                                and non_null_count > 0
                                and unique_count is not None
                                else None,
                            )

                        # Store metadata for Stage 3
                        columns_with_types[col_name] = (
                            column_profile,
                            col_type,
                            cardinality,
                            non_null_count,
                        )

                    # ----------------------------------------------------------------
                    # STAGE 3: NUMERIC STATS (Schedule all queries)
                    # ----------------------------------------------------------------
                    # Schedule min/max/mean/stdev/median queries for numeric and datetime columns
                    # This allows query combiner to batch them into ONE SQL statement
                    numeric_stats_futures: Dict[str, Dict[str, FutureResult[Any]]] = {}
                    for col_name, (
                        _column_profile,
                        col_type,
                        _cardinality,
                        _non_null_count,
                    ) in columns_with_types.items():
                        # Only calculate stats if not ignoring sampling for this column
                        if (
                            ignore_table_sampling
                            or col_name in columns_list_to_ignore_sampling
                        ):
                            continue

                        # Schedule numeric stats for numeric columns
                        if col_type in (ProfilerDataType.INT, ProfilerDataType.FLOAT):
                            numeric_stats_futures[col_name] = {}
                            if self.config.include_field_min_value:
                                numeric_stats_futures[col_name]["min"] = (
                                    runner.get_column_min(sql_table, col_name)
                                )
                            if self.config.include_field_max_value:
                                numeric_stats_futures[col_name]["max"] = (
                                    runner.get_column_max(sql_table, col_name)
                                )
                            if self.config.include_field_mean_value:
                                numeric_stats_futures[col_name]["mean"] = (
                                    runner.get_column_mean(sql_table, col_name)
                                )
                            if self.config.include_field_stddev_value:
                                numeric_stats_futures[col_name]["stdev"] = (
                                    runner.get_column_stdev(sql_table, col_name)
                                )
                            if self.config.include_field_median_value:
                                numeric_stats_futures[col_name]["median"] = (
                                    runner.get_column_median(sql_table, col_name)
                                )

                        # Schedule min/max for datetime columns
                        elif col_type == ProfilerDataType.DATETIME:
                            numeric_stats_futures[col_name] = {}
                            if self.config.include_field_min_value:
                                numeric_stats_futures[col_name]["min"] = (
                                    runner.get_column_min(sql_table, col_name)
                                )
                            if self.config.include_field_max_value:
                                numeric_stats_futures[col_name]["max"] = (
                                    runner.get_column_max(sql_table, col_name)
                                )

                    # Flush Stage 3: Execute ALL numeric stats queries in ONE batch
                    if numeric_stats_futures:
                        logger.debug(
                            f"profiling {pretty_name}: flushing stage 3 "
                            f"({len(numeric_stats_futures)} columns - numeric stats)"
                        )
                        query_combiner.flush()

                    # ----------------------------------------------------------------
                    # STAGE 3: NUMERIC STATS (Extract results) + COMPLEX QUERIES
                    # ----------------------------------------------------------------
                    # Extract numeric stats results and execute non-batchable complex queries
                    for col_name, (
                        column_profile,
                        col_type,
                        cardinality,
                        non_null_count,
                    ) in columns_with_types.items():
                        # Only calculate stats if not ignoring sampling for this column
                        if (
                            ignore_table_sampling
                            or col_name in columns_list_to_ignore_sampling
                        ):
                            continue

                        # Add sample values for all types (non-batchable)
                        if self.config.include_field_sample_values:
                            self._add_sample_values(
                                runner,
                                sql_table,
                                col_name,
                                column_profile,
                                non_null_count,
                                row_count,
                            )

                        # Process column stats by type
                        if col_type in (ProfilerDataType.INT, ProfilerDataType.FLOAT):
                            self._process_numeric_column_stats(
                                runner,
                                sql_table,
                                col_name,
                                column_profile,
                                col_type,
                                cardinality,
                                numeric_stats_futures,
                                pretty_name,
                                platform,
                            )
                        elif col_type == ProfilerDataType.STRING:
                            self._process_string_column_stats(
                                runner,
                                sql_table,
                                col_name,
                                column_profile,
                                cardinality,
                            )
                        elif col_type == ProfilerDataType.DATETIME:
                            self._process_datetime_column_stats(
                                runner,
                                sql_table,
                                col_name,
                                column_profile,
                                cardinality,
                                numeric_stats_futures,
                                pretty_name,
                            )
                        else:
                            self._process_other_column_stats(
                                runner,
                                sql_table,
                                col_name,
                                column_profile,
                                cardinality,
                            )

                    profile.fieldProfiles = field_profiles

                    time_taken = timer.elapsed_seconds()
                    logger.info(
                        f"Finished profiling {pretty_name}; took {time_taken:.3f} seconds"
                    )
                    self.times_taken.append(time_taken)
                    return profile

            except (
                sa.exc.SQLAlchemyError,
                ConnectionError,
                PermissionError,
            ) as e:
                # Handle expected database and connection errors
                if not self.config.catch_exceptions:
                    raise

                error_message = str(e).lower()
                if "permission denied" in error_message or isinstance(
                    e, PermissionError
                ):
                    self.report.warning(
                        title="Unauthorized to extract data profile statistics",
                        message="We were denied access while attempting to generate profiling statistics for some assets. Please ensure the provided user has permission to query these tables and views.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                else:
                    self.report.warning(
                        title="Failed to extract statistics for some assets",
                        message="Caught exception while attempting to extract profiling statistics for some assets.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                return None
            except Exception as e:
                # Unexpected errors - only catch if catch_exceptions is True
                if not self.config.catch_exceptions:
                    raise

                self.report.warning(
                    title="Unexpected error during profiling",
                    message="Caught unexpected exception while attempting to extract profiling statistics for some assets.",
                    context=f"Asset: {pretty_name}",
                    exc=e,
                )
                return None
            finally:
                # Cleanup temp resources using adapter
                adapter.cleanup(context)

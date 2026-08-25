import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
    DATE_TIME_TYPES,
    ISO_DATE_PATTERN,
    ISO_DATETIME_FLEX_PATTERN,
    PARTITION_GRANULARITY_HOUR,
    PARTITION_GRANULARITY_MONTH,
    PARTITION_GRANULARITY_YEAR,
    PARTITION_ID_YYYY_PATTERN,
    PARTITION_ID_YYYYMM_PATTERN,
    PARTITION_ID_YYYYMMDD_LENGTH,
    PARTITION_ID_YYYYMMDD_PATTERN,
    PARTITION_ID_YYYYMMDDHH_LENGTH,
    PARTITION_ID_YYYYMMDDHH_PATTERN,
    VALID_COLUMN_NAME_PATTERN,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    PartitionValue,
)

logger = logging.getLogger(__name__)

# Types whose column values span an entire year under yearly partitioning, so a
# bare-YYYY partition ID must become a range rather than a single-day equality.
# TIME is excluded: it cannot be yearly-partitioned.
YEARLY_RANGE_TYPES = {"DATE", "DATETIME", "TIMESTAMP"}


class FilterBuilder:
    @staticmethod
    def is_not_null(col_name: str) -> str:
        return f"`{col_name}` IS NOT NULL"

    @staticmethod
    def create_safe_filter(
        col_name: str, val: PartitionValue, col_type: Optional[str] = None
    ) -> str:
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        str_val = str(val)

        if any(pattern in str_val for pattern in [";", "--", "/*", "\\"]):
            raise ValueError(f"Invalid value for filter: {val}")

        if col_type and col_type.upper() in BIGQUERY_NUMERIC_TYPES:
            try:
                if "." in str_val:
                    float(str_val)
                else:
                    int(str_val)
                return f"`{col_name}` = {str_val}"
            except ValueError:
                # A YYYY-MM-DD date string for an integer column likely stores dates as
                # YYYYMMDD integers (a common BigQuery partition pattern). Convert to avoid
                # a type mismatch error (INT64 = STRING is invalid in BigQuery).
                if ISO_DATE_PATTERN.match(str_val):
                    int_val = str_val.replace("-", "")
                    logger.debug(
                        f"Converting ISO date '{str_val}' to YYYYMMDD integer {int_val} "
                        f"for {col_type} column {col_name}"
                    )
                    return f"`{col_name}` = {int_val}"
                # Emitting `col` = 'value' here would be an INT64 = STRING predicate that
                # BigQuery rejects at query time. Raise so the caller's skip-and-report
                # path handles it instead of building an invalid filter.
                raise ValueError(
                    f"Non-numeric value '{str_val}' for numeric column "
                    f"{col_name} ({col_type})"
                ) from None

        if col_type and col_type.upper() in DATE_TIME_TYPES:
            # A bare year (YYYY) for a date/datetime/timestamp column is a yearly
            # partition whose rows span the whole year, so an equality to Jan 1 would
            # exclude every row from the rest of the year. Emit a half-open full-year
            # range [YYYY-01-01, (YYYY+1)-01-01) covering the entire partition.
            if (
                col_type.upper() in YEARLY_RANGE_TYPES
                and PARTITION_ID_YYYY_PATTERN.match(str_val)
            ):
                return FilterBuilder._full_year_range(col_name, str_val, col_type)

            formatted_val = FilterBuilder._format_date_value(str_val, col_type.upper())
            if formatted_val:
                if col_type.upper() == "TIMESTAMP" and " " not in formatted_val:
                    return f"`{col_name}` = TIMESTAMP('{formatted_val}')"
                return f"`{col_name}` = '{formatted_val}'"
            # An unrecognized date shape for a DATE/DATETIME/TIMESTAMP column would
            # otherwise fall through to the generic quoted-string branch and emit an
            # uncastable predicate (e.g. DATE = 'garbage') that BigQuery rejects at
            # query time. Raise so the caller's skip-and-report path surfaces the drop,
            # mirroring the numeric branch above.
            raise ValueError(
                f"Could not format date value '{str_val}' for {col_type} column {col_name}"
            )

        if "'" in str_val:
            escaped_val = str_val.replace("'", "''")
            return f"`{col_name}` = '{escaped_val}'"
        else:
            return f"`{col_name}` = '{str_val}'"

    @staticmethod
    def create_lower_bound_filter(
        col_name: str, val: PartitionValue, col_type: Optional[str] = None
    ) -> str:
        # An integer RANGE partition ID is the bucket's inclusive lower bound, not an
        # exact value: a row belongs to the bucket when col is in [start, start+interval).
        # Equality (`col = start`) would only match rows exactly on the floor and miss the
        # rest of the bucket, so the max bucket is scanned with `col >= start`. Reuse
        # create_safe_filter for validation/normalization, then relax `=` to `>=`.
        equality = FilterBuilder.create_safe_filter(col_name, val, col_type)
        marker = f"`{col_name}` = "
        if equality.startswith(marker):
            return f"`{col_name}` >= {equality[len(marker) :]}"
        # A non-equality result (e.g. a date range) can't be reinterpreted as a lower
        # bound; RANGE partitions are integer-only, so this is not expected in practice.
        return equality

    @staticmethod
    def create_partition_datetime_filter(
        col_name: str,
        partition_datetime: datetime,
        col_type: str,
        granularity: Optional[str],
    ) -> str:
        # Build a half-open range covering the single time-unit partition that contains
        # partition_datetime, honoring profiling.partition_datetime. The range (rather
        # than an equality) is required because a DATETIME/TIMESTAMP partition column
        # holds every instant within the unit, so `col = floor(dt)` would match only the
        # unit boundary.
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        ctype = col_type.upper()
        lower, upper = FilterBuilder._partition_bounds(partition_datetime, granularity)

        lower_literal = FilterBuilder._format_bound_literal(lower, ctype)
        if upper is None:
            # A year-9999 partition has no representable exclusive upper bound
            # (10000-01-01 exceeds BigQuery's max year), so scan lower-bound-only.
            return f"`{col_name}` >= {lower_literal}"
        upper_literal = FilterBuilder._format_bound_literal(upper, ctype)
        return f"`{col_name}` >= {lower_literal} AND `{col_name}` < {upper_literal}"

    @staticmethod
    def _partition_bounds(
        moment: datetime, granularity: Optional[str]
    ) -> Tuple[datetime, Optional[datetime]]:
        gran = (granularity or "").upper()
        if gran == PARTITION_GRANULARITY_HOUR:
            lower = moment.replace(minute=0, second=0, microsecond=0)
            return lower, lower + timedelta(hours=1)
        if gran == PARTITION_GRANULARITY_MONTH:
            lower = moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if lower.month == 12:
                return lower, lower.replace(year=lower.year + 1, month=1)
            return lower, lower.replace(month=lower.month + 1)
        if gran == PARTITION_GRANULARITY_YEAR:
            lower = moment.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
            if lower.year >= 9999:
                return lower, None
            return lower, lower.replace(year=lower.year + 1)
        # Default and explicit DAY granularity.
        lower = moment.replace(hour=0, minute=0, second=0, microsecond=0)
        return lower, lower + timedelta(days=1)

    @staticmethod
    def _format_bound_literal(moment: datetime, col_type: str) -> str:
        if col_type == "DATE":
            return f"'{moment.strftime('%Y-%m-%d')}'"
        rendered = moment.strftime("%Y-%m-%d %H:%M:%S")
        if col_type == "TIMESTAMP":
            return f"TIMESTAMP('{rendered}')"
        return f"'{rendered}'"

    @staticmethod
    def _format_date_value(val: str, col_type: str) -> Optional[str]:
        # Normalize BigQuery partition date formats (YYYYMMDD/YYYYMM/YYYYMMDDHH) to YYYY-MM-DD.
        if ISO_DATE_PATTERN.match(val):
            return val
        # A datetime-shaped literal (space or 'T' separator, optional fractional
        # seconds, optional UTC offset). Only DATETIME and TIMESTAMP columns can hold
        # one, and a timezone-bearing literal is valid only for TIMESTAMP. A DATE/TIME
        # column, or a DATETIME column given a tz-bearing value, would build an
        # uncastable typed comparison (e.g. DATE = '2025-01-01T00:00:00Z'), so reject
        # it and let create_safe_filter surface the skip instead of emitting an
        # invalid predicate.
        datetime_match = ISO_DATETIME_FLEX_PATTERN.match(val)
        if datetime_match:
            has_timezone = datetime_match.group(2) is not None
            if col_type == "TIMESTAMP":
                return val
            if col_type == "DATETIME" and not has_timezone:
                return val
            return None

        if PARTITION_ID_YYYYMMDD_PATTERN.match(val):
            return f"{val[:4]}-{val[4:6]}-{val[6:8]}"

        if PARTITION_ID_YYYYMM_PATTERN.match(val):
            return f"{val[:4]}-{val[4:6]}-01"

        if PARTITION_ID_YYYYMMDDHH_PATTERN.match(val):
            date_part = f"{val[:4]}-{val[4:6]}-{val[6:8]}"
            hour_part = val[8:10]
            if col_type == "TIMESTAMP" or col_type == "DATETIME":
                return f"{date_part} {hour_part}:00:00"
            else:
                return date_part

        # A bare YYYY partition ID is a yearly partition and is handled as a full-year
        # range in create_safe_filter (an equality to Jan 1 would exclude the rest of
        # the year), so it must not be normalized to a single boundary value here.
        return None

    @staticmethod
    def _full_year_range(col_name: str, year: str, col_type: str) -> str:
        # col_name is already validated by create_safe_filter before this is reached.
        start = f"{year}-01-01"
        end_year = int(year) + 1
        is_timestamp = col_type.upper() == "TIMESTAMP"
        # BigQuery DATE/DATETIME/TIMESTAMP only accept years up to 9999, so a
        # year-9999 partition has no representable exclusive upper bound
        # (10000-01-01). Fall back to a lower-bound-only scan for that final year.
        if end_year > 9999:
            if is_timestamp:
                return f"`{col_name}` >= TIMESTAMP('{start}')"
            return f"`{col_name}` >= '{start}'"
        end = f"{end_year}-01-01"
        if is_timestamp:
            return (
                f"`{col_name}` >= TIMESTAMP('{start}') "
                f"AND `{col_name}` < TIMESTAMP('{end}')"
            )
        return f"`{col_name}` >= '{start}' AND `{col_name}` < '{end}'"

    @staticmethod
    def convert_partition_id_to_filters(
        partition_id: str,
        required_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
        is_range_partition: bool = False,
    ) -> Optional[List[str]]:
        # Errors (e.g. create_safe_filter raising on a type mismatch) propagate to the
        # caller, which holds the report and decides how to surface the skip.
        filters = []
        column_types = column_types or {}

        if "$" in partition_id:
            parts = partition_id.split("$")
            for part in parts:
                if "=" in part:
                    col, val = part.split("=", 1)
                    if col in required_columns:
                        col_type = column_types.get(col)
                        filters.append(
                            FilterBuilder.create_safe_filter(col, val, col_type)
                        )

        else:
            if len(required_columns) == 1:
                col_name = required_columns[0]
                col_type = column_types.get(col_name)

                # Hand the raw partition ID to create_safe_filter and let it normalize
                # per the column type (dates -> YYYY-MM-DD[ HH:00:00], numeric -> int,
                # string -> quoted). Pre-formatting an 8/10-digit ID to a date here would
                # corrupt STRING partitions ("20250115" is a real string value) and
                # hourly/numeric partitions. Only apply the legacy date normalization
                # when the column type is unavailable to guide create_safe_filter.
                filter_value: PartitionValue = partition_id
                if (
                    not col_type
                    and partition_id.isdigit()
                    and len(partition_id)
                    in (PARTITION_ID_YYYYMMDD_LENGTH, PARTITION_ID_YYYYMMDDHH_LENGTH)
                ):
                    filter_value = (
                        f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                    )
                if is_range_partition:
                    # The ID is the max range bucket's inclusive floor; scan `col >= id`
                    # rather than equality so the whole bucket is profiled.
                    filters.append(
                        FilterBuilder.create_lower_bound_filter(
                            col_name, filter_value, col_type
                        )
                    )
                else:
                    filters.append(
                        FilterBuilder.create_safe_filter(
                            col_name, filter_value, col_type
                        )
                    )
            else:
                logger.debug(
                    f"Complex partition mapping for {partition_id} with {len(required_columns)} columns"
                )
                return None

        return filters if filters else None

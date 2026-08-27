import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_BOOLEAN_TYPES,
    BIGQUERY_NUMERIC_TYPES,
    DATE_TIME_TYPES,
    ISO_DATE_PATTERN,
    ISO_DATETIME_FLEX_PATTERN,
    PARTITION_GRANULARITY_DAY,
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

        # Reject only the injection boundaries that also broaden the interpolated
        # predicate (statement terminator, line/block comment). Backslashes and quotes
        # are legitimate inside a STRING/Hive partition value and are escaped when the
        # quoted-literal branch builds the predicate below.
        if any(pattern in str_val for pattern in [";", "--", "/*", "#"]):
            raise ValueError(f"Invalid value for filter: {val}")

        if col_type and col_type.upper() in BIGQUERY_BOOLEAN_TYPES:
            normalized = str_val.strip().lower()
            if normalized in ("true", "1"):
                return f"`{col_name}` = TRUE"
            if normalized in ("false", "0"):
                return f"`{col_name}` = FALSE"
            raise ValueError(
                f"Non-boolean value '{str_val}' for boolean column "
                f"{col_name} ({col_type})"
            )

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

            # A compact partition-id (YYYYMM / YYYYMMDDHH, or YYYYMMDD on a sub-day
            # DATETIME/TIMESTAMP column) denotes a whole time-unit bucket. Emit the
            # granularity-aware half-open range rather than an equality to the bucket
            # start, which would drop the rest of the month/hour/day.
            range_filter = FilterBuilder._partition_id_range(
                col_name, str_val, col_type.upper()
            )
            if range_filter is not None:
                return range_filter

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

        # Escape backslashes first (BigQuery treats '\' as an escape char in string
        # literals), then double single quotes, so a legitimate value containing either
        # is encoded rather than rejected or emitted as broken SQL.
        escaped_val = str_val.replace("\\", "\\\\").replace("'", "''")
        return f"`{col_name}` = '{escaped_val}'"

    @staticmethod
    def create_lower_bound_filter(
        col_name: str, val: PartitionValue, col_type: Optional[str] = None
    ) -> str:
        # A RANGE partition ID is the max bucket's inclusive integer lower bound, not an
        # exact value: a row belongs to the bucket when col is in [start, start+interval).
        # Equality (`col = start`) would only match rows exactly on the floor and miss the
        # rest of the bucket, so the bucket is scanned with `col >= start`. BigQuery RANGE
        # partitioning is integer-only, so build the typed numeric predicate directly and
        # reject anything that isn't a valid integer bound rather than emitting an
        # unenforceable filter.
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")
        try:
            int_val = int(str(val).strip())
        except (TypeError, ValueError):
            raise ValueError(
                f"RANGE partition lower bound must be an integer for column "
                f"{col_name}, got '{val}'"
            ) from None
        return f"`{col_name}` >= {int_val}"

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
        moment = partition_datetime
        if ctype == "TIMESTAMP" and moment.tzinfo is not None:
            # BigQuery time-partitions TIMESTAMP columns on UTC boundaries. Normalize a
            # tz-aware instant to UTC *before* flooring, otherwise flooring the local
            # wall clock (e.g. 10:30+05:30) yields 04:30 UTC — not the 05:00 UTC
            # partition boundary — and would scan the wrong partition or span two.
            moment = moment.astimezone(timezone.utc)
        lower, upper = FilterBuilder._partition_bounds(moment, granularity)

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
        # datetime tops out at 9999-12-31 23:59:59.999999, so a partition at the very end
        # of the representable range has no expressible exclusive upper bound. Every
        # branch returns None for the upper bound in that case, and the caller scans
        # lower-bound-only (mirroring the year-9999 YEAR behaviour) instead of raising.
        gran = (granularity or "").upper()
        if gran == PARTITION_GRANULARITY_HOUR:
            lower = moment.replace(minute=0, second=0, microsecond=0)
            return lower, FilterBuilder._safe_add(lower, timedelta(hours=1))
        if gran == PARTITION_GRANULARITY_MONTH:
            lower = moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            return lower, FilterBuilder._next_month(lower)
        if gran == PARTITION_GRANULARITY_YEAR:
            lower = moment.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
            if lower.year >= 9999:
                return lower, None
            return lower, lower.replace(year=lower.year + 1)
        # Default and explicit DAY granularity.
        lower = moment.replace(hour=0, minute=0, second=0, microsecond=0)
        return lower, FilterBuilder._safe_add(lower, timedelta(days=1))

    @staticmethod
    def _safe_add(moment: datetime, delta: timedelta) -> Optional[datetime]:
        try:
            return moment + delta
        except OverflowError:
            return None

    @staticmethod
    def _next_month(lower: datetime) -> Optional[datetime]:
        if lower.month != 12:
            return lower.replace(month=lower.month + 1)
        try:
            return lower.replace(year=lower.year + 1, month=1)
        except ValueError:
            return None

    @staticmethod
    def _format_bound_literal(moment: datetime, col_type: str) -> str:
        # Build the date/time explicitly rather than via strftime("%Y"): C strftime does
        # not zero-pad years < 1000 on every platform (e.g. year 5 renders as "5"), which
        # BigQuery rejects. Explicit %04d guarantees a four-digit year everywhere.
        date_str = f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}"
        if col_type == "DATE":
            return f"'{date_str}'"
        rendered = (
            f"{date_str} {moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}"
        )
        if col_type == "TIMESTAMP":
            # Bounds for a tz-aware instant were normalized to UTC by the caller, so a
            # UTC offset is rendered explicitly (+00:00); a naive value is left bare and
            # interpreted by BigQuery as UTC. Either way the literal is on a UTC boundary.
            offset = moment.strftime("%z")  # +0000 for UTC, empty when tz-naive
            if offset:
                rendered = f"{rendered}{offset[:3]}:{offset[3:]}"
            return f"TIMESTAMP('{rendered}')"
        # DATETIME has no timezone; render it tz-free.
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
    def _partition_id_range(
        col_name: str, str_val: str, col_type: str
    ) -> Optional[str]:
        # col_name is already validated by create_safe_filter before this is reached.
        # Returns a granularity-aware half-open range for a recognized compact
        # partition-id bucket shape, or None to fall through to the existing
        # equality/normalization path (e.g. a plain YYYY-MM-DD value on a DATE column,
        # whose day partition is already exactly that date).

        # A YYYYMM month bucket spans the whole month for DATE, DATETIME and TIMESTAMP
        # alike, so a half-open month range is always the right filter.
        if col_type in (
            "DATE",
            "DATETIME",
            "TIMESTAMP",
        ) and PARTITION_ID_YYYYMM_PATTERN.match(str_val):
            moment = datetime(int(str_val[:4]), int(str_val[4:6]), 1)
            return FilterBuilder.create_partition_datetime_filter(
                col_name, moment, col_type, PARTITION_GRANULARITY_MONTH
            )

        # Hourly and sub-day day buckets only exist for DATETIME/TIMESTAMP columns. A
        # DATE column cannot represent an hour, so an hourly range would format both
        # bounds to the same YYYY-MM-DD and match zero rows; and a DATE day partition is
        # exactly its date. In both cases DATE must fall through to the plain date
        # normalization (equality on YYYY-MM-DD), so gate these to the temporal types.
        if col_type in ("DATETIME", "TIMESTAMP"):
            if PARTITION_ID_YYYYMMDDHH_PATTERN.match(str_val):
                moment = datetime(
                    int(str_val[:4]),
                    int(str_val[4:6]),
                    int(str_val[6:8]),
                    int(str_val[8:10]),
                )
                return FilterBuilder.create_partition_datetime_filter(
                    col_name, moment, col_type, PARTITION_GRANULARITY_HOUR
                )
            if PARTITION_ID_YYYYMMDD_PATTERN.match(str_val):
                moment = datetime(
                    int(str_val[:4]), int(str_val[4:6]), int(str_val[6:8])
                )
                return FilterBuilder.create_partition_datetime_filter(
                    col_name, moment, col_type, PARTITION_GRANULARITY_DAY
                )
        return None

    @staticmethod
    def _full_year_range(col_name: str, year: str, col_type: str) -> str:
        # col_name is already validated by create_safe_filter before this is reached.
        # Pad both years to four digits: the incoming YYYY id is already four chars, but
        # the exclusive end (year+1) must be formatted with %04d so a year such as 0999
        # produces "1000-01-01" and never a bare-int "1000" or an unpadded low year.
        start = f"{int(year):04d}-01-01"
        end_year = int(year) + 1
        is_timestamp = col_type.upper() == "TIMESTAMP"
        # BigQuery DATE/DATETIME/TIMESTAMP only accept years up to 9999, so a
        # year-9999 partition has no representable exclusive upper bound
        # (10000-01-01). Fall back to a lower-bound-only scan for that final year.
        if end_year > 9999:
            if is_timestamp:
                return f"`{col_name}` >= TIMESTAMP('{start}')"
            return f"`{col_name}` >= '{start}'"
        end = f"{end_year:04d}-01-01"
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

                if is_range_partition:
                    # The ID is the max RANGE bucket's inclusive integer floor; scan
                    # `col >= id` rather than equality so the whole bucket is profiled.
                    # RANGE buckets are integer-only, so the raw id must be passed
                    # through unchanged — date-normalizing an 8/10-digit id here (as the
                    # non-range path does when the type is unknown) would turn it into
                    # "YYYY-MM-DD" and then fail int() parsing, dropping the partition.
                    filters.append(
                        FilterBuilder.create_lower_bound_filter(
                            col_name, partition_id, col_type
                        )
                    )
                else:
                    # Hand the raw partition ID to create_safe_filter and let it normalize
                    # per the column type (dates -> YYYY-MM-DD[ HH:00:00], numeric -> int,
                    # string -> quoted). Pre-formatting an 8/10-digit ID to a date here
                    # would corrupt STRING partitions ("20250115" is a real string value)
                    # and hourly/numeric partitions. Only apply the legacy date
                    # normalization when the column type is unavailable to guide
                    # create_safe_filter.
                    filter_value: PartitionValue = partition_id
                    if (
                        not col_type
                        and partition_id.isdigit()
                        and len(partition_id)
                        in (
                            PARTITION_ID_YYYYMMDD_LENGTH,
                            PARTITION_ID_YYYYMMDDHH_LENGTH,
                        )
                    ):
                        filter_value = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
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

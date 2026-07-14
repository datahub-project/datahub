import logging
from typing import Dict, List, Optional

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
    DATE_TIME_TYPES,
    DATETIME_SECONDS_PATTERN,
    ISO_DATE_PATTERN,
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
            formatted_val = FilterBuilder._format_date_value(str_val, col_type.upper())
            if formatted_val:
                if col_type.upper() == "TIMESTAMP" and " " not in formatted_val:
                    return f"`{col_name}` = TIMESTAMP('{formatted_val}')"
                return f"`{col_name}` = '{formatted_val}'"
            else:
                logger.warning(
                    f"Could not format date value '{str_val}' for {col_type} column {col_name}"
                )

        if "'" in str_val:
            escaped_val = str_val.replace("'", "''")
            return f"`{col_name}` = '{escaped_val}'"
        else:
            return f"`{col_name}` = '{str_val}'"

    @staticmethod
    def _format_date_value(val: str, col_type: str) -> Optional[str]:
        # Normalize BigQuery partition date formats (YYYYMMDD/YYYYMM/YYYYMMDDHH) to YYYY-MM-DD.
        if ISO_DATE_PATTERN.match(val):
            return val
        if DATETIME_SECONDS_PATTERN.match(val):
            return val

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

        return None

    @staticmethod
    def convert_partition_id_to_filters(
        partition_id: str,
        required_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
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

                if (
                    len(partition_id) == PARTITION_ID_YYYYMMDD_LENGTH
                    and partition_id.isdigit()
                ) or (
                    len(partition_id) == PARTITION_ID_YYYYMMDDHH_LENGTH
                    and partition_id.isdigit()
                ):
                    date_str = (
                        f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                    )
                    filters.append(
                        FilterBuilder.create_safe_filter(col_name, date_str, col_type)
                    )
                else:
                    filters.append(
                        FilterBuilder.create_safe_filter(
                            col_name, partition_id, col_type
                        )
                    )
            else:
                logger.debug(
                    f"Complex partition mapping for {partition_id} with {len(required_columns)} columns"
                )
                return None

        return filters if filters else None

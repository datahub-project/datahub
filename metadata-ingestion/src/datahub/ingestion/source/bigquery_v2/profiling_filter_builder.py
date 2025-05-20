import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable

logger = logging.getLogger(__name__)


class BigQueryFilterBuilder:
    """
    Builder for BigQuery filters used in profiling queries.
    Handles the complex filter creation logic for different column types.
    """

    def __init__(self, execute_query_callback):
        """
        Initialize the filter builder.

        Args:
            execute_query_callback: Callback function to execute BigQuery queries
        """
        self._execute_query = execute_query_callback
        self._filter_verification_cache: Dict[str, bool] = {}

    def create_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        partition_values: Dict[str, Any],
    ) -> List[str]:
        """
        Create partition filter strings with robust type handling.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping column names to data types
            partition_values: Dictionary mapping column names to values

        Returns:
            List of filter strings
        """
        filters = []

        # Get accurate column types first
        col_names = list(partition_values.keys())
        accurate_types = self._get_accurate_column_types(
            table, project, schema, col_names
        )

        for col_name, value in partition_values.items():
            if value is None:
                continue

            # Use accurate type if available, otherwise use provided type
            if col_name in accurate_types:
                col_type = accurate_types[col_name]["data_type"]
            else:
                col_type = partition_columns.get(col_name, "")

            # Create robust filter with explicit type handling
            filter_str = self._create_robust_filter(
                table, project, schema, col_name, value, col_type
            )

            if filter_str:
                filters.append(filter_str)

        # If we couldn't create any filters, try alternative approaches
        if not filters and col_names:
            # Try each partition column with alternative filters
            for col_name in col_names:
                alt_filter = self._try_alternative_filters(
                    table, project, schema, col_name
                )
                if alt_filter:
                    filters.append(alt_filter)
                    break

        return filters

    def _get_accurate_column_types(
        self, table: BigqueryTable, project: str, schema: str, columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get accurate column type information directly from the table.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            columns: List of column names to check

        Returns:
            Dictionary mapping column names to detailed type information
        """
        if not columns:
            return {}

        # Safely quote column names for SQL
        quoted_columns = ", ".join(f"'{col}'" for col in columns)

        query = f"""
        SELECT 
            column_name,
            data_type,
            is_partitioning_column
        FROM 
            `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
        WHERE 
            table_name = '{table.name}'
            AND column_name IN ({quoted_columns})
        """

        cache_key = f"col_types_{project}_{schema}_{table.name}_{hash(quoted_columns)}"
        results = self._execute_query(query, cache_key, timeout=30)

        column_info = {}
        for row in results:
            column_info[row.column_name] = {
                "data_type": row.data_type,
                "is_partition": row.is_partitioning_column == "YES",
            }

        return column_info

    def _create_robust_filter(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        col_name: str,
        value: Any,
        col_type: Optional[str] = None,
    ) -> str:
        """
        Create a filter with explicit type casting based on the actual column type.

        This method handles type mismatches by explicitly casting values to the correct type.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            col_name: Column name
            value: Column value
            col_type: Optional column type if already known

        Returns:
            Filter string with appropriate type casting
        """
        # Handle NULL values
        if value is None:
            return f"`{col_name}` IS NULL"

        # Get accurate column type if not provided
        if not col_type:
            col_info = self._get_accurate_column_types(
                table, project, schema, [col_name]
            )
            if col_name in col_info:
                col_type = col_info[col_name]["data_type"]
            else:
                # Default handling if we can't determine type
                return self._create_safe_filter(col_name, value)

        col_type_upper = col_type.upper() if col_type else ""

        try:
            # Group column types into categories and call specialized handlers
            if col_type_upper in ("INT64", "INTEGER", "INT"):
                return self._create_integer_filter(col_name, value)
            elif col_type_upper == "DATE":
                return self._create_date_filter(col_name, value)
            elif col_type_upper in ("TIMESTAMP", "DATETIME"):
                return self._create_timestamp_filter(col_name, value)
            elif col_type_upper in ("STRING", "VARCHAR"):
                return self._create_string_filter(col_name, value)
            elif col_type_upper in ("BOOL", "BOOLEAN"):
                return self._create_boolean_filter(col_name, value)
            elif col_type_upper in ("FLOAT64", "FLOAT", "NUMERIC", "DECIMAL"):
                return self._create_numeric_filter(col_name, value)
            else:
                # Default case for other types
                return self._create_default_filter(col_name, value)

        except Exception as e:
            logger.debug(f"Error creating robust filter for {col_name}: {e}")
            return self._create_safe_filter(col_name, value)

    def _create_integer_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for integer columns with type conversion handling."""
        if isinstance(value, datetime):
            # Convert datetime to integer
            return f"`{col_name}` = {int(value.timestamp())}"
        elif isinstance(value, (int, float)):
            return f"`{col_name}` = {int(value)}"
        else:
            # Try to convert string to int
            try:
                # Handle TIMESTAMP literals in string
                if "TIMESTAMP" in str(value):
                    ts_str = str(value).replace("TIMESTAMP", "").strip().strip("'\"")
                    dt = datetime.fromisoformat(ts_str.replace(" ", "T"))
                    return f"`{col_name}` = {int(dt.timestamp())}"
                # Otherwise just convert to int
                return f"`{col_name}` = {int(float(str(value)))}"
            except (ValueError, TypeError):
                # Last resort - try IS NOT NULL
                return f"`{col_name}` IS NOT NULL"

    def _create_date_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for DATE columns with type conversion handling."""
        if isinstance(value, datetime):
            return f"`{col_name}` = DATE '{value.strftime('%Y-%m-%d')}'"
        else:
            # Handle DATE, TIMESTAMP, or plain string literals
            val_str = str(value)
            if "DATE" in val_str or "TIMESTAMP" in val_str:
                # Extract date part
                date_str = (
                    val_str.replace("DATE", "")
                    .replace("TIMESTAMP", "")
                    .strip()
                    .strip("'\"")
                )
                # Extract just YYYY-MM-DD part if it's a timestamp
                if " " in date_str:
                    date_str = date_str.split(" ")[0]
                return f"`{col_name}` = DATE '{date_str}'"
            else:
                # Assume it's already a date string
                val_str = val_str.replace("'", "").strip()
                return f"`{col_name}` = DATE '{val_str}'"

    def _create_timestamp_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for TIMESTAMP columns with type conversion handling."""
        if isinstance(value, datetime):
            return f"`{col_name}` = TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, (int, float)):
            return f"`{col_name}` = TIMESTAMP_SECONDS({int(value)})"
        else:
            val_str = str(value).replace("TIMESTAMP", "").strip().strip("'\"")
            return f"`{col_name}` = TIMESTAMP '{val_str}'"

    def _create_string_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for string columns with SQL injection protection."""
        if value is None:
            return f"`{col_name}` IS NULL"

        # Convert to string and escape single quotes for SQL safety
        # Use BigQuery's string escape convention: ' becomes ''
        safe_value = str(value).replace("'", "''")
        return f"`{col_name}` = '{safe_value}'"

    def _create_boolean_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for BOOLEAN columns with type conversion handling."""
        if isinstance(value, bool):
            bool_val = "true" if value else "false"
        else:
            bool_val = "true" if str(value).lower() in ("true", "1", "yes") else "false"
        return f"`{col_name}` = {bool_val}"

    def _create_numeric_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for numeric columns (FLOAT64, NUMERIC, etc.)."""
        if isinstance(value, (int, float)):
            return f"`{col_name}` = {value}"
        else:
            try:
                # Try to convert to numeric
                numeric_value = float(str(value))
                return f"`{col_name}` = {numeric_value}"
            except (ValueError, TypeError):
                # Fall back to safe filter
                return f"`{col_name}` IS NOT NULL"

    def _create_default_filter(self, col_name: str, value: Any) -> str:
        """Create a filter for unknown column types."""
        if isinstance(value, (int, float)):
            return f"`{col_name}` = {value}"
        elif isinstance(value, str):
            # Escape quotes for string values
            val_str = value.replace("'", "\\'")
            return f"`{col_name}` = '{val_str}'"
        else:
            # Convert to string for other types
            val_str = str(value).replace("'", "\\'")
            return f"`{col_name}` = '{val_str}'"

    def _create_safe_filter(self, col_name: str, value: Any) -> str:
        """
        Create a filter that's least likely to cause type mismatch errors.
        """
        if value is None:
            return f"`{col_name}` IS NULL"

        # IS NOT NULL is the safest filter as it doesn't involve value type matching
        return f"`{col_name}` IS NOT NULL"

    def _try_alternative_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        col_name: str,
        timeout: int = 20,
    ) -> Optional[str]:
        """
        Try alternative filters for a column when standard approaches fail.

        This method attempts various common filter patterns that are likely
        to work for different column types.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            col_name: Column name
            timeout: Query timeout

        Returns:
            Working filter string or None if none found
        """
        # Get column type
        col_info = self._get_accurate_column_types(table, project, schema, [col_name])
        if not col_info or col_name not in col_info:
            return None

        col_type = col_info[col_name]["data_type"].upper()

        # Try different filter approaches based on column type
        filters_to_try = []

        # For date/timestamp columns
        if col_type in ("DATE", "TIMESTAMP", "DATETIME"):
            # Try last 30 days
            filters_to_try.extend(
                [
                    f"`{col_name}` >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)",
                    f"`{col_name}` >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)",
                    f"`{col_name}` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)",
                ]
            )

        # For integer columns
        elif col_type in ("INT64", "INTEGER"):
            current_year = datetime.now().year
            # Try current year and previous year
            filters_to_try.extend(
                [
                    f"`{col_name}` = {current_year}",
                    f"`{col_name}` = {current_year - 1}",
                    f"`{col_name}` > 0",
                ]
            )

        # For any column type - IS NOT NULL is the most permissive filter
        filters_to_try.append(f"`{col_name}` IS NOT NULL")

        # Try each filter
        for filter_str in filters_to_try:
            try:
                # Check if this filter returns data
                test_query = f"""
                SELECT 1 
                FROM `{project}.{schema}.{table.name}`
                WHERE {filter_str}
                LIMIT 1
                """

                results = self._execute_query(test_query, None, timeout=timeout)
                if results:
                    logger.info(f"Found working alternative filter: {filter_str}")
                    return filter_str
            except Exception as e:
                logger.debug(f"Alternative filter {filter_str} failed: {e}")

        return None

    def verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        timeout: int = 30,
        max_retries: int = 2,
    ) -> bool:
        """
        Verify that a set of partition filters actually returns data efficiently.
        """
        if not filters:
            return False

        # Check cache first
        cache_key = f"{project}.{schema}.{table.name}.{hash(tuple(sorted(filters)))}"
        if cache_key in self._filter_verification_cache:
            logger.debug(f"Using cached filter verification result for {cache_key}")
            return True

        # Build WHERE clause from filters
        where_clause = " AND ".join(filters)

        # For tables with multiple partition filters, we need to be more careful
        # as combining filters might be too restrictive
        is_multi_filter = len(filters) > 1

        # First try with combined filters (fastest approach)
        if self._try_existence_check(
            table,
            project,
            schema,
            filters,
            where_clause,
            timeout,
            max_retries,
            cache_key,
        ):
            return True

        # If combined filters fail and we have multiple filters, try with a subset
        # This helps when multiple partition columns have a complex relationship
        if is_multi_filter:
            logger.info(
                "Combined filters failed, trying with important subset of filters"
            )

            # Try with just one filter at a time (prioritize date columns)
            for filter_str in filters:
                # Date/time filters are most likely to succeed
                is_date_filter = any(
                    term in filter_str.lower()
                    for term in ["date", "time", "year", "month", "day"]
                )

                if is_date_filter:
                    if self.verify_partition_has_data(
                        table, project, schema, [filter_str], timeout, 1
                    ):
                        logger.info(f"Single date filter succeeded: {filter_str}")
                        self._filter_verification_cache[cache_key] = True
                        return True

            # Try with any filter
            for filter_str in filters:
                if self.verify_partition_has_data(
                    table, project, schema, [filter_str], timeout, 1
                ):
                    logger.info(f"Single filter succeeded: {filter_str}")
                    self._filter_verification_cache[cache_key] = True
                    return True

        # If first attempt failed, try other strategies
        verification_strategies = [
            self._try_count_query,
            self._try_sample_query,
            self._try_syntax_check,
        ]

        for strategy in verification_strategies:
            result = strategy(
                table,
                project,
                schema,
                filters,
                where_clause,
                timeout,
                max_retries,
                cache_key,
            )
            if result:
                return True

        logger.warning(
            f"Partition verification found no data with filters: {where_clause}"
        )
        return False

    def _try_existence_check(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a simple existence check."""
        # For external tables or very large tables, use sampling to improve efficiency
        sampling_clause = ""
        if table.external or (
            table.size_in_bytes and table.size_in_bytes > 10_000_000_000
        ):
            sampling_clause = " TABLESAMPLE SYSTEM (0.1 PERCENT)"

        existence_query = f"""
        SELECT 1 
        FROM `{project}.{schema}.{table.name}`{sampling_clause}
        WHERE {where_clause}
        LIMIT 1
        """

        query_cache_key = (
            f"verify_exists_{project}_{schema}_{table.name}_{hash(where_clause)}"
        )

        # First try with shorter timeout (fast path)
        initial_timeout = max(5, timeout // 2)
        initial_result = self._execute_existence_query(
            existence_query, query_cache_key, initial_timeout, cache_key, filters
        )
        if initial_result is not None:
            return initial_result

        # If first attempt failed with multiple filters, try adding HINT to help BigQuery optimizer
        if len(filters) > 1 and not sampling_clause:
            # Add query hint for BigQuery to consider specific partition columns first
            # This can dramatically improve performance with multiple partition columns
            hint_query = f"""
            SELECT /*+ OPTIMIZER_HINTS() */ 1 
            FROM `{project}.{schema}.{table.name}`
            WHERE {where_clause}
            LIMIT 1
            """

            hint_cache_key = (
                f"verify_hint_{project}_{schema}_{table.name}_{hash(where_clause)}"
            )
            try:
                hint_results = self._execute_query(
                    hint_query, hint_cache_key, initial_timeout
                )
                if hint_results:
                    logger.info("Verified filters with query hint")
                    self._filter_verification_cache[cache_key] = True
                    return True
            except Exception as e:
                logger.debug(f"Hint query failed: {e}")

        # If first attempt failed, try with longer timeout and retries
        for attempt in range(max_retries):
            try:
                existence_results = self._execute_query(
                    existence_query, query_cache_key, timeout
                )

                if existence_results:
                    logger.info("Verified filters return data (existence check)")
                    self._filter_verification_cache[cache_key] = True
                    return True

            except Exception as e:
                error_str = str(e)

                # Handle type mismatch errors
                if "No matching signature for operator" in error_str:
                    fixed_filters = self._handle_type_mismatch(
                        filters,
                        error_str,
                        table,
                        project,
                        schema,
                        timeout,
                        max_retries - attempt - 1,
                    )
                    if fixed_filters:
                        return fixed_filters

                # Handle resource exhaustion errors by using sampling
                if "resources exceeded" in error_str.lower() and not sampling_clause:
                    logger.info("Resources exceeded, retrying with sampling")
                    sampling_query = f"""
                    SELECT 1 
                    FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                    WHERE {where_clause}
                    LIMIT 1
                    """

                    sample_cache_key = f"verify_sample_{project}_{schema}_{table.name}_{hash(where_clause)}"
                    try:
                        sample_results = self._execute_query(
                            sampling_query, sample_cache_key, timeout
                        )
                        if sample_results:
                            logger.info("Verified filters with sampling")
                            self._filter_verification_cache[cache_key] = True
                            return True
                    except Exception as sample_e:
                        logger.debug(f"Sampling query failed: {sample_e}")

                logger.debug(
                    f"Existence check attempt {attempt + 1} failed: {e}, retrying..."
                )
                time.sleep(1)  # Brief pause before retry

        logger.debug("All existence check attempts failed")
        return False

    def _execute_existence_query(
        self,
        query: str,
        query_cache_key: str,
        timeout: int,
        filter_cache_key: str,
        filters: List[str],
    ) -> Optional[bool]:
        """Execute existence query with caching and return result status."""
        try:
            existence_results = self._execute_query(query, query_cache_key, timeout)

            if existence_results:
                logger.info("Verified filters return data (existence check)")
                self._filter_verification_cache[filter_cache_key] = True
                return True
            return None
        except Exception:
            return None

    def _handle_type_mismatch(
        self,
        filters: List[str],
        error_str: str,
        table: BigqueryTable,
        project: str,
        schema: str,
        timeout: int,
        retries_left: int,
    ) -> bool:
        """
        Handle type mismatch errors by fixing filter expressions with improved
        handling of multiple partition columns.
        """
        logger.warning(f"Type mismatch detected: {error_str}")

        # For multiple filters, try with only one filter at a time first
        # This often works when the combined filters have type compatibility issues
        if len(filters) > 1:
            logger.info("Trying individual filters for type mismatch")
            for filter_str in filters:
                # Prioritize date/time filters as they're most likely to succeed
                if any(
                    term in filter_str.lower()
                    for term in ["date", "time", "year", "month", "day"]
                ):
                    if self.verify_partition_has_data(
                        table, project, schema, [filter_str], timeout, 1
                    ):
                        logger.info(f"Individual filter succeeded: {filter_str}")
                        return True

            # If time filters didn't work, try any other filter
            for filter_str in filters:
                if not any(
                    term in filter_str.lower()
                    for term in ["date", "time", "year", "month", "day"]
                ):
                    if self.verify_partition_has_data(
                        table, project, schema, [filter_str], timeout, 1
                    ):
                        logger.info(f"Individual filter succeeded: {filter_str}")
                        return True

        # Handle INT64 vs TIMESTAMP mismatch
        if "INT64" in error_str and "TIMESTAMP" in error_str:
            fixed_filters = self._fix_int64_timestamp_mismatch(filters)
            if fixed_filters != filters:
                logger.info(
                    f"Attempting with fixed INT64/TIMESTAMP filters: {fixed_filters}"
                )
                return self.verify_partition_has_data(
                    table, project, schema, fixed_filters, timeout, retries_left
                )

        # Handle STRING vs TIMESTAMP/DATE mismatch
        elif "STRING" in error_str and (
            "TIMESTAMP" in error_str or "DATE" in error_str
        ):
            fixed_filters = self._fix_string_date_mismatch(filters)
            if fixed_filters != filters:
                logger.info(
                    f"Attempting with fixed STRING/TIMESTAMP filters: {fixed_filters}"
                )
                return self.verify_partition_has_data(
                    table, project, schema, fixed_filters, timeout, retries_left
                )

        # Handle mixed type issues in hierarchical date columns (year/month/day)
        elif len(filters) > 1 and any(
            col in error_str.lower() for col in ["year", "month", "day", "hour"]
        ):
            # Extract the time hierarchy columns and try them in order
            time_filters = [
                f
                for f in filters
                if any(col in f.lower() for col in ["year", "month", "day", "hour"])
            ]
            if time_filters:
                logger.info(
                    f"Trying time hierarchy filters individually: {time_filters}"
                )
                for filter_str in time_filters:
                    if self.verify_partition_has_data(
                        table, project, schema, [filter_str], timeout // 2, 1
                    ):
                        logger.info(f"Time hierarchy filter succeeded: {filter_str}")
                        return True

        # For any type mismatch, try IS NOT NULL as a last resort
        simple_filters = self._create_is_not_null_filters(filters)
        if simple_filters != filters:
            logger.info(f"Attempting with IS NOT NULL filters: {simple_filters}")
            return self.verify_partition_has_data(
                table, project, schema, simple_filters, timeout // 2, 1
            )

        return False

    def _fix_int64_timestamp_mismatch(self, filters: List[str]) -> List[str]:
        """Fix filters with INT64/TIMESTAMP type mismatches."""
        fixed_filters = []
        for filter_str in filters:
            if "=" in filter_str:
                parts = filter_str.split("=", 1)
                if len(parts) == 2:
                    col, val = parts[0].strip(), parts[1].strip()

                    # Fix INT64 column with TIMESTAMP value
                    if "TIMESTAMP" in val:
                        try:
                            # Extract timestamp and convert to epoch seconds
                            timestamp_str = (
                                val.replace("TIMESTAMP", "").strip().strip("'\"")
                            )
                            dt = datetime.fromisoformat(timestamp_str.replace(" ", "T"))
                            epoch_seconds = int(dt.timestamp())
                            fixed_filters.append(f"{col} = {epoch_seconds}")
                            continue
                        except (ValueError, TypeError):
                            pass

                    # Integer value with TIMESTAMP column - use TIMESTAMP_SECONDS
                    if val.isdigit():
                        fixed_filters.append(f"{col} = TIMESTAMP_SECONDS({val})")
                        continue

            # Keep unchanged filters
            fixed_filters.append(filter_str)

        return fixed_filters

    def _fix_string_date_mismatch(self, filters: List[str]) -> List[str]:
        """Fix filters with STRING/DATE type mismatches."""
        fixed_filters = []
        for filter_str in filters:
            if "=" in filter_str:
                parts = filter_str.split("=", 1)
                if len(parts) == 2:
                    col, val = parts[0].strip(), parts[1].strip()

                    # Fix STRING column with TIMESTAMP/DATE value
                    if "TIMESTAMP" in val or "DATE" in val:
                        # Extract just the date/time part
                        date_str = (
                            val.replace("TIMESTAMP", "")
                            .replace("DATE", "")
                            .strip()
                            .strip("'\"")
                        )
                        fixed_filters.append(f"{col} = '{date_str}'")
                        continue

            # Keep unchanged filters
            fixed_filters.append(filter_str)

        return fixed_filters

    def _create_is_not_null_filters(self, filters: List[str]) -> List[str]:
        """Create IS NOT NULL filters from equality filters."""
        simple_filters = []
        for filter_str in filters:
            if "=" in filter_str:
                col = filter_str.split("=")[0].strip()
                simple_filters.append(f"{col} IS NOT NULL")
            else:
                simple_filters.append(filter_str)

        return simple_filters

    def _try_count_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a COUNT query."""
        count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT 1000
        """

        count_key = f"verify_count_{project}_{schema}_{table.name}_{hash(where_clause)}"

        try:
            count_results = self._execute_query(count_query, count_key, timeout)
            if (
                count_results
                and hasattr(count_results[0], "cnt")
                and count_results[0].cnt > 0
            ):
                logger.info(f"Verified filters return {count_results[0].cnt} rows")
                self._filter_verification_cache[cache_key] = True
                return True
        except Exception as e:
            logger.debug(f"Count verification failed: {e}")

        return False

    def _try_sample_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a TABLESAMPLE query for large tables."""
        if not (
            table.external
            or (table.size_in_bytes and table.size_in_bytes > 1_000_000_000)
        ):
            return False

        try:
            sample_rate = 0.01 if table.external else 0.1
            sample_query = f"""
            SELECT 1 
            FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM ({sample_rate} PERCENT)
            WHERE {where_clause}
            LIMIT 1
            """

            sample_key = (
                f"verify_sample_{project}_{schema}_{table.name}_{hash(where_clause)}"
            )
            sample_results = self._execute_query(sample_query, sample_key, timeout)

            if sample_results:
                logger.info(f"Verified filters using {sample_rate}% sample")
                self._filter_verification_cache[cache_key] = True
                return True
        except Exception as e:
            logger.debug(f"Sample verification failed: {e}")

        return False

    def _try_syntax_check(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a syntax-only verification."""
        if table.external or (
            table.size_in_bytes and table.size_in_bytes > 10_000_000_000
        ):
            return False

        try:
            dummy_query = f"""SELECT 1 WHERE {where_clause} LIMIT 0"""
            dummy_key = f"verify_dummy_{hash(where_clause)}"
            self._execute_query(dummy_query, dummy_key, 5)

            logger.warning(
                "Using syntactically valid filters without data verification"
            )
            self._filter_verification_cache[cache_key] = True
            return True
        except Exception as e:
            logger.debug(f"Filter syntax check failed: {e}")

        return False

    def check_sample_rate_in_ddl(self, ddl: str) -> bool:
        """
        Check if a sample rate is specified in the DDL.

        Args:
            ddl: The DDL string to check

        Returns:
            True if a sample rate is specified, False otherwise
        """
        return "TABLESAMPLE" in ddl.upper() if ddl else False

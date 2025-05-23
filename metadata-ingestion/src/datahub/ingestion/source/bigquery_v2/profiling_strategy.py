import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling_config import BigQueryProfilerConfig

logger = logging.getLogger(__name__)


class ProfileStrategy(ABC):
    """
    Abstract base class for profile strategies.
    Implements the Strategy Pattern for different profiling approaches.
    """

    def __init__(
        self,
        execute_query_callback: Callable[[str, Optional[str], int, int], List[Any]],
        config: BigQueryProfilerConfig,
    ):
        """
        Initialize the profile strategy.

        Args:
            execute_query_callback: Function to execute BigQuery queries
            config: Profiling configuration
        """
        self._execute_query = execute_query_callback
        self.config = config

    @abstractmethod
    def generate_profile_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: Optional[List[str]] = None,
    ) -> str:
        """
        Generate SQL query for profiling a table.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            partition_filters: Optional list of partition filter expressions

        Returns:
            SQL query string
        """
        pass

    @abstractmethod
    def extract_profile_data(
        self,
        query_results: List[Any],
        table: BigqueryTable,
    ) -> Dict[str, Any]:
        """
        Extract profile data from query results.

        Args:
            query_results: Results from executing the profile query
            table: BigqueryTable instance

        Returns:
            Dictionary containing profile data
        """
        pass

    def apply_sampling_clause(self, table: BigqueryTable, base_query: str) -> str:
        """
        Apply appropriate sampling clause to a query based on table size.

        Args:
            table: BigqueryTable instance
            base_query: SQL query to modify

        Returns:
            Modified SQL query with sampling
        """
        sampling_clause = ""

        # Apply sampling to external tables
        if table.external:
            sampling_percent = self.config.external_table_sampling_percent
            if sampling_percent < 100:
                sampling_clause = f" TABLESAMPLE SYSTEM ({sampling_percent} PERCENT)"

        # Apply sampling to large tables
        elif (
            table.size_in_bytes and table.size_in_bytes > 10_000_000_000  # > 10 GB
        ) or (
            table.rows_count and table.rows_count > 50_000_000  # > 50M rows
        ):
            sampling_percent = self.config.large_table_sampling_percent
            if sampling_percent < 100:
                sampling_clause = f" TABLESAMPLE SYSTEM ({sampling_percent} PERCENT)"

        # Apply the sampling clause if needed
        if sampling_clause:
            # Find the FROM clause
            from_index = base_query.upper().find("FROM")
            if from_index > 0:
                # Split the query at the FROM clause
                before_from = base_query[: from_index + 4]  # Include "FROM"
                after_from = base_query[from_index + 4 :]

                # Find the next clause after the table name
                table_end = after_from.find("WHERE")
                if table_end < 0:
                    table_end = after_from.find("GROUP BY")
                if table_end < 0:
                    table_end = after_from.find("ORDER BY")
                if table_end < 0:
                    table_end = after_from.find("LIMIT")

                if table_end > 0:
                    # Insert the sampling clause before the next clause
                    modified_query = (
                        before_from
                        + after_from[:table_end].strip()
                        + sampling_clause
                        + " "
                        + after_from[table_end:]
                    )
                    return modified_query
                else:
                    # No additional clauses, add sampling at the end
                    return before_from + after_from.strip() + sampling_clause

        return base_query


class BasicProfileStrategy(ProfileStrategy):
    """
    Basic profiling strategy that generates simple row count and size statistics.
    Useful for quick scans, large tables, or when full profiling fails.
    """

    def generate_profile_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: Optional[List[str]] = None,
    ) -> str:
        """Generate a basic profiling query with minimal column selection."""
        # Check for custom SQL first
        custom_sql = self.config.get_custom_sql_for_table(table.name)
        if custom_sql:
            return custom_sql

        # Basic query just counts rows and gets a few key metrics
        query = f"""
        SELECT
            COUNT(*) as row_count,
            CURRENT_TIMESTAMP() as profile_time
        FROM
            `{project}.{schema}.{table.name}`
        """

        # Add partition filters if provided
        if partition_filters and len(partition_filters) > 0:
            query += f"\nWHERE {' AND '.join(partition_filters)}"

        # Apply sampling for large tables
        return self.apply_sampling_clause(table, query)

    def extract_profile_data(
        self,
        query_results: List[Any],
        table: BigqueryTable,
    ) -> Dict[str, Any]:
        """Extract basic profile data from query results."""
        columns_count = 0
        if hasattr(table, "columns") and table.columns:
            columns_count = len(table.columns)

        last_modified = None
        if hasattr(table, "last_modified"):
            last_modified = table.last_modified
        elif hasattr(table, "last_altered"):
            last_modified = table.last_altered

        profile_data: Dict[str, Any] = {
            "rowCount": 0,
            "columnCount": columns_count,
            "sizeInBytes": table.size_in_bytes,
            "created": table.created,
            "lastModified": last_modified,
            "external": table.external,
            "columns": {},
        }

        if query_results and len(query_results) > 0:
            row = query_results[0]
            profile_data["rowCount"] = getattr(row, "row_count", 0)
            profile_data["profileTime"] = getattr(
                row, "profile_time", datetime.utcnow().isoformat()
            )

        return profile_data


class StandardProfileStrategy(ProfileStrategy):
    """
    Standard profiling strategy that generates comprehensive statistics
    including column-level metrics, min/max values, and string pattern analysis.
    """

    def generate_profile_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: Optional[List[str]] = None,
    ) -> str:
        """Generate a comprehensive profiling query with column statistics."""
        # Check for custom SQL first
        custom_sql = self.config.get_custom_sql_for_table(table.name)
        if custom_sql:
            return custom_sql

        # For table-level only profiling, use a simpler query
        if self.config.profile_table_level_only:
            return BasicProfileStrategy(
                self._execute_query, self.config
            ).generate_profile_query(table, project, schema, partition_filters)

        # Build column expressions for each column type
        column_expressions = []

        # Make sure table.columns exists before iterating
        if hasattr(table, "columns") and table.columns:
            for column in table.columns:
                column_name = column.name
                column_type = column.data_type

                # Check for column type override
                type_override = self.config.get_column_type_override(
                    table.name, column_name
                )
                if type_override:
                    column_type = type_override

                # Skip unsupported types
                if column_type in ("ARRAY", "STRUCT", "JSON"):
                    continue

                # Base count expression for all columns
                column_expressions.append(
                    f"COUNT({column_name}) AS {column_name}_count"
                )

                # Column-specific expressions based on data type
                if column_type in (
                    "INTEGER",
                    "INT64",
                    "FLOAT",
                    "FLOAT64",
                    "NUMERIC",
                    "BIGNUMERIC",
                ):
                    # Numeric statistics
                    column_expressions.extend(
                        [
                            f"MIN({column_name}) AS {column_name}_min",
                            f"MAX({column_name}) AS {column_name}_max",
                            f"AVG({column_name}) AS {column_name}_avg",
                            f"STDDEV({column_name}) AS {column_name}_stddev",
                            f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count",
                            f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count",
                        ]
                    )
                elif column_type in ("STRING", "VARCHAR"):
                    # String statistics
                    column_expressions.extend(
                        [
                            f"MIN(LENGTH({column_name})) AS {column_name}_min_length",
                            f"MAX(LENGTH({column_name})) AS {column_name}_max_length",
                            f"AVG(LENGTH({column_name})) AS {column_name}_avg_length",
                            f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count",
                            f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count",
                        ]
                    )
                elif column_type in ("DATE", "DATETIME", "TIMESTAMP"):
                    # Date/time statistics
                    column_expressions.extend(
                        [
                            f"MIN({column_name}) AS {column_name}_min",
                            f"MAX({column_name}) AS {column_name}_max",
                            f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count",
                            f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count",
                        ]
                    )
                elif column_type in ("BOOL", "BOOLEAN"):
                    # Boolean statistics
                    column_expressions.extend(
                        [
                            f"COUNTIF({column_name} = TRUE) AS {column_name}_true_count",
                            f"COUNTIF({column_name} = FALSE) AS {column_name}_false_count",
                            f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count",
                            f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count",
                        ]
                    )
                else:
                    # Generic statistics for other types
                    column_expressions.extend(
                        [
                            f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count",
                            f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count",
                        ]
                    )

        # Construct the full query
        column_expression_str = ",\n            ".join(column_expressions)
        query = f"""
        SELECT
            COUNT(*) as row_count,
            CURRENT_TIMESTAMP() as profile_time,
            {column_expression_str}
        FROM
            `{project}.{schema}.{table.name}`
        """

        # Add partition filters if provided
        if partition_filters and len(partition_filters) > 0:
            query += f"\nWHERE {' AND '.join(partition_filters)}"

        # Apply sampling for large tables
        return self.apply_sampling_clause(table, query)

    def extract_profile_data(
        self,
        query_results: List[Any],
        table: BigqueryTable,
    ) -> Dict[str, Any]:
        """Extract comprehensive profile data from query results."""
        columns_count = 0
        if hasattr(table, "columns") and table.columns:
            columns_count = len(table.columns)

        last_modified = None
        if hasattr(table, "last_modified"):
            last_modified = table.last_modified
        elif hasattr(table, "last_altered"):
            last_modified = table.last_altered

        profile_data: Dict[str, Any] = {
            "rowCount": 0,
            "columnCount": columns_count,
            "sizeInBytes": table.size_in_bytes,
            "created": table.created,
            "lastModified": last_modified,
            "external": table.external,
            "columns": {},
        }

        if not query_results or len(query_results) == 0:
            return profile_data

        row = query_results[0]
        profile_data["rowCount"] = getattr(row, "row_count", 0)
        profile_data["profileTime"] = getattr(
            row, "profile_time", datetime.utcnow().isoformat()
        )

        # Process column-level statistics
        if hasattr(table, "columns") and table.columns:
            for column in table.columns:
                column_name = column.name
                column_type = column.data_type

                # Initialize column data
                column_data: Dict[str, Any] = {
                    "name": column_name,
                    "type": column_type,
                    "description": column.description or "",
                    "nullable": column.nullable,
                }

                # Add common statistics
                self._add_common_column_stats(column_data, row, column_name)

                # Process type-specific statistics
                if column_type in (
                    "INTEGER",
                    "INT64",
                    "FLOAT",
                    "FLOAT64",
                    "NUMERIC",
                    "BIGNUMERIC",
                ):
                    self._add_numeric_stats(column_data, row, column_name)
                elif column_type in ("STRING", "VARCHAR"):
                    self._add_string_stats(column_data, row, column_name)
                elif column_type in ("DATE", "DATETIME", "TIMESTAMP"):
                    self._add_datetime_stats(column_data, row, column_name)
                elif column_type in ("BOOL", "BOOLEAN"):
                    self._add_boolean_stats(column_data, row, column_name)

                # Add to columns dictionary with type check
                if "columns" in profile_data and isinstance(
                    profile_data["columns"], dict
                ):
                    profile_data["columns"][column_name] = column_data

        return profile_data

    def _add_common_column_stats(
        self, column_data: Dict[str, Any], row: Any, column_name: str
    ) -> None:
        """Add common statistics for all column types."""
        # Add count data
        count_attr = f"{column_name}_count"
        if hasattr(row, count_attr):
            column_data["count"] = getattr(row, count_attr)

        # Add null count data
        null_count_attr = f"{column_name}_null_count"
        if hasattr(row, null_count_attr):
            column_data["nullCount"] = getattr(row, null_count_attr)

        # Add distinct count data
        distinct_count_attr = f"{column_name}_distinct_count"
        if hasattr(row, distinct_count_attr):
            column_data["distinctCount"] = getattr(row, distinct_count_attr)

    def _add_numeric_stats(
        self, column_data: Dict[str, Any], row: Any, column_name: str
    ) -> None:
        """Add statistics specific to numeric columns."""
        if hasattr(row, f"{column_name}_min"):
            column_data["min"] = getattr(row, f"{column_name}_min")
        if hasattr(row, f"{column_name}_max"):
            column_data["max"] = getattr(row, f"{column_name}_max")
        if hasattr(row, f"{column_name}_avg"):
            column_data["avg"] = getattr(row, f"{column_name}_avg")
        if hasattr(row, f"{column_name}_stddev"):
            column_data["stdDev"] = getattr(row, f"{column_name}_stddev")

    def _add_string_stats(
        self, column_data: Dict[str, Any], row: Any, column_name: str
    ) -> None:
        """Add statistics specific to string columns."""
        if hasattr(row, f"{column_name}_min_length"):
            column_data["minLength"] = getattr(row, f"{column_name}_min_length")
        if hasattr(row, f"{column_name}_max_length"):
            column_data["maxLength"] = getattr(row, f"{column_name}_max_length")
        if hasattr(row, f"{column_name}_avg_length"):
            column_data["avgLength"] = getattr(row, f"{column_name}_avg_length")

    def _add_datetime_stats(
        self, column_data: Dict[str, Any], row: Any, column_name: str
    ) -> None:
        """Add statistics specific to date/time columns."""
        if hasattr(row, f"{column_name}_min"):
            column_data["min"] = getattr(row, f"{column_name}_min")
        if hasattr(row, f"{column_name}_max"):
            column_data["max"] = getattr(row, f"{column_name}_max")

    def _add_boolean_stats(
        self, column_data: Dict[str, Any], row: Any, column_name: str
    ) -> None:
        """Add statistics specific to boolean columns."""
        if hasattr(row, f"{column_name}_true_count"):
            column_data["trueCount"] = getattr(row, f"{column_name}_true_count")
        if hasattr(row, f"{column_name}_false_count"):
            column_data["falseCount"] = getattr(row, f"{column_name}_false_count")


class HistogramProfileStrategy(ProfileStrategy):
    """
    Advanced profiling strategy that includes histogram generation
    for numeric, string, and datetime columns.
    """

    def generate_profile_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: Optional[List[str]] = None,
    ) -> str:
        """Generate a profiling query with histogram analysis."""
        # First, get the standard profile query
        standard_strategy = StandardProfileStrategy(self._execute_query, self.config)
        standard_query = standard_strategy.generate_profile_query(
            table, project, schema, partition_filters
        )

        # For histogram profiling, we need to run separate queries for each column
        # to generate the histograms. This single query is just the standard profile.
        return standard_query

    def extract_profile_data(
        self,
        query_results: List[Any],
        table: BigqueryTable,
    ) -> Dict[str, Any]:
        """
        Extract profile data with histogram information.
        This includes running additional queries to generate histograms.
        """
        # First, extract standard profile data
        standard_strategy = StandardProfileStrategy(self._execute_query, self.config)
        profile_data = standard_strategy.extract_profile_data(query_results, table)

        # Add histograms for selected columns
        # Note: In a real implementation, this would run additional queries
        # to generate histogram data for each column

        return profile_data

    def generate_histogram_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        column_name: str,
        column_type: str,
        partition_filters: Optional[List[str]] = None,
        num_buckets: int = 10,
    ) -> str:
        """Generate a query to create a histogram for a specific column."""
        where_clause = ""
        if partition_filters and len(partition_filters) > 0:
            where_clause = f"WHERE {' AND '.join(partition_filters)}"

        if column_type in (
            "INTEGER",
            "INT64",
            "FLOAT",
            "FLOAT64",
            "NUMERIC",
            "BIGNUMERIC",
        ):
            # Numeric histogram
            return f"""
            WITH stats AS (
                SELECT 
                    MIN({column_name}) as min_val,
                    MAX({column_name}) as max_val
                FROM 
                    `{project}.{schema}.{table.name}`
                {where_clause}
            ),
            bucket_size AS (
                SELECT (max_val - min_val) / {num_buckets} as size
                FROM stats
            ),
            buckets AS (
                SELECT 
                    FLOOR(({column_name} - stats.min_val) / bucket_size.size) as bucket_num,
                    stats.min_val + (FLOOR(({column_name} - stats.min_val) / bucket_size.size) * bucket_size.size) as bucket_start,
                    stats.min_val + ((FLOOR(({column_name} - stats.min_val) / bucket_size.size) + 1) * bucket_size.size) as bucket_end,
                    COUNT(*) as count
                FROM 
                    `{project}.{schema}.{table.name}`, stats, bucket_size
                {where_clause}
                GROUP BY bucket_num, bucket_start, bucket_end
                ORDER BY bucket_num
            )
            SELECT * FROM buckets
            """
        elif column_type in ("DATE", "DATETIME", "TIMESTAMP"):
            # Date histogram (group by day, week, month, etc. based on range)
            return f"""
            WITH date_counts AS (
                SELECT 
                    CAST({column_name} AS DATE) as date_val,
                    COUNT(*) as count
                FROM 
                    `{project}.{schema}.{table.name}`
                {where_clause}
                GROUP BY date_val
                ORDER BY date_val
            )
            SELECT * FROM date_counts
            LIMIT 100
            """
        elif column_type in ("STRING", "VARCHAR"):
            # String histogram (most frequent values)
            return f"""
            SELECT 
                {column_name} as value,
                COUNT(*) as count
            FROM 
                `{project}.{schema}.{table.name}`
            {where_clause}
            GROUP BY value
            ORDER BY count DESC
            LIMIT 20
            """
        else:
            # Default histogram for other types
            return f"""
            SELECT 
                {column_name} as value,
                COUNT(*) as count
            FROM 
                `{project}.{schema}.{table.name}`
            {where_clause}
            GROUP BY value
            ORDER BY count DESC
            LIMIT 20
            """

    def process_histogram_results(
        self,
        histogram_results: List[Any],
        column_name: str,
        column_type: str,
    ) -> Dict[str, Any]:
        """Process histogram query results into a standardized format."""
        histogram_data: Dict[str, Any] = {
            "columnName": column_name,
            "bins": [],
        }

        if not histogram_results:
            return histogram_data

        if column_type in (
            "INTEGER",
            "INT64",
            "FLOAT",
            "FLOAT64",
            "NUMERIC",
            "BIGNUMERIC",
        ):
            # Process numeric histogram
            for row in histogram_results:
                histogram_data["bins"].append(
                    {
                        "start": row.bucket_start,
                        "end": row.bucket_end,
                        "count": row.count,
                    }
                )
        elif column_type in ("DATE", "DATETIME", "TIMESTAMP"):
            # Process date histogram
            for row in histogram_results:
                histogram_data["bins"].append(
                    {
                        "date": row.date_val,
                        "count": row.count,
                    }
                )
        else:
            # Process generic histogram
            for row in histogram_results:
                histogram_data["bins"].append(
                    {
                        "value": row.value,
                        "count": row.count,
                    }
                )

        return histogram_data


class PartitionColumnProfileStrategy(ProfileStrategy):
    """
    Strategy optimized for profiling partition columns only.
    Useful for very large tables where full profiling is too expensive.
    """

    def generate_profile_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: Optional[List[str]] = None,
    ) -> str:
        """Generate a query focusing only on partition columns."""
        # First identify partition columns
        from datahub.ingestion.source.bigquery_v2.profiling_table_metadata_manager import (
            BigQueryTableMetadataManager,
        )

        metadata_manager = BigQueryTableMetadataManager(self._execute_query)
        metadata = metadata_manager.get_table_metadata(table, project, schema)
        partition_columns = metadata.get("partition_columns", {})

        if not partition_columns:
            # Fall back to basic profiling if no partition columns
            return BasicProfileStrategy(
                self._execute_query, self.config
            ).generate_profile_query(table, project, schema, partition_filters)

        # Build expressions only for partition columns
        column_expressions: List[str] = []

        for column_name, column_type in partition_columns.items():
            # Base count expression
            column_expressions.append(f"COUNT({column_name}) AS {column_name}_count")
            column_expressions.append(
                f"COUNT(DISTINCT {column_name}) AS {column_name}_distinct_count"
            )
            column_expressions.append(
                f"COUNTIF({column_name} IS NULL) AS {column_name}_null_count"
            )

            # Type-specific expressions
            if column_type in (
                "INTEGER",
                "INT64",
                "FLOAT",
                "FLOAT64",
                "NUMERIC",
                "BIGNUMERIC",
            ) or column_type in ("DATE", "DATETIME", "TIMESTAMP"):
                column_expressions.extend(
                    [
                        f"MIN({column_name}) AS {column_name}_min",
                        f"MAX({column_name}) AS {column_name}_max",
                    ]
                )

        # Build the query
        query = f"""
        SELECT
            COUNT(*) as row_count,
            CURRENT_TIMESTAMP() as profile_time,
            {", ".join(column_expressions)}
        FROM
            `{project}.{schema}.{table.name}`
        """

        # Add partition filters if provided
        if partition_filters and len(partition_filters) > 0:
            query += f"\nWHERE {' AND '.join(partition_filters)}"

        # Apply sampling for large tables
        return self.apply_sampling_clause(table, query)

    def extract_profile_data(
        self,
        query_results: List[Any],
        table: BigqueryTable,
    ) -> Dict[str, Any]:
        """Extract profile data focusing on partition columns."""
        # First identify partition columns
        from datahub.ingestion.source.bigquery_v2.profiling_table_metadata_manager import (
            BigQueryTableMetadataManager,
        )

        metadata_manager = BigQueryTableMetadataManager(self._execute_query)
        metadata = metadata_manager.get_table_metadata(
            table, project="", schema=""
        )  # Project/schema not needed here
        partition_columns = metadata.get("partition_columns", {})

        columns_count = 0
        if hasattr(table, "columns") and table.columns:
            columns_count = len(table.columns)

        last_modified = None
        if hasattr(table, "last_modified"):
            last_modified = table.last_modified
        elif hasattr(table, "last_altered"):
            last_modified = table.last_altered

        profile_data: Dict[str, Any] = {
            "rowCount": 0,
            "columnCount": columns_count,
            "sizeInBytes": table.size_in_bytes,
            "created": table.created,
            "lastModified": last_modified,
            "external": table.external,
            "columns": {},
        }

        if not query_results or len(query_results) == 0:
            return profile_data

        row = query_results[0]
        profile_data["rowCount"] = getattr(row, "row_count", 0)
        profile_data["profileTime"] = getattr(
            row, "profile_time", datetime.utcnow().isoformat()
        )

        # Process only partition columns
        for column_name, column_type in partition_columns.items():
            # Initialize column data
            column_data: Dict[str, Any] = {
                "name": column_name,
                "type": column_type,
                "isPartitionColumn": True,
            }

            # Add count data
            count_attr = f"{column_name}_count"
            if hasattr(row, count_attr):
                column_data["count"] = getattr(row, count_attr)

            # Add null count data
            null_count_attr = f"{column_name}_null_count"
            if hasattr(row, null_count_attr):
                column_data["nullCount"] = getattr(row, null_count_attr)

            # Add distinct count data
            distinct_count_attr = f"{column_name}_distinct_count"
            if hasattr(row, distinct_count_attr):
                column_data["distinctCount"] = getattr(row, distinct_count_attr)

            # Type-specific statistics
            if column_type in (
                "INTEGER",
                "INT64",
                "FLOAT",
                "FLOAT64",
                "NUMERIC",
                "BIGNUMERIC",
                "DATE",
                "DATETIME",
                "TIMESTAMP",
            ):
                if hasattr(row, f"{column_name}_min"):
                    column_data["min"] = getattr(row, f"{column_name}_min")
                if hasattr(row, f"{column_name}_max"):
                    column_data["max"] = getattr(row, f"{column_name}_max")

            # Add to columns dictionary with type check
            if "columns" in profile_data and isinstance(profile_data["columns"], dict):
                profile_data["columns"][column_name] = column_data

        return profile_data


def get_profile_strategy(
    profile_type: str,
    execute_query_callback: Callable[[str, Optional[str], int, int], List[Any]],
    config: BigQueryProfilerConfig,
) -> ProfileStrategy:
    """
    Factory method to get the appropriate profile strategy.

    Args:
        profile_type: Type of profiling strategy to use
        execute_query_callback: Function to execute BigQuery queries
        config: Profiling configuration

    Returns:
        Instance of a ProfileStrategy implementation
    """
    if profile_type == "basic":
        return BasicProfileStrategy(execute_query_callback, config)
    elif profile_type == "partition_columns_only":
        return PartitionColumnProfileStrategy(execute_query_callback, config)
    elif profile_type == "histogram":
        return HistogramProfileStrategy(execute_query_callback, config)
    else:
        # Default to standard profiling
        return StandardProfileStrategy(execute_query_callback, config)

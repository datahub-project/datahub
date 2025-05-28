import logging
from typing import Any, Dict, Optional, Set

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    PartitionInfo,
)

logger = logging.getLogger(__name__)


class BigQueryTableMetadataManager:
    """
    Manager for BigQuery table metadata operations.
    Handles metadata extraction, caching, and DDL parsing.
    """

    def __init__(self, execute_query_callback):
        """
        Initialize the table metadata manager.

        Args:
            execute_query_callback: Callback function to execute BigQuery queries
        """
        self._execute_query = execute_query_callback
        self._column_name_mapping = {}
        self._metadata_cache = {}

    def get_table_metadata(
        self, table: BigqueryTable, project: str, schema: str
    ) -> Dict[str, Any]:
        """
        Get comprehensive metadata about a table efficiently.
        Uses a multi-tiered approach to minimize API calls while gathering all needed information.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name

        Returns:
            Dictionary containing table metadata
        """
        # Check cache first
        cache_key = f"{project}.{schema}.{table.name}"
        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]

        # Start with basic info from the table object
        metadata = self._fetch_basic_table_metadata(table)

        # Track if we need to query INFORMATION_SCHEMA
        need_schema_query = len(metadata["partition_columns"]) == 0 or any(
            v is None for v in metadata["partition_columns"].values()
        )

        # If we still need more information or no partition columns were found,
        # fetch from INFORMATION_SCHEMA with a single efficient query
        if need_schema_query:
            metadata = self._fetch_schema_info(table, project, schema, metadata)

        # If still no partition columns but we have DDL, parse it
        if not metadata["partition_columns"] and metadata.get("ddl"):
            if "PARTITION BY" in metadata["ddl"].upper():
                self._extract_partitioning_from_ddl(
                    metadata["ddl"], metadata, project, schema, table.name
                )

        # If we need more specific table stats and didn't get them above, fetch them
        if not table.external:  # Only for internal tables
            metadata = self._fetch_table_stats(project, schema, table.name, metadata)

        # Cache the result
        self._metadata_cache[cache_key] = metadata
        return metadata

    def _fetch_basic_table_metadata(self, table: BigqueryTable) -> Dict[str, Any]:
        """Get basic metadata from table object."""
        metadata: Dict[str, Any] = {
            "partition_columns": {},
            "clustering_columns": {},
            "row_count": table.rows_count,
            "size_bytes": table.size_in_bytes,
            "is_external": table.external,
            "ddl": table.ddl,
            "creation_time": table.created,
            "last_modified_time": table.last_altered,
            "columns": {},  # Add column info for profiling
        }

        # Add column information
        if hasattr(table, "columns") and table.columns:
            for col in table.columns:
                metadata["columns"][col.name] = {
                    "type": col.data_type,
                    "description": col.description,
                    "mode": col.mode,
                }

        # Process partition info if available
        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                for field in table.partition_info.fields:
                    metadata["partition_columns"][field] = None

            if table.partition_info.columns:
                for col in table.partition_info.columns:
                    if col and col.name:
                        metadata["partition_columns"][col.name] = col.data_type

        # Process clustering fields if available
        if table.clustering_fields:
            for i, field in enumerate(table.clustering_fields):
                metadata["clustering_columns"][field] = {"position": i}

        return metadata

    def _fetch_schema_info(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch schema information from INFORMATION_SCHEMA."""
        try:
            # Get table metadata from TABLES
            tables_query = f"""
            SELECT 
                table_name,
                table_type,
                row_count,
                creation_time,
                last_modified_time
            FROM 
                `{project}.{schema}.INFORMATION_SCHEMA.TABLES`
            WHERE 
                table_name = '{table.name}'
            """
            tables_result = self._execute_query(tables_query)
            if tables_result and len(tables_result) > 0:
                table_info = tables_result[0]
                metadata.update(
                    {
                        "table_type": table_info["table_type"],
                        "rows_count": table_info["row_count"],
                        "created_at": table_info["creation_time"],
                        "last_modified": table_info["last_modified_time"],
                    }
                )

            # Get partition and clustering info from COLUMNS
            columns_query = f"""
            SELECT 
                column_name,
                data_type,
                is_partitioning_column,
                clustering_ordinal_position,
                is_nullable
            FROM 
                `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE 
                table_name = '{table.name}'
            ORDER BY 
                ordinal_position
            """
            columns_result = self._execute_query(columns_query)

            partition_columns = {}
            clustering_columns = []

            for col in columns_result:
                if col["is_partitioning_column"] == "YES":
                    partition_columns[col["column_name"]] = {
                        "type": col["data_type"],
                        "nullable": col["is_nullable"] == "YES",
                    }
                if col["clustering_ordinal_position"] is not None:
                    clustering_columns.append(col["column_name"])

            metadata["partition_columns"] = partition_columns
            if clustering_columns:
                metadata["clustering_columns"] = clustering_columns

            return metadata
        except Exception as e:
            logger.warning(f"Error fetching schema info: {e}")
            return metadata

    def _fetch_table_stats(
        self, project: str, schema: str, table_name: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get additional statistics for tables using region-aware queries.

        Args:
            project: Project ID
            schema: Dataset name
            table_name: Table name
            metadata: Existing metadata dictionary

        Returns:
            Updated metadata dictionary
        """
        if (
            metadata.get("row_count") is not None
            and metadata.get("size_bytes") is not None
            and metadata.get("last_modified_time") is not None
        ):
            return metadata  # Already have all the stats

        # Get the correct INFORMATION_SCHEMA path
        info_schema_path = self._get_information_schema_path(project)

        try:
            # Try using TABLE_STORAGE for detailed stats with region handling
            storage_query = f"""
            SELECT 
                total_rows as row_count,
                total_logical_bytes as size_bytes,
                storage_last_modified_time as last_modified_time
            FROM 
                `{info_schema_path}.TABLE_STORAGE`
            WHERE 
                table_name = '{table_name}'
            """

            storage_results = self._execute_query(
                storage_query, f"table_stats_{project}_{schema}_{table_name}"
            )

            if storage_results and len(storage_results) > 0:
                row = storage_results[0]
                if hasattr(row, "row_count") and row.row_count is not None:
                    metadata["row_count"] = row.row_count
                if hasattr(row, "size_bytes") and row.size_bytes is not None:
                    metadata["size_bytes"] = row.size_bytes
                if (
                    hasattr(row, "last_modified_time")
                    and row.last_modified_time is not None
                ):
                    metadata["last_modified_time"] = row.last_modified_time
        except Exception as e:
            logger.info(f"TABLE_STORAGE query failed: {e}, trying alternative source")

            # Fall back to TABLES view
            try:
                basic_info_query = f"""
                SELECT
                    row_count,
                    size_bytes,
                    last_modified_time
                FROM
                    `{project}.{schema}.INFORMATION_SCHEMA.TABLES`
                WHERE
                    table_name = '{table_name}'
                """

                basic_info_results = self._execute_query(
                    basic_info_query,
                    f"basic_info_{project}_{schema}_{table_name}",
                    timeout=30,
                )

                if basic_info_results and len(basic_info_results) > 0:
                    row = basic_info_results[0]
                    if hasattr(row, "row_count") and row.row_count is not None:
                        metadata["row_count"] = row.row_count
                    if hasattr(row, "size_bytes") and row.size_bytes is not None:
                        metadata["size_bytes"] = row.size_bytes
                    if (
                        hasattr(row, "last_modified_time")
                        and row.last_modified_time is not None
                    ):
                        metadata["last_modified_time"] = row.last_modified_time
            except Exception as inner_e:
                logger.warning(f"Failed to get alternative table info: {inner_e}")

        return metadata

    def _get_information_schema_path(
        self, project_id: str, dataset_name: Optional[str] = None
    ) -> str:
        """
        Get the correct path for INFORMATION_SCHEMA based on the project's region.

        Args:
            project_id: BigQuery project ID
            dataset_name: Optional dataset name for dataset-specific schemas

        Returns:
            Fully qualified path to INFORMATION_SCHEMA
        """
        # Try regions in order - US, EU, etc.
        regions = ["region-us", "region-eu", "region-asia"]

        for region in regions:
            try:
                # Try a simple test query with this region
                test_query = f"""
                SELECT 1 
                FROM `{region}.INFORMATION_SCHEMA.TABLES`
                LIMIT 1
                """
                results = self._execute_query(test_query, timeout=5)
                if results:
                    # Region works, return it
                    path = f"{region}.INFORMATION_SCHEMA"
                    logger.info(f"Using {path} for project {project_id}")
                    return path
            except Exception as e:
                logger.debug(f"Region {region} not accessible: {e}")

        # If no region-specific path works, fall back to project-level
        fallback_path = f"{project_id}.INFORMATION_SCHEMA"
        logger.info(f"Using fallback path {fallback_path} for project {project_id}")
        return fallback_path

    def _extract_partitioning_from_ddl(
        self,
        ddl: str,
        metadata: Dict[str, Any],
        project: str,
        schema: str,
        table_name: str,
    ) -> None:
        """
        Extract partitioning information from DDL.

        Args:
            ddl: DDL for the table
            metadata: Metadata dictionary to update
            project: Project ID
            schema: Schema name
            table_name: Table name
        """
        if not ddl:
            return

        # Normalize DDL for easier parsing, but preserve case for extraction
        ddl_upper = ddl.upper().replace("\n", " ").replace("\t", " ")
        ddl_norm = ddl.replace("\n", " ").replace("\t", " ")  # Preserve case

        # Track found partition cols
        found_partition_cols: Set[str] = set()

        # Case 1: Standard PARTITION BY column
        if "PARTITION BY" in ddl_upper:
            # Use case-preserved version for extraction
            partition_start = ddl_upper.find("PARTITION BY")
            if partition_start >= 0:
                # Get the same position in the original case version
                self._extract_partition_by_clause(
                    ddl_upper[partition_start:],
                    ddl_norm[partition_start:],
                    found_partition_cols,
                )

        # Case 2: Check for external table partitioning with URI pattern
        if (
            metadata.get("is_external")
            and "EXTERNAL" in ddl_upper
            and "URI_TEMPLATE" in ddl_upper
        ):
            uri_start = ddl_upper.find("URI_TEMPLATE")
            if uri_start >= 0:
                # Use the same position in the original case version
                self._extract_uri_template_partitioning(
                    ddl_upper[uri_start:], ddl_norm[uri_start:], found_partition_cols
                )

        # Get data types for identified partition columns
        if found_partition_cols:
            self._get_partition_column_types(
                found_partition_cols, metadata, project, schema, table_name
            )

        # Special case: If no partition columns found but table has partition_info
        # with time-based partitioning, add the implicit _PARTITIONTIME column
        if (
            not metadata["partition_columns"]
            and isinstance(metadata.get("partition_info"), PartitionInfo)
            and metadata["partition_info"].type in ["DAY", "MONTH", "YEAR", "HOUR"]
        ):
            metadata["partition_columns"]["_PARTITIONTIME"] = "TIMESTAMP"

    def _extract_partition_by_clause(
        self, upper_clause: str, original_clause: str, found_partition_cols: Set[str]
    ) -> None:
        """Extract partition columns from a PARTITION BY clause in DDL."""
        try:
            # Extract up to the next major clause using upper case for detection
            partition_clause_upper = self._extract_partition_clause(upper_clause)

            # Get the corresponding part from the original case
            # The length should be the same since we're just changing case
            partition_clause = original_clause[: len(partition_clause_upper)]

            # Remove the "PARTITION BY" part (using upper for the length)
            partition_by_len = len("PARTITION BY")
            if partition_clause_upper.startswith("PARTITION BY"):
                partition_clause_upper = partition_clause_upper[
                    partition_by_len:
                ].strip()
                partition_clause = partition_clause[partition_by_len:].strip()

            # Handle different partition specifications using both versions
            if partition_clause_upper.startswith(
                "DATE("
            ) or partition_clause_upper.startswith("TIMESTAMP("):
                self._extract_date_timestamp_function(
                    partition_clause_upper, partition_clause, found_partition_cols
                )
            elif (
                "TIMESTAMP_TRUNC" in partition_clause_upper
                or "DATE_TRUNC" in partition_clause_upper
            ):
                self._extract_trunc_function(
                    partition_clause_upper, partition_clause, found_partition_cols
                )
            elif any(
                unit in partition_clause_upper
                for unit in ["DAY", "MONTH", "YEAR", "HOUR"]
            ):
                # This is a time-unit partitioning without explicit column
                # Use _PARTITIONTIME as the implicit partition column
                found_partition_cols.add("_PARTITIONTIME")
            else:
                # Simple column partitioning
                self._extract_simple_columns(partition_clause, found_partition_cols)
        except Exception as e:
            logger.warning(f"Error parsing PARTITION BY clause from DDL: {e}")

    def _extract_partition_clause(self, partition_clause: str) -> str:
        """Extract the partition clause up to the next major clause."""
        for end_token in ["OPTIONS", "CLUSTER BY", "AS SELECT", ";"]:
            end_pos = partition_clause.find(end_token)
            if end_pos > 0:
                partition_clause = partition_clause[:end_pos].strip()
        return partition_clause

    def _extract_date_timestamp_function(
        self, upper_clause: str, original_clause: str, found_partition_cols: set
    ) -> None:
        """Extract column from DATE(...) or TIMESTAMP(...) function."""
        col_start = upper_clause.find("(") + 1
        col_end = upper_clause.find(")")
        if 0 < col_start < col_end:
            # Use the original case version to get the actual column name
            col_name = original_clause[col_start:col_end].strip(", `'\"")
            found_partition_cols.add(col_name)

    def _extract_trunc_function(
        self, upper_clause: str, original_clause: str, found_partition_cols: set
    ) -> None:
        """Extract column from TIMESTAMP_TRUNC(column, DAY) or DATE_TRUNC(column, DAY)."""
        trunc_start = upper_clause.find("(") + 1
        trunc_end = upper_clause.find(",")
        if trunc_end == -1:  # No comma found
            trunc_end = upper_clause.find(")")
        if 0 < trunc_start < trunc_end:
            # Use the original case version to get the actual column name
            col_name = original_clause[trunc_start:trunc_end].strip(", `'\"")
            found_partition_cols.add(col_name)

    def _extract_simple_columns(
        self, partition_clause: str, found_partition_cols: set
    ) -> None:
        """Extract simple column names from partition clause."""
        for part in partition_clause.split():
            # Get the part without quotes, parentheses, etc.
            cleaned_col = part.strip(", `'\"()").split(".")[-1]

            # Skip known keywords and empty strings (check lowercase)
            if cleaned_col and cleaned_col.upper() not in [
                "DATE",
                "TIMESTAMP",
                "BY",
                "PARTITION",
            ]:
                # Keep the original case
                found_partition_cols.add(cleaned_col)

    def _extract_uri_template_partitioning(
        self, upper_part: str, original_part: str, found_partition_cols: set
    ) -> None:
        """Extract partition columns from URI_TEMPLATE in external table DDL."""
        try:
            end_pos = upper_part.find(",")
            if end_pos > 0:
                upper_part = upper_part[:end_pos].strip(" ='\"`")
                original_part = original_part[:end_pos].strip(" ='\"`")

            # Look for Hive-style partitioning in URI template
            if "{" in original_part and "}" in original_part:
                parts = original_part.split("{")
                for _i, part in enumerate(parts):
                    if "}" in part:
                        # Extract the part between { and }
                        partition_var = part.split("}")[0].strip()
                        # Keep the original case
                        found_partition_cols.add(partition_var)
        except Exception as e:
            logger.debug(f"Error parsing URI_TEMPLATE for partitioning: {e}")

    def _get_partition_column_types(
        self,
        found_partition_cols: set,
        metadata: Dict[str, Any],
        project: str,
        schema: str,
        table_name: str,
    ) -> None:
        """Get data types for identified partition columns."""
        # For testing compatibility - if no columns, no query needed
        if not found_partition_cols:
            return

        try:
            # Construct query
            cols_list = "', '".join(found_partition_cols)
            col_types_query = f"""
            SELECT 
                column_name, 
                data_type
            FROM 
                `{project}.INFORMATION_SCHEMA.COLUMNS`
            WHERE 
                table_name = '{table_name}'
                AND column_name IN ('{cols_list}')
            """

            # Execute query
            results = self._execute_query(
                col_types_query,
                f"partition_cols_types_{project}_{schema}_{table_name}",
            )

            # Process results
            for result in results:
                try:
                    column_name = result.column_name
                    data_type = result.data_type
                    # Store the data type in the metadata
                    metadata["partition_columns"][column_name] = data_type
                except AttributeError:
                    # Handle regular dictionary results (just in case)
                    if hasattr(result, "items"):
                        column_name = result.get("column_name")
                        data_type = result.get("data_type")
                        if column_name and data_type:
                            metadata["partition_columns"][column_name] = data_type

        except Exception as e:
            logger.warning(f"Error fetching partition column types: {e}")

        # Add missing columns with None type
        for col in found_partition_cols:
            if col not in metadata["partition_columns"]:
                metadata["partition_columns"][col] = None

    def extract_partitioning_from_ddl(
        self,
        ddl: str,
        project: str = "",
        schema: str = "",
        table_name: str = "",
    ) -> Dict[str, str]:
        """
        Extract partitioning information from DDL and return as dictionary.
        This method is used by tests directly.

        Args:
            ddl: The DDL string to parse
            project: Optional project ID
            schema: Optional schema/dataset name
            table_name: Optional table name

        Returns:
            Dictionary mapping partition column names to their types
        """
        # Create a temporary metadata dictionary
        metadata: Dict[str, Any] = {"partition_columns": {}}

        # Call the internal method
        self._extract_partitioning_from_ddl(ddl, metadata, project, schema, table_name)

        # Return just the partition columns
        return metadata.get("partition_columns", {})

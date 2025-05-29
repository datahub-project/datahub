import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from dateutil.relativedelta import relativedelta

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable

# Import the new specialized components
from datahub.ingestion.source.bigquery_v2.profiling_cache_manager import (
    BigQueryCacheManager,
)
from datahub.ingestion.source.bigquery_v2.profiling_config import BigQueryProfilerConfig
from datahub.ingestion.source.bigquery_v2.profiling_filter_builder import (
    BigQueryFilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling_partition_manager import (
    BigQueryPartitionManager,
)
from datahub.ingestion.source.bigquery_v2.profiling_strategy import (
    BasicProfileStrategy,
    get_profile_strategy,
)
from datahub.ingestion.source.bigquery_v2.profiling_table_metadata_manager import (
    BigQueryTableMetadataManager,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigqueryProfiler(GenericProfiler):
    """
    Refactored BigQuery profiler that uses the strategy pattern and specialized
    components to better manage the complexity of profiling BigQuery tables.
    """

    config: BigQueryV2Config
    report: BigQueryV2Report
    cache_manager: BigQueryCacheManager
    table_metadata_manager: BigQueryTableMetadataManager
    filter_builder: BigQueryFilterBuilder
    partition_manager: BigQueryPartitionManager
    execute_query: Callable
    profiler_config: BigQueryProfilerConfig
    _problematic_tables: Set[str]
    _table_strategies: Dict[str, str]
    _successful_filters_cache: Dict[str, List[str]]
    _queried_tables: Set[str]

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report

        # Create profiler config from BigQueryV2Config
        self.profiler_config = self._create_profiler_config()

        # Initialize specialized components with dependency injection
        self.cache_manager = BigQueryCacheManager(
            max_cache_size=self.profiler_config.max_cache_size
        )

        # Set up the execute_query function with proper caching
        self.execute_query = self._create_execute_query_function()

        # Initialize components in proper order (dependency injection)
        self.table_metadata_manager = BigQueryTableMetadataManager(self.execute_query)
        self.filter_builder = BigQueryFilterBuilder(self.execute_query)
        self.partition_manager = BigQueryPartitionManager(
            self.execute_query, self.filter_builder
        )

        # Track tables that had issues during profiling
        self._problematic_tables: Set[str] = set()

        # Track profiling strategies for each table
        self._table_strategies: Dict[str, str] = {}

        # For compatibility with tests
        self._successful_filters_cache: Dict[str, List[str]] = {}
        self._queried_tables: Set[str] = set()

    def _create_profiler_config(self) -> BigQueryProfilerConfig:
        """Convert BigQueryV2Config to BigQueryProfilerConfig"""
        # Extract profiling parameters from config
        sample_size = getattr(self.config.profiling, "sample_size", 100_000)
        # Use the query_timeout directly from BigQueryV2Config
        query_timeout = self.config.query_timeout
        max_queries_per_table = getattr(self.config.profiling, "max_workers", 50)
        profile_table_level_only = getattr(
            self.config.profiling, "profile_table_level_only", False
        )
        tables_pattern = getattr(self.config, "tables", None)

        # Handle schema pattern from BigQueryV2Config
        schema_pattern = None
        config_schema = getattr(self.config, "schema", None)
        if config_schema is not None:
            if hasattr(config_schema, "allow"):
                schema_pattern = config_schema.allow
            elif isinstance(config_schema, str):
                schema_pattern = [config_schema]
            elif isinstance(config_schema, list):
                schema_pattern = config_schema

        external_table_sampling_percent = 0.1  # Default value
        large_table_sampling_percent = 1.0  # Default value

        # Create and return profiler config
        return BigQueryProfilerConfig(
            sample_size=sample_size,
            query_timeout=query_timeout,
            max_queries_per_table=max_queries_per_table,
            profile_table_level_only=profile_table_level_only,
            tables_pattern=tables_pattern,
            schema_pattern=schema_pattern,
            external_table_sampling_percent=external_table_sampling_percent,
            large_table_sampling_percent=large_table_sampling_percent,
        )

    def _create_execute_query_function(self) -> Callable:
        """
        Create a function for executing BigQuery queries with caching.
        """

        def execute_query(
            query: str,
            cache_key: Optional[str] = None,
            timeout: int = 60,  # Default 60 seconds
            max_retries: int = 2,
        ) -> List[Any]:
            # Check cache first if a cache key is provided
            if cache_key and cache_key in self.cache_manager._query_cache:
                cached_result = self.cache_manager.get_query_result(cache_key)
                # Ensure we never return None from the cache
                return [] if cached_result is None else cached_result

            # Get the BigQuery client from config
            client = self.config.get_bigquery_client()

            def execute_with_timeout() -> List[Any]:
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            lambda: list(client.query(query).result())
                        )
                        # Add a bit of extra time to the timeout for processing
                        result = future.result(timeout=timeout + 5)

                        # Cache the result if a cache key was provided
                        if cache_key and result is not None:
                            self.cache_manager.add_query_result(cache_key, result)

                        # Ensure we always return a list, never None
                        return [] if result is None else result
                except TimeoutError:
                    logger.warning(f"Query timed out after {timeout} seconds")
                    return []
                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"Query execution error: {error_msg}")
                    # Store the error message for partition extraction
                    if hasattr(self, "partition_manager") and self.partition_manager:
                        self.partition_manager._last_error_message = error_msg
                    return []

            # Execute the query with retries
            result = execute_with_timeout()
            retries = 0

            while not result and retries < max_retries:
                retries += 1
                logger.info(f"Retrying query, attempt {retries + 1}/{max_retries + 1}")
                result = execute_with_timeout()

            # Ensure we never return None
            if result is None:
                return []
            return result

        return execute_query

    def _should_profile_table(
        self, table: BigqueryTable, schema_name: str, db_name: str
    ) -> bool:
        """
        Determine if a table should be profiled based on configuration.

        Args:
            table: BigqueryTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            True if the table should be profiled, False otherwise
        """
        if not self.config.profiling.enabled:
            return False

        # Skip tables that had issues during profiling
        table_key = f"{db_name}.{schema_name}.{table.name}"
        if table_key in self._problematic_tables:
            logger.info(f"Skipping problematic table: {table_key}")
            return False

        # Check if table matches configuration patterns
        return self.profiler_config.should_profile_table(
            db_name, schema_name, table.name
        )

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """
        Create a profile request for a table, handling BigQuery-specific functionality.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            TableProfilerRequest if the table should be profiled, None otherwise
        """
        bq_table = cast(BigqueryTable, table)

        # Check if external table and if we should profile it
        if (
            hasattr(bq_table, "external")
            and bq_table.external
            and not getattr(self.config.profiling, "profile_external_tables", False)
        ):
            self.report.report_warning(
                title="Profile skipped for external table",
                message="Profile skipped as external table profiling is disabled",
                context=f"{db_name}.{schema_name}.{bq_table.name}",
            )
            return None

        # Get the basic profile request from parent class
        profile_request = super().get_profile_request(table, schema_name, db_name)
        if not profile_request:
            return None

        # The batch_kwargs will be set by get_batch_kwargs with partition information
        return profile_request

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """
        Get batch kwargs for profiling a table, handling partitions.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            Dictionary of batch kwargs
        """
        # Cast to BigqueryTable
        bq_table = cast(BigqueryTable, table)

        # Start with the basic batch kwargs
        batch_kwargs = {
            "schema": schema_name,  # Dataset name
            "table": bq_table.name,  # Table name
            "project": db_name,  # Project ID
        }

        # Handle partitioning
        try:
            custom_sql = None
            partition = None
            temp_table_name = None

            # First, see if we have partitioning
            if hasattr(bq_table, "partition_info") and bq_table.partition_info:
                logger.info(
                    f"Table {bq_table.name} has partition information, determining optimal filters"
                )

                # Use our partition_manager to get the right partition filters
                # This leverages all of our specialized partition handling logic
                partition_filters = (
                    self.partition_manager.get_required_partition_filters(
                        bq_table, db_name, schema_name
                    )
                )

                if partition_filters is None:
                    logger.warning(
                        f"Could not determine partition filters for {bq_table.name}, skipping profiling"
                    )
                    self.report.report_warning(
                        title="Profile skipped for partitioned table",
                        message="Profile skipped as partitioned table requires partition filters but none could be determined",
                        context=f"{db_name}.{schema_name}.{bq_table.name}",
                    )
                    return {}

                # If we have partition filters, create a temporary table or view with the partition filters
                if partition_filters:
                    logger.info(
                        f"Using partition filters for {bq_table.name}: {partition_filters}"
                    )
                    where_clause = " AND ".join(partition_filters)

                    # Store the partition ID for reference - use first filter as representative
                    partition = partition_filters[0] if partition_filters else None

                    # For BigQuery, we need to create a temporary table or view to ensure ALL queries use the partition filter
                    # This is critical - without this, the GE profiler will query the original table without filters
                    temp_table_name = f"temp_{bq_table.name}_{int(time.time())}"

                    # Use different approaches for external vs regular tables
                    if bq_table.external:
                        # For external tables, use a temporary view (more efficient, no data copy)
                        logger.info(
                            f"Creating temporary VIEW for external table {bq_table.name}"
                        )
                        create_temp_sql = f"""
                        CREATE OR REPLACE TEMPORARY VIEW `{temp_table_name}` AS
                        SELECT * 
                        FROM `{db_name}.{schema_name}.{bq_table.name}`
                        WHERE {where_clause}
                        """
                    else:
                        # For regular tables, use a temporary table
                        logger.info(
                            f"Creating temporary TABLE for regular table {bq_table.name}"
                        )
                        create_temp_sql = f"""
                        CREATE OR REPLACE TEMPORARY TABLE `{temp_table_name}` AS
                        SELECT * 
                        FROM `{db_name}.{schema_name}.{bq_table.name}`
                        WHERE {where_clause}
                        """

                    try:
                        # Execute the query to create the temporary table or view
                        logger.info(
                            f"Creating temporary {'view' if bq_table.external else 'table'} {temp_table_name} with partition filters"
                        )
                        self.execute_query(create_temp_sql, timeout=120)

                        # Update batch_kwargs to use the temporary table/view instead of the original
                        batch_kwargs["table"] = temp_table_name
                        # Remove schema since temp tables/views don't have a schema
                        del batch_kwargs["schema"]
                        # Add a flag to indicate we're using a temp table - use string "true"
                        # to ensure consistent types with other batch_kwargs values
                        batch_kwargs["is_temp_table"] = "true"
                        # Store the partition for reference - only if it's not None
                        if partition:
                            batch_kwargs["partition"] = partition
                    except Exception as e:
                        logger.warning(f"Failed to create temporary table: {str(e)}")
                        # Fall back to using custom SQL instead
                        custom_sql = f"""
                        SELECT * 
                        FROM `{db_name}.{schema_name}.{bq_table.name}`
                        WHERE {where_clause}
                        """

                else:
                    # No filters needed, but table is partitioned
                    logger.info(
                        f"Table {bq_table.name} is partitioned but no filters are required"
                    )

                    # Try the original approach as fallback if we couldn't get better filters
                    # but we know the table is partitioned
                    if not partition and bq_table.max_partition_id:
                        old_partition, old_custom_sql = (
                            self.generate_partition_profiler_query(
                                db_name,
                                schema_name,
                                bq_table,
                                getattr(
                                    self.config.profiling, "partition_datetime", None
                                ),
                            )
                        )

                        if old_partition and old_custom_sql:
                            logger.info(
                                f"Using fallback partition approach for {bq_table.name}"
                            )
                            partition = old_partition
                            custom_sql = old_custom_sql

            # Skip profiling if partitioning is disabled
            if partition is not None and not getattr(
                self.config.profiling, "partition_profiling_enabled", True
            ):
                logger.debug(
                    f"{db_name}.{schema_name}.{bq_table.name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
                )
                self.report.profiling_skipped_partition_profiling_disabled.append(
                    f"{db_name}.{schema_name}.{bq_table.name}"
                )
                return {}

            # For BigQuery, if we have custom SQL but no temp table, use it for profiling
            if custom_sql and not temp_table_name:
                logger.info(
                    f"Using custom SQL with partition filters for {bq_table.name}"
                )
                # Replace the schema/table with custom SQL to ensure partitioning is applied
                batch_kwargs["query"] = custom_sql
                # Remove schema and table when using custom SQL
                if "schema" in batch_kwargs:
                    del batch_kwargs["schema"]
                if "table" in batch_kwargs:
                    del batch_kwargs["table"]

            # Add a strategy tag so the GE profiler knows what type of profiling to use
            profile_strategy = self._select_profile_strategy(
                bq_table, db_name, schema_name
            )
            batch_kwargs["profile_strategy"] = profile_strategy

        except Exception as e:
            logger.warning(
                f"Error generating partition info for {db_name}.{schema_name}.{bq_table.name}: {str(e)}"
            )

        return batch_kwargs

    def _select_profile_strategy(
        self, table: BigqueryTable, project: str, schema: str
    ) -> str:
        """
        Select the appropriate profiling strategy based on table characteristics.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name

        Returns:
            String name of the profiling strategy to use
        """
        # Start with configuration preference
        if getattr(self.profiler_config, "profile_table_level_only", False):
            return "basic"

        if getattr(self.profiler_config, "profile_partition_columns_only", False):
            return "partition_columns_only"

        # Special case for external tables
        if table.external:
            # Simplified profiling is more reliable for external tables
            return "basic"

        # Check table size and complexity
        is_large_table = False
        if (
            table.size_in_bytes
            and table.size_in_bytes > 10_000_000_000
            or table.rows_count
            and table.rows_count > 50_000_000
        ):  # > 10 GB
            is_large_table = True

        # Count complex columns (arrays, structs, etc.)
        complex_columns = 0
        if hasattr(table, "columns") and table.columns:
            for col in table.columns:
                if col.data_type in ("ARRAY", "STRUCT", "JSON"):
                    complex_columns += 1

        # Large complex tables get simplified profiling
        if is_large_table and complex_columns > 5:
            return "basic"
        # Large tables get standard profiling
        elif (
            is_large_table
            or (table.size_in_bytes and table.size_in_bytes > 1_000_000_000)
            or complex_columns > 0
        ):
            return "standard"
        # Small, simple tables can get histogram profiling
        else:
            return "standard"  # Default to standard for now

    def _process_profile_results(
        self, results: List[Any], table: BigqueryTable, batch_kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process profile query results using the appropriate strategy.

        Args:
            results: Query results
            table: BigqueryTable instance
            batch_kwargs: Batch kwargs used for profiling

        Returns:
            Processed profile data
        """
        # Get the strategy used for this table
        strategy_name = batch_kwargs.get("profile_strategy", "standard")

        # Get strategy instance
        strategy = get_profile_strategy(
            strategy_name, self.execute_query, self.profiler_config
        )

        # Process results
        try:
            profile_data = strategy.extract_profile_data(results, table)
            return profile_data
        except Exception as e:
            logger.warning(
                f"Error processing profile results: {str(e)}. Falling back to basic processing."
            )
            # Fall back to basic profiling
            basic_strategy = BasicProfileStrategy(
                self.execute_query, self.profiler_config
            )
            return basic_strategy.extract_profile_data(results, table)

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate profile workunits for tables.

        Args:
            project_id: Project ID
            tables: Dictionary mapping dataset names to lists of tables

        Returns:
            Iterable of MetadataWorkUnit
        """
        # Create profile requests similar to the older implementation
        profile_requests: List[TableProfilerRequest] = []

        for schema_name, schema_tables in tables.items():
            # Profile tables
            for table in schema_tables:
                # Get profile request using our specialized method that handles partitions
                profile_request = self.get_profile_request(
                    table, schema_name, project_id
                )

                if profile_request is None:
                    continue

                # Use the partition determination but feed it into the standard profiling system
                self.report.report_entity_profiled(profile_request.pretty_name)
                profile_requests.append(profile_request)

                # Update the table_urn for state tracking (for compatibility with tests)
                table_urn = BigqueryTableIdentifier(
                    project_id=project_id,
                    dataset=schema_name,
                    table=table.name,
                ).get_table_name()

                if self.state_handler:
                    # Handle both old and new APIs
                    if hasattr(self.state_handler, "add_profiled_table"):
                        self.state_handler.add_profiled_table(table_urn)

        # If we don't have any tables to profile, return
        if len(profile_requests) == 0:
            return

        # Use the standard GenericProfiler to generate the actual profile workunits
        # This ensures we get column-level profiles
        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_dataset_urn(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get the dataset URN for a table."""
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=f"{db_name}.{schema_name}.{table_name}",
            env=self.config.env,
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """
        Implementation of the abstract method from GenericProfiler.
        Returns the dataset name for BigQuery tables.

        Args:
            table_name: Name of the table
            schema_name: Name of the schema/dataset
            db_name: Name of the database/project

        Returns:
            Formatted dataset name as required by GenericProfiler
        """
        return f"{db_name}.{schema_name}.{table_name}"

    def get_profile_as_workunit(
        self, dataset_urn: str, profile: Dict[str, Any]
    ) -> MetadataWorkUnit:
        """
        Wrapper method to create a profile workunit from profile data.

        Args:
            dataset_urn: Dataset URN
            profile: Profile data dictionary

        Returns:
            MetadataWorkUnit for the profile
        """
        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.metadata.schema_classes import DatasetProfileClass

        # Convert to DatasetProfileClass
        dataset_profile = DatasetProfileClass(**profile)

        # Create MCP
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="datasetProfile",
            aspect=dataset_profile,
        )

        # Create workunit
        wu = MetadataWorkUnit(id=f"{dataset_urn}-profile", mcp=mcp)
        return wu

    # ----- Compatibility methods for tests -----

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime] = None
    ) -> Tuple[datetime, datetime]:
        """
        Compatibility method for tests that calculates date range from a partition ID.
        """
        # Handle Hive-style partitions like 'year=2022/month=01/day=01'
        if "=" in partition_id:
            parts = {}
            for part in partition_id.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    parts[key.lower().strip()] = value.strip()

            # Extract year, month, day, hour if present
            year = int(parts.get("year", datetime.now().year))
            month = int(parts.get("month", 1))
            day = int(parts.get("day", 1))
            hour = int(parts.get("hour", 0))

            start_datetime = datetime(year, month, day, hour)

            # Calculate end datetime based on available parts
            if "hour" in parts:
                end_datetime = start_datetime + timedelta(hours=1)
            elif "day" in parts:
                end_datetime = start_datetime + timedelta(days=1)
            elif "month" in parts:
                # Handle month rollover properly
                if month == 12:
                    end_datetime = datetime(year + 1, 1, day, hour)
                else:
                    end_datetime = datetime(year, month + 1, day, hour)
            else:
                end_datetime = datetime(year + 1, month, day, hour)

            return start_datetime, end_datetime

        # Handle numeric format partitions (standard BigQuery pattern)
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        # Support for ISO format dates (YYYY-MM-DD)
        if (
            len(partition_id) == 10
            and partition_id[4] == "-"
            and partition_id[7] == "-"
        ):
            try:
                dt = datetime.strptime(partition_id, "%Y-%m-%d")
                return dt, dt + timedelta(days=1)
            except ValueError:
                pass

        # Basic numeric format
        if partition_range_map.get(len(partition_id)):
            try:
                (delta, format_str) = partition_range_map[len(partition_id)]
                if not partition_datetime:
                    partition_datetime = datetime.strptime(partition_id, format_str)
                else:
                    partition_datetime = datetime.strptime(
                        partition_datetime.strftime(format_str), format_str
                    )
                upper_bound_partition_datetime = partition_datetime + delta
                return partition_datetime, upper_bound_partition_datetime
            except ValueError as e:
                logger.warning(
                    f"Failed to parse partition_id {partition_id} with format {format_str}: {e}"
                )

        # If we reach here, we couldn't parse the partition_id
        raise ValueError(
            f"Invalid partition_id format: {partition_id}. It must be yearly (YYYY), "
            f"monthly (YYYYMM), daily (YYYYMMDD), hourly (YYYYMMDDHH), "
            f"ISO date (YYYY-MM-DD), or Hive style (year=YYYY/month=MM/...)."
        )

    def _get_table_metadata(
        self, table: BigqueryTable, project: str, schema: str
    ) -> Dict[str, Any]:
        """
        Compatibility method for tests that forwards to the table metadata manager.
        """
        return self.table_metadata_manager.get_table_metadata(table, project, schema)

    def generate_partition_profiler_query(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_datetime: Optional[datetime] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Method returns partition id if table is partitioned or sharded and generate custom partition query for
        partitioned table.
        See more about partitioned tables at https://cloud.google.com/bigquery/docs/partitioned-tables

        Args:
            project: Project ID
            schema: Dataset name
            table: BigqueryTable instance
            partition_datetime: Optional datetime to use for partition selection

        Returns:
            Tuple of (partition_id, custom_sql) or (None, None) if not applicable
        """
        logger.debug(
            f"generate partition profiler query for project: {project} schema: {schema} and table {table.name}, partition_datetime: {partition_datetime}"
        )

        # Try different partition handling strategies in order of preference

        # 1. Try multi-column partitioning using partition manager
        result = self._try_multi_column_partitioning(project, schema, table)
        if result:
            return result

        # 2. Try single-column partitioning (original approach)
        result = self._try_single_column_partitioning(
            project, schema, table, partition_datetime
        )
        if result:
            return result

        # 3. Try sharded table handling
        result = self._try_sharded_table_handling(table)
        if result:
            return result

        # 4. Try error-based partition extraction as last resort
        result = self._try_error_based_partition_extraction(project, schema, table)
        if result:
            return result

        # If all strategies fail, return None, None
        return None, None

    def _try_multi_column_partitioning(
        self, project: str, schema: str, table: BigqueryTable
    ) -> Tuple[Optional[str], Optional[str]]:
        """Try to handle multi-column partitioning using the partition manager."""
        if hasattr(self, "partition_manager") and self.partition_manager:
            # Try to get required partition filters from the partition manager
            required_partition_filters = (
                self.partition_manager.get_required_partition_filters(
                    table, project, schema
                )
            )

            # If we have required partition filters, use them to create the custom SQL
            if required_partition_filters and len(required_partition_filters) > 0:
                where_clause = " AND ".join(required_partition_filters)
                custom_sql = f"""
SELECT
    *
FROM
    `{project}.{schema}.{table.name}`
WHERE
    {where_clause}
                """
                # Return a unique identifier for this partition and the custom SQL
                partition_id = f"multi_partition_{int(time.time())}"
                return (partition_id, custom_sql)

        return None, None

    def _try_single_column_partitioning(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_datetime: Optional[datetime],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Try to handle single-column partitioning (original approach)."""
        partition = table.max_partition_id
        if not (table.partition_info and partition):
            return None, None

        if (
            hasattr(table.partition_info, "type")
            and table.partition_info.type == "RANGE"
        ):
            result = self._handle_range_partition(project, schema, table, partition)
            if result:
                return result
        else:
            result = self._handle_time_partition(
                project, schema, table, partition, partition_datetime
            )
            if result:
                return result

        return None, None

    def _handle_range_partition(
        self, project: str, schema: str, table: BigqueryTable, partition: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Handle RANGE type partitioning."""
        # Check that partition_info is not None before accessing .column
        if not table.partition_info:
            return None, None

        # Now we're sure partition_info is not None, check for column
        if hasattr(table.partition_info, "column") and table.partition_info.column:
            partition_where_clause = (
                f"{table.partition_info.column.name} >= {partition}"
            )

            custom_sql = self._create_partition_sql(
                project, schema, table.name, partition_where_clause
            )

            return (partition, custom_sql)
        else:
            logger.warning(f"Partitioned table {table.name} without partition column")
            if hasattr(self.report, "profiling_skipped_invalid_partition_ids"):
                self.report.profiling_skipped_invalid_partition_ids[
                    f"{project}.{schema}.{table.name}"
                ] = partition
            return None, None

    def _handle_time_partition(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition: str,
        partition_datetime: Optional[datetime],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Handle time-based partitioning."""
        # Check that partition_info is not None
        if not table.partition_info:
            return None, None

        logger.debug(f"{table.name} is partitioned and partition column is {partition}")
        try:
            (
                partition_datetime,
                upper_bound_partition_datetime,
            ) = self.get_partition_range_from_partition_id(
                partition, partition_datetime
            )
        except ValueError as e:
            logger.error(
                f"Unable to get partition range for partition id: {partition} it failed with exception {e}"
            )
            if hasattr(self.report, "profiling_skipped_invalid_partition_ids"):
                self.report.profiling_skipped_invalid_partition_ids[
                    f"{project}.{schema}.{table.name}"
                ] = partition
            return None, None

        partition_data_type, partition_column_name = self._get_partition_column_info(
            table
        )

        # Here we've already checked table.partition_info is not None above
        if hasattr(table.partition_info, "type") and table.partition_info.type in (
            "HOUR",
            "DAY",
            "MONTH",
            "YEAR",
        ):
            partition_where_clause = f"`{partition_column_name}` BETWEEN {partition_data_type}('{partition_datetime}') AND {partition_data_type}('{upper_bound_partition_datetime}')"

            custom_sql = self._create_partition_sql(
                project, schema, table.name, partition_where_clause
            )

            return (partition, custom_sql)
        else:
            logger.warning(f"Not supported partition type {table.partition_info.type}")
            if hasattr(self.report, "profiling_skipped_invalid_partition_type"):
                self.report.profiling_skipped_invalid_partition_type[
                    f"{project}.{schema}.{table.name}"
                ] = table.partition_info.type
            return None, None

    def _get_partition_column_info(self, table: BigqueryTable) -> Tuple[str, str]:
        """Get partition column data type and name."""
        partition_data_type: str = "TIMESTAMP"
        # Ingestion time partitioned tables has a pseudo column called _PARTITIONTIME
        # See more about this at
        # https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time
        partition_column_name = "_PARTITIONTIME"

        # Check that partition_info is not None
        if table.partition_info is not None:
            if hasattr(table.partition_info, "column") and table.partition_info.column:
                partition_column_name = table.partition_info.column.name
                partition_data_type = table.partition_info.column.data_type

        return partition_data_type, partition_column_name

    def _create_partition_sql(
        self, project: str, schema: str, table_name: str, where_clause: str
    ) -> str:
        """Create SQL with partition filtering."""
        return f"""
SELECT
    *
FROM
    `{project}.{schema}.{table_name}`
WHERE
    {where_clause}
        """

    def _try_sharded_table_handling(
        self, table: BigqueryTable
    ) -> Tuple[Optional[str], Optional[str]]:
        """Try to handle sharded tables."""
        if hasattr(table, "max_shard_id") and table.max_shard_id:
            # For sharded table we want to get the partition id but not needed to generate custom query
            return table.max_shard_id, None

        return None, None

    def _try_error_based_partition_extraction(
        self, project: str, schema: str, table: BigqueryTable
    ) -> Tuple[Optional[str], Optional[str]]:
        """Try to extract partition information from error messages."""
        if not (hasattr(self.partition_manager, "_extract_partition_info_from_error")):
            return None, None

        last_error = getattr(self.partition_manager, "_last_error_message", None)
        if not last_error:
            return None, None

        partition_info = self.partition_manager._extract_partition_info_from_error(
            last_error
        )
        if not (partition_info and "required_columns" in partition_info):
            return None, None

        required_cols = partition_info["required_columns"].split(",")
        # For now, just add a simple filter for the current date/time if these are time-related columns
        filters = self._create_filters_for_columns(required_cols)

        if not filters:
            return None, None

        where_clause = " AND ".join(filters)
        custom_sql = f"""
SELECT
    *
FROM
    `{project}.{schema}.{table.name}`
WHERE
    {where_clause}
        """
        return (f"error_extracted_{int(time.time())}", custom_sql)

    def _create_filters_for_columns(self, columns: List[str]) -> List[str]:
        """Create filters for the given columns based on their names."""
        filters = []
        now = datetime.now()

        for col in columns:
            col = col.strip()
            if col.lower() == "year":
                filters.append(f"`{col}` = {now.year}")
            elif col.lower() == "month":
                filters.append(f"`{col}` = {now.month}")
            elif col.lower() == "day":
                filters.append(f"`{col}` = {now.day}")
            else:
                # For other columns, just add a simple IS NOT NULL check
                filters.append(f"`{col}` IS NOT NULL")

        return filters

    def get_profile_args(self) -> Dict[str, Any]:
        """
        Get profiler arguments for the GenericProfiler.

        Returns:
            Dictionary of profiler arguments
        """
        # Get the base profiler args from parent class
        profiler_args = super().get_profile_args()

        # Add BigQuery-specific profiler args
        profiler_args.update(
            {
                # Pass query timeout to the GE profiler
                "query_timeout": self.config.query_timeout,
                # Pass the partition manager for any partition filtering needs
                "partition_manager": self.partition_manager,
                # Pass the filter builder for building queries
                "filter_builder": self.filter_builder,
                # Pass the table metadata manager for table metadata
                "table_metadata_manager": self.table_metadata_manager,
                # Use our cache manager for caching profiling results
                "cache_manager": self.cache_manager,
            }
        )

        return profiler_args

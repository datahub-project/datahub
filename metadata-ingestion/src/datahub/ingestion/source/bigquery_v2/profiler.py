import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta, timezone
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
        query_timeout = getattr(self.config.profiling, "query_timeout", 60)
        max_queries_per_table = getattr(self.config.profiling, "max_workers", 50)
        profile_table_level_only = getattr(
            self.config.profiling, "profile_table_level_only", False
        )
        tables_pattern = getattr(self.config, "tables", None)
        schema_pattern = getattr(self.config, "schema", None)
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
            timeout: int = 60,
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
                    logger.warning(f"Query execution error: {str(e)}")
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
        Create a profile request for a table.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            TableProfilerRequest if the table should be profiled, None otherwise
        """
        # Cast to BigqueryTable
        bq_table = cast(BigqueryTable, table)

        # Check if table should be profiled
        if not self._should_profile_table(bq_table, schema_name, db_name):
            return None

        # Create the profiler request
        profile_request = TableProfilerRequest(
            pretty_name=bq_table.name,
            batch_kwargs=self.get_batch_kwargs(bq_table, schema_name, db_name),
            table=bq_table,
        )

        return profile_request

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """
        Get batch kwargs for profiling a table.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            Dictionary of batch kwargs
        """
        # Cast to BigqueryTable
        bq_table = cast(BigqueryTable, table)

        # Get table key
        table_key = f"{db_name}.{schema_name}.{bq_table.name}"

        # Determine which profiling strategy to use
        profile_strategy = self._select_profile_strategy(bq_table, db_name, schema_name)

        # Get partition filters if needed
        partition_filters = None
        if getattr(self.profiler_config, "enable_partition_optimization", True):
            try:
                partition_filters = (
                    self.partition_manager.get_required_partition_filters(
                        bq_table, db_name, schema_name
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error getting partition filters for {table_key}: {str(e)}"
                )

        # Generate the profile query
        strategy = get_profile_strategy(
            profile_strategy, self.execute_query, self.profiler_config
        )

        try:
            profile_query = strategy.generate_profile_query(
                bq_table, db_name, schema_name, partition_filters
            )
        except Exception as e:
            logger.warning(f"Error generating profile query for {table_key}: {str(e)}")
            # Fall back to basic profiling
            profile_query = BasicProfileStrategy(
                self.execute_query, self.profiler_config
            ).generate_profile_query(bq_table, db_name, schema_name, partition_filters)

        # Store which strategy was used for this table
        self._table_strategies[table_key] = profile_strategy

        # Set up the batch kwargs
        batch_kwargs = {
            "schema": schema_name,
            "table": bq_table.name,
            "project": db_name,
            "custom_sql": profile_query,
            "partition_filters": partition_filters,
            "profile_strategy": profile_strategy,
            # Add partition_handling for test compatibility
            "partition_handling": "optimize" if partition_filters else "none",
        }

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
        # Similar implementation to parent class, but using our specialized components

        for schema_name, schema_tables in tables.items():
            # Profile tables
            for table in schema_tables:
                # Get profile request
                profile_request = self.get_profile_request(
                    table, schema_name, project_id
                )

                if profile_request is None:
                    continue

                # Execute profile query
                batch_kwargs = profile_request.batch_kwargs

                try:
                    # Get custom SQL and execute
                    custom_sql = batch_kwargs.get("custom_sql")
                    if not custom_sql:
                        continue

                    # Set an appropriate timeout based on table size
                    timeout = self.profiler_config.get_timeout_for_table(
                        table.size_in_bytes
                    )

                    # Execute the query
                    results = self.execute_query(
                        custom_sql,
                        f"profile_{project_id}_{schema_name}_{table.name}",
                        timeout=timeout,
                    )

                    if not results:
                        logger.warning(f"No results for {table.name}, skipping profile")
                        continue

                    # Process results
                    profile_data = self._process_profile_results(
                        results, table, batch_kwargs
                    )

                    # Create dataset URN
                    dataset_urn = self.get_dataset_name(
                        table.name, schema_name, project_id
                    )

                    # Create profile workunit
                    profile_wu = self.get_profile_as_workunit(
                        dataset_urn=dataset_urn,
                        profile=profile_data,
                    )

                    # Update report
                    if hasattr(self.report, "num_table_profiles_produced"):
                        self.report.num_table_profiles_produced += 1

                    # Update state
                    table_urn = BigqueryTableIdentifier(
                        project_id=project_id,
                        dataset=schema_name,
                        table=table.name,
                    ).get_table_name()

                    if self.state_handler:
                        # Handle both old and new APIs
                        if hasattr(self.state_handler, "add_profiled_table"):
                            self.state_handler.add_profiled_table(table_urn)
                        elif hasattr(self.state_handler, "record_profiled_dataset"):
                            now_millis = int(
                                datetime.now(timezone.utc).timestamp() * 1000
                            )
                            self.state_handler.record_profiled_dataset(
                                dataset_urn=table_urn,
                                last_profiled_timestamp=now_millis,
                            )

                    yield profile_wu

                except Exception as e:
                    # Mark as problematic to avoid retrying
                    table_key = f"{project_id}.{schema_name}.{table.name}"
                    self._problematic_tables.add(table_key)

                    # Log error
                    logger.error(f"Error profiling table {table_key}: {str(e)}")

                    # Update report
                    self.report.report_warning(
                        "profile",
                        f"Error profiling table {table_key}: {str(e)}",
                    )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get the dataset URN for a table."""
        return (
            f"urn:li:dataset:("
            f"urn:li:dataPlatform:{self.platform},"
            f"{db_name}.{schema_name}.{table_name}"
            f")"
        )

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

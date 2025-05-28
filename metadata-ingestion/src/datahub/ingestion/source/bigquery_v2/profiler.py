import logging
from datetime import datetime, timedelta
from typing import (
    Any,
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
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling_cache_manager import (
    BigQueryCacheManager,
)

# Import all specialized components
from datahub.ingestion.source.bigquery_v2.profiling_config import BigQueryProfilerConfig
from datahub.ingestion.source.bigquery_v2.profiling_filter_builder import (
    BigQueryFilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling_partition_manager import (
    BigQueryPartitionManager,
)
from datahub.ingestion.source.bigquery_v2.profiling_strategy import (
    ProfileStrategy,
    get_profile_strategy,
)
from datahub.ingestion.source.bigquery_v2.profiling_table_metadata_manager import (
    BigQueryTableMetadataManager,
)
from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigqueryProfiler(GenericProfiler):
    """
    BigQuery profiler that uses DatahubGEProfiler for profiling.
    """

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report
        self.profiler_config = self._create_profiler_config()
        self._problematic_tables: Set[str] = set()

        # Initialize all specialized components
        self._cache_manager = BigQueryCacheManager(
            max_cache_size=self.profiler_config.max_cache_size
        )
        self._filter_builder = BigQueryFilterBuilder(self._execute_query)
        self._partition_manager = BigQueryPartitionManager(
            self._execute_query, self._filter_builder
        )
        self._metadata_manager = BigQueryTableMetadataManager(self._execute_query)

        # Initialize profiling strategies
        self._profile_strategies = {
            "basic": get_profile_strategy(
                "basic", self._execute_query, self.profiler_config
            ),
            "standard": get_profile_strategy(
                "standard", self._execute_query, self.profiler_config
            ),
            "histogram": get_profile_strategy(
                "histogram", self._execute_query, self.profiler_config
            ),
            "partition": get_profile_strategy(
                "partition", self._execute_query, self.profiler_config
            ),
        }

    def _create_profiler_config(self) -> BigQueryProfilerConfig:
        """Convert BigQueryV2Config to BigQueryProfilerConfig"""
        sample_size = getattr(self.config.profiling, "sample_size", 100_000)
        query_timeout = getattr(self.config.profiling, "query_timeout", 60)
        max_queries_per_table = getattr(self.config.profiling, "max_workers", 50)
        profile_table_level_only = getattr(
            self.config.profiling, "profile_table_level_only", False
        )
        tables_pattern = getattr(self.config, "tables", None)

        schema_pattern = None
        config_schema = getattr(self.config, "schema", None)
        if config_schema is not None:
            if hasattr(config_schema, "allow"):
                schema_pattern = config_schema.allow
            elif isinstance(config_schema, str):
                schema_pattern = [config_schema]
            elif isinstance(config_schema, list):
                schema_pattern = config_schema

        return BigQueryProfilerConfig(
            sample_size=sample_size,
            query_timeout=query_timeout,
            max_queries_per_table=max_queries_per_table,
            profile_table_level_only=profile_table_level_only,
            tables_pattern=tables_pattern,
            schema_pattern=schema_pattern,
            external_table_sampling_percent=0.1,
            large_table_sampling_percent=1.0,
        )

    def _execute_query(
        self,
        query: str,
        cache_key: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> List[Any]:
        """Execute a BigQuery query with caching and retries.

        Args:
            query: The query to execute
            cache_key: Optional cache key for caching results
            timeout: Query timeout in seconds
            max_retries: Maximum number of retries for failed queries

        Returns:
            List of query results
        """
        # Use cache if enabled and cache key provided
        if self.profiler_config.enable_caching and cache_key:
            cached_result = self._cache_manager.get_query_result(cache_key)
            if cached_result:
                return cached_result

        from sqlalchemy import create_engine

        url = self.config.get_sql_alchemy_url()
        engine = create_engine(
            url,
            **self.config.get_options() if hasattr(self.config, "get_options") else {},
        )

        last_exception = None
        for attempt in range(max_retries):
            try:
                with engine.connect() as conn:
                    result = conn.execute(query)
                    results = result.fetchall()

                    # Cache results if caching is enabled
                    if self.profiler_config.enable_caching and cache_key:
                        self._cache_manager.add_query_result(cache_key, results)

                    return results
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Query execution failed (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    import time

                    time.sleep(min(2**attempt, 10))  # Exponential backoff
                    continue
                else:
                    logger.error(
                        f"Query execution failed after {max_retries} attempts: {e}"
                    )
                    raise Exception(
                        f"Query execution failed after {max_retries} attempts"
                    ) from last_exception

        # This should never be reached due to the raise in the loop
        raise Exception("Query execution failed") from None

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """Get batch kwargs for profiling, including sampling configuration."""
        table = cast(BigqueryTable, table)

        # Get comprehensive table metadata
        metadata = self._metadata_manager.get_table_metadata(
            table, db_name, schema_name
        )

        # Get partition filters using the partition manager
        partition_filters = None
        if metadata["partition_columns"]:
            try:
                # Get required partition filters
                partition_filters = (
                    self._partition_manager.get_required_partition_filters(
                        table, db_name, schema_name
                    )
                )

                if not partition_filters and not metadata["is_external"]:
                    # If no filters returned for non-external tables, try to get partition values
                    partition_values = self._partition_manager.get_partition_values(
                        table, db_name, schema_name, metadata["partition_columns"]
                    )
                    if partition_values:
                        partition_filters = (
                            self._filter_builder.create_partition_filters(
                                table,
                                db_name,
                                schema_name,
                                metadata["partition_columns"],
                                partition_values,
                            )
                        )
            except Exception as e:
                logger.warning(f"Failed to get partition filters: {e}")
                if metadata["is_external"]:
                    # For external tables, use empty filter list as fallback
                    partition_filters = []

        # Choose appropriate profiling strategy based on table characteristics
        strategy = self._get_profiling_strategy(table, metadata)

        # Generate profile query using the selected strategy
        query = strategy.generate_profile_query(
            table, db_name, schema_name, partition_filters
        )

        batch_kwargs = {
            "custom_sql": query,
            "table": f"{db_name}.{schema_name}.{table.name}",
            "schema": None,  # Schema is included in the table name
            "database": None,  # Database is included in the table name
        }

        return batch_kwargs

    def _get_profiling_strategy(
        self, table: BigqueryTable, metadata: Dict[str, Any]
    ) -> ProfileStrategy:
        """Choose the most appropriate profiling strategy based on table characteristics."""
        if self.profiler_config.profile_table_level_only:
            return self._profile_strategies["basic"]

        if metadata["is_external"]:
            # Use partition-focused strategy for external tables
            return self._profile_strategies["partition"]

        if (
            metadata.get("size_bytes", 0) > 10_000_000_000  # 10 GB
            or metadata.get("row_count", 0) > 100_000_000  # 100M rows
        ):
            # Use basic strategy for very large tables
            return self._profile_strategies["basic"]

        # Default to standard strategy
        return self._profile_strategies["standard"]

    def _should_profile_table(
        self, table: BigqueryTable, schema_name: str, db_name: str
    ) -> bool:
        """Determine if a table should be profiled based on configuration and cache."""
        if not self.config.profiling.enabled:
            return False

        table_key = f"{db_name}.{schema_name}.{table.name}"

        # Check if table is marked as problematic
        if self._cache_manager.is_problematic_table(table_key):
            logger.info(f"Skipping problematic table: {table_key}")
            return False

        return self.profiler_config.should_profile_table(
            db_name, schema_name, table.name
        )

    def get_profiler_instance(
        self, db_name: Optional[str] = None
    ) -> "DatahubGEProfiler":
        """Get a DatahubGEProfiler instance configured for BigQuery."""
        from sqlalchemy import create_engine

        assert db_name, "db_name is required for BigQuery profiling"

        # Get the SQLAlchemy URL for BigQuery
        url = self.config.get_sql_alchemy_url()

        # Create SQLAlchemy engine with proper configuration
        engine = create_engine(
            url,
            **self.config.get_options() if hasattr(self.config, "get_options") else {},
        )

        return DatahubGEProfiler(
            conn=engine.connect(),
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
        )

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate profiling workunits for BigQuery tables."""
        if not self.config.profiling.enabled:
            return

        profile_requests = []
        for schema, schema_tables in tables.items():
            for table in schema_tables:
                if not self._should_profile_table(table, schema, project_id):
                    continue

                profile_request = self.get_profile_request(table, schema, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)

        if len(profile_requests) == 0:
            return

        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
            db_name=project_id,
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get the dataset URN for a table."""
        return (
            f"urn:li:dataset:("
            f"urn:li:dataPlatform:{self.platform},"
            f"{db_name}.{schema_name}.{table_name}"
            f")"
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

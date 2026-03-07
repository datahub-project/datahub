"""BigQuery profiler: orchestrates table profiling with partition-aware query generation."""

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.common import BQ_SPECIAL_PARTITION_IDS
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BQ_SAFETY_ROW_LIMIT,
    BQ_SAFETY_ROW_LIMIT_THRESHOLD,
    DATE_FORMAT_PATTERNS,
    DATE_FORMAT_YYYY_MM_DD,
    DATE_FORMAT_YYYYMMDD,
    DATE_LIKE_COLUMN_NAMES,
    STRFTIME_FORMATS,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_and_filter_expressions,
    validate_bigquery_identifier,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigqueryProfiler(GenericProfiler):
    """Profiler for BigQuery tables.

    Extends GenericProfiler to add partition-aware query generation: tables that
    require a partition filter get a custom SQL SELECT with a WHERE clause derived
    from the latest real partition values, preventing expensive full-table scans.
    External tables use deferred parallel discovery so they don't block the main loop.
    """

    config: BigQueryV2Config
    report: BigQueryV2Report

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report
        self.partition_discovery = PartitionDiscovery(config)
        self.query_executor = QueryExecutor(config)
        self._tables_profiled = 0
        self._external_tables_processed = 0
        self._partition_discovery_calls = 0
        # {(project, dataset): {table_name: {partition_columns: [...], column_types: {...}}}}
        self._partition_metadata_cache: Dict[Tuple[str, str], Dict[str, Dict]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def _populate_partition_metadata_cache(self, project: str, dataset: str) -> None:
        """Pre-fetch partition metadata for all tables in a dataset in one query.

        A single INFORMATION_SCHEMA.COLUMNS query per dataset is far cheaper than
        issuing one query per table during get_batch_kwargs, which is called for
        every table and runs in the same thread.
        """
        cache_key = (project, dataset)

        if cache_key in self._partition_metadata_cache:
            return

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, dataset, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""
SELECT table_name, column_name, data_type
FROM {safe_info_schema_ref}
WHERE is_partitioning_column = 'YES'
ORDER BY table_name, ordinal_position
"""

            from google.cloud.bigquery import QueryJobConfig

            job_config = QueryJobConfig()

            rows = self.query_executor.execute_query_safely(
                query, job_config, f"partition metadata cache for {project}.{dataset}"
            )

            dataset_cache: Dict[str, Dict] = {}

            for row in rows:
                table_name = row.table_name
                if table_name not in dataset_cache:
                    dataset_cache[table_name] = {
                        "partition_columns": [],
                        "column_types": {},
                    }
                dataset_cache[table_name]["partition_columns"].append(row.column_name)
                dataset_cache[table_name]["column_types"][row.column_name] = (
                    row.data_type
                )

            self._partition_metadata_cache[cache_key] = dataset_cache

        except Exception as e:
            logger.warning(
                f"Failed to populate partition metadata cache for {project}.{dataset}: {e}"
            )
            self._partition_metadata_cache[cache_key] = {}

    def _get_cached_partition_metadata(
        self, project: str, dataset: str, table_name: str
    ) -> Optional[Dict]:
        cache_key = (project, dataset)

        if cache_key not in self._partition_metadata_cache:
            self._populate_partition_metadata_cache(project, dataset)

        dataset_cache = self._partition_metadata_cache.get(cache_key, {})
        table_metadata = dataset_cache.get(table_name)

        if table_metadata:
            self._cache_hits += 1
        else:
            self._cache_misses += 1

        return table_metadata

    def log_cache_statistics(self) -> None:
        total_requests = self._cache_hits + self._cache_misses
        if total_requests > 0:
            hit_rate = (self._cache_hits / total_requests) * 100
            logger.info(
                f"Partition metadata cache: "
                f"{self._cache_hits} hits, {self._cache_misses} misses, "
                f"{hit_rate:.1f}% hit rate, "
                f"{len(self._partition_metadata_cache)} datasets cached"
            )

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        return PartitionDiscovery.get_partition_range_from_partition_id(
            partition_id, partition_datetime
        )

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        bq_table = cast(BigqueryTable, table)
        table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

        # Raises ValueError for invalid identifiers; callers should catch and skip the table.
        safe_project = validate_bigquery_identifier(db_name, "project")
        safe_schema = validate_bigquery_identifier(schema_name, "dataset")
        safe_table = validate_bigquery_identifier(bq_table.name, "table")

        base_kwargs = {
            "schema": db_name,
            "table": f"{schema_name}.{table.name}",
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        if bq_table.external:
            base_kwargs["is_external"] = "true"
            # Partition discovery for external tables is deferred to parallel threads in
            # generate_profile_workunits_with_deferred_partitions to avoid blocking.
            return base_kwargs

        logger.debug(f"Starting partition discovery for {table_ref}")
        self._partition_discovery_calls += 1

        cached_metadata = self._get_cached_partition_metadata(
            db_name, schema_name, bq_table.name
        )

        partition_filters = self.partition_discovery.get_required_partition_filters(
            bq_table,
            db_name,
            schema_name,
            self.query_executor.execute_query_safely,
            cached_partition_metadata=cached_metadata,
        )

        if partition_filters is None:
            # Table requires a partition filter but one couldn't be constructed.
            # Callers should catch ValueError and skip the table.
            raise ValueError(
                f"Could not construct required partition filters for {table_ref}"
            )

        validated_filters: List[str] = []
        partition_where = ""

        if partition_filters:
            validated_filters = validate_and_filter_expressions(
                partition_filters, "batch kwargs"
            )

            # Apply partition date windowing once here so it isn't duplicated in get_profile_request.
            if (
                validated_filters
                and self.config.profiling.partition_datetime_window_days
            ):
                validated_filters = self._apply_partition_date_windowing(
                    validated_filters, bq_table
                )

            if validated_filters:
                partition_where = " AND ".join(validated_filters)
                logger.info(
                    f"Applied partition filters for {table_ref}: {partition_where}"
                )
            else:
                logger.info(
                    f"Partition filters were rejected during validation for {table_ref}, proceeding without filters"
                )

        safe_table_ref = f"{safe_project}.{safe_schema}.{safe_table}"
        custom_sql = None

        should_sample = (
            self.config.profiling.use_sampling
            and hasattr(bq_table, "rows_count")
            and bq_table.rows_count
            and bq_table.rows_count > self.config.profiling.sample_size
        )

        if should_sample:
            rows_count = bq_table.rows_count or 1
            sample_pc = self.config.profiling.sample_size / rows_count
            sample_percent = min(100 * sample_pc, 100.0)

            if partition_where:
                custom_sql = f"""SELECT * FROM {safe_table_ref}
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
WHERE {partition_where}"""
            else:
                custom_sql = f"""SELECT * FROM {safe_table_ref}
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)"""

            logger.info(
                f"Applied {sample_percent:.4f}% sampling to {table_ref} "
                f"({bq_table.rows_count:,} rows)"
            )
        elif partition_where:
            if self.config.profiling.profiling_row_limit > 0:
                row_limit = max(1, int(self.config.profiling.profiling_row_limit))
                custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
                logger.info(
                    f"Applied row limit ({row_limit:,}) to partitioned {table_ref}"
                )
            else:
                custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}"""
        elif (
            hasattr(bq_table, "rows_count")
            and bq_table.rows_count
            and self.config.profiling.profiling_row_limit > 0
            and bq_table.rows_count > self.config.profiling.profiling_row_limit
        ):
            # Apply the row limit for non-partitioned tables that exceed it to avoid
            # expensive full-table scans.
            row_limit = max(1, int(self.config.profiling.profiling_row_limit))
            custom_sql = f"SELECT * FROM {safe_table_ref} LIMIT {row_limit}"
            logger.info(
                f"Applied row limit ({row_limit:,}) to non-partitioned table {table_ref} ({bq_table.rows_count:,} rows)"
            )
        elif (
            hasattr(bq_table, "rows_count")
            and bq_table.rows_count
            and bq_table.rows_count > BQ_SAFETY_ROW_LIMIT_THRESHOLD
            and self.config.profiling.profiling_row_limit == 0
        ):
            # Safety cap for very large unpartitioned tables when no explicit limit is configured.
            custom_sql = f"SELECT * FROM {safe_table_ref} LIMIT {BQ_SAFETY_ROW_LIMIT}"
            logger.info(
                f"Applied safety limit of {BQ_SAFETY_ROW_LIMIT:,} rows to large table {table_ref} ({bq_table.rows_count:,} rows)"
            )
        # else: no custom_sql for non-partitioned, non-sampled tables that fit within limits —
        # let GE profile normally, preserving the default FULL_TABLE partitionSpec on the profile MCP.

        if custom_sql:
            base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})
            logger.debug(
                f"Generated batch kwargs for {table_ref} with custom_sql: {custom_sql[:100]}..."
            )
        else:
            logger.debug(f"Generated batch kwargs for {table_ref} without custom_sql")

        # Set the partition key so that profile MCPs carry the real partition ID and
        # type=PARTITION rather than type=QUERY/"SAMPLE".  This matches the master branch
        # behaviour where generate_partition_profiler_query returned (partition, custom_sql).
        partition_id = bq_table.max_partition_id or getattr(
            bq_table, "max_shard_id", None
        )
        if partition_id and partition_id not in BQ_SPECIAL_PARTITION_IDS:
            base_kwargs["partition"] = partition_id

        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        try:
            profile_request = super().get_profile_request(table, schema_name, db_name)
        except ValueError as e:
            # get_batch_kwargs raises ValueError for:
            # - identifiers that fail security validation
            # - partitioned tables that require a filter but one couldn't be constructed
            table_ref = f"{db_name}.{schema_name}.{table.name}"
            self.report.report_warning(
                title="Table skipped during profiling",
                message=str(e),
                context=table_ref,
            )
            return None

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)
        table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

        if self._should_skip_profiling_due_to_staleness(bq_table):
            logger.info(f"Skipping profiling of stale table: {table_ref}")
            return None

        if bq_table.external and not self.config.profiling.profile_external_tables:
            logger.info(
                f"Skipping profiling for external table {profile_request.pretty_name} (profiling.profile_external_tables is disabled)"
            )
            return None

        # Only skip profiling for tables that actually have partitions when
        # partition_profiling_enabled is False. Non-partitioned tables should still be profiled.
        if not self.config.profiling.partition_profiling_enabled:
            is_partitioned = (
                bq_table.max_partition_id
                or getattr(bq_table, "max_shard_id", None)
                or (hasattr(bq_table, "partition_info") and bq_table.partition_info)
            )
            if is_partitioned:
                logger.info(f"Skipping partition profiling (disabled): {table_ref}")
                self.report.profiling_skipped_partition_profiling_disabled.append(
                    profile_request.pretty_name
                )
                return None

        if bq_table.external:
            # External table partition discovery can't use INFORMATION_SCHEMA and may
            # require probing the actual data, which is slow. Defer it to parallel
            # threads in generate_profile_workunits_with_deferred_partitions so it
            # doesn't block the main loop from issuing other profile requests.
            logger.info(
                f"Deferring partition discovery for external table: {table_ref}"
            )
            profile_request.needs_partition_discovery = True  # type: ignore[attr-defined]
            profile_request.bq_table = bq_table  # type: ignore[attr-defined]
            profile_request.db_name = db_name  # type: ignore[attr-defined]
            profile_request.schema_name = schema_name  # type: ignore[attr-defined]
            self._external_tables_processed += 1

        logger.info(f"Successfully created profile request for {table_ref}")
        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        profile_requests: List[TableProfilerRequest] = []
        total_tables = sum(len(dataset_tables) for dataset_tables in tables.values())

        self._tables_profiled = 0
        self._external_tables_processed = 0
        self._partition_discovery_calls = 0

        for dataset in tables:
            dataset_tables = tables[dataset]
            logger.info(
                f"Processing dataset {project_id}.{dataset} ({len(dataset_tables)} tables)"
            )

            for table in dataset_tables:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                if table.external and not self.config.profiling.profile_external_tables:
                    self.report.profiling_skipped_other[f"{project_id}.{dataset}"] += 1
                    continue

                profile_request = self.get_profile_request(table, dataset, project_id)

                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                    self._tables_profiled += 1
                    logger.info(
                        f"Accepted table for profiling: {normalized_table_name}"
                    )
                else:
                    logger.debug(
                        f"Table not eligible for profiling: {normalized_table_name}"
                    )

        self._log_profiling_statistics(project_id, total_tables, len(profile_requests))

        if len(profile_requests) == 0:
            logger.warning(f"No tables eligible for profiling in project {project_id}")
            return

        yield from self.generate_profile_workunits_with_deferred_partitions(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def _log_profiling_statistics(
        self, project_id: str, total_tables: int, eligible_tables: int
    ) -> None:
        skipped_tables = total_tables - eligible_tables
        acceptance_rate = (
            (eligible_tables / total_tables * 100) if total_tables > 0 else 0.0
        )

        logger.info(
            f"Profiling statistics for {project_id}: "
            f"{eligible_tables}/{total_tables} tables accepted, "
            f"{skipped_tables} skipped, "
            f"{self._external_tables_processed} external, "
            f"{self._partition_discovery_calls} partition discovery calls, "
            f"{acceptance_rate:.1f}% acceptance rate"
        )

        self.report.profile_table_selection_criteria[project_id] = (
            f"{eligible_tables}/{total_tables} tables accepted for profiling "
            f"({acceptance_rate:.1f}% acceptance rate)"
        )

    def generate_profile_workunits_with_deferred_partitions(
        self,
        profile_requests: List[TableProfilerRequest],
        max_workers: int,
        platform: str,
        profiler_args: Dict,
    ) -> Iterable[MetadataWorkUnit]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_external_table_request(
            request: TableProfilerRequest,
        ) -> Optional[TableProfilerRequest]:
            if not hasattr(request, "needs_partition_discovery") or not getattr(
                request, "needs_partition_discovery", False
            ):
                return request

            try:
                bq_table = request.bq_table  # type: ignore[attr-defined]
                db_name = request.db_name  # type: ignore[attr-defined]
                schema_name = request.schema_name  # type: ignore[attr-defined]
                table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

                logger.info(
                    f"Starting parallel partition discovery for external table: {table_ref}"
                )

                partition_filters = (
                    self.partition_discovery.get_required_partition_filters(
                        bq_table,
                        db_name,
                        schema_name,
                        self.query_executor.execute_query_safely,
                    )
                )

                if partition_filters is None:
                    logger.warning(
                        f"Could not construct partition filters for external table {table_ref} - skipping profiling"
                    )
                    return None

                if partition_filters:
                    validated_filters = validate_and_filter_expressions(
                        partition_filters, "external table profile request"
                    )

                    if validated_filters:
                        windowed_filters = self._apply_partition_date_windowing(
                            validated_filters, bq_table
                        )

                        partition_where = " AND ".join(windowed_filters)
                        safe_table_ref = build_safe_table_reference(
                            db_name, schema_name, bq_table.name
                        )

                        if (
                            self.config.profiling.use_sampling
                            and hasattr(bq_table, "rows_count")
                            and bq_table.rows_count
                            and bq_table.rows_count > self.config.profiling.sample_size
                        ):
                            sample_pc = (
                                self.config.profiling.sample_size / bq_table.rows_count
                            )
                            sample_percent = min(100 * sample_pc, 100.0)

                            custom_sql = f"""SELECT * FROM {safe_table_ref} 
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
WHERE {partition_where}"""
                        else:
                            if self.config.profiling.profiling_row_limit > 0:
                                row_limit = max(
                                    1, int(self.config.profiling.profiling_row_limit)
                                )
                                custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
                            else:
                                custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}"""

                        request.batch_kwargs.update(
                            dict(custom_sql=custom_sql, partition_handling="true")
                        )

                delattr(request, "needs_partition_discovery")
                delattr(request, "bq_table")
                delattr(request, "db_name")
                delattr(request, "schema_name")

                return request

            except Exception as e:
                logger.error(
                    f"Error processing partition discovery for external table {request.pretty_name}: {e}"
                )
                return None

        external_requests = [
            req for req in profile_requests if hasattr(req, "needs_partition_discovery")
        ]
        regular_requests = [
            req
            for req in profile_requests
            if not hasattr(req, "needs_partition_discovery")
        ]

        processed_requests = list(regular_requests)

        if external_requests:
            logger.info(
                f"Processing partition discovery for {len(external_requests)} external table(s) in parallel"
            )

            with ThreadPoolExecutor(
                max_workers=min(max_workers, len(external_requests))
            ) as executor:
                future_to_request = {
                    executor.submit(process_external_table_request, req): req
                    for req in external_requests
                }

                for future in as_completed(future_to_request):
                    result = future.result()
                    if result is not None:
                        processed_requests.append(result)

        if processed_requests:
            yield from super().generate_profile_workunits(
                processed_requests,
                max_workers=max_workers,
                platform=platform,
                profiler_args=profiler_args,
            )

        self.log_cache_statistics()

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()

    def _should_skip_profiling_due_to_staleness(self, table: BigqueryTable) -> bool:
        if not self.config.profiling.skip_stale_tables:
            return False

        now = datetime.now(timezone.utc)
        threshold_date = now - timedelta(
            days=self.config.profiling.staleness_threshold_days
        )

        if table.last_altered:
            last_modified = table.last_altered
        else:
            logger.debug(
                f"Table {table.name} has no last_altered time, will not skip profiling"
            )
            return False

        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)

        is_stale = last_modified < threshold_date

        if is_stale:
            days_since_modified = (now - last_modified).days
            logger.info(
                f"Skipping profiling for stale table {table.name} - "
                f"last modified {days_since_modified} days ago ({last_modified.strftime('%Y-%m-%d')})"
            )
            return True

        return False

    def _apply_partition_date_windowing(
        self, partition_filters: List[str], table: BigqueryTable
    ) -> List[str]:
        if not self.config.profiling.partition_datetime_window_days:
            return partition_filters

        window_days = self.config.profiling.partition_datetime_window_days
        windowed_filters = partition_filters.copy()

        date_columns = self._extract_date_columns_from_filters(partition_filters)

        if not date_columns:
            logger.debug(
                f"No date columns found in partition filters for {table.name}, skipping date windowing"
            )
            return partition_filters

        reference_date = self._get_reference_date_from_filters(
            partition_filters, date_columns
        )
        if not reference_date:
            reference_date = datetime.now(timezone.utc).date()

        start_date = reference_date - timedelta(days=window_days)

        for col_name in date_columns:
            existing_format = self._detect_date_format_in_filters(
                partition_filters, col_name
            )

            if existing_format == DATE_FORMAT_YYYYMMDD:
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYYMMDD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"
            elif existing_format == DATE_FORMAT_YYYY_MM_DD:
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYY_MM_DD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"
            else:
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYY_MM_DD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"

            range_filter = (
                f"`{col_name}` >= {start_date_str} AND `{col_name}` <= {end_date_str}"
            )
            windowed_filters.append(range_filter)

        logger.debug(
            f"Applied {window_days}-day partition window for {table.name}: "
            f"{start_date.strftime('%Y-%m-%d')} to {reference_date.strftime('%Y-%m-%d')}"
        )

        return windowed_filters

    def _detect_date_format_in_filters(
        self, partition_filters: List[str], col_name: str
    ) -> Optional[str]:
        col_prefix = f"`{re.escape(col_name)}`"

        for filter_expr in partition_filters:
            if col_prefix not in filter_expr:
                continue

            for format_name, pattern in DATE_FORMAT_PATTERNS.items():
                if pattern.search(filter_expr):
                    return format_name

        return None

    def _extract_date_columns_from_filters(
        self, partition_filters: List[str]
    ) -> List[str]:
        """Uses pattern matching as a heuristic since partition filters are constructed without full schema metadata."""
        date_columns = []

        for filter_expr in partition_filters:
            filter_lower = filter_expr.lower()
            for pattern in DATE_LIKE_COLUMN_NAMES:
                if f"`{pattern}`" in filter_lower:
                    if pattern not in date_columns:
                        date_columns.append(pattern)

        return date_columns

    def _get_reference_date_from_filters(
        self, partition_filters: List[str], date_columns: List[str]
    ) -> Optional[date]:
        for filter_expr in partition_filters:
            for col_name in date_columns:
                pattern = rf"`{col_name}`\s*=\s*'(\d{{4}}-\d{{2}}-\d{{2}})'"
                match = re.search(pattern, filter_expr, re.IGNORECASE)
                if match:
                    try:
                        date_str = match.group(1)
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue

        return None

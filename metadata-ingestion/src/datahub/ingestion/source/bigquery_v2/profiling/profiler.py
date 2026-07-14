import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

import requests
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from sqlalchemy import create_engine, inspect
from typing_extensions import TypeGuard

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.common import BQ_SPECIAL_PARTITION_IDS
from datahub.ingestion.source.bigquery_v2.profiling import queries
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BACKTICK_COLUMN_NAME_RE,
    BQ_SAFETY_ROW_LIMIT,
    BQ_SAFETY_ROW_LIMIT_THRESHOLD,
    CUSTOM_SQL_KWARG,
    DATE_FORMAT_YYYY_MM_DD,
    DATE_LIKE_COLUMN_NAMES,
    DATE_LITERAL_SHAPES,
    PARTITION_EQ_LITERAL_RE,
    PARTITION_HANDLING_ENABLED,
    PARTITION_HANDLING_KWARG,
    PARTITIONING_COLUMN_FLAG,
    STRFTIME_FORMATS,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    CachedPartitionMetadata,
)
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_and_filter_expressions,
    validate_bigquery_identifier,
)
from datahub.ingestion.source.profiling.common import create_datahub_ge_profiler
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


def _widen_client_connection_pool(client: bigquery.Client, size: int) -> None:
    # The BigQuery client shares one urllib3 pool (default pool_maxsize=10) across
    # every profiling thread. When profiling.max_workers exceeds 10, urllib3
    # discards the surplus connections after each request — forcing fresh TLS
    # handshakes and spamming "Connection pool is full" warnings. Size the pool to
    # the worker count so connections are reused instead.
    session = getattr(client, "_http", None)
    if session is None or not hasattr(session, "mount"):
        return
    adapter = requests.adapters.HTTPAdapter(pool_connections=size, pool_maxsize=size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


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
        self.partition_discovery = PartitionDiscovery(config, report)
        self.query_executor = QueryExecutor(config)
        self._tables_profiled = 0
        self._external_tables_processed = 0
        self._partition_discovery_calls = 0
        # {(project, dataset): {table_name: {partition_columns: [...], column_types: {...}}}}
        self._partition_metadata_cache: Dict[
            Tuple[str, str], Dict[str, CachedPartitionMetadata]
        ] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    # Return type is left unannotated on purpose: annotating it would need
    # DatahubGEProfiler, whose import pulls in great_expectations at module
    # load, and profiler modules must import without great_expectations
    # installed (enforced by tests/unit/test_no_ge_dependency.py).
    def get_profiler_instance(self, db_name=None):
        # Override the parent so the SQLAlchemy engine reuses our in-memory
        # bigquery.Client (built with explicit credentials) instead of letting
        # the dialect fall back to google.auth.default() — which would require
        # the GOOGLE_APPLICATION_CREDENTIALS env var to be set and would leak
        # the service account key through the process environment.
        from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
            SQLAlchemyProfiler,
        )

        logger.debug(f"Getting profiler instance from {self.platform}")
        url = self.config.get_sql_alchemy_url()
        connect_args: Dict[str, object] = {}
        if self.config.has_explicit_credentials():
            # user_supplied_client=true tells the BigQuery dialect to short
            # circuit its own client construction and use the one we pass via
            # connect_args. Requires sqlalchemy-bigquery>=1.5.0.
            # Covers both the service-account and WIF paths — anywhere we hold
            # an in-memory credential, we pass it through explicitly rather
            # than leaking the key via GOOGLE_APPLICATION_CREDENTIALS.
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}user_supplied_client=true"
            client = self.config.get_bigquery_client()
            _widen_client_connection_pool(
                client, max(self.config.profiling.max_workers, 10)
            )
            connect_args["client"] = client
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, connect_args=connect_args, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)

        if self.config.profiling.method == "sqlalchemy":
            logger.info(
                f"Using SQLAlchemyProfiler for profiling (platform: {self.platform})"
            )
            return SQLAlchemyProfiler(
                conn=inspector.bind,
                report=self.report,
                config=self.config.profiling,
                platform=self.platform,
                env=self.config.env,
            )
        else:
            # TODO: Remove this branch once Great Expectations is fully
            # deprecated. The entire if/else then collapses to the
            # SQLAlchemyProfiler return above.
            logger.info(
                f"Using DatahubGEProfiler (Great Expectations) for profiling (platform: {self.platform})"
            )
            return create_datahub_ge_profiler(
                conn=inspector.bind,
                report=self.report,
                config=self.config.profiling,
                platform=self.platform,
                env=self.config.env,
            )

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

            query = queries.PARTITION_METADATA_CACHE.format(
                info_schema_ref=safe_info_schema_ref, flag=PARTITIONING_COLUMN_FLAG
            )

            from google.cloud.bigquery import QueryJobConfig

            job_config = QueryJobConfig()

            rows = self.query_executor.execute_query_safely(
                query, job_config, f"partition metadata cache for {project}.{dataset}"
            )

            dataset_cache: Dict[str, CachedPartitionMetadata] = {}

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
            # Caching {} here means every table in this dataset is subsequently treated
            # as unpartitioned (partition discovery falls back to per-table probing).
            # Surface it in the report so operators know why partition filters may be
            # missing across a whole dataset (commonly an IAM/permissions issue).
            self.report.warning(
                title="Partition metadata cache population failed",
                message="Failed to pre-fetch partition columns from "
                "INFORMATION_SCHEMA.COLUMNS for a dataset; tables in it will fall back "
                "to per-table partition probing and may be full-scanned or skipped",
                context=f"{project}.{dataset}: {e}",
            )
            self._partition_metadata_cache[cache_key] = {}

    def _get_cached_partition_metadata(
        self, project: str, dataset: str, table_name: str
    ) -> Optional[CachedPartitionMetadata]:
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

    def _sample_percent(self, rows_count: int) -> float:
        sample_pc = self.config.profiling.sample_size / rows_count
        return min(100 * sample_pc, 100.0)

    def _should_sample(self, rows_count: Optional[int]) -> TypeGuard[int]:
        return bool(
            self.config.profiling.use_sampling
            and rows_count
            and rows_count > self.config.profiling.sample_size
        )

    def _build_partition_profiling_sql(
        self, safe_table_ref: str, partition_where: str, bq_table: BigqueryTable
    ) -> str:
        """Build the profiling SELECT for a table that has a partition filter.

        Applies TABLESAMPLE when sampling is enabled and the table is large enough,
        otherwise the configured row limit. Shared by the inline (internal table) and
        deferred (external table) code paths so their SQL cannot drift.
        """
        select_all = queries.SELECT_ALL.format(table_ref=safe_table_ref)
        where_clause = queries.WHERE_CLAUSE.format(where=partition_where)

        rows_count = getattr(bq_table, "rows_count", None)
        if self._should_sample(rows_count):
            tablesample = queries.TABLESAMPLE_SYSTEM.format(
                sample_percent=self._sample_percent(rows_count)
            )
            return f"{select_all}\n{tablesample}\n{where_clause}"

        if self.config.profiling.profiling_row_limit > 0:
            row_limit = max(1, int(self.config.profiling.profiling_row_limit))
            limit_clause = queries.LIMIT_CLAUSE.format(limit=row_limit)
            return f"{select_all}\n{where_clause}\n{limit_clause}"

        return f"{select_all}\n{where_clause}"

    def _build_custom_sql(
        self, safe_table_ref: str, table_ref: str, bq_table: BigqueryTable
    ) -> Optional[str]:
        # Custom SQL for a table without a partition filter: sample, cap at the row
        # limit, or apply the safety cap for very large tables. Returns None when the
        # table fits within limits so GE profiles it normally (FULL_TABLE partitionSpec).
        rows_count = getattr(bq_table, "rows_count", None)
        select_all = queries.SELECT_ALL.format(table_ref=safe_table_ref)

        if self._should_sample(rows_count):
            sample_percent = self._sample_percent(rows_count)
            logger.info(
                f"Applied {sample_percent:.4f}% sampling to {table_ref} "
                f"({rows_count:,} rows)"
            )
            return (
                f"{select_all}\n"
                f"{queries.TABLESAMPLE_SYSTEM.format(sample_percent=sample_percent)}"
            )

        if not rows_count:
            return None

        row_limit = self.config.profiling.profiling_row_limit
        if row_limit > 0 and rows_count > row_limit:
            limit = max(1, int(row_limit))
            logger.info(
                f"Applied row limit ({limit:,}) to non-partitioned table "
                f"{table_ref} ({rows_count:,} rows)"
            )
            return f"{select_all} {queries.LIMIT_CLAUSE.format(limit=limit)}"

        if row_limit == 0 and rows_count > BQ_SAFETY_ROW_LIMIT_THRESHOLD:
            logger.info(
                f"Applied safety limit of {BQ_SAFETY_ROW_LIMIT:,} rows to large table "
                f"{table_ref} ({rows_count:,} rows)"
            )
            return (
                f"{select_all} {queries.LIMIT_CLAUSE.format(limit=BQ_SAFETY_ROW_LIMIT)}"
            )

        return None

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
                # Discovery produced filters but all failed validation, so partition_where
                # stays empty and the table is profiled unfiltered (a full scan, or an
                # error if it has require_partition_filter=true). Surface it, don't just log.
                self.report.warning(
                    title="Partition filters rejected during profiling",
                    message="Discovered partition filters failed validation; profiling "
                    "will proceed without a partition filter (full scan), or fail if the "
                    "table requires a partition filter.",
                    context=table_ref,
                )

        safe_table_ref = f"{safe_project}.{safe_schema}.{safe_table}"

        # A partitioned table gets the partition-filtered SELECT; otherwise fall back to
        # sampling / row-limit SQL (or None, so GE profiles the full table normally).
        if partition_where:
            custom_sql: Optional[str] = self._build_partition_profiling_sql(
                safe_table_ref, partition_where, bq_table
            )
            logger.info(f"Applied partition profiling SQL for {table_ref}")
        else:
            custom_sql = self._build_custom_sql(safe_table_ref, table_ref, bq_table)

        if custom_sql:
            base_kwargs.update(
                {
                    CUSTOM_SQL_KWARG: custom_sql,
                    PARTITION_HANDLING_KWARG: PARTITION_HANDLING_ENABLED,
                }
            )
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
        bq_table = cast(BigqueryTable, table)
        table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

        # Check staleness before super().get_profile_request(), which runs partition
        # discovery (issuing BigQuery queries). last_altered is already available, so
        # there's no need to discover partitions for a table we're about to skip.
        if self._should_skip_profiling_due_to_staleness(bq_table):
            # Surface in the report, not just the log: skip_stale_tables defaults to True,
            # so without this operators silently lose profiling on long-idle tables.
            self.report.warning(
                title="Profiling skipped for stale table",
                message="Table was not modified within profiling.staleness_threshold_days; "
                "profiling was skipped. Set profiling.skip_stale_tables=false to profile it anyway.",
                context=table_ref,
            )
            return None

        try:
            profile_request = super().get_profile_request(table, schema_name, db_name)
        except (ValueError, AttributeError, KeyError, GoogleAPICallError) as e:
            # get_batch_kwargs raises ValueError for identifiers that fail security
            # validation or partitioned tables whose filter couldn't be built; the other
            # types cover BigQuery API/IAM/quota errors escaping discovery. Kept symmetric
            # with the external deferred path so neither aborts the whole project.
            self.report.warning(
                title="Table skipped during profiling",
                message=str(e),
                context=table_ref,
            )
            return None

        if not profile_request:
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
                    # Mirror the internal path (get_profile_request) which reports skips
                    # via the report: an external table that requires a filter we can't
                    # build is dropped from output, and operators need to see why.
                    self.report.warning(
                        title="External table skipped during profiling",
                        message="Could not construct required partition filters for this "
                        "external table; profiling was skipped to avoid a full scan.",
                        context=table_ref,
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

                        custom_sql = self._build_partition_profiling_sql(
                            safe_table_ref, partition_where, bq_table
                        )

                        request.batch_kwargs.update(
                            {
                                CUSTOM_SQL_KWARG: custom_sql,
                                PARTITION_HANDLING_KWARG: PARTITION_HANDLING_ENABLED,
                            }
                        )
                    else:
                        # Mirror the internal path: discovery produced filters but all
                        # failed validation, so this external table is profiled unfiltered.
                        self.report.warning(
                            title="Partition filters rejected during profiling",
                            message="Discovered partition filters for this external table "
                            "failed validation; profiling will proceed without a partition "
                            "filter (full scan), or fail if the table requires one.",
                            context=table_ref,
                        )

                delattr(request, "needs_partition_discovery")
                delattr(request, "bq_table")
                delattr(request, "db_name")
                delattr(request, "schema_name")

                return request

            except (
                ValueError,
                AttributeError,
                KeyError,
                GoogleAPICallError,
            ) as e:
                # Same reporting as the internal path. Catch only the failures partition
                # discovery / validation realistically raises (bad identifiers, missing
                # request plumbing, BigQuery API/IAM/quota errors) — an unexpected error
                # should surface loudly rather than silently drop the table.
                self.report.warning(
                    title="External table skipped during profiling",
                    message="Partition discovery failed for this external table; "
                    "profiling was skipped.",
                    context=f"{request.pretty_name}: {e}",
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
                    req = future_to_request[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        # The worker only catches the errors partition discovery
                        # realistically raises; anything else (RetryError, token
                        # RefreshError, TimeoutError, connection drops) would otherwise
                        # propagate out of as_completed and abort profiling for every
                        # remaining table. Report and continue instead.
                        self.report.warning(
                            title="External table skipped during profiling",
                            message="Partition discovery worker failed with an "
                            "unexpected error; profiling was skipped for this table.",
                            context=f"{req.pretty_name}: {e}",
                        )
                        continue
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
            logger.debug(
                f"Table {table.name} is stale - "
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

        date_columns = self._extract_date_columns_from_filters(partition_filters)

        if not date_columns:
            logger.debug(
                f"No date columns found in partition filters for {table.name}, skipping date windowing"
            )
            return partition_filters

        # Build one range per date column, reproducing the exact shape (format,
        # quoting, and any DATE()/TIMESTAMP() wrapper) of that column's equality
        # literal so the comparison keeps the column's type — a STRING column
        # must not be compared against an unquoted integer, which BigQuery
        # rejects with "No matching signature for operator >=".
        ranges: Dict[str, str] = {}
        for col_name in date_columns:
            literal = self._find_equality_literal(partition_filters, col_name)
            if literal is None:
                # No pinned date (e.g. an IS NOT NULL fallback): window from today
                # using a quoted ISO literal, which BigQuery coerces for
                # STRING/DATE/TIMESTAMP columns.
                fmt, quoted, wrapper = DATE_FORMAT_YYYY_MM_DD, True, None
                reference_date = datetime.now(timezone.utc).date()
            else:
                spec = self._parse_date_literal(literal)
                if spec is None:
                    # Unrecognised literal shape: leave the equality untouched
                    # rather than risk a type-mismatched range.
                    continue
                fmt, quoted, wrapper, reference_date = spec

            start_date = reference_date - timedelta(days=window_days)
            start_str = self._render_date_bound(start_date, fmt, quoted, wrapper)
            end_str = self._render_date_bound(reference_date, fmt, quoted, wrapper)
            ranges[col_name] = (
                f"`{col_name}` >= {start_str} AND `{col_name}` <= {end_str}"
            )

        if not ranges:
            return partition_filters

        # Drop each windowed column's single-date equality; the range replaces it.
        # Keeping `col = X` would AND-collapse the range back to `col = X`.
        windowed_filters = [
            f
            for f in partition_filters
            if not any(f"`{col}` = " in f for col in ranges)
        ]
        windowed_filters.extend(ranges.values())

        logger.debug(
            f"Applied {window_days}-day partition window for {table.name}: "
            f"windowed columns {sorted(ranges)}"
        )

        return windowed_filters

    def _find_equality_literal(
        self, partition_filters: List[str], col_name: str
    ) -> Optional[str]:
        for filter_expr in partition_filters:
            match = PARTITION_EQ_LITERAL_RE.search(filter_expr)
            if match and match.group(1) == col_name:
                return match.group(2).strip()
        return None

    def _parse_date_literal(
        self, literal: str
    ) -> Optional[Tuple[str, bool, Optional[str], date]]:
        """Parse a partition equality literal into (format, quoted, wrapper, date).

        Recognises bare and quoted YYYYMMDD / YYYY-MM-DD / YYYYMMDDHH literals,
        optionally wrapped in DATE()/DATETIME()/TIMESTAMP(). Returns None for
        anything else so the caller can leave that filter unchanged.
        """
        text = literal.strip()
        wrapper: Optional[str] = None
        wrapper_match = re.fullmatch(
            r"(DATE|DATETIME|TIMESTAMP)\((.+)\)", text, re.IGNORECASE
        )
        if wrapper_match:
            wrapper = wrapper_match.group(1).upper()
            text = wrapper_match.group(2).strip()

        quoted = len(text) >= 2 and text[0] == "'" and text[-1] == "'"
        inner = text[1:-1] if quoted else text

        for fmt, pattern in DATE_LITERAL_SHAPES:
            if pattern.fullmatch(inner):
                try:
                    parsed = datetime.strptime(inner, STRFTIME_FORMATS[fmt]).date()
                except ValueError:
                    return None
                return fmt, quoted, wrapper, parsed
        return None

    @staticmethod
    def _render_date_bound(
        value: date, fmt: str, quoted: bool, wrapper: Optional[str]
    ) -> str:
        rendered = value.strftime(STRFTIME_FORMATS[fmt])
        if quoted:
            rendered = f"'{rendered}'"
        if wrapper:
            rendered = f"{wrapper}({rendered})"
        return rendered

    def _extract_date_columns_from_filters(
        self, partition_filters: List[str]
    ) -> List[str]:
        """Heuristic: partition filters are built without full schema metadata, so a
        column is treated as a date column when its full name or any underscore-separated
        token is a known date-like name (so ``event_ts`` matches via ``ts``)."""
        date_columns: List[str] = []

        for filter_expr in partition_filters:
            for col_name in BACKTICK_COLUMN_NAME_RE.findall(filter_expr):
                if col_name in date_columns:
                    continue
                name_lower = col_name.lower()
                tokens = set(name_lower.split("_"))
                if name_lower in DATE_LIKE_COLUMN_NAMES or (
                    tokens & DATE_LIKE_COLUMN_NAMES
                ):
                    date_columns.append(col_name)

        return date_columns

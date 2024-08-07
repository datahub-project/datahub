import functools
import logging
import pathlib
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, MutableMapping, Optional, TypedDict

from google.cloud.bigquery import Client
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    get_time_bucket,
)
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryBaseConfig
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryProject,
    BigQuerySchemaApi,
    get_projects,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATETIME_FORMAT,
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlAggregatorReport,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
    FileBackedList,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class BigQueryTableReference(TypedDict):
    projectId: str
    datasetId: str
    tableId: str


class DMLJobStatistics(TypedDict):
    inserted_row_count: int
    deleted_row_count: int
    updated_row_count: int


class BigQueryJob(TypedDict):
    job_id: str
    project_id: str
    creation_time: datetime
    start_time: datetime
    end_time: datetime
    total_slot_ms: int
    user_email: str
    statement_type: str
    job_type: Optional[str]
    query: str
    destination_table: Optional[BigQueryTableReference]
    # NOTE: This does not capture referenced_view unlike GCP Logging Event
    referenced_tables: List[BigQueryTableReference]
    total_bytes_billed: int
    total_bytes_processed: int
    dml_statistics: Optional[DMLJobStatistics]
    session_id: Optional[str]
    query_hash: Optional[str]


class BigQueryQueriesExtractorConfig(BigQueryBaseConfig):
    # TODO: Support stateful ingestion for the time windows.
    window: BaseTimeWindowConfig = BaseTimeWindowConfig()

    local_temp_path: Optional[pathlib.Path] = Field(
        default=None,
        description="Local path to store the audit log.",
        # TODO: For now, this is simply an advanced config to make local testing easier.
        # Eventually, we will want to store date-specific files in the directory and use it as a cache.
        hidden_from_docs=True,
    )

    user_email_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for user emails to filter in usage.",
    )

    include_lineage: bool = True
    include_queries: bool = True
    include_usage_statistics: bool = True
    include_query_usage_statistics: bool = False
    include_operations: bool = True

    region_qualifiers: List[str] = Field(
        default=["region-us", "region-eu"],
        description="BigQuery regions to be scanned for bigquery jobs. See [this](https://cloud.google.com/bigquery/docs/information-schema-jobs) for details.",
    )


@dataclass
class BigQueryQueriesExtractorReport(Report):
    query_log_fetch_timer: PerfTimer = field(default_factory=PerfTimer)
    audit_log_preprocessing_timer: PerfTimer = field(default_factory=PerfTimer)
    audit_log_load_timer: PerfTimer = field(default_factory=PerfTimer)
    sql_aggregator: Optional[SqlAggregatorReport] = None
    num_queries_by_project: TopKDict[str, int] = field(default_factory=int_top_k_dict)

    num_total_queries: int = 0
    num_unique_queries: int = 0


class BigQueryQueriesExtractor:
    def __init__(
        self,
        connection: Client,
        schema_api: BigQuerySchemaApi,
        config: BigQueryQueriesExtractorConfig,
        structured_report: SourceReport,
        filters: BigQueryFilter,
        identifiers: BigQueryIdentifierBuilder,
        graph: Optional[DataHubGraph] = None,
        schema_resolver: Optional[SchemaResolver] = None,
        discovered_tables: Optional[List[str]] = None,
    ):
        self.connection = connection

        self.config = config
        self.filters = filters
        self.identifiers = identifiers
        self.schema_api = schema_api
        self.report = BigQueryQueriesExtractorReport()
        # self.filters = filters
        self.discovered_tables = discovered_tables

        self._structured_report = structured_report

        self.aggregator = SqlParsingAggregator(
            platform=self.identifiers.platform,
            platform_instance=self.identifiers.identifier_config.platform_instance,
            env=self.identifiers.identifier_config.env,
            schema_resolver=schema_resolver,
            graph=graph,
            eager_graph_load=False,
            generate_lineage=self.config.include_lineage,
            generate_queries=self.config.include_queries,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_query_usage_statistics=self.config.include_query_usage_statistics,
            usage_config=BaseUsageConfig(
                bucket_duration=self.config.window.bucket_duration,
                start_time=self.config.window.start_time,
                end_time=self.config.window.end_time,
                user_email_pattern=self.config.user_email_pattern,
            ),
            generate_operations=self.config.include_operations,
            is_temp_table=self.is_temp_table,
            is_allowed_table=self.is_allowed_table,
            format_queries=False,
        )
        self.report.sql_aggregator = self.aggregator.report

    @property
    def structured_report(self) -> SourceReport:
        return self._structured_report

    @functools.cached_property
    def local_temp_path(self) -> pathlib.Path:
        if self.config.local_temp_path:
            assert self.config.local_temp_path.is_dir()
            return self.config.local_temp_path

        path = pathlib.Path(tempfile.mkdtemp())
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using local temp path: {path}")
        return path

    def is_temp_table(self, name: str) -> bool:
        try:
            return BigqueryTableIdentifier.from_string_name(name).dataset.startswith(
                self.config.temp_table_dataset_prefix
            )
        except Exception:
            logger.warning(f"Error parsing table name {name} ")
            return False

    def is_allowed_table(self, name: str) -> bool:
        try:
            table_id = BigqueryTableIdentifier.from_string_name(name)
            if self.discovered_tables and str(table_id) not in self.discovered_tables:
                return False
            return self.filters.is_allowed(table_id)
        except Exception:
            logger.warning(f"Error parsing table name {name} ")
            return False

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        # TODO: Add some logic to check if the cached audit log is stale or not.
        audit_log_file = self.local_temp_path / "audit_log.sqlite"
        use_cached_audit_log = audit_log_file.exists()

        queries: FileBackedList[ObservedQuery]
        if use_cached_audit_log:
            logger.info("Using cached audit log")
            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
        else:
            audit_log_file.unlink(missing_ok=True)

            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
            entry: ObservedQuery

            with self.report.query_log_fetch_timer:
                for project in get_projects(
                    self.schema_api, self.structured_report, self.filters
                ):
                    for entry in self.fetch_query_log(project):
                        self.report.num_queries_by_project[project.id] += 1
                        queries.append(entry)
                self.report.num_total_queries = len(queries)

        with self.report.audit_log_preprocessing_timer:
            # Preprocessing stage that deduplicates the queries using query hash per usage bucket
            queries_deduped: MutableMapping[str, Dict[int, ObservedQuery]]
            queries_deduped = self.deduplicate_queries(queries)
            self.report.num_unique_queries = len(queries_deduped)

        with self.report.audit_log_load_timer:
            i = 0
            # Is FileBackedDict OrderedDict ? i.e. keys / values are retrieved in same order as added ?
            # Does aggregator expect to see queries in same order as they were executed ?
            for query_instances in queries_deduped.values():
                for _, query in query_instances.items():
                    if i > 0 and i % 1000 == 0:
                        logger.info(f"Added {i} query log entries to SQL aggregator")

                    self.aggregator.add(query)
                    i += 1

        yield from auto_workunit(self.aggregator.gen_metadata())

    def deduplicate_queries(
        self, queries: FileBackedList[ObservedQuery]
    ) -> MutableMapping[str, Dict[int, ObservedQuery]]:
        queries_deduped: FileBackedDict[Dict[int, ObservedQuery]] = FileBackedDict()
        for query in queries:
            time_bucket = (
                datetime_to_ts_millis(
                    get_time_bucket(query.timestamp, self.config.window.bucket_duration)
                )
                if query.timestamp
                else 0
            )
            query_hash = get_query_fingerprint(
                query.query, self.identifiers.platform, fast=True
            )
            query.query_hash = query_hash
            if query_hash not in queries_deduped:
                queries_deduped[query_hash] = {time_bucket: query}
            else:
                seen_query = queries_deduped[query_hash]
                if time_bucket not in seen_query:
                    seen_query[time_bucket] = query
                else:
                    observed_query = seen_query[time_bucket]
                    observed_query.usage_multiplier += 1
                    observed_query.timestamp = query.timestamp
                queries_deduped[query_hash] = seen_query

        return queries_deduped

    def fetch_query_log(self, project: BigqueryProject) -> Iterable[ObservedQuery]:

        # Multi-regions from https://cloud.google.com/bigquery/docs/locations#supported_locations
        regions = self.config.region_qualifiers

        for region in regions:
            # Each region needs to be a different query
            query_log_query = _build_enriched_query_log_query(
                project_id=project.id,
                region=region,
                start_time=self.config.window.start_time,
                end_time=self.config.window.end_time,
            )

            with self.structured_report.report_exc(
                f"Error fetching query log from BigQuery Project {project.id} Region {region}"
            ):
                logger.info(
                    f"Fetching query log from BigQuery Project {project.id} Region {region}"
                )
                resp = self.connection.query(query_log_query)

                for i, row in enumerate(resp):
                    if i > 0 and i % 1000 == 0:
                        logger.info(f"Processed {i} query log rows so far")

                    try:
                        entry = self._parse_audit_log_row(row)
                    except Exception as e:
                        self.structured_report.warning(
                            "Error parsing query log row",
                            context=f"{row}",
                            exc=e,
                        )
                    else:
                        yield entry

    def _parse_audit_log_row(self, row: BigQueryJob) -> ObservedQuery:
        timestamp: datetime = row["creation_time"]
        timestamp = timestamp.astimezone(timezone.utc)

        entry = ObservedQuery(
            query=row["query"],
            session_id=row["session_id"],
            timestamp=row["creation_time"],
            # TODO: Move user urn generation to BigQueryIdentifierBuilder
            user=(
                self.identifiers.gen_user_urn(row["user_email"])
                if row["user_email"]
                else None
            ),
            default_db=row["project_id"],
            default_schema=None,
            # Not using BQ query hash as it's not always present
            # query_hash=row["query_hash"],
        )

        return entry


def _build_enriched_query_log_query(
    project_id: str,
    region: str,
    start_time: datetime,
    end_time: datetime,
) -> str:

    audit_start_time = start_time.strftime(BQ_DATETIME_FORMAT)
    audit_end_time = end_time.strftime(BQ_DATETIME_FORMAT)

    # NOTE the use of creation_time as timestamp here
    # as JOBS table is partitioned by creation_time.
    # Using this column filter significantly reduces processed bytes.
    return f"""
        SELECT
            job_id,
            project_id,
            creation_time,
            start_time,
            end_time,
            total_slot_ms,
            user_email,
            statement_type,
            job_type,
            query,
            destination_table,
            referenced_tables,
            total_bytes_billed,
            total_bytes_processed,
            dml_statistics,
            session_info.session_id as session_id,
            query_info.query_hashes.normalized_literals as query_hash
        FROM
            `{project_id}`.`{region}`.INFORMATION_SCHEMA.JOBS
        WHERE
            creation_time >= '{audit_start_time}' AND
            creation_time <= '{audit_end_time}' AND
            error_result is null AND
            not CONTAINS_SUBSTR(query, '.INFORMATION_SCHEMA.')
        ORDER BY creation_time
    """

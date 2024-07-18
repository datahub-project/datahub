import functools
import logging
import pathlib
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, List, Optional, TypedDict, Union

from google.cloud.bigquery import Client
from pydantic import Field

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryBaseConfig,
    BigQueryFilterConfig,
    BigQueryIdentifierConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryProject,
    BigQuerySchemaApi,
)
from datahub.ingestion.source.bigquery_v2.common import BQ_DATETIME_FORMAT
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    SqlAggregatorReport,
    SqlParsingAggregator,
)
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedList
from datahub.utilities.perf_timer import PerfTimer

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

    include_lineage: bool = True
    include_queries: bool = True
    include_usage_statistics: bool = True
    include_query_usage_statistics: bool = False
    include_operations: bool = True


@dataclass
class BigQueryQueriesExtractorReport(Report):
    query_log_fetch_timer: PerfTimer = field(default_factory=PerfTimer)
    audit_log_load_timer: PerfTimer = field(default_factory=PerfTimer)
    sql_aggregator: Optional[SqlAggregatorReport] = None


class BigQueryQueriesExtractor:
    def __init__(
        self,
        connection: Client,
        schema_api: BigQuerySchemaApi,
        config: BigQueryQueriesExtractorConfig,
        structured_report: SourceReport,
        # TODO: take mixins for filter and identifier instead of config itself
        filter_config: BigQueryFilterConfig,
        identifier_config: BigQueryIdentifierConfig,
        graph: Optional[DataHubGraph] = None,
        schema_resolver: Optional[SchemaResolver] = None,
        discovered_tables: Optional[List[str]] = None,
    ):
        self.connection = connection

        self.config = config
        self.filter_config = filter_config
        self.identifier_config = identifier_config
        self.schema_api = schema_api
        self.report = BigQueryQueriesExtractorReport()
        # self.filters = filters
        self.discovered_tables = discovered_tables

        self._structured_report = structured_report

        self.aggregator = SqlParsingAggregator(
            platform="bigquery",
            platform_instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
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
                # TODO make the rest of the fields configurable
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
        return BigqueryTableIdentifier.from_string_name(name).dataset.startswith(
            self.config.temp_table_dataset_prefix
        )

    def is_allowed_table(self, name: str) -> bool:
        if (
            self.discovered_tables
            and str(BigqueryTableIdentifier.from_string_name(name))
            not in self.discovered_tables
        ):
            return False
        return True

    # TODO: Remove the code duplication. Also present in main bigquery source
    def _get_projects(self) -> List[BigqueryProject]:
        logger.info("Getting projects")
        if self.config.project_ids:
            return [
                BigqueryProject(id=project_id, name=project_id)
                for project_id in self.config.project_ids
            ]
        else:
            return list(self._query_project_list())

    def _query_project_list(self) -> Iterable[BigqueryProject]:
        try:
            projects = self.schema_api.get_projects()

            if (
                not projects
            ):  # Report failure on exception and if empty list is returned
                self.structured_report.failure(
                    title="Get projects didn't return any project. ",
                    message="Maybe resourcemanager.projects.get permission is missing for the service account. "
                    "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
                )
        except Exception as e:
            self.structured_report.failure(
                title="Failed to get BigQuery Projects",
                message="Maybe resourcemanager.projects.get permission is missing for the service account. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
                exc=e,
            )
            projects = []

        for project in projects:
            if self.filter_config.project_id_pattern.allowed(project.id):
                yield project

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        # TODO: Add some logic to check if the cached audit log is stale or not.
        audit_log_file = self.local_temp_path / "audit_log.sqlite"
        use_cached_audit_log = audit_log_file.exists()

        queries: FileBackedList[Union[PreparsedQuery, ObservedQuery]]
        if use_cached_audit_log:
            logger.info("Using cached audit log")
            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
        else:
            audit_log_file.unlink(missing_ok=True)

            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
            entry: Union[PreparsedQuery, ObservedQuery]

            with self.report.query_log_fetch_timer:
                for project in self._get_projects():
                    for entry in self.fetch_query_log(project):
                        queries.append(entry)

        with self.report.audit_log_load_timer:
            for query in queries:
                self.aggregator.add(query)

        yield from auto_workunit(self.aggregator.gen_metadata())

    def fetch_query_log(
        self, project: BigqueryProject
    ) -> Iterable[Union[PreparsedQuery, ObservedQuery]]:

        # TODO: support all regions - maybe a config
        regions = ["region-us"]

        for region in regions:
            # Each region needs to be a different query
            query_log_query = _build_enriched_query_log_query(
                project_id=project.id,
                region=region,
                start_time=self.config.window.start_time,
                end_time=self.config.window.end_time,
                # TODO: filters: deny users based on config
            )

            with self.structured_report.report_exc(
                f"Error fetching query log from BigQuery Project {project.id}"
            ):
                logger.info(f"Fetching query log from BigQuery Project {project.id}")
                resp = self.connection.query(query_log_query)

                for i, row in enumerate(resp):
                    if i % 1000 == 0:
                        logger.info(f"Processed {i} query log rows")

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

    def _parse_audit_log_row(
        self, row: BigQueryJob
    ) -> Union[ObservedQuery, PreparsedQuery]:

        timestamp: datetime = row["creation_time"]
        timestamp = timestamp.astimezone(timezone.utc)

        entry = ObservedQuery(
            query=row["query"],
            session_id=row["session_id"],
            timestamp=row["creation_time"],
            user=make_user_urn(row["user_email"]) if row["user_email"] else None,
            default_db=row["project_id"],
            default_schema=None,
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

    # NOTE the use of creation_time instead of start_time
    # as JOBS table is partitioned by creation_time
    # Using this column significantly reduces processed bytes
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
            session_info.session_id as session_id
        FROM
            `{project_id}`.`{region}`.INFORMATION_SCHEMA.JOBS
        WHERE
            creation_time >= '{audit_start_time}' AND
            creation_time <= '{audit_end_time}'
    """

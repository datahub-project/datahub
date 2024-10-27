import functools
import logging
import pathlib
import tempfile
from datetime import datetime, timezone
from typing import Collection, Dict, Iterable, List, Optional, TypedDict

from google.cloud.bigquery import Client
from pydantic import Field, PositiveInt

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    get_time_bucket,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryBaseConfig
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryQueriesExtractorReport,
)
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
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
    FileBackedList,
)
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class BigQueryTableReference(TypedDict):
    project_id: str
    dataset_id: str
    table_id: str


class DMLJobStatistics(TypedDict):
    inserted_row_count: int
    deleted_row_count: int
    updated_row_count: int


class BigQueryJob(TypedDict):
    job_id: str
    project_id: str
    creation_time: datetime
    user_email: str
    query: str
    session_id: Optional[str]
    query_hash: Optional[str]

    statement_type: str
    destination_table: Optional[BigQueryTableReference]
    referenced_tables: List[BigQueryTableReference]
    # NOTE: This does not capture referenced_view unlike GCP Logging Event


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

    top_n_queries: PositiveInt = Field(
        default=10, description="Number of top queries to save to each table."
    )

    include_lineage: bool = True
    include_queries: bool = True
    include_usage_statistics: bool = True
    include_query_usage_statistics: bool = True
    include_operations: bool = True

    region_qualifiers: List[str] = Field(
        default=["region-us", "region-eu"],
        description="BigQuery regions to be scanned for bigquery jobs. "
        "See [this](https://cloud.google.com/bigquery/docs/information-schema-jobs#scope_and_syntax) for details.",
    )


class BigQueryQueriesExtractor(Closeable):
    """
    Extracts query audit log and generates usage/lineage/operation workunits.

    Some notable differences in this wrt older usage extraction method are:
    1. For every lineage/operation workunit, corresponding query id is also present
    2. Operation aspect for a particular query is emitted at max once(last occurence) for a day
    3. "DROP" operation accounts for usage here
    4. userEmail is not populated in datasetUsageStatistics aspect, only user urn

    """

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
        discovered_tables: Optional[Collection[str]] = None,
    ):
        self.connection = connection

        self.config = config
        self.filters = filters
        self.identifiers = identifiers
        self.schema_api = schema_api
        self.report = BigQueryQueriesExtractorReport()
        self.discovered_tables = (
            set(
                map(
                    self.identifiers.standardize_identifier_case,
                    discovered_tables,
                )
            )
            if discovered_tables
            else None
        )

        self.structured_report = structured_report

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
                top_n_queries=self.config.top_n_queries,
            ),
            generate_operations=self.config.include_operations,
            is_temp_table=self.is_temp_table,
            is_allowed_table=self.is_allowed_table,
            format_queries=False,
        )

        self.report.sql_aggregator = self.aggregator.report
        self.report.num_discovered_tables = (
            len(self.discovered_tables) if self.discovered_tables else None
        )

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
            table = BigqueryTableIdentifier.from_string_name(name)

            if table.dataset.startswith(self.config.temp_table_dataset_prefix):
                return True

            # This is also a temp table if
            #   1. this name would be allowed by the dataset patterns, and
            #   2. we have a list of discovered tables, and
            #   3. it's not in the discovered tables list
            if (
                self.filters.is_allowed(table)
                and self.discovered_tables
                and str(BigQueryTableRef(table)) not in self.discovered_tables
            ):
                logger.debug(f"inferred as temp table {name}")
                self.report.inferred_temp_tables.add(name)
                return True

        except Exception:
            logger.warning(f"Error parsing table name {name} ")
        return False

    def is_allowed_table(self, name: str) -> bool:
        try:
            table = BigqueryTableIdentifier.from_string_name(name)
            if (
                self.discovered_tables
                and str(BigQueryTableRef(table)) not in self.discovered_tables
            ):
                logger.debug(f"not allowed table {name}")
                return False
            return self.filters.is_allowed(table)
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
        logger.info(f"Found {self.report.num_total_queries} total queries")

        with self.report.audit_log_preprocessing_timer:
            # Preprocessing stage that deduplicates the queries using query hash per usage bucket
            # Note: FileBackedDict is an ordered dictionary, so the order of execution of
            # queries is inherently maintained
            queries_deduped: FileBackedDict[Dict[int, ObservedQuery]]
            queries_deduped = self.deduplicate_queries(queries)
            self.report.num_unique_queries = len(queries_deduped)
            logger.info(f"Found {self.report.num_unique_queries} unique queries")

        with self.report.audit_log_load_timer, queries_deduped:
            i = 0
            for _, query_instances in queries_deduped.items():
                for query in query_instances.values():
                    if i > 0 and i % 10000 == 0:
                        logger.info(
                            f"Added {i} query log equeries_dedupedntries to SQL aggregator"
                        )
                        if self.report.sql_aggregator:
                            logger.info(self.report.sql_aggregator.as_string())

                    self.aggregator.add(query)
                    i += 1

        yield from auto_workunit(self.aggregator.gen_metadata())

        if not use_cached_audit_log:
            queries.close()
            shared_connection.close()
            audit_log_file.unlink(missing_ok=True)

    def deduplicate_queries(
        self, queries: FileBackedList[ObservedQuery]
    ) -> FileBackedDict[Dict[int, ObservedQuery]]:

        # This fingerprint based deduplication is done here to reduce performance hit due to
        # repetitive sql parsing while adding observed query to aggregator that would otherwise
        # parse same query multiple times. In future, aggregator may absorb this deduplication.
        # With current implementation, it is possible that "Operation"(e.g. INSERT) is reported
        # only once per day, although it may have happened multiple times throughout the day.

        queries_deduped: FileBackedDict[Dict[int, ObservedQuery]] = FileBackedDict()

        for i, query in enumerate(queries):
            if i > 0 and i % 10000 == 0:
                logger.info(f"Preprocessing completed for {i} query log entries")

            # query = ObservedQuery(**asdict(query))

            time_bucket = 0
            if query.timestamp:
                time_bucket = datetime_to_ts_millis(
                    get_time_bucket(query.timestamp, self.config.window.bucket_duration)
                )

            # Not using original BQ query hash as it's not always present
            query.query_hash = get_query_fingerprint(
                query.query, self.identifiers.platform, fast=True
            )

            query_instances = queries_deduped.setdefault(query.query_hash, {})

            observed_query = query_instances.setdefault(time_bucket, query)

            # If the query already exists for this time bucket, update its attributes
            if observed_query is not query:
                observed_query.usage_multiplier += 1
                observed_query.timestamp = query.timestamp

        return queries_deduped

    def fetch_query_log(self, project: BigqueryProject) -> Iterable[ObservedQuery]:

        # Multi-regions from https://cloud.google.com/bigquery/docs/locations#supported_locations
        regions = self.config.region_qualifiers

        for region in regions:
            with self.structured_report.report_exc(
                f"Error fetching query log from BQ Project {project.id} for {region}"
            ):
                yield from self.fetch_region_query_log(project, region)

    def fetch_region_query_log(
        self, project: BigqueryProject, region: str
    ) -> Iterable[ObservedQuery]:

        # Each region needs to be a different query
        query_log_query = _build_enriched_query_log_query(
            project_id=project.id,
            region=region,
            start_time=self.config.window.start_time,
            end_time=self.config.window.end_time,
        )

        logger.info(f"Fetching query log from BQ Project {project.id} for {region}")
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

        # Usually bigquery identifiers are always referred as <dataset>.<table> and only
        # temporary tables are referred as <table> alone without project or dataset name.
        # Note that temporary tables can also be referenced using _SESSION.<table>
        # More details here - https://cloud.google.com/bigquery/docs/multi-statement-queries
        # Also _ at start considers this as temp dataset as per `temp_table_dataset_prefix` config
        TEMP_TABLE_QUALIFIER = "_SESSION"

        query = _extract_query_text(row)

        entry = ObservedQuery(
            query=query,
            session_id=row["session_id"],
            timestamp=row["creation_time"],
            user=(
                CorpUserUrn.from_string(
                    self.identifiers.gen_user_urn(row["user_email"])
                )
                if row["user_email"]
                else None
            ),
            default_db=row["project_id"],
            default_schema=TEMP_TABLE_QUALIFIER,
            query_hash=row["query_hash"],
            extra_info={
                "job_id": row["job_id"],
                "statement_type": row["statement_type"],
                "destination_table": row["destination_table"],
                "referenced_tables": row["referenced_tables"],
            },
        )

        return entry

    def close(self) -> None:
        self.aggregator.close()


def _extract_query_text(row: BigQueryJob) -> str:
    # We wrap select statements in a CTE to make them parseable as DML statement.
    # This is a workaround to support the case where the user runs a query and inserts the result into a table.
    # NOTE This will result in showing modified query instead of original query in DataHub UI
    # Alternatively, this support needs to be added more natively in aggregator.add_observed_query
    if (
        row["statement_type"] == "SELECT"
        and row["destination_table"]
        and not row["destination_table"]["table_id"].startswith("anon")
    ):
        table_name = BigqueryTableIdentifier(
            row["destination_table"]["project_id"],
            row["destination_table"]["dataset_id"],
            row["destination_table"]["table_id"],
        ).raw_table_name()
        query = f"""CREATE TABLE `{table_name}` AS
                (
                    {row["query"]}
                )"""
    else:
        query = row["query"]
    return query


def _build_enriched_query_log_query(
    project_id: str,
    region: str,
    start_time: datetime,
    end_time: datetime,
) -> str:

    audit_start_time = start_time.strftime(BQ_DATETIME_FORMAT)
    audit_end_time = end_time.strftime(BQ_DATETIME_FORMAT)

    # List of all statement types
    # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.QueryStatementType
    UNSUPPORTED_STATEMENT_TYPES = [
        # procedure
        "CREATE_PROCEDURE",
        "DROP_PROCEDURE",
        "CALL",
        "SCRIPT",  # individual statements in executed procedure are present as separate jobs
        # schema
        "CREATE_SCHEMA",
        "DROP_SCHEMA",
        # function
        "CREATE_FUNCTION",
        "CREATE_TABLE_FUNCTION",
        "DROP_FUNCTION",
        # policies
        "CREATE_ROW_ACCESS_POLICY",
        "DROP_ROW_ACCESS_POLICY",
    ]

    unsupported_statement_types = ",".join(
        [f"'{statement_type}'" for statement_type in UNSUPPORTED_STATEMENT_TYPES]
    )

    # NOTE the use of partition column creation_time as timestamp here.
    # Currently, only required columns are fetched. There are more columns such as
    # total_slot_ms, job_type, total_bytes_billed, dml_statistics(inserted_row_count, etc)
    # that may be fetched as required in future. Refer below link for list of all columns
    # https://cloud.google.com/bigquery/docs/information-schema-jobs#schema
    return f"""\
        SELECT
            job_id,
            project_id,
            creation_time,
            user_email,
            query,
            session_info.session_id as session_id,
            query_info.query_hashes.normalized_literals as query_hash,
            statement_type,
            destination_table,
            referenced_tables
        FROM
            `{project_id}`.`{region}`.INFORMATION_SCHEMA.JOBS
        WHERE
            creation_time >= '{audit_start_time}' AND
            creation_time <= '{audit_end_time}' AND
            error_result is null AND
            not CONTAINS_SUBSTR(query, '.INFORMATION_SCHEMA.') AND
            statement_type not in ({unsupported_statement_types})
        ORDER BY creation_time
    """

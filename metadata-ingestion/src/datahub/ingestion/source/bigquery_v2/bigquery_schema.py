import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, FrozenSet, Iterable, Iterator, List, Optional

from google.api_core import retry
from google.cloud import bigquery, datacatalog_v1, resourcemanager_v3
from google.cloud.bigquery import retry as bq_retry
from google.cloud.bigquery.table import (
    RowIterator,
    TableListItem,
    TimePartitioning,
    TimePartitioningType,
)

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_helper import parse_labels
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryFilter
from datahub.ingestion.source.bigquery_v2.queries import (
    BigqueryQuery,
    BigqueryTableType,
)
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable, BaseView
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.ratelimiter import RateLimiter

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigqueryColumn(BaseColumn):
    field_path: str
    is_partition_column: bool
    cluster_column_position: Optional[int]
    policy_tags: Optional[List[str]] = None


@dataclass
class BigqueryTableConstraint:
    name: str
    project_id: str
    dataset_name: str
    table_name: str
    type: str
    field_path: str
    referenced_project_id: Optional[str] = None
    referenced_dataset: Optional[str] = None
    referenced_table_name: Optional[str] = None
    referenced_column_name: Optional[str] = None


RANGE_PARTITION_NAME: str = "RANGE"


@dataclass
class PartitionInfo:
    field: str
    # Data type is optional as we not have it when we set it from TimePartitioning
    column: Optional[BigqueryColumn] = None
    type: str = TimePartitioningType.DAY
    expiration_ms: Optional[int] = None
    require_partition_filter: bool = False

    # TimePartitioning field doesn't provide data_type so we have to add it afterwards
    @classmethod
    def from_time_partitioning(
        cls, time_partitioning: TimePartitioning
    ) -> "PartitionInfo":
        return cls(
            field=time_partitioning.field or "_PARTITIONTIME",
            type=time_partitioning.type_,
            expiration_ms=time_partitioning.expiration_ms,
            require_partition_filter=time_partitioning.require_partition_filter,
        )

    @classmethod
    def from_range_partitioning(
        cls, range_partitioning: Dict[str, Any]
    ) -> Optional["PartitionInfo"]:
        field: Optional[str] = range_partitioning.get("field")
        if not field:
            return None

        return cls(
            field=field,
            type=RANGE_PARTITION_NAME,
        )

    @classmethod
    def from_table_info(cls, table_info: TableListItem) -> Optional["PartitionInfo"]:
        RANGE_PARTITIONING_KEY: str = "rangePartitioning"

        if table_info.time_partitioning:
            return PartitionInfo.from_time_partitioning(table_info.time_partitioning)
        elif RANGE_PARTITIONING_KEY in table_info._properties:
            return PartitionInfo.from_range_partitioning(
                table_info._properties[RANGE_PARTITIONING_KEY]
            )
        else:
            return None


@dataclass
class BigqueryTable(BaseTable):
    expires: Optional[datetime] = None
    clustering_fields: Optional[List[str]] = None
    labels: Optional[Dict[str, str]] = None
    num_partitions: Optional[int] = None
    max_partition_id: Optional[str] = None
    max_shard_id: Optional[str] = None
    active_billable_bytes: Optional[int] = None
    long_term_billable_bytes: Optional[int] = None
    partition_info: Optional[PartitionInfo] = None
    columns_ignore_from_profiling: List[str] = field(default_factory=list)
    external: bool = False
    constraints: List[BigqueryTableConstraint] = field(default_factory=list)
    table_type: Optional[str] = None


@dataclass
class BigqueryView(BaseView):
    columns: List[BigqueryColumn] = field(default_factory=list)
    materialized: bool = False
    labels: Optional[Dict[str, str]] = None


@dataclass
class BigqueryTableSnapshot(BaseTable):
    # Upstream table identifier
    base_table_identifier: Optional[BigqueryTableIdentifier] = None
    snapshot_time: Optional[datetime] = None
    columns: List[BigqueryColumn] = field(default_factory=list)


@dataclass
class BigqueryDataset:
    name: str
    labels: Optional[Dict[str, str]] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    location: Optional[str] = None
    comment: Optional[str] = None
    tables: List[BigqueryTable] = field(default_factory=list)
    views: List[BigqueryView] = field(default_factory=list)
    snapshots: List[BigqueryTableSnapshot] = field(default_factory=list)
    columns: List[BigqueryColumn] = field(default_factory=list)


@dataclass
class BigqueryProject:
    id: str
    name: str
    datasets: List[BigqueryDataset] = field(default_factory=list)


class BigQuerySchemaApi:
    def __init__(
        self,
        report: BigQuerySchemaApiPerfReport,
        client: bigquery.Client,
        projects_client: resourcemanager_v3.ProjectsClient,
        datacatalog_client: Optional[datacatalog_v1.PolicyTagManagerClient] = None,
    ) -> None:
        self.bq_client = client
        self.projects_client = projects_client
        self.report = report
        self.datacatalog_client = datacatalog_client

    def get_query_result(self, query: str) -> RowIterator:
        def _should_retry(exc: BaseException) -> bool:
            logger.debug(f"Exception occurred for job query. Reason: {exc}")
            # Jobs sometimes fail with transient errors.
            # This is not currently handled by the python-bigquery client.
            # https://github.com/googleapis/python-bigquery/issues/23
            return "Retrying the job may solve the problem" in str(exc)

        logger.debug(f"Query : {query}")
        resp = self.bq_client.query(
            query,
            job_retry=retry.Retry(
                predicate=lambda exc: (
                    bq_retry.DEFAULT_JOB_RETRY._predicate(exc) or _should_retry(exc)
                ),
                deadline=bq_retry.DEFAULT_JOB_RETRY._deadline,
            ),
        )
        return resp.result()

    @lru_cache(maxsize=1)
    def get_projects(self, max_results_per_page: int = 100) -> List[BigqueryProject]:
        def _should_retry(exc: BaseException) -> bool:
            logger.debug(
                f"Exception occurred for project.list api. Reason: {exc}. Retrying api request..."
            )
            self.report.num_list_projects_retry_request += 1
            return True

        page_token = None
        projects: List[BigqueryProject] = []
        with self.report.list_projects_timer:
            while True:
                try:
                    self.report.num_list_projects_api_requests += 1
                    # Bigquery API has limit in calling project.list request i.e. 2 request per second.
                    # https://cloud.google.com/bigquery/quotas#api_request_quotas
                    # Whenever this limit reached an exception occur with msg
                    # 'Quota exceeded: Your user exceeded quota for concurrent project.lists requests.'
                    # Hence, added the api request retry of 15 min.
                    # We already tried adding rate_limit externally, proving max_result and page_size
                    # to restrict the request calls inside list_project but issue still occurred.
                    projects_iterator = self.bq_client.list_projects(
                        max_results=max_results_per_page,
                        page_token=page_token,
                        timeout=900,
                        retry=retry.Retry(
                            predicate=_should_retry,
                            initial=10,
                            maximum=180,
                            multiplier=4,
                            timeout=900,
                        ),
                    )
                    _projects: List[BigqueryProject] = [
                        BigqueryProject(id=p.project_id, name=p.friendly_name)
                        for p in projects_iterator
                    ]
                    projects.extend(_projects)
                    self.report.num_listed_projects = len(projects)
                    page_token = projects_iterator.next_page_token
                    if not page_token:
                        break
                except Exception as e:
                    logger.error(f"Error getting projects. {e}", exc_info=True)
                    return []
        return projects

    @lru_cache(maxsize=1)
    def get_projects_with_labels(self, labels: FrozenSet[str]) -> List[BigqueryProject]:
        with self.report.list_projects_with_labels_timer:
            try:
                projects = []
                labels_query = " OR ".join([f"labels.{label}" for label in labels])
                for project in self.projects_client.search_projects(query=labels_query):
                    projects.append(
                        BigqueryProject(
                            id=project.project_id, name=project.display_name
                        )
                    )

                return projects

            except Exception as e:
                logger.error(
                    f"Error getting projects with labels: {labels}. {e}", exc_info=True
                )
                return []

    def get_datasets_for_project_id(
        self, project_id: str, maxResults: Optional[int] = None
    ) -> List[BigqueryDataset]:
        with self.report.list_datasets_timer:
            self.report.num_list_datasets_api_requests += 1
            datasets = self.bq_client.list_datasets(project_id, max_results=maxResults)
            return [
                BigqueryDataset(
                    name=d.dataset_id,
                    labels=d.labels,
                    location=(
                        d._properties.get("location")
                        if hasattr(d, "_properties") and isinstance(d._properties, dict)
                        else None
                    ),
                )
                for d in datasets
            ]

    # This is not used anywhere
    def get_datasets_for_project_id_with_information_schema(
        self, project_id: str
    ) -> List[BigqueryDataset]:
        """
        This method is not used as of now, due to below limitation.
        Current query only fetches datasets in US region
        We'll need Region wise separate queries to fetch all datasets
        https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata
        """
        schemas = self.get_query_result(
            BigqueryQuery.datasets_for_project_id.format(project_id=project_id),
        )
        return [
            BigqueryDataset(
                name=s.table_schema,
                created=s.created,
                location=s.location,
                last_altered=s.last_altered,
                comment=s.comment,
            )
            for s in schemas
        ]

    def list_tables(
        self, dataset_name: str, project_id: str
    ) -> Iterator[TableListItem]:
        with PerfTimer() as current_timer:
            for table in self.bq_client.list_tables(f"{project_id}.{dataset_name}"):
                with current_timer.pause():
                    yield table
            self.report.num_list_tables_api_requests += 1
            self.report.list_tables_sec += current_timer.elapsed_seconds()

    def get_tables_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        tables: Dict[str, TableListItem],
        report: BigQueryV2Report,
        with_partitions: bool = False,
    ) -> Iterator[BigqueryTable]:
        with PerfTimer() as current_timer:
            filter_clause: str = ", ".join(f"'{table}'" for table in tables.keys())

            if with_partitions:
                query_template = BigqueryQuery.tables_for_dataset
            else:
                query_template = BigqueryQuery.tables_for_dataset_without_partition_data

            # Tables are ordered by name and table suffix to make sure we always process the latest sharded table
            # and skip the others. Sharded tables are tables with suffix _20220102
            cur = self.get_query_result(
                query_template.format(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_filter=(
                        f" and t.table_name in ({filter_clause})"
                        if filter_clause
                        else ""
                    ),
                ),
            )

            for table in cur:
                try:
                    with current_timer.pause():
                        yield BigQuerySchemaApi._make_bigquery_table(
                            table, tables.get(table.table_name)
                        )
                except Exception as e:
                    table_name = f"{project_id}.{dataset_name}.{table.table_name}"
                    report.warning(
                        title="Failed to process table",
                        message="Error encountered while processing table",
                        context=table_name,
                        exc=e,
                    )
            self.report.num_get_tables_for_dataset_api_requests += 1
            self.report.get_tables_for_dataset_sec += current_timer.elapsed_seconds()

    @staticmethod
    def _make_bigquery_table(
        table: bigquery.Row, table_basic: Optional[TableListItem]
    ) -> BigqueryTable:
        # Some properties we want to capture are only available from the TableListItem
        # we get from an earlier query of the list of tables.
        try:
            expiration = table_basic.expires if table_basic else None
        except OverflowError:
            logger.info(f"Invalid expiration time for table {table.table_name}.")
            expiration = None

        _, shard = BigqueryTableIdentifier.get_table_and_shard(table.table_name)
        return BigqueryTable(
            name=table.table_name,
            created=table.created,
            table_type=table.table_type,
            last_altered=(
                datetime.fromtimestamp(
                    table.get("last_altered") / 1000, tz=timezone.utc
                )
                if table.get("last_altered") is not None
                else None
            ),
            size_in_bytes=table.get("bytes"),
            rows_count=table.get("row_count"),
            comment=table.comment,
            ddl=table.ddl,
            expires=expiration,
            labels=table_basic.labels if table_basic else None,
            partition_info=(
                PartitionInfo.from_table_info(table_basic) if table_basic else None
            ),
            clustering_fields=table_basic.clustering_fields if table_basic else None,
            max_partition_id=table.get("max_partition_id"),
            max_shard_id=shard,
            num_partitions=table.get("num_partitions"),
            active_billable_bytes=table.get("active_billable_bytes"),
            long_term_billable_bytes=table.get("long_term_billable_bytes"),
            external=(table.table_type == BigqueryTableType.EXTERNAL),
        )

    def get_views_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        has_data_read: bool,
        report: BigQueryV2Report,
    ) -> Iterator[BigqueryView]:
        with PerfTimer() as current_timer:
            if has_data_read:
                # If profiling is enabled
                cur = self.get_query_result(
                    BigqueryQuery.views_for_dataset.format(
                        project_id=project_id, dataset_name=dataset_name
                    ),
                )
            else:
                cur = self.get_query_result(
                    BigqueryQuery.views_for_dataset_without_data_read.format(
                        project_id=project_id, dataset_name=dataset_name
                    ),
                )

            for table in cur:
                try:
                    with current_timer.pause():
                        yield BigQuerySchemaApi._make_bigquery_view(table)
                except Exception as e:
                    view_name = f"{project_id}.{dataset_name}.{table.table_name}"
                    report.warning(
                        title="Failed to process view",
                        message="Error encountered while processing view",
                        context=view_name,
                        exc=e,
                    )
            self.report.num_get_views_for_dataset_api_requests += 1
            self.report.get_views_for_dataset_sec += current_timer.elapsed_seconds()

    @staticmethod
    def _make_bigquery_view(view: bigquery.Row) -> BigqueryView:
        return BigqueryView(
            name=view.table_name,
            created=view.created,
            last_altered=(
                datetime.fromtimestamp(view.get("last_altered") / 1000, tz=timezone.utc)
                if view.get("last_altered") is not None
                else None
            ),
            comment=view.comment,
            view_definition=view.view_definition,
            materialized=view.table_type == BigqueryTableType.MATERIALIZED_VIEW,
            size_in_bytes=view.get("size_bytes"),
            rows_count=view.get("row_count"),
            labels=parse_labels(view.labels) if view.get("labels") else None,
        )

    def get_policy_tags_for_column(
        self,
        project_id: str,
        dataset_name: str,
        table_name: str,
        column_name: str,
        report: BigQueryV2Report,
        rate_limiter: Optional[RateLimiter] = None,
    ) -> Iterable[str]:
        assert self.datacatalog_client

        try:
            # Get the table schema
            table_ref = f"{project_id}.{dataset_name}.{table_name}"
            table = self.bq_client.get_table(table_ref)
            schema = table.schema

            # Find the specific field in the schema
            field = next((f for f in schema if f.name == column_name), None)
            if not field or not field.policy_tags:
                return

            # Retrieve policy tag display names
            for policy_tag_name in field.policy_tags.names:
                try:
                    if rate_limiter:
                        with rate_limiter:
                            policy_tag = self.datacatalog_client.get_policy_tag(
                                name=policy_tag_name
                            )
                    else:
                        policy_tag = self.datacatalog_client.get_policy_tag(
                            name=policy_tag_name
                        )
                    yield policy_tag.display_name
                except Exception as e:
                    report.warning(
                        title="Failed to retrieve policy tag",
                        message="Unexpected error when retrieving policy tag for column",
                        context=f"policy tag {policy_tag_name} for column {column_name} in table {table_ref}",
                        exc=e,
                    )
        except Exception as e:
            report.warning(
                title="Failed to retrieve policy tag for table",
                message="Unexpected error retrieving policy tag for table",
                context=table_ref,
                exc=e,
            )

    def get_table_constraints_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        report: BigQueryV2Report,
    ) -> Optional[Dict[str, List[BigqueryTableConstraint]]]:
        constraints: Dict[str, List[BigqueryTableConstraint]] = defaultdict(list)
        with PerfTimer() as timer:
            try:
                cur = self.get_query_result(
                    BigqueryQuery.constraints_for_table.format(
                        project_id=project_id, dataset_name=dataset_name
                    )
                )
            except Exception as e:
                report.warning(
                    title="Failed to retrieve table constraints for dataset",
                    message="Query to get table constraints for dataset failed with exception",
                    context=f"{project_id}.{dataset_name}",
                    exc=e,
                )
                return None

            for constraint in cur:
                constraints[constraint.table_name].append(
                    BigqueryTableConstraint(
                        name=constraint.constraint_name,
                        project_id=constraint.table_catalog,
                        dataset_name=constraint.table_schema,
                        table_name=constraint.table_name,
                        type=constraint.constraint_type,
                        field_path=constraint.column_name,
                        referenced_project_id=constraint.referenced_catalog
                        if constraint.constraint_type == "FOREIGN KEY"
                        else None,
                        referenced_dataset=constraint.referenced_schema
                        if constraint.constraint_type == "FOREIGN KEY"
                        else None,
                        referenced_table_name=constraint.referenced_table
                        if constraint.constraint_type == "FOREIGN KEY"
                        else None,
                        referenced_column_name=constraint.referenced_column
                        if constraint.constraint_type == "FOREIGN KEY"
                        else None,
                    )
                )
            self.report.num_get_table_constraints_for_dataset_api_requests += 1
            self.report.get_table_constraints_for_dataset_sec += timer.elapsed_seconds()

        return constraints

    def get_columns_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        column_limit: int,
        report: BigQueryV2Report,
        run_optimized_column_query: bool = False,
        extract_policy_tags_from_catalog: bool = False,
        rate_limiter: Optional[RateLimiter] = None,
    ) -> Optional[Dict[str, List[BigqueryColumn]]]:
        columns: Dict[str, List[BigqueryColumn]] = defaultdict(list)
        with PerfTimer() as timer:
            try:
                cur = self.get_query_result(
                    (
                        BigqueryQuery.columns_for_dataset.format(
                            project_id=project_id, dataset_name=dataset_name
                        )
                        if not run_optimized_column_query
                        else BigqueryQuery.optimized_columns_for_dataset.format(
                            project_id=project_id,
                            dataset_name=dataset_name,
                            column_limit=column_limit,
                        )
                    ),
                )
            except Exception as e:
                report.warning(
                    title="Failed to retrieve columns for dataset",
                    message="Query to get columns for dataset failed with exception",
                    context=f"{project_id}.{dataset_name}",
                    exc=e,
                )
                return None

            last_seen_table: str = ""
            for column in cur:
                with timer.pause():
                    if (
                        column_limit
                        and column.table_name in columns
                        and len(columns[column.table_name]) >= column_limit
                    ):
                        if last_seen_table != column.table_name:
                            logger.warning(
                                f"{project_id}.{dataset_name}.{column.table_name} contains more than {column_limit} columns, only processing {column_limit} columns"
                            )
                            last_seen_table = column.table_name
                    else:
                        columns[column.table_name].append(
                            BigqueryColumn(
                                name=column.column_name,
                                ordinal_position=column.ordinal_position,
                                field_path=column.field_path,
                                is_nullable=column.is_nullable == "YES",
                                data_type=column.data_type,
                                comment=column.comment,
                                is_partition_column=column.is_partitioning_column
                                == "YES",
                                cluster_column_position=column.clustering_ordinal_position,
                                policy_tags=(
                                    list(
                                        self.get_policy_tags_for_column(
                                            project_id,
                                            dataset_name,
                                            column.table_name,
                                            column.column_name,
                                            report,
                                            rate_limiter,
                                        )
                                    )
                                    if extract_policy_tags_from_catalog
                                    else []
                                ),
                            )
                        )
            self.report.num_get_columns_for_dataset_api_requests += 1
            self.report.get_columns_for_dataset_sec += timer.elapsed_seconds()

        return columns

    def get_snapshots_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        has_data_read: bool,
        report: BigQueryV2Report,
    ) -> Iterator[BigqueryTableSnapshot]:
        with PerfTimer() as current_timer:
            if has_data_read:
                # If profiling is enabled
                cur = self.get_query_result(
                    BigqueryQuery.snapshots_for_dataset.format(
                        project_id=project_id, dataset_name=dataset_name
                    ),
                )
            else:
                cur = self.get_query_result(
                    BigqueryQuery.snapshots_for_dataset_without_data_read.format(
                        project_id=project_id, dataset_name=dataset_name
                    ),
                )

            for table in cur:
                try:
                    with current_timer.pause():
                        yield BigQuerySchemaApi._make_bigquery_table_snapshot(table)
                except Exception as e:
                    snapshot_name = f"{project_id}.{dataset_name}.{table.table_name}"
                    report.report_warning(
                        title="Failed to process snapshot",
                        message="Error encountered while processing snapshot",
                        context=snapshot_name,
                        exc=e,
                    )
            self.report.num_get_snapshots_for_dataset_api_requests += 1
            self.report.get_snapshots_for_dataset_sec += current_timer.elapsed_seconds()

    @staticmethod
    def _make_bigquery_table_snapshot(snapshot: bigquery.Row) -> BigqueryTableSnapshot:
        return BigqueryTableSnapshot(
            name=snapshot.table_name,
            created=snapshot.created,
            last_altered=(
                datetime.fromtimestamp(
                    snapshot.get("last_altered") / 1000, tz=timezone.utc
                )
                if snapshot.get("last_altered") is not None
                else None
            ),
            comment=snapshot.comment,
            ddl=snapshot.ddl,
            snapshot_time=snapshot.snapshot_time,
            size_in_bytes=snapshot.get("size_bytes"),
            rows_count=snapshot.get("row_count"),
            base_table_identifier=BigqueryTableIdentifier(
                project_id=snapshot.base_table_catalog,
                dataset=snapshot.base_table_schema,
                table=snapshot.base_table_name,
            ),
        )


def query_project_list(
    schema_api: BigQuerySchemaApi,
    report: SourceReport,
    filters: BigQueryFilter,
) -> Iterable[BigqueryProject]:
    try:
        projects = schema_api.get_projects()

        if not projects:  # Report failure on exception and if empty list is returned
            report.failure(
                title="Get projects didn't return any project. ",
                message="Maybe resourcemanager.projects.get permission is missing for the service account. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
            )
    except Exception as e:
        report.failure(
            title="Failed to get BigQuery Projects",
            message="Maybe resourcemanager.projects.get permission is missing for the service account. "
            "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
            exc=e,
        )
        projects = []

    for project in projects:
        if filters.filter_config.project_id_pattern.allowed(project.id):
            yield project
        else:
            logger.debug(
                f"Ignoring project {project.id} as it's not allowed by project_id_pattern"
            )


def get_projects(
    schema_api: BigQuerySchemaApi,
    report: SourceReport,
    filters: BigQueryFilter,
) -> List[BigqueryProject]:
    logger.info("Getting projects")
    if filters.filter_config.project_ids:
        return [
            BigqueryProject(id=project_id, name=project_id)
            for project_id in filters.filter_config.project_ids
        ]
    elif filters.filter_config.project_labels:
        return list(query_project_list_from_labels(schema_api, report, filters))
    else:
        return list(query_project_list(schema_api, report, filters))


def query_project_list_from_labels(
    schema_api: BigQuerySchemaApi,
    report: SourceReport,
    filters: BigQueryFilter,
) -> Iterable[BigqueryProject]:
    projects = schema_api.get_projects_with_labels(
        frozenset(filters.filter_config.project_labels)
    )

    if not projects:  # Report failure on exception and if empty list is returned
        report.report_failure(
            "metadata-extraction",
            "Get projects didn't return any project with any of the specified label(s). "
            "Maybe resourcemanager.projects.list permission is missing for the service account. "
            "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
        )

    for project in projects:
        if filters.filter_config.project_id_pattern.allowed(project.id):
            yield project
        else:
            logger.debug(
                f"Ignoring project {project.id} as it's not allowed by project_id_pattern"
            )

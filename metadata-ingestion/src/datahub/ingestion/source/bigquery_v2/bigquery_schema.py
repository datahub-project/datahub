import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery.table import (
    RowIterator,
    TableListItem,
    TimePartitioning,
    TimePartitioningType,
)

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.queries import (
    BigqueryQuery,
    BigqueryTableType,
)
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable, BaseView

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigqueryColumn(BaseColumn):
    field_path: str
    is_partition_column: bool
    cluster_column_position: Optional[int]


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
            field=time_partitioning.field
            if time_partitioning.field
            else "_PARTITIONTIME",
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


@dataclass
class BigqueryView(BaseView):
    columns: List[BigqueryColumn] = field(default_factory=list)
    materialized: bool = False


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
    columns: List[BigqueryColumn] = field(default_factory=list)


@dataclass
class BigqueryProject:
    id: str
    name: str
    datasets: List[BigqueryDataset] = field(default_factory=list)


class BigQuerySchemaApi:
    def __init__(
        self, report: BigQuerySchemaApiPerfReport, client: bigquery.Client
    ) -> None:
        self.bq_client = client
        self.report = report

    def get_query_result(self, query: str) -> RowIterator:
        logger.debug(f"Query : {query}")
        resp = self.bq_client.query(query)
        return resp.result()

    def get_projects(self) -> List[BigqueryProject]:
        with self.report.list_projects:
            try:
                projects = self.bq_client.list_projects()

                return [
                    BigqueryProject(id=p.project_id, name=p.friendly_name)
                    for p in projects
                ]
            except Exception as e:
                logger.error(f"Error getting projects. {e}", exc_info=True)
                return []

    def get_datasets_for_project_id(
        self, project_id: str, maxResults: Optional[int] = None
    ) -> List[BigqueryDataset]:
        with self.report.list_datasets:
            datasets = self.bq_client.list_datasets(project_id, max_results=maxResults)
            return [
                BigqueryDataset(name=d.dataset_id, labels=d.labels) for d in datasets
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
        with self.report.list_tables as current_timer:
            for table in self.bq_client.list_tables(f"{project_id}.{dataset_name}"):
                with current_timer.pause():
                    yield table

    def get_tables_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        tables: Dict[str, TableListItem],
        with_data_read_permission: bool = False,
        report: Optional[BigQueryV2Report] = None,
    ) -> Iterator[BigqueryTable]:
        with self.report.get_tables_for_dataset as current_timer:
            filter_clause: str = ", ".join(f"'{table}'" for table in tables.keys())

            if with_data_read_permission:
                # Tables are ordered by name and table suffix to make sure we always process the latest sharded table
                # and skip the others. Sharded tables are tables with suffix _20220102
                cur = self.get_query_result(
                    BigqueryQuery.tables_for_dataset.format(
                        project_id=project_id,
                        dataset_name=dataset_name,
                        table_filter=f" and t.table_name in ({filter_clause})"
                        if filter_clause
                        else "",
                    ),
                )
            else:
                # Tables are ordered by name and table suffix to make sure we always process the latest sharded table
                # and skip the others. Sharded tables are tables with suffix _20220102
                cur = self.get_query_result(
                    BigqueryQuery.tables_for_dataset_without_partition_data.format(
                        project_id=project_id,
                        dataset_name=dataset_name,
                        table_filter=f" and t.table_name in ({filter_clause})"
                        if filter_clause
                        else "",
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
                    logger.warning(
                        f"Error while processing table {table_name}",
                        exc_info=True,
                    )
                    if report:
                        report.report_warning(
                            "metadata-extraction",
                            f"Failed to get table {table_name}: {e}",
                        )

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
            last_altered=datetime.fromtimestamp(
                table.get("last_altered") / 1000, tz=timezone.utc
            )
            if table.get("last_altered") is not None
            else table.created,
            size_in_bytes=table.get("bytes"),
            rows_count=table.get("row_count"),
            comment=table.comment,
            ddl=table.ddl,
            expires=expiration,
            labels=table_basic.labels if table_basic else None,
            partition_info=PartitionInfo.from_table_info(table_basic)
            if table_basic
            else None,
            clustering_fields=table_basic.clustering_fields if table_basic else None,
            max_partition_id=table.get("max_partition_id"),
            max_shard_id=shard,
            num_partitions=table.get("num_partitions"),
            active_billable_bytes=table.get("active_billable_bytes"),
            long_term_billable_bytes=table.get("long_term_billable_bytes"),
        )

    def get_views_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        has_data_read: bool,
        report: Optional[BigQueryV2Report] = None,
    ) -> Iterator[BigqueryView]:
        with self.report.get_views_for_dataset as current_timer:
            if has_data_read:
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
                    logger.warning(
                        f"Error while processing view {view_name}",
                        exc_info=True,
                    )
                    if report:
                        report.report_warning(
                            "metadata-extraction",
                            f"Failed to get view {view_name}: {e}",
                        )

    @staticmethod
    def _make_bigquery_view(view: bigquery.Row) -> BigqueryView:
        return BigqueryView(
            name=view.table_name,
            created=view.created,
            last_altered=datetime.fromtimestamp(
                view.get("last_altered") / 1000, tz=timezone.utc
            )
            if view.get("last_altered") is not None
            else view.created,
            comment=view.comment,
            view_definition=view.view_definition,
            materialized=view.table_type == BigqueryTableType.MATERIALIZED_VIEW,
        )

    def get_columns_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
        column_limit: int,
        run_optimized_column_query: bool = False,
    ) -> Optional[Dict[str, List[BigqueryColumn]]]:
        columns: Dict[str, List[BigqueryColumn]] = defaultdict(list)
        with self.report.get_columns_for_dataset:
            try:
                cur = self.get_query_result(
                    BigqueryQuery.columns_for_dataset.format(
                        project_id=project_id, dataset_name=dataset_name
                    )
                    if not run_optimized_column_query
                    else BigqueryQuery.optimized_columns_for_dataset.format(
                        project_id=project_id,
                        dataset_name=dataset_name,
                        column_limit=column_limit,
                    ),
                )
            except Exception as e:
                logger.warning(f"Columns for dataset query failed with exception: {e}")
                # Error - Information schema query returned too much data.
                # Please repeat query with more selective predicates.
                return None

            last_seen_table: str = ""
            for column in cur:
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
                            is_partition_column=column.is_partitioning_column == "YES",
                            cluster_column_position=column.clustering_ordinal_position,
                        )
                    )

        return columns

    # This is not used anywhere
    def get_columns_for_table(
        self,
        table_identifier: BigqueryTableIdentifier,
        column_limit: Optional[int],
    ) -> List[BigqueryColumn]:
        cur = self.get_query_result(
            BigqueryQuery.columns_for_table.format(table_identifier=table_identifier),
        )

        columns: List[BigqueryColumn] = []
        last_seen_table: str = ""
        for column in cur:
            if (
                column_limit
                and column.table_name in columns
                and len(columns[column.table_name]) >= column_limit
            ):
                if last_seen_table != column.table_name:
                    logger.warning(
                        f"{table_identifier.project_id}.{table_identifier.dataset}.{column.table_name} contains more than {column_limit} columns, only processing {column_limit} columns"
                    )
            else:
                columns.append(
                    BigqueryColumn(
                        name=column.column_name,
                        ordinal_position=column.ordinal_position,
                        is_nullable=column.is_nullable == "YES",
                        field_path=column.field_path,
                        data_type=column.data_type,
                        comment=column.comment,
                        is_partition_column=column.is_partitioning_column == "YES",
                        cluster_column_position=column.clustering_ordinal_position,
                    )
                )
            last_seen_table = column.table_name

        return columns

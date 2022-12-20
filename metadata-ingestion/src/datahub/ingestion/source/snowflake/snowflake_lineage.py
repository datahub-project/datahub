import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Iterable, List, Optional, Set

from pydantic import Field
from pydantic.error_wrappers import ValidationError
from snowflake.connector import SnowflakeConnection

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeColumnReference,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakePermissionError,
    SnowflakeQueryMixin,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeColumnWithLineage(SnowflakeColumnReference):
    class Config:
        # This is for backward compatibility and can be removed later
        allow_population_by_field_name = True

    directSourceColumns: Optional[List[SnowflakeColumnReference]] = Field(
        default=None, alias="directSources"
    )


@dataclass(frozen=True)
class SnowflakeColumnId:
    columnName: str
    objectName: str
    objectDomain: Optional[str] = None


@dataclass(frozen=True)
class SnowflakeColumnFineGrainedLineage:
    """
    Fie grained upstream of column,
    which represents a transformation applied on input columns"""

    inputColumns: FrozenSet[SnowflakeColumnId]
    # Transform function, query etc can be added here


@dataclass
class SnowflakeColumnUpstreams:
    """All upstreams of a column"""

    upstreams: Set[SnowflakeColumnFineGrainedLineage] = field(
        default_factory=set, init=False
    )

    def update_column_lineage(
        self, directSourceColumns: List[SnowflakeColumnReference]
    ) -> None:
        input_columns = frozenset(
            [
                SnowflakeColumnId(
                    upstream_col.columnName,
                    upstream_col.objectName,
                    upstream_col.objectDomain,
                )
                for upstream_col in directSourceColumns
                if upstream_col.objectName
            ]
        )
        if not input_columns:
            return
        upstream = SnowflakeColumnFineGrainedLineage(inputColumns=input_columns)
        if upstream not in self.upstreams:
            self.upstreams.add(upstream)


@dataclass
class SnowflakeUpstreamTable:
    upstreamDataset: str
    upstreamColumns: List[SnowflakeColumnReference]
    downstreamColumns: List[SnowflakeColumnWithLineage]

    @classmethod
    def from_dict(
        cls,
        dataset: str,
        upstreams_columns_json: Optional[str],
        downstream_columns_json: Optional[str],
    ) -> "SnowflakeUpstreamTable":
        try:
            upstreams_columns_list = []
            downstream_columns_list = []
            if upstreams_columns_json is not None:
                upstreams_columns_list = json.loads(upstreams_columns_json)
            if downstream_columns_json is not None:
                downstream_columns_list = json.loads(downstream_columns_json)

            table_with_upstreams = cls(
                dataset,
                [
                    SnowflakeColumnReference.parse_obj(col)
                    for col in upstreams_columns_list
                ],
                [
                    SnowflakeColumnWithLineage.parse_obj(col)
                    for col in downstream_columns_list
                ],
            )
        except ValidationError:
            # Earlier versions of column lineage did not include columnName, only columnId
            table_with_upstreams = cls(dataset, [], [])
        return table_with_upstreams


@dataclass
class SnowflakeTableLineage:
    # key: upstream table name
    upstreamTables: Dict[str, SnowflakeUpstreamTable] = field(
        default_factory=dict, init=False
    )

    # key: downstream column name
    columnLineages: Dict[str, SnowflakeColumnUpstreams] = field(
        default_factory=lambda: defaultdict(SnowflakeColumnUpstreams), init=False
    )

    def update_lineage(
        self, table: SnowflakeUpstreamTable, include_column_lineage: bool = True
    ) -> None:
        if table.upstreamDataset not in self.upstreamTables.keys():
            self.upstreamTables[table.upstreamDataset] = table

        if include_column_lineage and table.downstreamColumns:
            for col in table.downstreamColumns:
                if col.directSourceColumns:
                    self.columnLineages[col.columnName].update_column_lineage(
                        col.directSourceColumns
                    )


class SnowflakeLineageExtractor(SnowflakeQueryMixin, SnowflakeCommonMixin):
    def __init__(self, config: SnowflakeV2Config, report: SnowflakeV2Report) -> None:
        self._lineage_map: Dict[str, SnowflakeTableLineage] = defaultdict(
            SnowflakeTableLineage
        )
        self._external_lineage_map: Dict[str, Set[str]] = defaultdict(set)
        self.config = config
        self.platform = "snowflake"
        self.report = report
        self.logger = logger

    def get_workunits(
        self, discovered_tables: List[str], discovered_views: List[str]
    ) -> Iterable[MetadataWorkUnit]:

        try:
            conn = self.get_connection()
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.report_error("permission-error", str(e))
            else:
                logger.debug(e, exc_info=e)
                self.report_error(
                    "snowflake-connection",
                    f"Failed to connect to snowflake instance due to error {e}.",
                )
            return

        if self.report.edition == SnowflakeEdition.STANDARD:
            logger.info(
                "Snowflake Account is Standard Edition. Table to Table Lineage Feature is not supported."
            )
        else:
            with PerfTimer() as timer:
                self._populate_lineage(conn)
                self.report.table_lineage_query_secs = timer.elapsed_seconds()

        if self.config.include_view_lineage:
            if len(discovered_views) > 0:
                self._populate_view_lineage(conn)
            else:
                logger.info("No views found. Skipping View Lineage Extraction.")

        with PerfTimer() as timer:
            self._populate_external_lineage(conn)
            self.report.external_lineage_queries_secs = timer.elapsed_seconds()

        if (
            len(self._lineage_map.keys()) == 0
            and len(self._external_lineage_map.keys()) == 0
        ):
            logger.debug("No lineage found.")
            return

        yield from self.get_table_upstream_workunits(discovered_tables)
        yield from self.get_view_upstream_workunits(discovered_views)

    def get_table_upstream_workunits(self, discovered_tables):
        if self.config.include_table_lineage:
            for dataset_name in discovered_tables:
                if self._is_dataset_pattern_allowed(dataset_name, "table"):
                    dataset_urn = builder.make_dataset_urn_with_platform_instance(
                        self.platform,
                        dataset_name,
                        self.config.platform_instance,
                        self.config.env,
                    )
                    upstream_lineage = self._get_upstream_lineage_info(dataset_name)
                    if upstream_lineage is not None:
                        yield self.wrap_aspect_as_workunit(
                            "dataset", dataset_urn, "upstreamLineage", upstream_lineage
                        )

    def get_view_upstream_workunits(self, discovered_views):
        if self.config.include_view_lineage:
            for view_name in discovered_views:
                if self._is_dataset_pattern_allowed(view_name, "view"):
                    dataset_urn = builder.make_dataset_urn_with_platform_instance(
                        self.platform,
                        view_name,
                        self.config.platform_instance,
                        self.config.env,
                    )
                    upstream_lineage = self._get_upstream_lineage_info(view_name)
                    if upstream_lineage is not None:
                        yield self.wrap_aspect_as_workunit(
                            "dataset", dataset_urn, "upstreamLineage", upstream_lineage
                        )

    def _get_upstream_lineage_info(
        self, dataset_name: str
    ) -> Optional[UpstreamLineage]:
        lineage = self._lineage_map[dataset_name]
        external_lineage = self._external_lineage_map[dataset_name]
        if not (lineage.upstreamTables or lineage.columnLineages or external_lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None

        upstream_tables: List[UpstreamClass] = []
        finegrained_lineages: List[FineGrainedLineage] = []

        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        # Populate the table-lineage in aspect
        self.update_upstream_tables_lineage(upstream_tables, lineage)

        # Populate the column-lineage in aspect
        self.update_upstream_columns_lineage(dataset_urn, finegrained_lineages, lineage)

        # Populate the external-table-lineage(s3->snowflake) in aspect
        self.update_external_tables_lineage(upstream_tables, external_lineage)

        if len(upstream_tables) > 0:
            logger.debug(
                f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
            if self.config.upstream_lineage_in_report:
                self.report.upstream_lineage[dataset_name] = [
                    u.dataset for u in upstream_tables
                ]
            return UpstreamLineage(
                upstreams=upstream_tables,
                fineGrainedLineages=sorted(
                    finegrained_lineages, key=lambda x: (x.downstreams, x.upstreams)
                )
                or None,
            )
        else:
            return None

    def _populate_view_lineage(self, conn: SnowflakeConnection) -> None:
        with PerfTimer() as timer:
            self._populate_view_upstream_lineage(conn)
            self.report.view_upstream_lineage_query_secs = timer.elapsed_seconds()
        if self.report.edition == SnowflakeEdition.STANDARD:
            logger.info(
                "Snowflake Account is Standard Edition. View to Table Lineage Feature is not supported."
            )
        else:
            with PerfTimer() as timer:
                self._populate_view_downstream_lineage(conn)
                self.report.view_downstream_lineage_query_secs = timer.elapsed_seconds()

    def _populate_external_lineage(self, conn: SnowflakeConnection) -> None:

        num_edges: int = 0
        if self.report.edition == SnowflakeEdition.STANDARD:
            logger.info(
                "Snowflake Account is Standard Edition. External Lineage Feature via Access History is not supported."
            )
        else:
            self._populate_external_lineage_from_access_history(conn)

        # Handles the case for explicitly created external tables.
        # NOTE: Snowflake does not log this information to the access_history table.
        external_tables_query: str = SnowflakeQuery.show_external_tables()
        try:
            for db_row in self.query(conn, external_tables_query):
                key = self.get_dataset_identifier(
                    db_row["name"], db_row["schema_name"], db_row["database_name"]
                )

                if not self._is_dataset_pattern_allowed(key, "table"):
                    continue
                self._external_lineage_map[key].add(db_row["location"])
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via show external tables"
                )
                num_edges += 1
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "external_lineage",
                f"Populating external table lineage from Snowflake failed due to error {e}.",
            )
        logger.info(f"Found {num_edges} external lineage edges.")
        self.report.num_external_table_edges_scanned = num_edges

    def _populate_external_lineage_from_access_history(self, conn):
        # Handles the case where a table is populated from an external location via copy.
        # Eg: copy into category_english from 's3://acryl-snow-demo-olist/olist_raw_data/category_english'credentials=(aws_key_id='...' aws_secret_key='...')  pattern='.*.csv';
        query: str = SnowflakeQuery.external_table_lineage_history(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        try:
            for db_row in self.query(conn, query):
                # key is the down-stream table name
                key: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["DOWNSTREAM_TABLE_NAME"]
                )
                if not self._is_dataset_pattern_allowed(key, "table"):
                    continue
                self._external_lineage_map[key] |= {
                    *json.loads(db_row["UPSTREAM_LOCATIONS"])
                }
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via access_history"
                )
        except SnowflakePermissionError:
            error_msg = "Failed to get external lineage. Please grant imported privileges on SNOWFLAKE database. "
            self.warn_if_stateful_else_error("lineage-permission-error", error_msg)
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "external_lineage",
                f"Populating table external lineage from Snowflake failed due to error {e}.",
            )

    def _populate_lineage(self, conn: SnowflakeConnection) -> None:
        query: str = SnowflakeQuery.table_to_table_lineage_history(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
            include_column_lineage=self.config.include_column_lineage,
        )
        num_edges: int = 0
        try:
            for db_row in self.query(conn, query):
                # key is the down-stream table name
                key: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["DOWNSTREAM_TABLE_NAME"]
                )
                upstream_table_name = self.get_dataset_identifier_from_qualified_name(
                    db_row["UPSTREAM_TABLE_NAME"]
                )
                if not self._is_dataset_pattern_allowed(key, "table") or not (
                    self._is_dataset_pattern_allowed(upstream_table_name, "table")
                ):
                    continue

                self._lineage_map[key].update_lineage(
                    # (<upstream_table_name>, <json_list_of_upstream_columns>, <json_list_of_downstream_columns>)
                    SnowflakeUpstreamTable.from_dict(
                        upstream_table_name,
                        db_row["UPSTREAM_TABLE_COLUMNS"],
                        db_row["DOWNSTREAM_TABLE_COLUMNS"],
                    ),
                    self.config.include_column_lineage,
                )
                num_edges += 1
                logger.debug(
                    f"Lineage[Table(Down)={key}]:Table(Up)={self._lineage_map[key]}"
                )
        except SnowflakePermissionError:
            error_msg = "Failed to get table to table lineage. Please grant imported privileges on SNOWFLAKE database. "
            self.warn_if_stateful_else_error("lineage-permission-error", error_msg)
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "lineage",
                f"Extracting lineage from Snowflake failed due to error {e}.",
            )
        logger.info(
            f"A total of {num_edges} Table->Table edges found"
            f" for {len(self._lineage_map)} downstream tables.",
        )
        self.report.num_table_to_table_edges_scanned = num_edges

    def _populate_view_upstream_lineage(self, conn: SnowflakeConnection) -> None:
        # NOTE: This query captures only the upstream lineage of a view (with no column lineage).
        # For more details see: https://docs.snowflake.com/en/user-guide/object-dependencies.html#object-dependencies
        # and also https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        view_upstream_lineage_query: str = SnowflakeQuery.view_dependencies()

        num_edges: int = 0

        try:
            for db_row in self.query(conn, view_upstream_lineage_query):
                # Process UpstreamTable/View/ExternalTable/Materialized View->View edge.
                view_upstream: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["VIEW_UPSTREAM"]
                )
                view_name: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["DOWNSTREAM_VIEW"]
                )

                if not self._is_dataset_pattern_allowed(
                    dataset_name=view_name,
                    dataset_type=db_row["REFERENCING_OBJECT_DOMAIN"],
                ) or not self._is_dataset_pattern_allowed(
                    view_upstream, db_row["REFERENCED_OBJECT_DOMAIN"]
                ):
                    continue

                # key is the downstream view name
                self._lineage_map[view_name].update_lineage(
                    # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                    SnowflakeUpstreamTable.from_dict(view_upstream, None, None),
                    self.config.include_column_lineage,
                )
                num_edges += 1
                logger.debug(
                    f"Upstream->View: Lineage[View(Down)={view_name}]:Upstream={view_upstream}"
                )
        except SnowflakePermissionError:
            error_msg = "Failed to get table to view lineage. Please grant imported privileges on SNOWFLAKE database."
            self.warn_if_stateful_else_error("lineage-permission-error", error_msg)
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "view-upstream-lineage",
                f"Extracting the upstream view lineage from Snowflake failed due to error {e}.",
            )
        else:
            logger.info(f"A total of {num_edges} View upstream edges found.")
        self.report.num_table_to_view_edges_scanned = num_edges

    def _populate_view_downstream_lineage(self, conn: SnowflakeConnection) -> None:
        # This query captures the downstream table lineage for views.
        # See https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        # Eg: For viewA->viewB->ViewC->TableD, snowflake does not yet log intermediate view logs, resulting in only the viewA->TableD edge.
        view_lineage_query: str = SnowflakeQuery.view_lineage_history(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
            include_column_lineage=self.config.include_column_lineage,
        )

        self.report.num_view_to_table_edges_scanned = 0

        try:
            db_rows = self.query(conn, view_lineage_query)
        except SnowflakePermissionError:
            error_msg = "Failed to get view to table lineage. Please grant imported privileges on SNOWFLAKE database. "
            self.warn_if_stateful_else_error("lineage-permission-error", error_msg)
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "view-downstream-lineage",
                f"Extracting the view lineage from Snowflake failed due to error {e}.",
            )
        else:
            for db_row in db_rows:
                view_name: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["VIEW_NAME"]
                )
                downstream_table: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["DOWNSTREAM_TABLE_NAME"]
                )
                if not self._is_dataset_pattern_allowed(
                    view_name, db_row["VIEW_DOMAIN"]
                ) or not self._is_dataset_pattern_allowed(
                    downstream_table, db_row["DOWNSTREAM_TABLE_DOMAIN"]
                ):
                    continue

                # Capture view->downstream table lineage.
                self._lineage_map[downstream_table].update_lineage(
                    # (<upstream_view_name>, <json_list_of_upstream_view_columns>, <json_list_of_downstream_columns>)
                    SnowflakeUpstreamTable.from_dict(
                        view_name,
                        db_row["VIEW_COLUMNS"],
                        db_row["DOWNSTREAM_TABLE_COLUMNS"],
                    ),
                    self.config.include_column_lineage,
                )
                self.report.num_view_to_table_edges_scanned += 1

                logger.debug(
                    f"View->Table: Lineage[Table(Down)={downstream_table}]:View(Up)={self._lineage_map[downstream_table]}"
                )

        logger.info(
            f"Found {self.report.num_view_to_table_edges_scanned} View->Table edges."
        )

    def update_upstream_tables_lineage(self, upstream_tables, lineage):
        for lineage_entry in sorted(
            lineage.upstreamTables.values(), key=lambda x: x.upstreamDataset
        ):
            upstream_table_name = lineage_entry.upstreamDataset
            upstream_table_urn = builder.make_dataset_urn_with_platform_instance(
                self.platform,
                upstream_table_name,
                self.config.platform_instance,
                self.config.env,
            )
            upstream_table = UpstreamClass(
                dataset=upstream_table_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

    def update_upstream_columns_lineage(
        self, dataset_urn, finegrained_lineages, lineage
    ):
        for col, col_upstreams in lineage.columnLineages.items():
            for fine_upstream in col_upstreams.upstreams:
                fieldPath = col
                finegrained_lineage_entry = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=sorted(
                        [
                            builder.make_schema_field_urn(
                                builder.make_dataset_urn_with_platform_instance(
                                    self.platform,
                                    self.get_dataset_identifier_from_qualified_name(
                                        upstream_col.objectName
                                    ),
                                    self.config.platform_instance,
                                    self.config.env,
                                ),
                                self.snowflake_identifier(upstream_col.columnName),
                            )
                            for upstream_col in fine_upstream.inputColumns
                            if upstream_col.objectName
                            and upstream_col.columnName
                            and self._is_dataset_pattern_allowed(
                                upstream_col.objectName, upstream_col.objectDomain
                            )
                        ]
                    ),
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=sorted(
                        [
                            builder.make_schema_field_urn(
                                dataset_urn, self.snowflake_identifier(fieldPath)
                            )
                        ]
                    ),
                )
                if finegrained_lineage_entry.upstreams:
                    finegrained_lineages.append(finegrained_lineage_entry)

    def update_external_tables_lineage(self, upstream_tables, external_lineage):
        for external_lineage_entry in sorted(external_lineage):
            # For now, populate only for S3
            if external_lineage_entry.startswith("s3://"):
                external_upstream_table = UpstreamClass(
                    dataset=make_s3_urn(external_lineage_entry, self.config.env),
                    type=DatasetLineageTypeClass.COPY,
                )
                upstream_tables.append(external_upstream_table)

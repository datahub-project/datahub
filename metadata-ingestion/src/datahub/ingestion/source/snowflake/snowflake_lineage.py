import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from pydantic import Field
from pydantic.error_wrappers import ValidationError
from snowflake.connector import SnowflakeConnection

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeColumnReference,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
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
        self._lineage_map: Optional[Dict[str, SnowflakeTableLineage]] = None
        self._external_lineage_map: Optional[Dict[str, Set[str]]] = None
        self.config = config
        self.platform = "snowflake"
        self.report = report
        self.logger = logger

    # Rewrite implementation for readability, efficiency and extensibility
    def _get_upstream_lineage_info(
        self, dataset_name: str
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:
        if self._lineage_map is None or self._external_lineage_map is None:
            conn = self.config.get_connection()
        if self._lineage_map is None:
            with PerfTimer() as timer:
                self._populate_lineage(conn)
                self.report.table_lineage_query_secs = timer.elapsed_seconds()
            if self.config.include_view_lineage:
                self._populate_view_lineage(conn)

        if self._external_lineage_map is None:
            with PerfTimer() as timer:
                self._populate_external_lineage(conn)
                self.report.external_lineage_queries_secs = timer.elapsed_seconds()

        assert self._lineage_map is not None
        assert self._external_lineage_map is not None

        lineage = self._lineage_map[dataset_name]
        external_lineage = self._external_lineage_map[dataset_name]
        if not (lineage.upstreamTables or lineage.columnLineages or external_lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []
        finegrained_lineages: List[FineGrainedLineage] = []
        fieldset_finegrained_lineages: List[FineGrainedLineage] = []
        column_lineage: Dict[str, str] = {}
        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        for lineage_entry in sorted(
            lineage.upstreamTables.values(), key=lambda x: x.upstreamDataset
        ):
            # Update the table-lineage
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

            if lineage_entry.upstreamColumns and lineage_entry.downstreamColumns:
                # This is not used currently. This indicates same column lineage as was set
                # in customProperties earlier - not accurate.
                fieldset_finegrained_lineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD_SET
                    if len(lineage_entry.downstreamColumns) > 1
                    else FineGrainedLineageDownstreamType.FIELD,
                    upstreams=sorted(
                        [
                            builder.make_schema_field_urn(
                                upstream_table_urn,
                                self.snowflake_identifier(d.columnName),
                            )
                            for d in lineage_entry.upstreamColumns
                        ]
                    ),
                    downstreams=sorted(
                        [
                            builder.make_schema_field_urn(
                                dataset_urn, self.snowflake_identifier(d.columnName)
                            )
                            for d in lineage_entry.downstreamColumns
                        ]
                    ),
                )
                fieldset_finegrained_lineages.append(fieldset_finegrained_lineage)

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
                            for upstream_col in fine_upstream.inputColumns  # type:ignore
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

        for external_lineage_entry in sorted(external_lineage):
            # For now, populate only for S3
            if external_lineage_entry.startswith("s3://"):
                external_upstream_table = UpstreamClass(
                    dataset=make_s3_urn(external_lineage_entry, self.config.env),
                    type=DatasetLineageTypeClass.COPY,
                )
                upstream_tables.append(external_upstream_table)

        if upstream_tables:
            logger.debug(
                f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
            if self.config.upstream_lineage_in_report:
                self.report.upstream_lineage[dataset_name] = [
                    u.dataset for u in upstream_tables
                ]
            return (
                UpstreamLineage(
                    upstreams=upstream_tables,
                    fineGrainedLineages=sorted(
                        finegrained_lineages, key=lambda x: (x.downstreams, x.upstreams)
                    )
                    or None,
                ),
                column_lineage,
            )
        return None

    def _populate_view_lineage(self, conn: SnowflakeConnection) -> None:
        with PerfTimer() as timer:
            self._populate_view_upstream_lineage(conn)
            self.report.view_upstream_lineage_query_secs = timer.elapsed_seconds()
        with PerfTimer() as timer:
            self._populate_view_downstream_lineage(conn)
            self.report.view_downstream_lineage_query_secs = timer.elapsed_seconds()

    def _populate_external_lineage(self, conn: SnowflakeConnection) -> None:
        # Handles the case where a table is populated from an external location via copy.
        # Eg: copy into category_english from 's3://acryl-snow-demo-olist/olist_raw_data/category_english'credentials=(aws_key_id='...' aws_secret_key='...')  pattern='.*.csv';
        query: str = SnowflakeQuery.external_table_lineage_history(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        num_edges: int = 0
        self._external_lineage_map = defaultdict(set)
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
        except Exception as e:
            self.warn(
                "external_lineage",
                f"Populating table external lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}.",
            )
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
            self.warn(
                "external_lineage",
                f"Populating external table lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}.",
            )
        logger.info(f"Found {num_edges} external lineage edges.")
        self.report.num_external_table_edges_scanned = num_edges

    def _populate_lineage(self, conn: SnowflakeConnection) -> None:
        query: str = SnowflakeQuery.table_to_table_lineage_history(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
            include_column_lineage=self.config.include_column_lineage,
        )
        num_edges: int = 0
        self._lineage_map = defaultdict(SnowflakeTableLineage)
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
        except Exception as e:
            logger.error(e, exc_info=e)
            self.warn(
                "lineage",
                f"Extracting lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}.",
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

        assert self._lineage_map is not None
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
        except Exception as e:
            self.warn(
                "view_upstream_lineage",
                "Extracting the upstream view lineage from Snowflake failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )
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

        assert self._lineage_map is not None
        self.report.num_view_to_table_edges_scanned = 0

        try:
            db_rows = self.query(conn, view_lineage_query)
        except Exception as e:
            self.warn(
                "view_downstream_lineage",
                f"Extracting the view lineage from Snowflake failed."
                f"Please check your permissions. Continuing...\nError was {e}.",
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

    def warn(self, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        self.logger.warning(f"{key} => {reason}")

    def error(self, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        self.logger.error(f"{key} => {reason}")

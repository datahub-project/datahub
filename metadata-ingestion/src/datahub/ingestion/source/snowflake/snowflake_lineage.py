import json
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from snowflake.connector import SnowflakeConnection

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeQueryMixin,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeLineageExtractor(SnowflakeQueryMixin, SnowflakeCommonMixin):
    def __init__(self, config: SnowflakeV2Config, report: SnowflakeV2Report) -> None:
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self._external_lineage_map: Optional[Dict[str, Set[str]]] = None
        self.config = config
        self.platform = "snowflake"
        self.report = report
        self.logger = logger

    # Rewrite implementation for readability, efficiency and extensibility
    def _get_upstream_lineage_info(
        self, dataset_name: str
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:

        if not self.config.include_table_lineage:
            return None

        if self._lineage_map is None or self._external_lineage_map is None:
            conn = self.config.get_connection()
        if self._lineage_map is None:
            self._populate_lineage(conn)
            self._populate_view_lineage(conn)
        if self._external_lineage_map is None:
            self._populate_external_lineage(conn)

        assert self._lineage_map is not None
        assert self._external_lineage_map is not None

        lineage = self._lineage_map[dataset_name]
        external_lineage = self._external_lineage_map[dataset_name]
        if not (lineage or external_lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []
        column_lineage: Dict[str, str] = {}
        for lineage_entry in lineage:
            # Update the table-lineage
            upstream_table_name = lineage_entry[0]
            if not self._is_dataset_pattern_allowed(upstream_table_name, "table"):
                continue
            upstream_table = UpstreamClass(
                dataset=builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
            # Update column-lineage for each down-stream column.
            upstream_columns = [
                self.snowflake_identifier(d["columnName"])
                for d in json.loads(lineage_entry[1])
            ]
            downstream_columns = [
                self.snowflake_identifier(d["columnName"])
                for d in json.loads(lineage_entry[2])
            ]
            upstream_column_str = (
                f"{upstream_table_name}({', '.join(sorted(upstream_columns))})"
            )
            downstream_column_str = (
                f"{dataset_name}({', '.join(sorted(downstream_columns))})"
            )
            column_lineage_key = f"column_lineage[{upstream_table_name}]"
            column_lineage_value = (
                f"{{{upstream_column_str} -> {downstream_column_str}}}"
            )
            column_lineage[column_lineage_key] = column_lineage_value
            logger.debug(f"{column_lineage_key}:{column_lineage_value}")

        for external_lineage_entry in external_lineage:
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
            return UpstreamLineage(upstreams=upstream_tables), column_lineage
        return None

    def _populate_view_lineage(self, conn: SnowflakeConnection) -> None:
        if not self.config.include_view_lineage:
            return
        self._populate_view_upstream_lineage(conn)
        self._populate_view_downstream_lineage(conn)

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
                    db_row["downstream_table_name"]
                )
                if not self._is_dataset_pattern_allowed(key, "table"):
                    continue
                self._external_lineage_map[key] |= {
                    *json.loads(db_row["upstream_locations"])
                }
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via access_history"
                )
        except Exception as e:
            logger.warning(
                f"Populating table external lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
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
        )
        num_edges: int = 0
        self._lineage_map = defaultdict(list)
        try:
            for db_row in self.query(conn, query):
                # key is the down-stream table name
                key: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["downstream_table_name"]
                )
                upstream_table_name = self.get_dataset_identifier_from_qualified_name(
                    db_row["upstream_table_name"]
                )
                if not (
                    self._is_dataset_pattern_allowed(key, "table")
                    or self._is_dataset_pattern_allowed(upstream_table_name, "table")
                ):
                    continue
                self._lineage_map[key].append(
                    # (<upstream_table_name>, <json_list_of_upstream_columns>, <json_list_of_downstream_columns>)
                    (
                        upstream_table_name,
                        db_row["upstream_table_columns"],
                        db_row["downstream_table_columns"],
                    )
                )
                num_edges += 1
                logger.debug(
                    f"Lineage[Table(Down)={key}]:Table(Up)={self._lineage_map[key]}"
                )
        except Exception as e:
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
                    db_row["view_upstream"]
                )
                view_name: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["downstream_view"]
                )
                if not self._is_dataset_pattern_allowed(
                    dataset_name=view_name,
                    dataset_type=db_row["referencing_object_domain"],
                ):
                    continue
                # key is the downstream view name
                self._lineage_map[view_name].append(
                    # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                    (view_upstream, "[]", "[]")
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
                    db_row["view_name"]
                )
                if not self._is_dataset_pattern_allowed(
                    view_name, db_row["view_domain"]
                ):
                    continue
                downstream_table: str = self.get_dataset_identifier_from_qualified_name(
                    db_row["downstream_table_name"]
                )
                # Capture view->downstream table lineage.
                self._lineage_map[downstream_table].append(
                    # (<upstream_view_name>, <json_list_of_upstream_view_columns>, <json_list_of_downstream_columns>)
                    (
                        view_name,
                        db_row["view_columns"],
                        db_row["downstream_table_columns"],
                    )
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

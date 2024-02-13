import logging
import traceback
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import redshift_connector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
from datahub.ingestion.source.redshift.lineage import LineageCollectorType
from datahub.ingestion.source.redshift.query import RedshiftQuery
from datahub.ingestion.source.redshift.redshift_schema import RedshiftDataDictionary
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


class RedshiftSqlLineageV2:
    # does lineage and usage based on SQL parsing.

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        context: PipelineContext,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.config = config
        self.report = report
        self.context = context

        self.aggregator = SqlParsingAggregator(
            platform="redshift",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=True,
            generate_operations=True,
            graph=self.context.graph,
        )
        self.report.sql_aggregator = self.aggregator.report

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            self.report.stateful_lineage_ingestion_enabled = True
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def build(self, database: str, connection: redshift_connector.Connection) -> None:
        if self.config.resolve_temp_table_in_lineage:
            self._init_temp_table_schema(
                database=database,
                temp_tables=self.get_temp_tables(connection=connection),
            )

        populate_calls: List[Tuple[str, LineageCollectorType]] = []

        table_renames: Dict[str, str] = {}
        if self.config.include_table_rename_lineage:
            table_renames, all_tables_set = self._process_table_renames(
                database=database,
                connection=connection,
            )

        if self.config.table_lineage_mode in {
            LineageMode.STL_SCAN_BASED,
            LineageMode.MIXED,
        }:
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            query = RedshiftQuery.stl_scan_based_lineage_query(
                self.config.database,
                self.start_time,
                self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SCAN))
        if self.config.table_lineage_mode in {
            LineageMode.SQL_BASED,
            LineageMode.MIXED,
        }:
            # Populate table level lineage by parsing table creating sqls
            query = RedshiftQuery.list_insert_create_queries_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SQL_PARSER))

        if self.config.include_views and self.config.include_view_lineage:
            # Populate table level lineage for views
            query = RedshiftQuery.view_lineage_query()
            populate_calls.append((query, LineageCollectorType.VIEW))

            # Populate table level lineage for late binding views
            query = RedshiftQuery.list_late_view_ddls_query()
            populate_calls.append((query, LineageCollectorType.VIEW_DDL_SQL_PARSING))

        if self.config.include_copy_lineage:
            query = RedshiftQuery.list_copy_commands_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.COPY))

        if self.config.include_unload_lineage:
            query = RedshiftQuery.list_unload_commands_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )

            populate_calls.append((query, LineageCollectorType.UNLOAD))

        for query, lineage_type in populate_calls:
            self._populate_lineage_map(
                query=query,
                database=database,
                lineage_type=lineage_type,
                connection=connection,
            )

        # Handling for alter table statements.
        self._update_lineage_map_for_table_renames(table_renames=table_renames)

    def _populate_lineage_map(
        self,
        query: str,
        database: str,
        lineage_type: LineageCollectorType,
        connection: redshift_connector.Connection,
    ) -> None:
        """
        This method generate table level lineage based with the given query.
        The query should return the following columns: target_schema, target_table, source_table, source_schema
        source_table and source_schema can be omitted if the sql_field is set because then it assumes the source_table
        and source_schema will be extracted from the sql_field by sql parsing.

        :param query: The query to run to extract lineage.
        :type query: str
        :param lineage_type: The way the lineage should be processed
        :type lineage_type: LineageType
        return: The method does not return with anything as it directly modify the self._lineage_map property.
        :rtype: None
        """

        logger.info(f"Extracting {lineage_type.name} lineage for db {database}")
        try:
            logger.debug(f"Processing lineage query: {query}")
            raw_db_name = database

            for lineage_row in RedshiftDataDictionary.get_lineage_rows(
                conn=connection, query=query
            ):

                if lineage_type == LineageCollectorType.QUERY_SQL_PARSER:
                    self._process_sql_parser_lineage(
                        lineage_row=lineage_row,
                        database=raw_db_name,
                    )
                elif lineage_type in {
                    LineageCollectorType.VIEW,
                    LineageCollectorType.VIEW_DDL_SQL_PARSING,
                }:
                    self._process_view_lineage(
                        lineage_row=lineage_row,
                        database=raw_db_name,
                    )
                elif lineage_type == LineageCollectorType.QUERY_SCAN:
                    self._process_scan_based_lineage(
                        lineage_row=lineage_row,
                        database=raw_db_name,
                    )
                elif lineage_type == LineageCollectorType.COPY:
                    self._process_copy_lineage(
                        lineage_row=lineage_row,
                        database=raw_db_name,
                    )
                elif lineage_type == LineageCollectorType.UNLOAD:
                    self._process_unload_lineage(
                        lineage_row=lineage_row,
                        database=raw_db_name,
                    )
                else:
                    raise ValueError(f"Unknown lineage type {lineage_type}")
        except Exception as e:
            self.report.warning(
                logger,
                f"extract-{lineage_type.name}",
                f"Error was {e}, {traceback.format_exc()}",
            )
            self._report_status(f"extract-{lineage_type.name}", False)

    def _report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

    def generate(self) -> Iterable[MetadataWorkUnit]:
        # TODO
        pass

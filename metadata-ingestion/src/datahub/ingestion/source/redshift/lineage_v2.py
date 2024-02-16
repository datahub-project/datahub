import logging
import traceback
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import redshift_connector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
from datahub.ingestion.source.redshift.lineage import (
    LineageCollectorType,
    RedshiftLineageExtractor,
)
from datahub.ingestion.source.redshift.query import RedshiftQuery
from datahub.ingestion.source.redshift.redshift_schema import (
    LineageRow,
    RedshiftDataDictionary,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


class RedshiftSqlLineageV2:
    # does lineage and usage based on SQL parsing.

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        context: PipelineContext,
        database: str,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.config = config
        self.report = report
        self.context = context

        self.database = database
        self.aggregator = SqlParsingAggregator(
            platform="redshift",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config,
            graph=self.context.graph,
        )
        self.report.sql_aggregator = self.aggregator.report

        self._lineage_v1 = RedshiftLineageExtractor(
            config=config,
            report=report,
            context=context,
            redundant_run_skip_handler=redundant_run_skip_handler,
        )

        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self._lineage_v1.get_time_window()

    def build(self, connection: redshift_connector.Connection) -> None:
        if self.config.resolve_temp_table_in_lineage:
            for temp_row in self._lineage_v1.get_temp_tables(connection=connection):
                self.aggregator.add_observed_query(
                    query=temp_row.query_text,
                    default_db=self.database,
                    default_schema=self.config.default_schema,
                    session_id=temp_row.session_id,
                    is_known_temp_table=True,
                )

        populate_calls: List[Tuple[LineageCollectorType, str, Callable]] = []

        table_renames: Dict[str, str] = {}
        if self.config.include_table_rename_lineage:
            pass
            # table_renames, all_tables_set = self._process_table_renames(
            #     connection=connection,
            # )  # TODO table_renames

        if self.config.table_lineage_mode in {
            LineageMode.SQL_BASED,
            LineageMode.MIXED,
        }:
            # Populate lineage by parsing table creating sqls
            query = RedshiftQuery.list_insert_create_queries_sql(
                db_name=self.database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append(
                (
                    LineageCollectorType.QUERY_SQL_PARSER,
                    query,
                    self._process_sql_parser_lineage,
                )
            )
        if self.config.table_lineage_mode in {
            LineageMode.STL_SCAN_BASED,
            LineageMode.MIXED,
        }:
            # Populate lineage by getting upstream tables from stl_scan redshift table
            query = RedshiftQuery.stl_scan_based_lineage_query(
                self.database,
                self.start_time,
                self.end_time,
            )
            # populate_calls.append((query, LineageCollectorType.QUERY_SCAN))  # TODO

        if self.config.include_views and self.config.include_view_lineage:
            # Populate lineage for views
            query = RedshiftQuery.view_lineage_query()
            populate_calls.append(
                (LineageCollectorType.VIEW, query, self._process_view_lineage)
            )

            # Populate lineage for late binding views
            query = RedshiftQuery.list_late_view_ddls_query()
            populate_calls.append(
                (
                    LineageCollectorType.VIEW_DDL_SQL_PARSING,
                    query,
                    self._process_view_lineage,
                )
            )

        if self.config.include_copy_lineage:
            # Populate lineage for copy commands.
            query = RedshiftQuery.list_copy_commands_sql(
                db_name=self.database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append(
                (LineageCollectorType.COPY, query, self._process_copy_command)
            )

        if self.config.include_unload_lineage:
            # Populate lineage for unload commands.
            query = RedshiftQuery.list_unload_commands_sql(
                db_name=self.database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append(
                (LineageCollectorType.UNLOAD, query, self._process_unload_command)
            )

        for lineage_type, query, processor in populate_calls:
            self._populate_lineage_agg(
                query=query,
                lineage_type=lineage_type,
                processor=processor,
                connection=connection,
            )

        # TODO add lineage for external tables

        # Handling for alter table statements.
        if self.config.include_table_rename_lineage:
            # self._update_lineage_map_for_table_renames(
            #     table_renames=table_renames
            # )  # TODO
            pass

    def _populate_lineage_agg(
        self,
        query: str,
        lineage_type: LineageCollectorType,
        processor: Callable[[LineageRow], None],
        connection: redshift_connector.Connection,
    ) -> None:
        logger.info(f"Extracting {lineage_type.name} lineage for db {self.database}")
        try:
            logger.debug(f"Processing lineage query: {query}")

            for lineage_row in RedshiftDataDictionary.get_lineage_rows(
                conn=connection, query=query
            ):
                processor(lineage_row)
        except Exception as e:
            self.report.warning(
                f"extract-{lineage_type.name}",
                f"Error was {e}, {traceback.format_exc()}",
            )
            self._lineage_v1.report_status(f"extract-{lineage_type.name}", False)

    def _process_sql_parser_lineage(self, lineage_row: LineageRow) -> None:
        ddl = lineage_row.ddl
        if ddl is None:
            return

        # TODO query timestamp
        # TODO actor
        # TODO session id

        self.aggregator.add_observed_query(
            query=ddl,
            default_db=self.database,
            default_schema=self.config.default_schema,
        )

    def _process_view_lineage(self, lineage_row: LineageRow) -> None:
        ddl = lineage_row.ddl
        if ddl is None:
            return

        target_name = (
            f"{self.database}.{lineage_row.target_schema}.{lineage_row.target_table}"
        )
        target = DatasetUrn.create_from_ids(
            "redshift",
            target_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        self.aggregator.add_view_definition(
            view_urn=target,
            view_definition=ddl,
            default_db=self.database,
            default_schema=self.config.default_schema,
        )

    def _process_copy_command(self, lineage_row: LineageRow) -> None:
        source = self._lineage_v1._get_sources(
            lineage_type=LineageCollectorType.COPY,
            db_name=self.database,
            source_schema=None,
            source_table=None,
            ddl=None,
            filename=lineage_row.filename,
        )[0]
        if not source:
            return
        s3_urn = source[0].urn

        if not lineage_row.target_schema or not lineage_row.target_table:
            return
        target = DatasetUrn.create_from_ids(
            "redshift",
            f"{self.database}.{lineage_row.target_schema}.{lineage_row.target_table}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        self.aggregator.add_known_lineage_mapping(
            upstream_urn=s3_urn, downstream_urn=target.urn()
        )

    def _process_unload_command(self, lineage_row: LineageRow) -> None:
        lineage_entry = self._lineage_v1._get_target_lineage(
            alias_db_name=self.database,
            lineage_row=lineage_row,
            lineage_type=LineageCollectorType.UNLOAD,
            all_tables_set={},
        )
        if not lineage_entry:
            return
        output_urn = lineage_entry.dataset.urn

        if not lineage_row.source_schema or not lineage_row.source_table:
            return
        source = DatasetUrn.create_from_ids(
            "redshift",
            f"{self.database}.{lineage_row.source_schema}.{lineage_row.source_table}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        self.aggregator.add_known_lineage_mapping(
            upstream_urn=source.urn(), downstream_urn=output_urn
        )

    def generate(self) -> Iterable[MetadataWorkUnit]:
        for mcp in self.aggregator.gen_metadata():
            yield mcp.as_workunit()

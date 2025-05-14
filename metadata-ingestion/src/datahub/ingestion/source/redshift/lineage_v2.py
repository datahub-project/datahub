import collections
import logging
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import redshift_connector

from datahub.emitter import mce_builder
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
from datahub.ingestion.source.redshift.lineage import (
    LineageCollectorType,
    RedshiftLineageExtractor,
)
from datahub.ingestion.source.redshift.query import (
    RedshiftCommonQuery,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)
from datahub.ingestion.source.redshift.redshift_schema import (
    LineageRow,
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


class RedshiftSqlLineageV2(Closeable):
    # does lineage and usage based on SQL parsing.

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        context: PipelineContext,
        database: str,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.platform = "redshift"
        self.config = config
        self.report = report
        self.context = context

        self.database = database
        self.known_urns: Set[str] = set()  # will be set later

        self.aggregator = SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            generate_lineage=True,
            generate_queries=self.config.lineage_v2_generate_queries,
            generate_usage_statistics=False,
            generate_operations=False,
            usage_config=self.config,
            graph=self.context.graph,
            is_temp_table=self._is_temp_table,
        )
        self.report.sql_aggregator = self.aggregator.report

        self.queries: RedshiftCommonQuery = RedshiftProvisionedQuery()
        if self.config.is_serverless:
            self.queries = RedshiftServerlessQuery()

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

    def _is_temp_table(self, name: str) -> bool:
        return (
            DatasetUrn.create_from_ids(
                self.platform,
                name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            ).urn()
            not in self.known_urns
        )

    def build(
        self,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
        db_schemas: Dict[str, Dict[str, RedshiftSchema]],
    ) -> None:
        # Assume things not in `all_tables` as temp tables.
        self.known_urns = {
            DatasetUrn.create_from_ids(
                self.platform,
                f"{db}.{schema}.{table.name}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            ).urn()
            for db, schemas in all_tables.items()
            for schema, tables in schemas.items()
            for table in tables
        }

        # Handle all the temp tables up front.
        if self.config.resolve_temp_table_in_lineage:
            for temp_row in self._lineage_v1.get_temp_tables(connection=connection):
                self.aggregator.add_observed_query(
                    ObservedQuery(
                        query=temp_row.query_text,
                        default_db=self.database,
                        default_schema=self.config.default_schema,
                        session_id=temp_row.session_id,
                        timestamp=temp_row.start_time,
                    ),
                    # The "temp table" query actually returns all CREATE TABLE statements, even if they
                    # aren't explicitly a temp table. As such, setting is_known_temp_table=True
                    # would not be correct. We already have mechanisms to autodetect temp tables,
                    # so we won't lose anything by not setting it.
                    is_known_temp_table=False,
                )

        populate_calls: List[Tuple[LineageCollectorType, str, Callable]] = []

        if self.config.include_table_rename_lineage:
            # Process all the ALTER TABLE RENAME statements
            table_renames, _ = self._lineage_v1._process_table_renames(
                database=self.database,
                connection=connection,
                all_tables=collections.defaultdict(
                    lambda: collections.defaultdict(set)
                ),
            )
            for entry in table_renames.values():
                self.aggregator.add_table_rename(entry)

        if self.config.table_lineage_mode in {
            LineageMode.SQL_BASED,
            LineageMode.MIXED,
        }:
            # Populate lineage by parsing table creating sqls
            query = self.queries.list_insert_create_queries_sql(
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
            query = self.queries.stl_scan_based_lineage_query(
                self.database,
                self.start_time,
                self.end_time,
            )
            populate_calls.append(
                (LineageCollectorType.QUERY_SCAN, query, self._process_stl_scan_lineage)
            )

        if self.config.include_views and self.config.include_view_lineage:
            # Populate lineage for views
            query = self.queries.view_lineage_query()
            populate_calls.append(
                (LineageCollectorType.VIEW, query, self._process_view_lineage)
            )

            # Populate lineage for late binding views
            query = self.queries.list_late_view_ddls_query()
            populate_calls.append(
                (
                    LineageCollectorType.VIEW_DDL_SQL_PARSING,
                    query,
                    self._process_view_lineage,
                )
            )

        if self.config.include_copy_lineage:
            # Populate lineage for copy commands.
            query = self.queries.list_copy_commands_sql(
                db_name=self.database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append(
                (LineageCollectorType.COPY, query, self._process_copy_command)
            )

        if self.config.include_unload_lineage:
            # Populate lineage for unload commands.
            query = self.queries.list_unload_commands_sql(
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

        # Populate lineage for external tables.
        if not self.config.skip_external_tables:
            self._process_external_tables(all_tables=all_tables, db_schemas=db_schemas)

    def _populate_lineage_agg(
        self,
        query: str,
        lineage_type: LineageCollectorType,
        processor: Callable[[LineageRow], None],
        connection: redshift_connector.Connection,
    ) -> None:
        logger.info(f"Extracting {lineage_type.name} lineage for db {self.database}")
        try:
            logger.debug(f"Processing {lineage_type.name} lineage query: {query}")

            timer = self.report.lineage_phases_timer.setdefault(
                lineage_type.name, PerfTimer()
            )
            with timer:
                for lineage_row in RedshiftDataDictionary.get_lineage_rows(
                    conn=connection, query=query
                ):
                    processor(lineage_row)
        except Exception as e:
            self.report.warning(
                title="Failed to extract some lineage",
                message=f"Failed to extract lineage of type {lineage_type.name}",
                context=f"Query: '{query}'",
                exc=e,
            )
            self._lineage_v1.report_status(f"extract-{lineage_type.name}", False)

    def _process_sql_parser_lineage(self, lineage_row: LineageRow) -> None:
        ddl = lineage_row.ddl
        if ddl is None:
            return

        # TODO actor

        self.aggregator.add_observed_query(
            ObservedQuery(
                query=ddl,
                default_db=self.database,
                default_schema=self.config.default_schema,
                timestamp=lineage_row.timestamp,
                session_id=lineage_row.session_id,
            )
        )

    def _make_filtered_target(self, lineage_row: LineageRow) -> Optional[DatasetUrn]:
        target = DatasetUrn.create_from_ids(
            self.platform,
            f"{self.database}.{lineage_row.target_schema}.{lineage_row.target_table}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        if target.urn() not in self.known_urns:
            logger.debug(
                f"Skipping lineage for {target.urn()} as it is not in known_urns"
            )
            return None

        return target

    def _process_stl_scan_lineage(self, lineage_row: LineageRow) -> None:
        target = self._make_filtered_target(lineage_row)
        if not target:
            return

        source = DatasetUrn.create_from_ids(
            self.platform,
            f"{self.database}.{lineage_row.source_schema}.{lineage_row.source_table}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        if lineage_row.ddl is None:
            logger.warning(
                f"stl scan entry is missing query text for {lineage_row.source_schema}.{lineage_row.source_table}"
            )
            return
        self.aggregator.add_known_query_lineage(
            KnownQueryLineageInfo(
                query_text=lineage_row.ddl,
                downstream=target.urn(),
                upstreams=[source.urn()],
                timestamp=lineage_row.timestamp,
            ),
            merge_lineage=True,
        )

    def _process_view_lineage(self, lineage_row: LineageRow) -> None:
        ddl = lineage_row.ddl
        if ddl is None:
            return

        target = self._make_filtered_target(lineage_row)
        if not target:
            return

        self.aggregator.add_view_definition(
            view_urn=target,
            view_definition=ddl,
            default_db=self.database,
            default_schema=self.config.default_schema,
        )

    def _process_copy_command(self, lineage_row: LineageRow) -> None:
        logger.debug(f"Processing COPY command for lineage row: {lineage_row}")
        sources = self._lineage_v1._get_sources(
            lineage_type=LineageCollectorType.COPY,
            db_name=self.database,
            source_schema=None,
            source_table=None,
            ddl=None,
            filename=lineage_row.filename,
        )
        logger.debug(f"Recognized sources: {sources}")
        source = sources[0]
        if not source:
            logger.debug("Ignoring command since couldn't recognize proper source")
            return
        s3_urn = source[0].urn
        logger.debug(f"Recognized s3 dataset urn: {s3_urn}")
        if not lineage_row.target_schema or not lineage_row.target_table:
            logger.debug(
                f"Didn't find target schema (found: {lineage_row.target_schema}) or target table (found: {lineage_row.target_table})"
            )
            return
        target = self._make_filtered_target(lineage_row)
        if not target:
            return

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
            self.platform,
            f"{self.database}.{lineage_row.source_schema}.{lineage_row.source_table}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        if source.urn() not in self.known_urns:
            logger.debug(
                f"Skipping unload lineage for {source.urn()} as it is not in known_urns"
            )
            return

        self.aggregator.add_known_lineage_mapping(
            upstream_urn=source.urn(), downstream_urn=output_urn
        )

    def _process_external_tables(
        self,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
        db_schemas: Dict[str, Dict[str, RedshiftSchema]],
    ) -> None:
        for schema_name, tables in all_tables[self.database].items():
            logger.info(f"External table lineage: checking schema {schema_name}")
            if not db_schemas[self.database].get(schema_name):
                logger.warning(f"Schema {schema_name} not found")
                continue
            for table in tables:
                schema = db_schemas[self.database][schema_name]
                if (
                    table.is_external_table()
                    and schema.is_external_schema()
                    and schema.external_platform
                ):
                    logger.info(
                        f"External table lineage: processing table {schema_name}.{table.name}"
                    )
                    # external_db_params = schema.option
                    upstream_platform = schema.external_platform.lower()

                    table_urn = mce_builder.make_dataset_urn_with_platform_instance(
                        self.platform,
                        f"{self.database}.{schema_name}.{table.name}",
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                    if upstream_platform == self.platform:
                        upstream_schema = schema.get_upstream_schema_name() or "public"
                        upstream_dataset_name = (
                            f"{schema.external_database}.{upstream_schema}.{table.name}"
                        )
                        upstream_platform_instance = self.config.platform_instance
                    else:
                        upstream_dataset_name = (
                            f"{schema.external_database}.{table.name}"
                        )
                        upstream_platform_instance = (
                            self.config.platform_instance_map.get(upstream_platform)
                            if self.config.platform_instance_map
                            else None
                        )

                    upstream_urn = mce_builder.make_dataset_urn_with_platform_instance(
                        upstream_platform,
                        upstream_dataset_name,
                        platform_instance=upstream_platform_instance,
                        env=self.config.env,
                    )

                    self.aggregator.add_known_lineage_mapping(
                        upstream_urn=upstream_urn,
                        downstream_urn=table_urn,
                    )

    def generate(self) -> Iterable[MetadataWorkUnit]:
        for mcp in self.aggregator.gen_metadata():
            yield mcp.as_workunit()
        if len(self.aggregator.report.observed_query_parse_failures) > 0:
            self.report.report_warning(
                title="Failed to extract some SQL lineage",
                message="Unexpected error(s) while attempting to extract lineage from SQL queries. See the full logs for more details.",
                context=f"Query Parsing Failures: {self.aggregator.report.observed_query_parse_failures}",
            )

    def close(self) -> None:
        self.aggregator.close()

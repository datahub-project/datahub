import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from snowflake.connector import SnowflakeConnection

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.snowflake.constants import (
    LINEAGE_PERMISSION_ERROR,
    SnowflakeEdition,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakePermissionError,
    SnowflakeQueryMixin,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlglot_lineage import (
    SchemaResolver,
    SqlParsingResult,
    sqlglot_lineage,
)
from datahub.utilities.time import ts_millis_to_datetime

logger: logging.Logger = logging.getLogger(__name__)

EXTERNAL_LINEAGE = "external_lineage"
TABLE_LINEAGE = "table_lineage"
VIEW_LINEAGE = "view_lineage"


@dataclass(frozen=True)
class SnowflakeColumnId:
    columnName: str
    objectName: str
    objectDomain: Optional[str] = None


class SnowflakeLineageExtractor(
    SnowflakeQueryMixin, SnowflakeConnectionMixin, SnowflakeCommonMixin
):
    """
    Extracts Lineage from Snowflake.
    Following lineage edges are considered.

    1. "Table to View" lineage via `snowflake.account_usage.object_dependencies` view
    2. "S3 to Table" lineage via `show external tables` query.
    3. "View to Table" lineage via `snowflake.account_usage.access_history` view (requires Snowflake Enterprise Edition or above)
    4. "Table to Table" lineage via `snowflake.account_usage.access_history` view (requires Snowflake Enterprise Edition or above)
    5. "S3 to Table" lineage via `snowflake.account_usage.access_history` view (requires Snowflake Enterprise Edition or above)

    Edition Note - Snowflake Standard Edition does not have Access History Feature. So it does not support lineage extraction for edges 3, 4, 5 mentioned above.
    """

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        dataset_urn_builder: Callable[[str], str],
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler],
    ) -> None:
        self._external_lineage_map: Dict[str, Set[str]] = defaultdict(set)
        self.config = config
        self.report = report
        self.logger = logger
        self.dataset_urn_builder = dataset_urn_builder
        self.connection: Optional[SnowflakeConnection] = None

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time
                if not self.config.ignore_start_time_lineage
                else ts_millis_to_datetime(0),
                self.config.end_time,
            )
        else:
            return (
                self.config.start_time
                if not self.config.ignore_start_time_lineage
                else ts_millis_to_datetime(0),
                self.config.end_time,
            )

    def get_workunits(
        self,
        discovered_tables: List[str],
        discovered_views: List[str],
        schema_resolver: SchemaResolver,
        view_definitions: MutableMapping[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_lineage():
            return

        self.connection = self.create_connection()
        if self.connection is None:
            return

        self._populate_external_lineage_map(discovered_tables)
        if self.config.include_view_lineage:
            if len(discovered_views) > 0:
                yield from self.get_view_upstream_workunits(
                    discovered_views=discovered_views,
                    schema_resolver=schema_resolver,
                    view_definitions=view_definitions,
                )
            else:
                logger.info("No views found. Skipping View Lineage Extraction.")

        yield from self.get_table_upstream_workunits(discovered_tables)

        if self._external_lineage_map:  # Some external lineage is yet to be emitted
            yield from self.get_table_external_upstream_workunits()

        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                self.config.start_time
                if not self.config.ignore_start_time_lineage
                else ts_millis_to_datetime(0),
                self.config.end_time,
            )

    def get_table_external_upstream_workunits(self) -> Iterable[MetadataWorkUnit]:
        for (
            dataset_name,
            external_lineage,
        ) in self._external_lineage_map.items():
            upstreams = self.get_external_upstreams(external_lineage)
            if upstreams:
                self.report.num_tables_with_external_upstreams_only += 1
                yield self._create_upstream_lineage_workunit(dataset_name, upstreams)
        logger.info(
            f"Only upstream external lineage detected for {self.report.num_tables_with_external_upstreams_only} tables.",
        )

    def get_table_upstream_workunits(
        self, discovered_tables: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        if self.report.edition == SnowflakeEdition.STANDARD:
            logger.info(
                "Snowflake Account is Standard Edition. Table to Table and View to Table Lineage Feature is not supported."
            )  # See Edition Note above for why
        else:
            with PerfTimer() as timer:
                results = self._fetch_upstream_lineages_for_tables()

                if not results:
                    return

                yield from self._gen_workunits_from_query_result(
                    discovered_tables, results
                )
                self.report.table_lineage_query_secs = timer.elapsed_seconds()
            logger.info(
                f"Upstream lineage detected for {self.report.num_tables_with_upstreams} tables.",
            )

    def _gen_workunits_from_query_result(
        self,
        discovered_assets: Collection[str],
        results: Iterable[dict],
        upstream_for_view: bool = False,
    ) -> Iterable[MetadataWorkUnit]:
        for db_row in results:
            dataset_name = self.get_dataset_identifier_from_qualified_name(
                db_row["DOWNSTREAM_TABLE_NAME"]
            )
            if dataset_name not in discovered_assets:
                continue
            (
                upstreams,
                fine_upstreams,
            ) = self.get_upstreams_from_query_result_row(dataset_name, db_row)
            if upstreams:
                if upstream_for_view:
                    self.report.num_views_with_upstreams += 1
                else:
                    self.report.num_tables_with_upstreams += 1
                yield self._create_upstream_lineage_workunit(
                    dataset_name, upstreams, fine_upstreams
                )
            else:
                logger.debug(f"No lineage found for {dataset_name}")

    def get_view_upstream_workunits(
        self,
        discovered_views: List[str],
        schema_resolver: SchemaResolver,
        view_definitions: MutableMapping[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        views_failed_parsing = set()
        if self.config.include_view_column_lineage:
            with PerfTimer() as timer:
                builder = SqlParsingBuilder(
                    generate_lineage=True,
                    generate_usage_statistics=False,
                    generate_operations=False,
                )
                for view_identifier, view_definition in view_definitions.items():
                    result = self._run_sql_parser(
                        view_identifier, view_definition, schema_resolver
                    )
                    if result and result.out_tables:
                        self.report.num_views_with_upstreams += 1
                        # This does not yield any workunits but we use
                        # yield here to execute this method
                        yield from builder.process_sql_parsing_result(
                            result=result,
                            query=view_definition,
                            is_view_ddl=True,
                        )
                    else:
                        views_failed_parsing.add(view_identifier)

                yield from builder.gen_workunits()
                self.report.view_lineage_parse_secs = timer.elapsed_seconds()

        with PerfTimer() as timer:
            results = self._fetch_upstream_lineages_for_views()

            if results:
                yield from self._gen_workunits_from_query_result(
                    views_failed_parsing,
                    results,
                    upstream_for_view=True,
                )
            self.report.view_upstream_lineage_query_secs = timer.elapsed_seconds()
        logger.info(
            f"Upstream lineage detected for {self.report.num_views_with_upstreams} views.",
        )

    def _run_sql_parser(
        self, dataset_identifier: str, query: str, schema_resolver: SchemaResolver
    ) -> Optional[SqlParsingResult]:
        try:
            database, schema, _view = dataset_identifier.split(".")
        except ValueError:
            logger.warning(f"Invalid view identifier: {dataset_identifier}")
            return None
        raw_lineage = sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=database,
            default_schema=schema,
        )
        if raw_lineage.debug_info.table_error:
            logger.debug(
                f"Failed to parse lineage for view {dataset_identifier}: "
                f"{raw_lineage.debug_info.table_error}"
            )
            self.report.num_view_definitions_failed_parsing += 1
            return None
        elif raw_lineage.debug_info.column_error:
            self.report.num_view_definitions_failed_column_parsing += 1
        else:
            self.report.num_view_definitions_parsed += 1
        return raw_lineage

    def _create_upstream_lineage_workunit(
        self,
        dataset_name: str,
        upstreams: Sequence[UpstreamClass],
        fine_upstreams: Sequence[FineGrainedLineage] = (),
    ) -> MetadataWorkUnit:
        logger.debug(
            f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstreams]}"
        )
        if self.config.upstream_lineage_in_report:
            self.report.upstream_lineage[dataset_name] = [u.dataset for u in upstreams]

        upstream_lineage = UpstreamLineage(
            upstreams=sorted(upstreams, key=lambda x: x.dataset),
            fineGrainedLineages=sorted(
                fine_upstreams,
                key=lambda x: (x.downstreams, x.upstreams),
            )
            or None,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn_builder(dataset_name), aspect=upstream_lineage
        ).as_workunit()

    def get_upstreams_from_query_result_row(
        self, dataset_name: str, db_row: dict
    ) -> Tuple[List[UpstreamClass], List[FineGrainedLineage]]:
        upstreams: List[UpstreamClass] = []
        fine_upstreams: List[FineGrainedLineage] = []

        if "UPSTREAM_TABLES" in db_row and db_row["UPSTREAM_TABLES"] is not None:
            upstreams = self.map_query_result_upstreams(
                json.loads(db_row["UPSTREAM_TABLES"])
            )

        if (
            self.config.include_column_lineage
            and "UPSTREAM_COLUMNS" in db_row
            and db_row["UPSTREAM_COLUMNS"] is not None
        ):
            fine_upstreams = self.map_query_result_fine_upstreams(
                self.dataset_urn_builder(dataset_name),
                json.loads(db_row["UPSTREAM_COLUMNS"]),
            )

        # Populate the external-table-lineage(s3->snowflake), if present
        if dataset_name in self._external_lineage_map:
            external_lineage = self._external_lineage_map.pop(dataset_name)
            upstreams += self.get_external_upstreams(external_lineage)

        return upstreams, fine_upstreams

    def _populate_external_lineage_map(self, discovered_tables: List[str]) -> None:
        with PerfTimer() as timer:
            self.report.num_external_table_edges_scanned = 0

            self._populate_external_lineage_from_copy_history(discovered_tables)
            logger.info(
                "Done populating external lineage from copy history. "
                f"Found {self.report.num_external_table_edges_scanned} external lineage edges so far."
            )

            self._populate_external_lineage_from_show_query(discovered_tables)
            logger.info(
                "Done populating external lineage from show external tables. "
                f"Found {self.report.num_external_table_edges_scanned} external lineage edges so far."
            )

            self.report.external_lineage_queries_secs = timer.elapsed_seconds()
        if len(self._external_lineage_map.keys()) == 0:
            logger.debug("No external lineage found.")

    # Handles the case for explicitly created external tables.
    # NOTE: Snowflake does not log this information to the access_history table.
    def _populate_external_lineage_from_show_query(
        self, discovered_tables: List[str]
    ) -> None:
        external_tables_query: str = SnowflakeQuery.show_external_tables()
        try:
            for db_row in self.query(external_tables_query):
                key = self.get_dataset_identifier(
                    db_row["name"], db_row["schema_name"], db_row["database_name"]
                )

                if key not in discovered_tables:
                    continue
                self._external_lineage_map[key].add(db_row["location"])
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via show external tables"
                )
                self.report.num_external_table_edges_scanned += 1
        except Exception as e:
            logger.debug(e, exc_info=e)
            self.report_warning(
                "external_lineage",
                f"Populating external table lineage from Snowflake failed due to error {e}.",
            )
            self.report_status(EXTERNAL_LINEAGE, False)

    # Handles the case where a table is populated from an external stage/s3 location via copy.
    # Eg: copy into category_english from @external_s3_stage;
    # Eg: copy into category_english from 's3://acryl-snow-demo-olist/olist_raw_data/category_english'credentials=(aws_key_id='...' aws_secret_key='...')  pattern='.*.csv';
    # NOTE: Snowflake does not log this information to the access_history table.
    def _populate_external_lineage_from_copy_history(
        self, discovered_tables: List[str]
    ) -> None:
        query: str = SnowflakeQuery.copy_lineage_history(
            start_time_millis=int(self.start_time.timestamp() * 1000),
            end_time_millis=int(self.end_time.timestamp() * 1000),
            downstreams_deny_pattern=self.config.temporary_tables_pattern,
        )

        try:
            for db_row in self.query(query):
                self._process_external_lineage_result_row(db_row, discovered_tables)
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = "Failed to get external lineage. Please grant imported privileges on SNOWFLAKE database. "
                self.warn_if_stateful_else_error(LINEAGE_PERMISSION_ERROR, error_msg)
            else:
                logger.debug(e, exc_info=e)
                self.report_warning(
                    "external_lineage",
                    f"Populating table external lineage from Snowflake failed due to error {e}.",
                )
            self.report_status(EXTERNAL_LINEAGE, False)

    def _process_external_lineage_result_row(
        self, db_row: dict, discovered_tables: List[str]
    ) -> None:
        # key is the down-stream table name
        key: str = self.get_dataset_identifier_from_qualified_name(
            db_row["DOWNSTREAM_TABLE_NAME"]
        )
        if key not in discovered_tables:
            return

        if db_row["UPSTREAM_LOCATIONS"] is not None:
            external_locations = json.loads(db_row["UPSTREAM_LOCATIONS"])

            for loc in external_locations:
                if loc not in self._external_lineage_map[key]:
                    self._external_lineage_map[key].add(loc)
                    self.report.num_external_table_edges_scanned += 1

            logger.debug(
                f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via access_history"
            )

    def _fetch_upstream_lineages_for_tables(self) -> Iterable[Dict]:
        query: str = SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=int(self.start_time.timestamp() * 1000),
            end_time_millis=int(self.end_time.timestamp() * 1000),
            upstreams_deny_pattern=self.config.temporary_tables_pattern,
            include_view_lineage=self.config.include_view_lineage,
            include_column_lineage=self.config.include_column_lineage,
        )
        try:
            for db_row in self.query(query):
                yield db_row
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = "Failed to get table/view to table lineage. Please grant imported privileges on SNOWFLAKE database. "
                self.warn_if_stateful_else_error(LINEAGE_PERMISSION_ERROR, error_msg)
            else:
                logger.debug(e, exc_info=e)
                self.report_warning(
                    "table-upstream-lineage",
                    f"Extracting lineage from Snowflake failed due to error {e}.",
                )
            self.report_status(TABLE_LINEAGE, False)

    def map_query_result_upstreams(
        self, upstream_tables: Optional[List[dict]]
    ) -> List[UpstreamClass]:
        if not upstream_tables:
            return []
        upstreams: List[UpstreamClass] = []
        for upstream_table in upstream_tables:
            if upstream_table:
                try:
                    self._process_add_single_upstream(upstreams, upstream_table)
                except Exception as e:
                    logger.debug(e, exc_info=e)
        return upstreams

    def _process_add_single_upstream(
        self, upstreams: List[UpstreamClass], upstream_table: dict
    ) -> None:
        upstream_name = self.get_dataset_identifier_from_qualified_name(
            upstream_table["upstream_object_name"]
        )
        if upstream_name and self._is_dataset_pattern_allowed(
            upstream_name, upstream_table["upstream_object_domain"], is_upstream=True
        ):
            upstreams.append(
                UpstreamClass(
                    dataset=self.dataset_urn_builder(upstream_name),
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

    def map_query_result_fine_upstreams(
        self, dataset_urn: str, column_wise_upstreams: Optional[List[dict]]
    ) -> List[FineGrainedLineage]:
        if not column_wise_upstreams:
            return []
        fine_upstreams: List[FineGrainedLineage] = []
        for column_with_upstreams in column_wise_upstreams:
            if column_with_upstreams:
                try:
                    self._process_add_single_column_upstream(
                        dataset_urn, fine_upstreams, column_with_upstreams
                    )
                except Exception as e:
                    logger.debug(e, exc_info=e)
        return fine_upstreams

    def _process_add_single_column_upstream(
        self,
        dataset_urn: str,
        fine_upstreams: List[FineGrainedLineage],
        column_with_upstreams: Dict,
    ) -> None:
        column_name = column_with_upstreams["column_name"]
        upstream_jobs = column_with_upstreams["upstreams"]
        if column_name and upstream_jobs:
            for upstream_columns in upstream_jobs:
                if not upstream_columns:
                    continue
                fine_upstream = self.build_finegrained_lineage(
                    dataset_urn=dataset_urn,
                    col=column_name,
                    upstream_columns={
                        SnowflakeColumnId(
                            columnName=col["column_name"],
                            objectName=col["object_name"],
                            objectDomain=col["object_domain"],
                        )
                        for col in upstream_columns
                    },
                )
                if not fine_upstream:
                    continue
                fine_upstreams.append(fine_upstream)

    def _fetch_upstream_lineages_for_views(self):
        # NOTE: This query captures only the upstream lineage of a view (with no column lineage).
        # For more details see: https://docs.snowflake.com/en/user-guide/object-dependencies.html#object-dependencies
        # and also https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        view_upstream_lineage_query: str = SnowflakeQuery.view_dependencies_v2()

        try:
            for db_row in self.query(view_upstream_lineage_query):
                yield db_row
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = "Failed to get table to view lineage. Please grant imported privileges on SNOWFLAKE database."
                self.warn_if_stateful_else_error(LINEAGE_PERMISSION_ERROR, error_msg)
            else:
                logger.debug(e, exc_info=e)
                self.report_warning(
                    "view-upstream-lineage",
                    f"Extracting the upstream view lineage from Snowflake failed due to error {e}.",
                )
            self.report_status(VIEW_LINEAGE, False)

    def build_finegrained_lineage(
        self,
        dataset_urn: str,
        col: str,
        upstream_columns: Set[SnowflakeColumnId],
    ) -> Optional[FineGrainedLineage]:
        column_upstreams = self.build_finegrained_lineage_upstreams(upstream_columns)
        if not column_upstreams:
            return None
        finegrained_lineage_entry = FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            # Sorting the list of upstream lineage events in order to avoid creating multiple aspects in backend
            # even if the lineage is same but the order is different.
            upstreams=sorted(column_upstreams),
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[
                builder.make_schema_field_urn(
                    dataset_urn, self.snowflake_identifier(col)
                )
            ],
        )

        return finegrained_lineage_entry

    def build_finegrained_lineage_upstreams(
        self, upstream_columms: Set[SnowflakeColumnId]
    ) -> List[str]:
        column_upstreams = []
        for upstream_col in upstream_columms:
            if (
                upstream_col.objectName
                and upstream_col.columnName
                and self._is_dataset_pattern_allowed(
                    upstream_col.objectName,
                    upstream_col.objectDomain,
                    is_upstream=True,
                )
            ):
                upstream_dataset_name = self.get_dataset_identifier_from_qualified_name(
                    upstream_col.objectName
                )
                column_upstreams.append(
                    builder.make_schema_field_urn(
                        self.dataset_urn_builder(upstream_dataset_name),
                        self.snowflake_identifier(upstream_col.columnName),
                    )
                )
        return column_upstreams

    def get_external_upstreams(self, external_lineage: Set[str]) -> List[UpstreamClass]:
        external_upstreams = []
        for external_lineage_entry in sorted(external_lineage):
            # For now, populate only for S3
            if external_lineage_entry.startswith("s3://"):
                external_upstream_table = UpstreamClass(
                    dataset=make_s3_urn_for_lineage(
                        external_lineage_entry, self.config.env
                    ),
                    type=DatasetLineageTypeClass.COPY,
                )
                external_upstreams.append(external_upstream_table)
        return external_upstreams

    def _should_ingest_lineage(self) -> bool:
        if (
            self.redundant_run_skip_handler
            and self.redundant_run_skip_handler.should_skip_this_run(
                cur_start_time=self.config.start_time
                if not self.config.ignore_start_time_lineage
                else ts_millis_to_datetime(0),
                cur_end_time=self.config.end_time,
            )
        ):
            # Skip this run
            self.report.report_warning(
                "lineage-extraction",
                "Skip this run as there was already a run for current ingestion window.",
            )
            return False
        return True

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

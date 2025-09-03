import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import redshift_connector
import sqlglot

import datahub.sql_parsing.sqlglot_lineage as sqlglot_l
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import strip_s3_prefix
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
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
    TempTableRow,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
    TableRename,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


class LineageDatasetPlatform(Enum):
    S3 = "s3"
    REDSHIFT = "redshift"


class LineageCollectorType(Enum):
    QUERY_SCAN = "query_scan"
    QUERY_SQL_PARSER = "query_sql_parser"
    VIEW = "view"
    VIEW_DDL_SQL_PARSING = "view-ddl-sql-parsing"
    COPY = "copy"
    UNLOAD = "unload"


@dataclass(frozen=True, eq=True)
class LineageDataset:
    platform: LineageDatasetPlatform
    urn: str


@dataclass()
class LineageItem:
    dataset: LineageDataset
    upstreams: Set[LineageDataset]
    cll: Optional[List[sqlglot_l.ColumnLineageInfo]]
    collector_type: LineageCollectorType
    dataset_lineage_type: str = field(init=False)

    def __post_init__(self):
        if self.collector_type == LineageCollectorType.COPY:
            self.dataset_lineage_type = DatasetLineageTypeClass.COPY
        elif self.collector_type in [
            LineageCollectorType.VIEW,
            LineageCollectorType.VIEW_DDL_SQL_PARSING,
        ]:
            self.dataset_lineage_type = DatasetLineageTypeClass.VIEW
        else:
            self.dataset_lineage_type = DatasetLineageTypeClass.TRANSFORMED


def parse_alter_table_rename(default_schema: str, query: str) -> Tuple[str, str, str]:
    """
    Parses an ALTER TABLE ... RENAME TO ... query and returns the schema, previous table name, and new table name.
    """

    parsed_query = parse_statement(query, dialect=get_dialect("redshift"))
    assert isinstance(parsed_query, sqlglot.exp.Alter)
    prev_name = parsed_query.this.name
    rename_clause = parsed_query.args["actions"][0]
    assert isinstance(rename_clause, sqlglot.exp.AlterRename)
    new_name = rename_clause.this.name

    schema = parsed_query.this.db or default_schema

    return schema, prev_name, new_name


class RedshiftSqlLineage(Closeable):
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
        self.redundant_run_skip_handler = redundant_run_skip_handler

        self.aggregator = SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            generate_lineage=True,
            generate_queries=self.config.lineage_generate_queries,
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

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

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

    def _get_s3_path(self, path: str) -> Optional[str]:
        if self.config.s3_lineage_config:
            for path_spec in self.config.s3_lineage_config.path_specs:
                if path_spec.allowed(path):
                    _, table_path = path_spec.extract_table_name_and_path(path)
                    return table_path

            if (
                self.config.s3_lineage_config.ignore_non_path_spec_path
                and len(self.config.s3_lineage_config.path_specs) > 0
            ):
                self.report.num_lineage_dropped_s3_path += 1
                logger.debug(
                    f"Skipping s3 path {path} as it does not match any path spec."
                )
                return None

            if self.config.s3_lineage_config.strip_urls:
                if "/" in urlparse(path).path:
                    return str(path.rsplit("/", 1)[0])

        return path

    def _build_s3_path_from_row(self, filename: str) -> Optional[str]:
        path = filename.strip()
        if urlparse(path).scheme != "s3":
            raise ValueError(
                f"Only s3 source supported with copy/unload. The source was: {path}"
            )
        s3_path = self._get_s3_path(path)
        return strip_s3_prefix(s3_path) if s3_path else None

    def _get_sources_from_query(
        self,
        db_name: str,
        query: str,
        parsed_result: Optional[sqlglot_l.SqlParsingResult] = None,
    ) -> Tuple[List[LineageDataset], Optional[List[sqlglot_l.ColumnLineageInfo]]]:
        sources: List[LineageDataset] = list()

        if parsed_result is None:
            parsed_result = sqlglot_l.create_lineage_sql_parsed_result(
                query=query,
                platform=LineageDatasetPlatform.REDSHIFT.value,
                platform_instance=self.config.platform_instance,
                default_db=db_name,
                default_schema=str(self.config.default_schema),
                graph=self.context.graph,
                env=self.config.env,
            )

        if parsed_result is None:
            logger.debug(f"native query parsing failed for {query}")
            return sources, None
        elif parsed_result.debug_info.table_error:
            logger.debug(
                f"native query parsing failed for {query} with error: {parsed_result.debug_info.table_error}"
            )
            return sources, None

        logger.debug(f"parsed_result = {parsed_result}")

        for table_urn in parsed_result.in_tables:
            source = LineageDataset(
                platform=LineageDatasetPlatform.REDSHIFT,
                urn=table_urn,
            )
            sources.append(source)

        return (
            sources,
            (
                parsed_result.column_lineage
                if self.config.include_view_column_lineage
                else None
            ),
        )

    def _get_sources(
        self,
        lineage_type: LineageCollectorType,
        db_name: str,
        source_schema: Optional[str],
        source_table: Optional[str],
        ddl: Optional[str],
        filename: Optional[str],
    ) -> Tuple[List[LineageDataset], Optional[List[sqlglot_l.ColumnLineageInfo]]]:
        sources: List[LineageDataset] = list()
        # Source
        cll: Optional[List[sqlglot_l.ColumnLineageInfo]] = None

        if (
            lineage_type
            in {
                lineage_type.QUERY_SQL_PARSER,
                lineage_type.VIEW_DDL_SQL_PARSING,
            }
            and ddl is not None
        ):
            try:
                sources, cll = self._get_sources_from_query(db_name=db_name, query=ddl)
            except Exception as e:
                logger.warning(
                    f"Error parsing query {ddl} for getting lineage. Error was {e}."
                )
                self.report.num_lineage_dropped_query_parser += 1
        else:
            if lineage_type == lineage_type.COPY and filename is not None:
                platform = LineageDatasetPlatform.S3
                path = filename.strip()
                if urlparse(path).scheme != "s3":
                    logger.warning(
                        "Only s3 source supported with copy. The source was: {path}."
                    )
                    self.report.num_lineage_dropped_not_support_copy_path += 1
                    return [], None
                s3_path = self._get_s3_path(path)
                if s3_path is None:
                    return [], None

                path = strip_s3_prefix(s3_path)
                urn = make_dataset_urn_with_platform_instance(
                    platform=platform.value,
                    name=path,
                    env=self.config.env,
                    platform_instance=(
                        self.config.platform_instance_map.get(platform.value)
                        if self.config.platform_instance_map is not None
                        else None
                    ),
                )
            elif source_schema is not None and source_table is not None:
                platform = LineageDatasetPlatform.REDSHIFT
                path = f"{db_name}.{source_schema}.{source_table}"
                urn = make_dataset_urn_with_platform_instance(
                    platform=platform.value,
                    platform_instance=self.config.platform_instance,
                    name=path,
                    env=self.config.env,
                )
            else:
                return [], cll

            sources = [
                LineageDataset(
                    platform=platform,
                    urn=urn,
                )
            ]

        return sources, cll

    def _get_target_lineage(
        self,
        alias_db_name: str,
        lineage_row: LineageRow,
        lineage_type: LineageCollectorType,
        all_tables_set: Dict[str, Dict[str, Set[str]]],
    ) -> Optional[LineageItem]:
        if (
            lineage_type != LineageCollectorType.UNLOAD
            and lineage_row.target_schema
            and lineage_row.target_table
        ):
            if (
                not self.config.schema_pattern.allowed(lineage_row.target_schema)
                or not self.config.table_pattern.allowed(
                    f"{alias_db_name}.{lineage_row.target_schema}.{lineage_row.target_table}"
                )
            ) and not (
                # We also check the all_tables_set, since this might be a renamed table
                # that we don't want to drop lineage for.
                alias_db_name in all_tables_set
                and lineage_row.target_schema in all_tables_set[alias_db_name]
                and lineage_row.target_table
                in all_tables_set[alias_db_name][lineage_row.target_schema]
            ):
                return None
        # Target
        if lineage_type == LineageCollectorType.UNLOAD and lineage_row.filename:
            try:
                target_platform = LineageDatasetPlatform.S3
                # Following call requires 'filename' key in lineage_row
                target_path = self._build_s3_path_from_row(lineage_row.filename)
                if target_path is None:
                    return None
                urn = make_dataset_urn_with_platform_instance(
                    platform=target_platform.value,
                    name=target_path,
                    env=self.config.env,
                    platform_instance=(
                        self.config.platform_instance_map.get(target_platform.value)
                        if self.config.platform_instance_map is not None
                        else None
                    ),
                )
            except ValueError as e:
                self.report.warning("non-s3-lineage", str(e))
                return None
        else:
            target_platform = LineageDatasetPlatform.REDSHIFT
            target_path = f"{alias_db_name}.{lineage_row.target_schema}.{lineage_row.target_table}"
            urn = make_dataset_urn_with_platform_instance(
                platform=target_platform.value,
                platform_instance=self.config.platform_instance,
                name=target_path,
                env=self.config.env,
            )

        return LineageItem(
            dataset=LineageDataset(platform=target_platform, urn=urn),
            upstreams=set(),
            collector_type=lineage_type,
            cll=None,
        )

    def _process_table_renames(
        self,
        database: str,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, Set[str]]],
    ) -> Tuple[Dict[str, TableRename], Dict[str, Dict[str, Set[str]]]]:
        logger.info(f"Processing table renames for db {database}")

        # new urn -> prev urn
        table_renames: Dict[str, TableRename] = {}

        query = self.queries.alter_table_rename_query(
            db_name=database,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        for rename_row in RedshiftDataDictionary.get_alter_table_commands(
            connection, query
        ):
            # Redshift's system table has some issues where it encodes newlines as \n instead a proper
            # newline character. This can cause issues in our parser.
            query_text = rename_row.query_text.replace("\\n", "\n")

            try:
                schema, prev_name, new_name = parse_alter_table_rename(
                    default_schema=self.config.default_schema,
                    query=query_text,
                )
            except Exception as e:
                logger.info(f"Failed to parse alter table rename: {e}")
                self.report.num_alter_table_parse_errors += 1
                continue

            prev_urn = make_dataset_urn_with_platform_instance(
                platform=LineageDatasetPlatform.REDSHIFT.value,
                platform_instance=self.config.platform_instance,
                name=f"{database}.{schema}.{prev_name}",
                env=self.config.env,
            )
            new_urn = make_dataset_urn_with_platform_instance(
                platform=LineageDatasetPlatform.REDSHIFT.value,
                platform_instance=self.config.platform_instance,
                name=f"{database}.{schema}.{new_name}",
                env=self.config.env,
            )

            table_renames[new_urn] = TableRename(
                prev_urn, new_urn, query_text, timestamp=rename_row.start_time
            )

            # We want to generate lineage for the previous name too.
            all_tables[database][schema].add(prev_name)

        logger.info(f"Discovered {len(table_renames)} table renames")
        return table_renames, all_tables

    def get_temp_tables(
        self, connection: redshift_connector.Connection
    ) -> Iterable[TempTableRow]:
        ddl_query: str = self.queries.temp_table_ddl_query(
            start_time=self.config.start_time,
            end_time=self.config.end_time,
        )

        logger.debug(f"Temporary table ddl query = {ddl_query}")

        for row in RedshiftDataDictionary.get_temporary_rows(
            conn=connection,
            query=ddl_query,
        ):
            yield row

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
            for temp_row in self.get_temp_tables(connection=connection):
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
            table_renames, _ = self._process_table_renames(
                database=self.database,
                connection=connection,
                all_tables=defaultdict(lambda: defaultdict(set)),
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
            self.report_status(f"extract-{lineage_type.name}", False)

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
        sources = self._get_sources(
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
        lineage_entry = self._get_target_lineage(
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

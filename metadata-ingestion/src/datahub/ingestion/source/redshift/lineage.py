import logging
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import humanfriendly
import redshift_connector
import sqlglot

import datahub.emitter.mce_builder as builder
import datahub.sql_parsing.sqlglot_lineage as sqlglot_l
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import TableRename
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement
from datahub.utilities import memory_footprint
from datahub.utilities.dedup_list import deduplicate_list

logger: logging.Logger = logging.getLogger(__name__)


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

    def merge_lineage(
        self,
        upstreams: Set[LineageDataset],
        cll: Optional[List[sqlglot_l.ColumnLineageInfo]],
    ) -> None:
        self.upstreams = self.upstreams.union(upstreams)

        # Merge CLL using the output column name as the merge key.
        self.cll = self.cll or []
        existing_cll: Dict[str, sqlglot_l.ColumnLineageInfo] = {
            c.downstream.column: c for c in self.cll
        }
        for c in cll or []:
            if c.downstream.column in existing_cll:
                # Merge using upstream + column name as the merge key.
                existing_cll[c.downstream.column].upstreams = deduplicate_list(
                    [*existing_cll[c.downstream.column].upstreams, *c.upstreams]
                )
            else:
                # New output column, just add it as is.
                self.cll.append(c)

        self.cll = self.cll or None


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


def split_qualified_table_name(urn: str) -> Tuple[str, str, str]:
    qualified_table_name = DatasetUrn.from_string(urn).name

    # -3 because platform instance is optional and that can cause the split to have more than 3 elements
    db, schema, table = qualified_table_name.split(".")[-3:]

    return db, schema, table


class RedshiftLineageExtractor:
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
        self._lineage_map: Dict[str, LineageItem] = defaultdict()

        self.queries: RedshiftCommonQuery = RedshiftProvisionedQuery()
        if self.config.is_serverless:
            self.queries = RedshiftServerlessQuery()

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

        self.temp_tables: Dict[str, TempTableRow] = {}

    def _init_temp_table_schema(
        self, database: str, temp_tables: List[TempTableRow]
    ) -> None:
        if self.context.graph is None:  # to silent lint
            return

        schema_resolver: SchemaResolver = self.context.graph._make_schema_resolver(
            platform=LineageDatasetPlatform.REDSHIFT.value,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        dataset_vs_columns: Dict[str, List[SchemaField]] = {}
        # prepare dataset_urn vs List of schema fields
        for table in temp_tables:
            logger.debug(
                f"Processing temp table: {table.create_command} with query text {table.query_text}"
            )
            result = sqlglot_l.create_lineage_sql_parsed_result(
                platform=LineageDatasetPlatform.REDSHIFT.value,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                default_db=database,
                default_schema=self.config.default_schema,
                query=table.query_text,
                graph=self.context.graph,
            )

            if (
                result is None
                or result.column_lineage is None
                or not result.query_type.is_create()
                or not result.out_tables
            ):
                logger.debug(f"Unsupported temp table query found: {table.query_text}")
                continue

            table.parsed_result = result
            if result.column_lineage[0].downstream.table:
                table.urn = result.column_lineage[0].downstream.table

            self.temp_tables[result.out_tables[0]] = table

        for table in self.temp_tables.values():
            if (
                table.parsed_result is None
                or table.urn is None
                or table.parsed_result.column_lineage is None
            ):
                continue

            # Initialise the temp table urn, we later need this to merge CLL
            downstream_urn = table.urn
            if downstream_urn not in dataset_vs_columns:
                dataset_vs_columns[downstream_urn] = []
            dataset_vs_columns[downstream_urn].extend(
                sqlglot_l.infer_output_schema(table.parsed_result) or []
            )

        # Add datasets, and it's respective fields in schema_resolver, so that later schema_resolver would be able
        # correctly generates the upstreams for temporary tables
        for urn in dataset_vs_columns:
            db, schema, table_name = split_qualified_table_name(urn)
            schema_resolver.add_schema_metadata(
                urn=urn,
                schema_metadata=SchemaMetadata(
                    schemaName=table_name,
                    platform=builder.make_data_platform_urn(
                        LineageDatasetPlatform.REDSHIFT.value
                    ),
                    version=0,
                    hash="",
                    platformSchema=OtherSchema(rawSchema=""),
                    fields=dataset_vs_columns[urn],
                ),
            )

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            self.report.stateful_lineage_ingestion_enabled = True
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        # TODO: Remove this method.
        self.report.warning(key, reason)

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

    def _build_s3_path_from_row(self, filename: str) -> Optional[str]:
        path = filename.strip()
        if urlparse(path).scheme != "s3":
            raise ValueError(
                f"Only s3 source supported with copy/unload. The source was: {path}"
            )
        s3_path = self._get_s3_path(path)
        return strip_s3_prefix(s3_path) if s3_path else None

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

    def _populate_lineage_map(
        self,
        query: str,
        database: str,
        lineage_type: LineageCollectorType,
        connection: redshift_connector.Connection,
        all_tables_set: Dict[str, Dict[str, Set[str]]],
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
            cll: Optional[List[sqlglot_l.ColumnLineageInfo]] = None
            raw_db_name = database
            alias_db_name = self.config.database

            for lineage_row in RedshiftDataDictionary.get_lineage_rows(
                conn=connection, query=query
            ):
                target = self._get_target_lineage(
                    alias_db_name,
                    lineage_row,
                    lineage_type,
                    all_tables_set=all_tables_set,
                )
                if not target:
                    continue

                logger.debug(
                    f"Processing {lineage_type.name} lineage row: {lineage_row}"
                )

                sources, cll = self._get_sources(
                    lineage_type,
                    alias_db_name,
                    source_schema=lineage_row.source_schema,
                    source_table=lineage_row.source_table,
                    ddl=lineage_row.ddl,
                    filename=lineage_row.filename,
                )

                target.upstreams.update(
                    self._get_upstream_lineages(
                        sources=sources,
                        target_table=target.dataset.urn,
                        target_dataset_cll=cll,
                        all_tables_set=all_tables_set,
                        alias_db_name=alias_db_name,
                        raw_db_name=raw_db_name,
                        connection=connection,
                    )
                )
                target.cll = cll

                # Merging upstreams if dataset already exists and has upstreams
                if target.dataset.urn in self._lineage_map:
                    self._lineage_map[target.dataset.urn].merge_lineage(
                        upstreams=target.upstreams, cll=target.cll
                    )
                else:
                    self._lineage_map[target.dataset.urn] = target

                logger.debug(
                    f"Lineage[{target}]:{self._lineage_map[target.dataset.urn]}"
                )
        except Exception as e:
            self.warn(
                logger,
                f"extract-{lineage_type.name}",
                f"Error was {e}, {traceback.format_exc()}",
            )
            self.report_status(f"extract-{lineage_type.name}", False)

    def _update_lineage_map_for_table_renames(
        self, table_renames: Dict[str, TableRename]
    ) -> None:
        if not table_renames:
            return

        logger.info(f"Updating lineage map for {len(table_renames)} table renames")
        for entry in table_renames.values():
            # This table was renamed from some other name, copy in the lineage
            # for the previous name as well.
            prev_table_lineage = self._lineage_map.get(entry.original_urn)
            if prev_table_lineage:
                logger.debug(
                    f"including lineage for {entry.original_urn} in {entry.new_urn} due to table rename"
                )
                self._lineage_map[entry.new_urn].merge_lineage(
                    upstreams=prev_table_lineage.upstreams,
                    cll=prev_table_lineage.cll,
                )

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
                self.warn(logger, "non-s3-lineage", str(e))
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

    def _get_upstream_lineages(
        self,
        sources: List[LineageDataset],
        target_table: str,
        all_tables_set: Dict[str, Dict[str, Set[str]]],
        alias_db_name: str,
        raw_db_name: str,
        connection: redshift_connector.Connection,
        target_dataset_cll: Optional[List[sqlglot_l.ColumnLineageInfo]],
    ) -> List[LineageDataset]:
        target_source = []
        probable_temp_tables: List[str] = []

        for source in sources:
            if source.platform == LineageDatasetPlatform.REDSHIFT:
                db, schema, table = split_qualified_table_name(source.urn)
                if db == raw_db_name:
                    db = alias_db_name
                    path = f"{db}.{schema}.{table}"
                    source = LineageDataset(
                        platform=source.platform,
                        urn=make_dataset_urn_with_platform_instance(
                            platform=LineageDatasetPlatform.REDSHIFT.value,
                            platform_instance=self.config.platform_instance,
                            name=path,
                            env=self.config.env,
                        ),
                    )

                # Filtering out tables which does not exist in Redshift
                # It was deleted in the meantime or query parser did not capture well the table name
                # Or it might be a temp table
                if (
                    db not in all_tables_set
                    or schema not in all_tables_set[db]
                    or table not in all_tables_set[db][schema]
                ):
                    logger.debug(
                        f"{source.urn} missing table. Adding it to temp table list for target table {target_table}.",
                    )
                    probable_temp_tables.append(f"{schema}.{table}")
                    self.report.num_lineage_tables_dropped += 1
                    continue

            target_source.append(source)

        if probable_temp_tables and self.config.resolve_temp_table_in_lineage:
            self.report.num_lineage_processed_temp_tables += len(probable_temp_tables)
            # Generate lineage dataset from temporary tables
            number_of_permanent_dataset_found: int = (
                self.update_table_and_column_lineage(
                    db_name=raw_db_name,
                    connection=connection,
                    temp_table_names=probable_temp_tables,
                    target_source_dataset=target_source,
                    target_dataset_cll=target_dataset_cll,
                )
            )

            logger.debug(
                f"Number of permanent datasets found for {target_table} = {number_of_permanent_dataset_found} in "
                f"temp tables {probable_temp_tables}"
            )

        return target_source

    def populate_lineage(
        self,
        database: str,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> None:
        if self.config.resolve_temp_table_in_lineage:
            self._init_temp_table_schema(
                database=database,
                temp_tables=list(self.get_temp_tables(connection=connection)),
            )

        populate_calls: List[Tuple[str, LineageCollectorType]] = []

        all_tables_set: Dict[str, Dict[str, Set[str]]] = {
            db: {schema: {t.name for t in tables} for schema, tables in schemas.items()}
            for db, schemas in all_tables.items()
        }

        table_renames: Dict[str, TableRename] = {}
        if self.config.include_table_rename_lineage:
            table_renames, all_tables_set = self._process_table_renames(
                database=database,
                connection=connection,
                all_tables=all_tables_set,
            )

        if self.config.table_lineage_mode in {
            LineageMode.STL_SCAN_BASED,
            LineageMode.MIXED,
        }:
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            query = self.queries.stl_scan_based_lineage_query(
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
            query = self.queries.list_insert_create_queries_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SQL_PARSER))

        if self.config.include_views and self.config.include_view_lineage:
            # Populate table level lineage for views
            query = self.queries.view_lineage_query()
            populate_calls.append((query, LineageCollectorType.VIEW))

            # Populate table level lineage for late binding views
            query = self.queries.list_late_view_ddls_query()
            populate_calls.append((query, LineageCollectorType.VIEW_DDL_SQL_PARSING))

        if self.config.include_copy_lineage:
            query = self.queries.list_copy_commands_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.COPY))

        if self.config.include_unload_lineage:
            query = self.queries.list_unload_commands_sql(
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
                all_tables_set=all_tables_set,
            )

        # Handling for alter table statements.
        self._update_lineage_map_for_table_renames(table_renames=table_renames)

        self.report.lineage_mem_size[self.config.database] = humanfriendly.format_size(
            memory_footprint.total_size(self._lineage_map)
        )

    def make_fine_grained_lineage_class(
        self, lineage_item: LineageItem, dataset_urn: str
    ) -> List[FineGrainedLineage]:
        fine_grained_lineages: List[FineGrainedLineage] = []

        if (
            self.config.extract_column_level_lineage is False
            or lineage_item.cll is None
        ):
            logger.debug("CLL extraction is disabled")
            return fine_grained_lineages

        logger.debug("Extracting column level lineage")

        cll: List[sqlglot_l.ColumnLineageInfo] = lineage_item.cll

        for cll_info in cll:
            downstream = (
                [builder.make_schema_field_urn(dataset_urn, cll_info.downstream.column)]
                if cll_info.downstream is not None
                and cll_info.downstream.column is not None
                else []
            )

            upstreams = [
                builder.make_schema_field_urn(column_ref.table, column_ref.column)
                for column_ref in cll_info.upstreams
            ]

            fine_grained_lineages.append(
                FineGrainedLineage(
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        logger.debug(f"Created fine_grained_lineage for {dataset_urn}")

        return fine_grained_lineages

    def get_lineage(
        self,
        table: Union[RedshiftTable, RedshiftView],
        dataset_urn: str,
        schema: RedshiftSchema,
    ) -> Optional[UpstreamLineageClass]:
        upstream_lineage: List[UpstreamClass] = []

        cll_lineage: List[FineGrainedLineage] = []

        if dataset_urn in self._lineage_map:
            item = self._lineage_map[dataset_urn]
            for upstream in item.upstreams:
                upstream_table = UpstreamClass(
                    dataset=upstream.urn,
                    type=item.dataset_lineage_type,
                )
                upstream_lineage.append(upstream_table)

            cll_lineage = self.make_fine_grained_lineage_class(
                lineage_item=item,
                dataset_urn=dataset_urn,
            )

        tablename = table.name
        if table.type == "EXTERNAL_TABLE":
            # external_db_params = schema.option
            upstream_platform = schema.type.lower()
            catalog_upstream = UpstreamClass(
                mce_builder.make_dataset_urn_with_platform_instance(
                    upstream_platform,
                    f"{schema.external_database}.{tablename}",
                    platform_instance=(
                        self.config.platform_instance_map.get(upstream_platform)
                        if self.config.platform_instance_map
                        else None
                    ),
                    env=self.config.env,
                ),
                DatasetLineageTypeClass.COPY,
            )
            upstream_lineage.append(catalog_upstream)

        if upstream_lineage:
            self.report.upstream_lineage[dataset_urn] = [
                u.dataset for u in upstream_lineage
            ]
        else:
            return None

        return UpstreamLineage(
            upstreams=upstream_lineage,
            fineGrainedLineages=cll_lineage or None,
        )

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

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

    def find_temp_tables(
        self, temp_table_rows: List[TempTableRow], temp_table_names: List[str]
    ) -> List[TempTableRow]:
        matched_temp_tables: List[TempTableRow] = []

        for table_name in temp_table_names:
            prefixes = self.queries.get_temp_table_clause(table_name)
            prefixes.extend(
                self.queries.get_temp_table_clause(table_name.split(".")[-1])
            )

            for row in temp_table_rows:
                if any(
                    row.create_command.lower().startswith(prefix) for prefix in prefixes
                ):
                    matched_temp_tables.append(row)

        return matched_temp_tables

    def resolve_column_refs(
        self, column_refs: List[sqlglot_l.ColumnRef], depth: int = 0
    ) -> List[sqlglot_l.ColumnRef]:
        """
        This method resolves the column reference to the original column reference.
        For example, if the column reference is to a temporary table, it will be resolved to the original column
        reference.
        """
        max_depth = 10

        resolved_column_refs: List[sqlglot_l.ColumnRef] = []
        if not column_refs:
            return column_refs

        if depth >= max_depth:
            logger.warning(
                f"Max depth reached for resolving temporary columns: {column_refs}"
            )
            self.report.num_unresolved_temp_columns += 1
            return column_refs

        for ref in column_refs:
            resolved = False
            if ref.table in self.temp_tables:
                table = self.temp_tables[ref.table]
                if table.parsed_result and table.parsed_result.column_lineage:
                    for column_lineage in table.parsed_result.column_lineage:
                        if (
                            column_lineage.downstream.table == ref.table
                            and column_lineage.downstream.column == ref.column
                        ):
                            resolved_column_refs.extend(
                                self.resolve_column_refs(
                                    column_lineage.upstreams, depth=depth + 1
                                )
                            )
                            resolved = True
                            break
                    # If we reach here, it means that we were not able to resolve the column reference.
                    if resolved is False:
                        logger.warning(
                            f"Unable to resolve column reference {ref} to a permanent table"
                        )
            else:
                logger.debug(
                    f"Resolved column reference {ref} is not resolved because referenced table {ref.table} is not a temp table or not found. Adding reference as non-temp table. This is normal."
                )
                resolved_column_refs.append(ref)
        return resolved_column_refs

    def _update_target_dataset_cll(
        self,
        temp_table_urn: str,
        target_dataset_cll: List[sqlglot_l.ColumnLineageInfo],
        source_dataset_cll: List[sqlglot_l.ColumnLineageInfo],
    ) -> None:
        for target_column_lineage in target_dataset_cll:
            upstreams: List[sqlglot_l.ColumnRef] = []
            # Look for temp_table_urn in upstream of column_lineage, if found then we need to replace it with
            # column of permanent table
            for target_column_ref in target_column_lineage.upstreams:
                if target_column_ref.table == temp_table_urn:
                    # Look for column_ref.table and column_ref.column in downstream of source_dataset_cll.
                    # The source_dataset_cll contains CLL generated from create statement of temp table (temp_table_urn)
                    for source_column_lineage in source_dataset_cll:
                        if (
                            source_column_lineage.downstream.table
                            == target_column_ref.table
                            and source_column_lineage.downstream.column
                            == target_column_ref.column
                        ):
                            resolved_columns = self.resolve_column_refs(
                                source_column_lineage.upstreams
                            )
                            # Add all upstream of above temporary column into upstream of target column
                            upstreams.extend(resolved_columns)
                    continue

                upstreams.append(target_column_ref)

            if upstreams:
                # update the upstreams
                target_column_lineage.upstreams = upstreams

    def _add_permanent_datasets_recursively(
        self,
        db_name: str,
        temp_table_rows: List[TempTableRow],
        visited_tables: Set[str],
        connection: redshift_connector.Connection,
        permanent_lineage_datasets: List[LineageDataset],
        target_dataset_cll: Optional[List[sqlglot_l.ColumnLineageInfo]],
    ) -> None:
        transitive_temp_tables: List[TempTableRow] = []

        for temp_table in temp_table_rows:
            logger.debug(
                f"Processing temp table with transaction id: {temp_table.transaction_id} and query text {temp_table.query_text}"
            )

            intermediate_l_datasets, cll = self._get_sources_from_query(
                db_name=db_name,
                query=temp_table.query_text,
                parsed_result=temp_table.parsed_result,
            )

            if (
                temp_table.urn is not None
                and target_dataset_cll is not None
                and cll is not None
            ):  # condition to silent the lint
                self._update_target_dataset_cll(
                    temp_table_urn=temp_table.urn,
                    target_dataset_cll=target_dataset_cll,
                    source_dataset_cll=cll,
                )

            # make sure lineage dataset should not contain a temp table
            # if such dataset is present then add it to transitive_temp_tables to resolve it to original permanent table
            for lineage_dataset in intermediate_l_datasets:
                db, schema, table = split_qualified_table_name(lineage_dataset.urn)

                if table in visited_tables:
                    # The table is already processed
                    continue

                # Check if table found is again a temp table
                repeated_temp_table: List[TempTableRow] = self.find_temp_tables(
                    temp_table_rows=list(self.temp_tables.values()),
                    temp_table_names=[table],
                )

                if not repeated_temp_table:
                    logger.debug(f"Unable to find table {table} in temp tables.")

                if repeated_temp_table:
                    transitive_temp_tables.extend(repeated_temp_table)
                    visited_tables.add(table)
                    continue

                permanent_lineage_datasets.append(lineage_dataset)

        if transitive_temp_tables:
            # recursive call
            self._add_permanent_datasets_recursively(
                db_name=db_name,
                temp_table_rows=transitive_temp_tables,
                visited_tables=visited_tables,
                connection=connection,
                permanent_lineage_datasets=permanent_lineage_datasets,
                target_dataset_cll=target_dataset_cll,
            )

    def update_table_and_column_lineage(
        self,
        db_name: str,
        temp_table_names: List[str],
        connection: redshift_connector.Connection,
        target_source_dataset: List[LineageDataset],
        target_dataset_cll: Optional[List[sqlglot_l.ColumnLineageInfo]],
    ) -> int:
        permanent_lineage_datasets: List[LineageDataset] = []

        temp_table_rows: List[TempTableRow] = self.find_temp_tables(
            temp_table_rows=list(self.temp_tables.values()),
            temp_table_names=temp_table_names,
        )

        visited_tables: Set[str] = set(temp_table_names)

        self._add_permanent_datasets_recursively(
            db_name=db_name,
            temp_table_rows=temp_table_rows,
            visited_tables=visited_tables,
            connection=connection,
            permanent_lineage_datasets=permanent_lineage_datasets,
            target_dataset_cll=target_dataset_cll,
        )

        target_source_dataset.extend(permanent_lineage_datasets)

        return len(permanent_lineage_datasets)

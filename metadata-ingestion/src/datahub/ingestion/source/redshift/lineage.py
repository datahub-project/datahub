import logging
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import humanfriendly
import redshift_connector
from sqllineage.runner import LineageRunner

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.aws.s3_util import strip_s3_prefix
from datahub.ingestion.source.redshift.common import get_db_name
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
from datahub.ingestion.source.redshift.query import RedshiftQuery
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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities import memory_footprint

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
    path: str


@dataclass()
class LineageItem:
    dataset: LineageDataset
    upstreams: Set[LineageDataset]
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


class RedshiftLineageExtractor:
    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.config = config
        self.report = report
        self._lineage_map: Dict[str, LineageItem] = defaultdict()

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

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def _get_s3_path(self, path: str) -> str:
        if self.config.s3_lineage_config:
            for path_spec in self.config.s3_lineage_config.path_specs:
                if path_spec.allowed(path):
                    _, table_path = path_spec.extract_table_name_and_path(path)
                    return table_path

            if self.config.s3_lineage_config.strip_urls:
                if "/" in urlparse(path).path:
                    return str(path.rsplit("/", 1)[0])

        return path

    def _get_sources_from_query(self, db_name: str, query: str) -> List[LineageDataset]:
        sources: List[LineageDataset] = list()

        parser = LineageRunner(query)

        for table in parser.source_tables:
            split = str(table).split(".")
            if len(split) == 3:
                db_name, source_schema, source_table = split
            elif len(split) == 2:
                source_schema, source_table = split
            else:
                raise ValueError(
                    f"Invalid table name {table} in query {query}. "
                    f"Expected format: [db_name].[schema].[table] or [schema].[table] or [table]."
                )

            if source_schema == "<default>":
                source_schema = str(self.config.default_schema)

            source = LineageDataset(
                platform=LineageDatasetPlatform.REDSHIFT,
                path=f"{db_name}.{source_schema}.{source_table}",
            )
            sources.append(source)

        return sources

    def _build_s3_path_from_row(self, filename: str) -> str:
        path = filename.strip()
        if urlparse(path).scheme != "s3":
            raise ValueError(
                f"Only s3 source supported with copy/unload. The source was: {path}"
            )
        return strip_s3_prefix(self._get_s3_path(path))

    def _get_sources(
        self,
        lineage_type: LineageCollectorType,
        db_name: str,
        source_schema: Optional[str],
        source_table: Optional[str],
        ddl: Optional[str],
        filename: Optional[str],
    ) -> List[LineageDataset]:
        sources: List[LineageDataset] = list()
        # Source
        if (
            lineage_type
            in {
                lineage_type.QUERY_SQL_PARSER,
                lineage_type.VIEW_DDL_SQL_PARSING,
            }
            and ddl is not None
        ):
            try:
                sources = self._get_sources_from_query(db_name=db_name, query=ddl)
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
                    return sources
                path = strip_s3_prefix(self._get_s3_path(path))
            elif source_schema is not None and source_table is not None:
                platform = LineageDatasetPlatform.REDSHIFT
                path = f"{db_name}.{source_schema}.{source_table}"
            else:
                return []

            sources = [
                LineageDataset(
                    platform=platform,
                    path=path,
                )
            ]

        return sources

    def _populate_lineage_map(
        self,
        query: str,
        database: str,
        lineage_type: LineageCollectorType,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
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
        try:
            raw_db_name = database
            alias_db_name = get_db_name(self.config)

            for lineage_row in RedshiftDataDictionary.get_lineage_rows(
                conn=connection, query=query
            ):
                target = self._get_target_lineage(
                    alias_db_name, lineage_row, lineage_type
                )
                if not target:
                    continue

                sources = self._get_sources(
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
                        all_tables=all_tables,
                        alias_db_name=alias_db_name,
                        raw_db_name=raw_db_name,
                    )
                )

                # Merging downstreams if dataset already exists and has downstreams
                if target.dataset.path in self._lineage_map:
                    self._lineage_map[
                        target.dataset.path
                    ].upstreams = self._lineage_map[
                        target.dataset.path
                    ].upstreams.union(
                        target.upstreams
                    )

                else:
                    self._lineage_map[target.dataset.path] = target

                logger.debug(
                    f"Lineage[{target}]:{self._lineage_map[target.dataset.path]}"
                )
        except Exception as e:
            self.warn(
                logger,
                f"extract-{lineage_type.name}",
                f"Error was {e}, {traceback.format_exc()}",
            )
            self.report_status(f"extract-{lineage_type.name}", False)

    def _get_target_lineage(
        self,
        alias_db_name: str,
        lineage_row: LineageRow,
        lineage_type: LineageCollectorType,
    ) -> Optional[LineageItem]:
        if (
            lineage_type != LineageCollectorType.UNLOAD
            and lineage_row.target_schema
            and lineage_row.target_table
        ):
            if not self.config.schema_pattern.allowed(
                lineage_row.target_schema
            ) or not self.config.table_pattern.allowed(
                f"{alias_db_name}.{lineage_row.target_schema}.{lineage_row.target_table}"
            ):
                return None
        # Target
        if lineage_type == LineageCollectorType.UNLOAD and lineage_row.filename:
            try:
                target_platform = LineageDatasetPlatform.S3
                # Following call requires 'filename' key in lineage_row
                target_path = self._build_s3_path_from_row(lineage_row.filename)
            except ValueError as e:
                self.warn(logger, "non-s3-lineage", str(e))
                return None
        else:
            target_platform = LineageDatasetPlatform.REDSHIFT
            target_path = f"{alias_db_name}.{lineage_row.target_schema}.{lineage_row.target_table}"

        return LineageItem(
            dataset=LineageDataset(platform=target_platform, path=target_path),
            upstreams=set(),
            collector_type=lineage_type,
        )

    def _get_upstream_lineages(
        self,
        sources: List[LineageDataset],
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
        alias_db_name: str,
        raw_db_name: str,
    ) -> List[LineageDataset]:
        targe_source = []
        for source in sources:
            if source.platform == LineageDatasetPlatform.REDSHIFT:
                db, schema, table = source.path.split(".")
                if db == raw_db_name:
                    db = alias_db_name
                    path = f"{db}.{schema}.{table}"
                    source = LineageDataset(platform=source.platform, path=path)

                # Filtering out tables which does not exist in Redshift
                # It was deleted in the meantime or query parser did not capture well the table name
                if (
                    db not in all_tables
                    or schema not in all_tables[db]
                    or not any(table == t.name for t in all_tables[db][schema])
                ):
                    logger.debug(
                        f"{source.path} missing table, dropping from lineage.",
                    )
                    self.report.num_lineage_tables_dropped += 1
                    continue

            targe_source.append(source)
        return targe_source

    def populate_lineage(
        self,
        database: str,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> None:
        populate_calls: List[Tuple[str, LineageCollectorType]] = []

        if self.config.table_lineage_mode == LineageMode.STL_SCAN_BASED:
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            query = RedshiftQuery.stl_scan_based_lineage_query(
                self.config.database,
                self.start_time,
                self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SCAN))
        elif self.config.table_lineage_mode == LineageMode.SQL_BASED:
            # Populate table level lineage by parsing table creating sqls
            query = RedshiftQuery.list_insert_create_queries_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SQL_PARSER))
        elif self.config.table_lineage_mode == LineageMode.MIXED:
            # Populate table level lineage by parsing table creating sqls
            query = RedshiftQuery.list_insert_create_queries_sql(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SQL_PARSER))

            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            query = RedshiftQuery.stl_scan_based_lineage_query(
                db_name=database,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            populate_calls.append((query, LineageCollectorType.QUERY_SCAN))

        if self.config.include_views:
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
                all_tables=all_tables,
            )

        self.report.lineage_mem_size[self.config.database] = humanfriendly.format_size(
            memory_footprint.total_size(self._lineage_map)
        )

    def get_lineage(
        self,
        table: Union[RedshiftTable, RedshiftView],
        dataset_urn: str,
        schema: RedshiftSchema,
    ) -> Optional[Tuple[UpstreamLineageClass, Dict[str, str]]]:
        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return None

        upstream_lineage: List[UpstreamClass] = []

        if dataset_key.name in self._lineage_map:
            item = self._lineage_map[dataset_key.name]
            for upstream in item.upstreams:
                upstream_table = UpstreamClass(
                    dataset=make_dataset_urn_with_platform_instance(
                        upstream.platform.value,
                        upstream.path,
                        platform_instance=self.config.platform_instance_map.get(
                            upstream.platform.value
                        )
                        if self.config.platform_instance_map
                        else None,
                        env=self.config.env,
                    ),
                    type=item.dataset_lineage_type,
                )
                upstream_lineage.append(upstream_table)

        tablename = table.name
        if table.type == "EXTERNAL_TABLE":
            # external_db_params = schema.option
            upstream_platform = schema.type.lower()
            catalog_upstream = UpstreamClass(
                mce_builder.make_dataset_urn_with_platform_instance(
                    upstream_platform,
                    f"{schema.external_database}.{tablename}",
                    platform_instance=self.config.platform_instance_map.get(
                        upstream_platform
                    )
                    if self.config.platform_instance_map
                    else None,
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

        return UpstreamLineage(upstreams=upstream_lineage), {}

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

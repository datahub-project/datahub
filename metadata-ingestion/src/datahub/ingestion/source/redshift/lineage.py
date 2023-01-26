import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import humanfriendly
import redshift_connector
from sqllineage.runner import LineageRunner

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.aws.s3_util import strip_s3_prefix
from datahub.ingestion.source.redshift.common import (
    get_db_name,
    redshift_datetime_format,
)
from datahub.ingestion.source.redshift.config import LineageMode, RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
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
    NON_BINDING_VIEW = "non-binding-view"
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
            LineageCollectorType.NON_BINDING_VIEW,
        ]:
            self.dataset_lineage_type = DatasetLineageTypeClass.VIEW
        else:
            self.dataset_lineage_type = DatasetLineageTypeClass.TRANSFORMED


class LineageExtractor:
    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
    ):
        self.config = config
        self.report = report
        self.lineage_metadata: Dict[str, Set[str]] = defaultdict(set)
        self._lineage_map: Dict[str, LineageItem] = defaultdict()

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
        sources = list()

        parser = LineageRunner(query)

        for table in parser.source_tables:
            source_schema, source_table = str(table).split(".")
            if source_schema == "<default>":
                source_schema = str(self.config.default_schema)

            source = LineageDataset(
                platform=LineageDatasetPlatform.REDSHIFT,
                path=f"{db_name}.{source_schema}.{source_table}",
            )
            sources.append(source)

        return sources

    def _populate_lineage_map(
        self,
        query: str,
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
            cursor = connection.cursor()
            raw_db_name = self.config.database
            alias_db_name = get_db_name(self.config)
            db_name = alias_db_name
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]

            rows = cursor.fetchmany()
            while rows:
                for db_row in rows:
                    if not self.config.schema_pattern.allowed(
                        db_row[field_names.index("target_schema")]
                    ) or not self.config.table_pattern.allowed(
                        f'{db_name}.{db_row[field_names.index("target_schema")]}.{db_row[field_names.index("target_table")]}'
                    ):
                        continue

                    # Target
                    target_path = f'{db_name}.{db_row[field_names.index("target_schema")]}.{db_row[field_names.index("target_table")]}'
                    target = LineageItem(
                        dataset=LineageDataset(
                            platform=LineageDatasetPlatform.REDSHIFT, path=target_path
                        ),
                        upstreams=set(),
                        collector_type=lineage_type,
                    )

                    sources: List[LineageDataset] = list()
                    # Source
                    if lineage_type in [
                        lineage_type.QUERY_SQL_PARSER,
                        lineage_type.NON_BINDING_VIEW,
                    ]:
                        try:
                            sources = self._get_sources_from_query(
                                db_name=db_name, query=db_row[field_names.index("ddl")]
                            )
                        except Exception as e:
                            self.warn(
                                logger,
                                "parsing-query",
                                f'Error parsing query {db_row[field_names.index("ddl")]} for getting lineage .'
                                f"\nError was {e}.",
                            )
                    else:
                        if lineage_type == lineage_type.COPY:
                            platform = LineageDatasetPlatform.S3
                            path = db_row[field_names.index("filename")].strip()
                            if urlparse(path).scheme != "s3":
                                self.warn(
                                    logger,
                                    "non-s3-lineage",
                                    f"Only s3 source supported with copy. The source was: {path}.",
                                )
                                continue
                            path = strip_s3_prefix(self._get_s3_path(path))
                        else:
                            platform = LineageDatasetPlatform.REDSHIFT
                            path = f'{db_name}.{db_row[field_names.index("source_schema")]}.{db_row[field_names.index("source_table")]}'

                        sources = [
                            LineageDataset(
                                platform=platform,
                                path=path,
                            )
                        ]

                    for source in sources:
                        if source.platform == LineageDatasetPlatform.REDSHIFT:
                            db, schema, table = source.path.split(".")
                            if db == raw_db_name:
                                db = alias_db_name
                                path = f"{db}.{schema}.{table}"
                                source = LineageDataset(
                                    platform=source.platform, path=path
                                )

                            # Filtering out tables which does not exist in Redshift
                            # It was deleted in the meantime or query parser did not capture well the table name
                            if (
                                db not in all_tables
                                or schema not in all_tables[db]
                                or not any(
                                    table == t.name for t in all_tables[db][schema]
                                )
                            ):
                                self.warn(
                                    logger,
                                    "missing-table",
                                    f"{source.path} missing table",
                                )
                                continue

                        target.upstreams.add(source)

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
                rows = cursor.fetchmany()
        except Exception as e:
            self.warn(logger, f"extract-{lineage_type.name}", f"Error was {e}")

    def populate_lineage(
        self,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> None:

        stl_scan_based_lineage_query: str = """
                select
                    distinct cluster,
                    target_schema,
                    target_table,
                    username,
                    source_schema,
                    source_table
                from
                        (
                    select
                        distinct tbl as target_table_id,
                        sti.schema as target_schema,
                        sti.table as target_table,
                        sti.database as cluster,
                        query,
                        starttime
                    from
                        stl_insert
                    join SVV_TABLE_INFO sti on
                        sti.table_id = tbl
                    where starttime >= '{start_time}'
                    and starttime < '{end_time}'
                    and cluster = '{db_name}'
                        ) as target_tables
                join ( (
                    select
                        pu.usename::varchar(40) as username,
                        ss.tbl as source_table_id,
                        sti.schema as source_schema,
                        sti.table as source_table,
                        scan_type,
                        sq.query as query
                    from
                        (
                        select
                            distinct userid,
                            query,
                            tbl,
                            type as scan_type
                        from
                            stl_scan
                    ) ss
                    join SVV_TABLE_INFO sti on
                        sti.table_id = ss.tbl
                    left join pg_user pu on
                        pu.usesysid = ss.userid
                    left join stl_query sq on
                        ss.query = sq.query
                    where
                        pu.usename <> 'rdsdb')
                ) as source_tables
                        using (query)
                where
                    scan_type in (1, 2, 3)
                order by cluster, target_schema, target_table, starttime asc
            """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )
        view_lineage_query = """
                select
                    distinct
                    srcnsp.nspname as source_schema
                    ,
                    srcobj.relname as source_table
                    ,
                    tgtnsp.nspname as target_schema
                    ,
                    tgtobj.relname as target_table
                from
                    pg_catalog.pg_class as srcobj
                inner join
                    pg_catalog.pg_depend as srcdep
                        on
                    srcobj.oid = srcdep.refobjid
                inner join
                    pg_catalog.pg_depend as tgtdep
                        on
                    srcdep.objid = tgtdep.objid
                join
                    pg_catalog.pg_class as tgtobj
                        on
                    tgtdep.refobjid = tgtobj.oid
                    and srcobj.oid <> tgtobj.oid
                left outer join
                    pg_catalog.pg_namespace as srcnsp
                        on
                    srcobj.relnamespace = srcnsp.oid
                left outer join
                    pg_catalog.pg_namespace tgtnsp
                        on
                    tgtobj.relnamespace = tgtnsp.oid
                where
                    tgtdep.deptype = 'i'
                    --dependency_internal
                    and tgtobj.relkind = 'v'
                    --i=index, v=view, s=sequence
                    and tgtnsp.nspname not in ('pg_catalog', 'information_schema')
                    order by target_schema, target_table asc
            """

        list_late_binding_views_query = """
            SELECT
                n.nspname AS target_schema
                ,c.relname AS target_table
                , COALESCE(pg_get_viewdef(c.oid, TRUE), '') AS ddl
            FROM
                pg_catalog.pg_class AS c
            INNER JOIN
                pg_catalog.pg_namespace AS n
                ON c.relnamespace = n.oid
            WHERE relkind = 'v'
--            and ddl like '%%with no schema binding%%'
            and
            n.nspname not in ('pg_catalog', 'information_schema')
            """

        list_insert_create_queries_sql = """
            select
                distinct cluster,
                target_schema,
                target_table,
                username,
                querytxt as ddl
            from
                    (
                select
                    distinct tbl as target_table_id,
                    sti.schema as target_schema,
                    sti.table as target_table,
                    sti.database as cluster,
                    usename as username,
                    querytxt,
                    si.starttime as starttime
                from
                    stl_insert as si
                join SVV_TABLE_INFO sti on
                    sti.table_id = tbl
                left join pg_user pu on
                    pu.usesysid = si.userid
                left join stl_query sq on
                    si.query = sq.query
                left join stl_load_commits slc on
                    slc.query = si.query
                where
                    pu.usename <> 'rdsdb'
                    and sq.aborted = 0
                    and slc.query IS NULL
                    and cluster = '{db_name}'
                    and si.starttime >= '{start_time}'
                    and si.starttime < '{end_time}'
                ) as target_tables
                order by cluster, target_schema, target_table, starttime asc
            """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )

        list_copy_commands_sql = """
            select
                distinct
                    "schema" as target_schema,
                    "table" as target_table,
                    filename
            from
                stl_insert as si
            join stl_load_commits as c on
                si.query = c.query
            join SVV_TABLE_INFO sti on
                sti.table_id = tbl
            where
                database = '{db_name}'
                and si.starttime >= '{start_time}'
                and si.starttime < '{end_time}'
            order by target_schema, target_table, starttime asc
            """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )

        if not self._lineage_map:
            self._lineage_map = defaultdict()

        if self.config.table_lineage_mode == LineageMode.STL_SCAN_BASED:
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            self._populate_lineage_map(
                query=stl_scan_based_lineage_query,
                lineage_type=LineageCollectorType.QUERY_SCAN,
                connection=connection,
                all_tables=all_tables,
            )
        elif self.config.table_lineage_mode == LineageMode.SQL_BASED:
            # Populate table level lineage by parsing table creating sqls
            self._populate_lineage_map(
                query=list_insert_create_queries_sql,
                lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
                connection=connection,
                all_tables=all_tables,
            )
        elif self.config.table_lineage_mode == LineageMode.MIXED:
            # Populate table level lineage by parsing table creating sqls
            self._populate_lineage_map(
                query=list_insert_create_queries_sql,
                lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
                connection=connection,
                all_tables=all_tables,
            )
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            self._populate_lineage_map(
                query=stl_scan_based_lineage_query,
                lineage_type=LineageCollectorType.QUERY_SCAN,
                connection=connection,
                all_tables=all_tables,
            )

        if self.config.include_views:
            # Populate table level lineage for views
            self._populate_lineage_map(
                query=view_lineage_query,
                lineage_type=LineageCollectorType.VIEW,
                connection=connection,
                all_tables=all_tables,
            )

            # Populate table level lineage for late binding views
            self._populate_lineage_map(
                query=list_late_binding_views_query,
                lineage_type=LineageCollectorType.NON_BINDING_VIEW,
                connection=connection,
                all_tables=all_tables,
            )
        if self.config.include_copy_lineage:
            self._populate_lineage_map(
                query=list_copy_commands_sql,
                lineage_type=LineageCollectorType.COPY,
                connection=connection,
                all_tables=all_tables,
            )

        self.report.lineage_mem_size[self.config.database] = humanfriendly.format_size(
            memory_footprint.total_size(self._lineage_map)
        )

    def get_lineage_mcp(
        self,
        table: Union[RedshiftTable, RedshiftView],
        dataset_urn: str,
        schema: RedshiftSchema,
    ) -> Optional[MetadataChangeProposalWrapper]:
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
                    "{database}.{table}".format(
                        database=schema.external_database,
                        table=tablename,
                    ),
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

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspectName="upstreamLineage",
            aspect=UpstreamLineage(upstreams=upstream_lineage),
        )

        return mcp

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
                    "{database}.{table}".format(
                        database=schema.external_database,
                        table=tablename,
                    ),
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

        return (UpstreamLineage(upstreams=upstream_lineage), {})

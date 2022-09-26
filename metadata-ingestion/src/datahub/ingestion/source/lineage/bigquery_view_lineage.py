import atexit
import collections
import sqlalchemy_bigquery
import os
import functools
import logging
from typing import Iterable, Tuple, Any, Optional, Dict, Set
from sqlalchemy.engine.reflection import Inspector

from unittest.mock import patch
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    BigQueryDatasetKey,
    PlatformKey,
    ProjectIdKey,
    gen_containers,
)

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.usage.bigquery_usage import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    BigQueryTableRef,
    QueryEvent,
)

from datahub.ingestion.source_config.sql.bigquery import BigQueryConfig
from datahub.ingestion.source_report.sql.bigquery import BigQueryReport
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import RecordTypeClass
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

from datahub.utilities.bigquery_sql_parser import BigQuerySQLParser

logger = logging.getLogger(__name__)

def cleanup(config: BigQueryConfig) -> None:
    if config._credentials_path is not None:
        logger.debug(
            f"Deleting temporary credential file at {config._credentials_path}"
        )
        os.unlink(config._credentials_path)

# Handle the GEOGRAPHY type. We will temporarily patch the _type_map
# in the get_workunits method of the source.
GEOGRAPHY = make_sqlalchemy_type("GEOGRAPHY")
register_custom_type(GEOGRAPHY)
assert sqlalchemy_bigquery._types._type_map
# STRUCT is a custom sqlalchemy data type defined by the sqlalchemy_bigquery library
# https://github.com/googleapis/python-bigquery-sqlalchemy/blob/934e25f705fd9f226e438d075c7e00e495cce04e/sqlalchemy_bigquery/_types.py#L47
register_custom_type(sqlalchemy_bigquery.STRUCT, output=RecordTypeClass)

def get_view_definition(self, connection, view_name, schema=None, **kw):
    view = self._get_table(connection, view_name, schema)
    return view.view_query

sqlalchemy_bigquery.BigQueryDialect.get_view_definition = get_view_definition

@config_class(BigQueryConfig)
@platform_name("BigQuery")
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class BigQueryViewLineageSource(SQLAlchemySource):
    """
    This plugin extracts the following:
    * Table level lineage from views

    :::note
    This source only does lineage statistics from views. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` plugin.
    :::
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")
        self.config: BigQueryConfig = config
        self.report: BigQueryReport = BigQueryReport()
        self.view_lineage_metadata: Optional[Dict[str, Set[str]]] = {}
        self.table_map: Dict[str, str] = dict()
        atexit.register(cleanup, config)

    def get_multiproject_project_id(
        self, inspector: Optional[Inspector] = None, run_on_compute: bool = False
    ) -> Optional[str]:
        """
        Use run_on_compute = true when running queries on storage project
        where you don't have job create rights
        """
        if self.config.storage_project_id and (not run_on_compute):
            return self.config.storage_project_id
        elif self.config.project_id:
            return self.config.project_id
        else:
            if inspector:
                return self._get_project_id(inspector)
            else:
                return None

    def get_db_name(self, inspector: Inspector) -> str:
        """
        DO NOT USE this to get project name when running queries.
            That can cause problems with multi-project setups.
            Use get_multiproject_project_id with run_on_compute = True
        """
        db_name = self.get_multiproject_project_id(inspector)
        # db name can't be empty here as we pass in inpector to get_multiproject_project_id
        assert db_name
        return db_name

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigQueryViewLineageSource":
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    @functools.lru_cache()
    def _get_project_id(inspector: Inspector) -> str:
        with inspector.bind.connect() as connection:
            project_id = connection.connection._client.project
            return project_id

    def add_config_to_report(self):
        self.report.window_start_time = self.config.start_time
        self.report.window_end_time = self.config.end_time
        self.report.include_table_lineage = self.config.include_table_lineage
        self.report.use_date_sharded_audit_log_tables = (
            self.config.use_date_sharded_audit_log_tables
        )
        self.report.log_page_size = self.config.log_page_size
        self.report.use_exported_bigquery_audit_metadata = (
            self.config.use_exported_bigquery_audit_metadata
        )
        self.report.use_v2_audit_metadata = self.config.use_v2_audit_metadata

    def _is_table_allow(self, table_ref: Optional[BigQueryTableRef]) -> bool:
        return (
            table_ref is not None
            and self.config.dataset_pattern.allowed(table_ref.dataset)
            and self.config.table_pattern.allowed(table_ref.table)
        )

    def compute_big_query_view_lineage(
        self, table_short_name: str, view_definition: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        if view_definition is None or view_definition == "":
            logger.debug("No view definition so skipping getting mcp")
            return None
        view_query: str = "CREATE VIEW " + table_short_name + " AS\n" + view_definition
        # project_id, dataset_name, tablename = dataset_key.name.split(".")
        try:
            parser = BigQuerySQLParser(view_query)
            parser_tables = set(
                map(lambda x: ".".join(x.split(".")[-2:]), parser.get_tables())
            )
            if table_short_name not in self.view_lineage_metadata:
                self.view_lineage_metadata[table_short_name] = set()
            self.view_lineage_metadata[table_short_name].update(parser_tables)
        except Exception as ex:
            logger.warning(
                f"Sql Parser failed on query: {view_query}. It will be skipped from lineage. The error was {ex}"
            )
            self.report.num_skipped_lineage_entries_sql_parser_failure += 1
            return None

    def get_lineage_mcp(self) -> Optional[MetadataChangeProposalWrapper]:
        for table_short_name in self.view_lineage_metadata.keys():
            upstream_list: List[UpstreamClass] = []
            for upstream_table in self.view_lineage_metadata[table_short_name]:
                if upstream_table in self.table_map:
                    upstream_table_class = UpstreamClass(
                        self.table_map[upstream_table],
                        DatasetLineageTypeClass.TRANSFORMED,
                    )
                    if self.config.upstream_lineage_in_report:
                        current_lineage_maps: Set = self.report.upstream_lineage.get(
                            table_short_name, set()
                        )
                        current_lineage_map.add(str(upstream_table))
                        self.report.upstream_lineage[table_short_name] = current_lineage_map
                    upstream_list.append(upstream_table_class)
            if upstream_list:
                upstream_lineage = UpstreamLineageClass(upstreams=upstream_list)
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=self.table_map[table_short_name],
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                yield mcp

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.add_config_to_report()
        # _client: BigQueryClient = BigQueryClient(project=project_id)
        with patch.dict(
            "sqlalchemy_bigquery._types._type_map",
            {"GEOGRAPHY": GEOGRAPHY},
            clear=False,
        ):
            for wu in super().get_workunits():
                yield wu
                if (
                    isinstance(wu, SqlWorkUnit)
                    and isinstance(wu.metadata, MetadataChangeEvent)
                    and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
                    and self.config.include_table_lineage
                ):
                    dataset_key: Optional[DatasetKey] = mce_builder.dataset_urn_to_key(wu.metadata.proposedSnapshot.urn)
                    if dataset_key is None:
                        continue
                    project_id, dataset_name, tablename = dataset_key.name.split(".")
                    table_short_name = f"{dataset_name}.{tablename}".lower()
                    if table_short_name not in self.table_map:
                        self.table_map[table_short_name] = wu.metadata.proposedSnapshot.urn
                    for aspect in wu.metadata.proposedSnapshot.aspects:
                        if (
                            isinstance(aspect, DatasetPropertiesClass)
                            and 'view_definition' in aspect.customProperties
                        ):
                            self.compute_big_query_view_lineage(table_short_name, aspect.customProperties['view_definition'])
            for lineage_mcp in self.get_lineage_mcp():
                lineage_wu = MetadataWorkUnit(
                    id=f"{self.platform}-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}",
                    mcp=lineage_mcp,
                )
                yield lineage_wu
                self.report.report_workunit(lineage_wu)

    def normalise_dataset_name(self, dataset_name: str) -> str:
        (project_id, schema, table) = dataset_name.split(".")

        trimmed_table_name = (
            BigQueryTableRef.from_spec_obj(
                {"projectId": project_id, "datasetId": schema, "tableId": table}
            )
            .remove_extras(self.config.sharded_table_pattern)
            .table
        )
        return f"{project_id}.{schema}.{trimmed_table_name}"

    def get_identifier(
        self,
        *,
        schema: str,
        entity: str,
        inspector: Inspector,
        **kwargs: Any,
    ) -> str:
        assert inspector
        project_id = self._get_project_id(inspector)
        table_name = BigQueryTableRef.from_spec_obj(
            {"projectId": project_id, "datasetId": schema, "tableId": entity}
        ).table
        return f"{project_id}.{schema}.{table_name}"

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # The get_table_names() method of the BigQuery driver returns table names
        # formatted as "<schema>.<table>" as the table name. Since later calls
        # pass both schema and table, schema essentially is passed in twice. As
        # such, one of the schema names is incorrectly interpreted as the
        # project ID. By removing the schema from the table name, we avoid this
        # issue.
        segments = entity.split(".")
        if len(segments) != 2:
            raise ValueError(f"expected table to contain schema name already {entity}")
        if segments[0] != schema:
            raise ValueError(f"schema {schema} does not match table {entity}")
        return segments[0], segments[1]

    def gen_schema_key(self, db_name: str, schema: str) -> PlatformKey:
        return BigQueryDatasetKey(
            project_id=db_name,
            dataset_id=schema,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_database_key(self, database: str) -> PlatformKey:
        return ProjectIdKey(
            project_id=database,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_database_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_database_key(database)

        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=["Project"],
            domain_urn=domain_urn,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_schema_key(db_name, schema)

        database_container_key = self.gen_database_key(database=db_name)

        container_workunits = gen_containers(
            schema_container_key,
            schema,
            ["Dataset"],
            database_container_key,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

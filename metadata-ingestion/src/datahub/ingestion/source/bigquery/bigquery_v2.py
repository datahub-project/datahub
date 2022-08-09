import atexit
import logging
import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, cast

import pydantic
from avrogen.dict_wrapper import DictWrapper
from google.cloud import bigquery

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    BigQueryDatasetKey,
    PlatformKey,
    ProjectIdKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.source import (
    CapabilityReport,
    Source,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery.bigquery_schema import (
    BigqueryColumn,
    BigQueryDataDictionary,
    BigqueryDataset,
    BigqueryProject,
    BigqueryTable,
    BigqueryView,
)
from datahub.ingestion.source.bigquery.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery.usage import BigQueryUsageExtractor
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetProperties,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanType,
    BytesType,
    MySqlDDL,
    NullType,
    NumberType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.utilities.mapping import Constants
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)

# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
BIGQUERY_FIELD_TYPE_MAPPINGS: dict[
    str, Type[Union[BytesType, BooleanType, NumberType, StringType, TimeType, NullType]]
] = {
    "BYTES": BytesType,
    "BOOL": BooleanType,
    "DECIMAL": NumberType,
    "NUMERIC": NumberType,
    "BIGNUMERIC": NumberType,
    "BIGDECIMAL": NumberType,
    "FLOAT64": NumberType,
    "INT": NumberType,
    "INT64": NumberType,
    "SMALLINT": NumberType,
    "INTEGER": NumberType,
    "BIGINT": NumberType,
    "TINYINT": NumberType,
    "BYTEINT": NumberType,
    "STRING": StringType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "DATE": TimeType,
    "DATETIME": TimeType,
    "GEOGRAPHY": NullType,
    "JSON": NullType,
    "INTERVAL": NullType,
}
# Handle table snapshots
# See https://cloud.google.com/bigquery/docs/table-snapshots-intro.
SNAPSHOT_TABLE_REGEX = re.compile(r"^(.+)@(\d{13})$")


def cleanup(config: BigQueryV2Config) -> None:
    if config._credentials_path is not None:
        logger.debug(
            f"Deleting temporary credential file at {config._credentials_path}"
        )
        os.unlink(config._credentials_path)


@platform_name("Bigquery")
@config_class(BigQueryV2Config)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration, only table level profiling is supported",
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
    supported=True,
)
class BigqueryV2Source(StatefulIngestionSourceBase, TestableSource):
    def __init__(self, ctx: PipelineContext, config: BigQueryV2Config):
        super(BigqueryV2Source, self).__init__(config, ctx)
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = BigQueryV2Report()
        self.platform: str = "bigquery"

        # For database, schema, tables, views, etc
        self.data_dictionary = BigQueryDataDictionary()
        self.lineage_extractor = BigqueryLineageExtractor(config, self.report)
        self.usage_extractor = BigQueryUsageExtractor(config, self.report)
        # Currently caching using instance variables
        # TODO - rewrite cache for readability or use out of the box solution
        self.db_tables: Dict[str, Optional[Dict[str, List[BigqueryTable]]]] = {}
        self.db_views: Dict[str, Optional[Dict[str, List[BigqueryView]]]] = {}

        # For column related queries and constraints, we currently query at schema level
        # In future, we may consider using queries and caching at database level first
        self.schema_columns: Dict[
            Tuple[str, str], Optional[Dict[str, List[BigqueryColumn]]]
        ] = {}

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        atexit.register(cleanup, config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = BigQueryV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()

        try:
            BigQueryV2Config.Config.extra = (
                pydantic.Extra.allow
            )  # we are okay with extra fields during this stage

            client: bigquery.Client = bigquery.Client()
            assert client

            ret = client.query("select 1")
            if ret.error_result:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=f"{ret.error_result['message']}"
                )
            else:
                test_report.basic_connectivity = CapabilityReport(capable=True)

            return test_report

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{e}"
            )
            return test_report

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id == self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that sql_common provides.
        Subclasses can override as needed.
        """
        return JobId("common_ingest_from_sql_source")

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create the custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=BaseSQLAlchemyCheckpointState(),
            )
        return None

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = MetadataWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        config_dict = self.config.dict()
        host_port = config_dict.get("host_port", "no_host_port")
        database = config_dict.get("database", "no_database")
        return f"{self.platform}_{host_port}_{database}"

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseSQLAlchemyCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            def soft_delete_item(urn: str, type: str) -> Iterable[MetadataWorkUnit]:
                entity_type: str = "dataset"

                if type == "container":
                    entity_type = "container"

                logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
                mcp = MetadataChangeProposalWrapper(
                    entityType=entity_type,
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=StatusClass(removed=True),
                )
                wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
                self.report.report_workunit(wu)
                self.report.report_stale_entity_soft_deleted(urn)
                yield wu

            last_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, last_checkpoint.state
            )
            cur_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, cur_checkpoint.state
            )

            for table_urn in last_checkpoint_state.get_table_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(table_urn, "table")

            for view_urn in last_checkpoint_state.get_view_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(view_urn, "view")

            for container_urn in last_checkpoint_state.get_container_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(container_urn, "container")

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

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        return domain_urn

    def gen_database_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_database_key(database)

        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=["Project"],
            domain_urn=domain_urn,
        )

        if self.is_stateful_ingestion_configured():
            cur_checkpoint = self.get_current_checkpoint(
                self.get_default_ingestion_job_id()
            )
            if cur_checkpoint is not None:
                checkpoint_state = cast(
                    BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                )
                checkpoint_state.add_container_guid(
                    make_container_urn(
                        guid=database_container_key.guid(),
                    )
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

        if self.is_stateful_ingestion_configured():
            cur_checkpoint = self.get_current_checkpoint(
                self.get_default_ingestion_job_id()
            )
            if cur_checkpoint is not None:
                checkpoint_state = cast(
                    BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                )
                checkpoint_state.add_container_guid(
                    make_container_urn(
                        guid=schema_container_key.guid(),
                    )
                )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def add_table_to_schema_container(
        self, dataset_urn: str, db_name: str, schema: str
    ) -> Iterable[Union[MetadataWorkUnit]]:
        schema_container_key = self.gen_schema_key(db_name, schema)
        container_workunits = add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def get_workunits(self) -> Iterable[WorkUnit]:

        conn: bigquery.Client = bigquery.Client()
        self.add_config_to_report()

        projects: List[BigqueryProject] = self.data_dictionary.get_project_ids(conn)
        for project_id in projects:
            if not self.config.project_id_pattern.allowed(project_id.id):
                self.report.report_dropped(project_id.id)
                continue

            yield from self._process_database(conn, project_id)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.report_usage_stat()

        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    def _process_database(
        self, conn: bigquery.Client, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        project_id = bigquery_project.id

        database_workunits = self.gen_database_containers(project_id)

        for wu in database_workunits:
            self.report.report_workunit(wu)
            yield wu

        bigquery_project.datasets = self.data_dictionary.get_datasets_for_project_id(
            conn, project_id
        )

        for bigquery_dataset in bigquery_project.datasets:

            if not self.config.schema_pattern.allowed(bigquery_dataset.name):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                continue

            yield from self._process_schema(conn, bigquery_dataset, project_id)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.generate_usage_for_project(project_id)

    def _get_shard_from_table(self, table: str) -> Tuple[str, Optional[str]]:
        match = re.search(self.config.sharded_table_pattern, table, re.IGNORECASE)
        if match:
            table_name = match.group(2)
            shard = match.group(3)
            return table_name, shard
        return table, None

    def _process_schema(
        self, conn: bigquery.Client, bigquery_dataset: BigqueryDataset, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = bigquery_dataset.name
        schema_workunits = self.gen_schema_containers(
            dataset_name,
            project_id,
        )

        for wu in schema_workunits:
            self.report.report_workunit(wu)
            yield wu

        if self.config.include_tables:
            bigquery_dataset.tables = self.get_tables_for_dataset(
                conn, dataset_name, project_id
            )
            last_table_identifier: Optional[str] = None
            for table in bigquery_dataset.tables:
                table_identifier = self.get_identifier(
                    table.name, dataset_name, project_id
                )
                if last_table_identifier and last_table_identifier == table_identifier:
                    logger.debug(
                        f"Skipping table {table.name} with identifier {last_table_identifier} as we already processed this table"
                    )
                    continue
                else:
                    last_table_identifier = table_identifier

                if table_identifier.startswith(self.config.temp_table_dataset_prefix):
                    logger.debug(f"Dropping temporary table {table_identifier}")
                    self.report.report_dropped(table_identifier)
                    continue

                yield from self._process_table(conn, table, dataset_name, project_id)

        if self.config.include_views:
            bigquery_dataset.views = self.get_views_for_dataset(
                conn, dataset_name, project_id
            )

            for view in bigquery_dataset.views:
                yield from self._process_view(conn, view, dataset_name, project_id)

    def _process_table(
        self,
        conn: bigquery.Client,
        table: BigqueryTable,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = self.get_identifier(table.name, schema_name, db_name)

        self.report.report_entity_scanned(table_identifier)

        if not self.config.table_pattern.allowed(table_identifier):
            self.report.report_dropped(table_identifier)
            return

        table.columns = self.get_columns_for_table(
            conn, table.name, schema_name, db_name
        )
        dataset_name = self.get_identifier(table.name, schema_name, db_name)

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None

        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                dataset_name, self.platform
            )

        table_workunits = self.gen_dataset_workunits(
            table, schema_name, db_name, lineage_info
        )
        for wu in table_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _process_view(
        self,
        conn: bigquery.Client,
        view: BigqueryView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:

        table_identifier = self.get_identifier(view.name, schema_name, db_name)

        self.report.report_entity_scanned(table_identifier, "view")

        if not self.config.view_pattern.allowed(table_identifier):
            self.report.report_dropped(table_identifier)
            return

        view.columns = self.get_columns_for_table(conn, view.name, schema_name, db_name)
        dataset_name = self.get_identifier(view.name, schema_name, db_name)

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None
        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                dataset_name, self.platform
            )

        view_workunits = self.gen_dataset_workunits(
            view, schema_name, db_name, lineage_info
        )
        for wu in view_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _get_domain_wu(
        self,
        dataset_name: str,
        entity_urn: str,
        entity_type: str,
    ) -> Iterable[MetadataWorkUnit]:

        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type=entity_type,
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def gen_dataset_workunits(
        self,
        table: Union[BigqueryTable, BigqueryView],
        dataset_name: str,
        project_id: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]],
    ) -> Iterable[MetadataWorkUnit]:
        datahub_dataset_name = self.get_identifier(table.name, dataset_name, project_id)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            datahub_dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        if lineage_info is not None:
            upstream_lineage, upstream_column_props = lineage_info
        else:
            upstream_column_props = {}
            upstream_lineage = None

        status = Status(removed=False)
        yield self.wrap_aspect_as_workunit("dataset", dataset_urn, "status", status)

        schema_metadata = self.get_schema_metadata(table, datahub_dataset_name)
        yield self.wrap_aspect_as_workunit(
            "dataset", dataset_urn, "schemaMetadata", schema_metadata
        )

        dataset_properties = DatasetProperties(
            name=datahub_dataset_name,
            description=table.comment,
            qualifiedName=datahub_dataset_name,
            customProperties={**upstream_column_props},
        )
        yield self.wrap_aspect_as_workunit(
            "dataset", dataset_urn, "datasetProperties", dataset_properties
        )

        yield from self.add_table_to_schema_container(
            dataset_urn,
            project_id,
            dataset_name,
        )
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        subTypes = SubTypes(
            typeNames=["view"] if isinstance(table, BigqueryView) else ["table"]
        )
        yield self.wrap_aspect_as_workunit("dataset", dataset_urn, "subTypes", subTypes)

        yield from self._get_domain_wu(
            dataset_name=datahub_dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
        )

        if upstream_lineage is not None:
            # Emit the lineage work unit
            yield self.wrap_aspect_as_workunit(
                "dataset", dataset_urn, "upstreamLineage", upstream_lineage
            )

        if isinstance(table, BigqueryTable) and self.config.profiling.enabled:
            if self.config.profiling.allow_deny_patterns.allowed(datahub_dataset_name):
                # Emit the profile work unit
                dataset_profile = DatasetProfile(
                    timestampMillis=round(datetime.now().timestamp() * 1000),
                    columnCount=len(table.columns),
                    rowCount=table.rows_count,
                )
                self.report.report_entity_profiled(datahub_dataset_name)
                yield self.wrap_aspect_as_workunit(
                    "dataset", dataset_urn, "datasetProfile", dataset_profile
                )

            else:
                self.report.report_dropped(f"Profile for {datahub_dataset_name}")

        if isinstance(table, BigqueryView):
            view = cast(BigqueryView, table)
            view_definition_string = view.ddl
            view_properties_aspect = ViewProperties(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            yield self.wrap_aspect_as_workunit(
                "dataset", dataset_urn, "viewProperties", view_properties_aspect
            )

        if self.is_stateful_ingestion_configured():
            cur_checkpoint = self.get_current_checkpoint(
                self.get_default_ingestion_job_id()
            )
            if cur_checkpoint is not None:
                checkpoint_state = cast(
                    BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                )
                if isinstance(table, BigqueryTable):
                    checkpoint_state.add_table_urn(dataset_urn)
                else:
                    checkpoint_state.add_view_urn(dataset_urn)

    def get_schema_metadata(
        self,
        table: Union[BigqueryTable, BigqueryView],
        dataset_name: str,
    ) -> SchemaMetadata:
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=[
                SchemaField(
                    fieldPath=col.name,
                    type=SchemaFieldDataType(
                        BIGQUERY_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
                    ),
                    # NOTE: nativeDataType will not be in sync with older connector
                    nativeDataType=col.data_type,
                    description=col.comment,
                    nullable=col.is_nullable,
                    globalTags=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                make_tag_urn(Constants.TAG_PARTITION_KEY)
                            )
                        ]
                    )
                    if col.is_partition_column
                    else GlobalTagsClass(tags=[]),
                )
                for col in table.columns
            ],
        )
        return schema_metadata

    def get_identifier(
        self, table_name: str, dataset_name: str, project_id: str
    ) -> str:
        table_name, _ = self._get_shard_from_table(table_name)
        if not table_name:
            table_name = dataset_name

        matches = SNAPSHOT_TABLE_REGEX.match(table_name)
        if matches:
            table_name = matches.group(1)
            logger.debug(f"Found table snapshot. Using {table_name} as the table name.")

        # Handle exceptions
        invalid_chars_in_table_name: List[str] = [
            c for c in {"$", "@"} if c in table_name
        ]
        if invalid_chars_in_table_name:
            raise ValueError(
                f"Cannot handle {self} - poorly formatted table name, contains {invalid_chars_in_table_name}"
            )

        return f"{project_id}.{dataset_name}.{table_name}"

    def get_report(self) -> SourceReport:
        return self.report

    def get_tables_for_dataset(
        self, conn: bigquery.Client, dataset_name: str, project_id: str
    ) -> List[BigqueryTable]:

        tables = self.db_tables.get(dataset_name)

        # In bigquery there is no way to query all tables in a Project id
        if tables is None:
            return self.data_dictionary.get_tables_for_dataset(
                conn, dataset_name, project_id
            )

        # Some schema may not have any table
        return tables.get(dataset_name, [])

    def get_views_for_dataset(
        self, conn: bigquery.Client, dataset_name: str, project_id: str
    ) -> List[BigqueryView]:

        views = self.db_views.get(project_id)

        # get all views for database failed,
        # falling back to get views for schema
        if views is None:
            return self.data_dictionary.get_views_for_dataset(
                conn, dataset_name, project_id
            )

        # Some schema may not have any table
        return views.get(dataset_name, [])

    def get_columns_for_table(
        self, conn: bigquery.Client, table_name: str, dataset_name: str, project_id: str
    ) -> List[BigqueryColumn]:

        if (project_id, dataset_name) not in self.schema_columns.keys():
            columns = self.data_dictionary.get_columns_for_dataset(
                conn, dataset_name, project_id
            )
            self.schema_columns[(project_id, dataset_name)] = columns
        else:
            columns = self.schema_columns[(project_id, dataset_name)]

        # get all columns for schema failed,
        # falling back to get columns for table
        if columns is None:
            return self.data_dictionary.get_columns_for_table(
                conn, table_name, dataset_name, project_id
            )

        # Access to table but none of its columns - is this possible ?
        return columns.get(table_name, [])

    def add_config_to_report(self):
        return

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def wrap_aspect_as_workunit(
        self, entityName: str, entityUrn: str, aspectName: str, aspect: DictWrapper
    ) -> MetadataWorkUnit:
        wu = MetadataWorkUnit(
            id=f"{aspectName}-for-{entityUrn}",
            mcp=MetadataChangeProposalWrapper(
                entityType=entityName,
                entityUrn=entityUrn,
                aspectName=aspectName,
                aspect=aspect,
                changeType=ChangeType.UPSERT,
            ),
        )
        self.report.report_workunit(wu)
        return wu

    def close(self) -> None:
        self.prepare_for_commit()

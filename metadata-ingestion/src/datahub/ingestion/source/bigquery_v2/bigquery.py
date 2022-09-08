import atexit
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, cast

import pydantic
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
    wrap_aspect_as_workunit,
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
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigQueryDataDictionary,
    BigqueryDataset,
    BigqueryProject,
    BigqueryTable,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
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

# Handle table snapshots
# See https://cloud.google.com/bigquery/docs/table-snapshots-intro.
SNAPSHOT_TABLE_REGEX = re.compile(r"^(.+)@(\d{13})$")


# We can't use close as it is not called if the ingestion is not successful
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
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    BIGQUERY_FIELD_TYPE_MAPPINGS: Dict[
        str,
        Type[Union[BytesType, BooleanType, NumberType, StringType, TimeType, NullType]],
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

    def __init__(self, ctx: PipelineContext, config: BigQueryV2Config):
        super(BigqueryV2Source, self).__init__(config, ctx)
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = BigQueryV2Report()
        self.platform: str = "bigquery"

        # For database, schema, tables, views, etc
        self.lineage_extractor = BigqueryLineageExtractor(config, self.report)
        self.usage_extractor = BigQueryUsageExtractor(config, self.report)
        # Currently caching using instance variables
        # TODO - rewrite cache for readability or use out of the box solution
        self.db_tables: Dict[str, Optional[Dict[str, List[BigqueryTable]]]] = {}
        self.db_views: Dict[str, Optional[Dict[str, List[BigqueryView]]]] = {}

        self.schema_columns: Dict[
            Tuple[str, str], Optional[Dict[str, List[BigqueryColumn]]]
        ] = {}

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        atexit.register(cleanup, config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigqueryV2Source":
        config = BigQueryV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = self.config.extra_client_options
        return bigquery.Client(**client_options)

    @staticmethod
    def connectivity_test(client: bigquery.Client) -> CapabilityReport:
        ret = client.query("select 1")
        if ret.error_result:
            return CapabilityReport(
                capable=False, failure_reason=f"{ret.error_result['message']}"
            )
        else:
            return CapabilityReport(capable=True)

    @staticmethod
    def lineage_capability_test(
        connection_conf: BigQueryV2Config,
        project_ids: List[str],
        report: BigQueryV2Report,
    ) -> CapabilityReport:
        lineage_extractor = BigqueryLineageExtractor(connection_conf, report)
        for project_id in project_ids:
            try:
                logger.info((f"Lineage capability test for project {project_id}"))
                lineage_extractor.test_capability(project_id)
            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Lineage capability test failed with: {e}",
                )

        return CapabilityReport(capable=True)

    @staticmethod
    def usage_capability_test(
        connection_conf: BigQueryV2Config,
        project_ids: List[str],
        report: BigQueryV2Report,
    ) -> CapabilityReport:
        usage_extractor = BigQueryUsageExtractor(connection_conf, report)
        for project_id in project_ids:
            try:
                logger.info((f"Usage capability test for project {project_id}"))
                usage_extractor.test_capability(project_id)
            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Usage capability test failed with: {e} for project {project_id}",
                )
        return CapabilityReport(capable=True)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        _report: Dict[Union[SourceCapability, str], CapabilityReport] = dict()

        try:
            BigQueryV2Config.Config.extra = (
                pydantic.Extra.allow
            )  # we are okay with extra fields during this stage
            connection_conf = BigQueryV2Config.parse_obj(config_dict)
            client: bigquery.Client = bigquery.Client()
            assert client

            test_report.basic_connectivity = BigqueryV2Source.connectivity_test(client)

            connection_conf.start_time = datetime.now()
            connection_conf.end_time = datetime.now() + timedelta(minutes=1)

            report: BigQueryV2Report = BigQueryV2Report()
            project_ids: List[str] = []
            projects = client.list_projects()

            for project in projects:
                if connection_conf.project_id_pattern.allowed(project.project_id):
                    project_ids.append(project.project_id)

            lineage_capability = BigqueryV2Source.lineage_capability_test(
                connection_conf, project_ids, report
            )
            if SourceCapability.LINEAGE_COARSE not in _report:
                _report[SourceCapability.LINEAGE_COARSE] = lineage_capability

            usage_capability = BigqueryV2Source.usage_capability_test(
                connection_conf, project_ids, report
            )
            if SourceCapability.USAGE_STATS not in _report:
                _report[SourceCapability.USAGE_STATS] = usage_capability

            test_report.capability_report = _report
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
        Keeping the same job_id as for the old source
        """
        return JobId("ingest_from_bigquery_source")

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
            aspect = DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )

            return wrap_aspect_as_workunit(
                "dataset", dataset_urn, "dataPlatformInstance", aspect
            )
        else:
            return None

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        return f"{self.platform}"

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
                entity_type: str = type if type == "container" else "dataset"

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

    def gen_dataset_key(self, db_name: str, schema: str) -> PlatformKey:
        return BigQueryDatasetKey(
            project_id=db_name,
            dataset_id=schema,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_project_id_key(self, database: str) -> PlatformKey:
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

    def gen_project_id_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_project_id_key(database)

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

    def gen_dataset_containers(
        self, dataset: str, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_dataset_key(project_id, dataset)

        database_container_key = self.gen_project_id_key(database=project_id)

        container_workunits = gen_containers(
            schema_container_key,
            dataset,
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

    def add_table_to_dataset_container(
        self, dataset_urn: str, db_name: str, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_dataset_key(db_name, schema)
        container_workunits = add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def get_workunits(self) -> Iterable[WorkUnit]:

        conn: bigquery.Client = self.get_bigquery_client()
        self.add_config_to_report()

        projects: List[BigqueryProject] = BigQueryDataDictionary.get_projects(conn)
        for project_id in projects:
            if not self.config.project_id_pattern.allowed(project_id.id):
                self.report.report_dropped(project_id.id)
                continue

            yield from self._process_project(conn, project_id)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.report_usage_stat()

        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    def _process_project(
        self, conn: bigquery.Client, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        project_id = bigquery_project.id

        database_workunits = self.gen_project_id_containers(project_id)

        for wu in database_workunits:
            self.report.report_workunit(wu)
            yield wu

        bigquery_project.datasets = BigQueryDataDictionary.get_datasets_for_project_id(
            conn, project_id
        )

        for bigquery_dataset in bigquery_project.datasets:

            if not self.config.dataset_pattern.allowed(bigquery_dataset.name):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                continue

            yield from self._process_schema(conn, project_id, bigquery_dataset)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.generate_usage_for_project(project_id)

    def _process_schema(
        self, conn: bigquery.Client, project_id: str, bigquery_dataset: BigqueryDataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = bigquery_dataset.name
        schema_workunits = self.gen_dataset_containers(
            dataset_name,
            project_id,
        )

        for wu in schema_workunits:
            self.report.report_workunit(wu)
            yield wu

        if self.config.include_tables:
            bigquery_dataset.tables = self.get_tables_for_dataset(
                conn, project_id, dataset_name
            )
            last_table_identifier: Optional[BigqueryTableIdentifier] = None
            for table in bigquery_dataset.tables:
                table_identifier = BigqueryTableIdentifier(
                    project_id, dataset_name, table.name
                )
                if (
                    last_table_identifier
                    and last_table_identifier.get_table_name()
                    == table_identifier.get_table_name()
                ):
                    logger.debug(
                        f"Skipping table {table.name} with identifier {last_table_identifier} as we already processed this table"
                    )
                    continue
                else:
                    last_table_identifier = table_identifier

                if str(table_identifier).startswith(
                    self.config.temp_table_dataset_prefix
                ):
                    logger.debug(f"Dropping temporary table {table_identifier}")
                    self.report.report_dropped(table_identifier.raw_table_name())
                    continue

                yield from self._process_table(conn, table, project_id, dataset_name)

        if self.config.include_views:
            bigquery_dataset.views = self.get_views_for_dataset(
                conn, project_id, dataset_name
            )

            for view in bigquery_dataset.views:
                yield from self._process_view(conn, view, project_id, dataset_name)

    def _process_table(
        self,
        conn: bigquery.Client,
        table: BigqueryTable,
        project_id: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, schema_name, table.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name())

        if not self.config.table_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        table.columns = self.get_columns_for_table(conn, table_identifier)

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None

        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                table_identifier, self.platform
            )

        table_workunits = self.gen_dataset_workunits(
            table, project_id, schema_name, lineage_info
        )
        for wu in table_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _process_view(
        self,
        conn: bigquery.Client,
        view: BigqueryView,
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:

        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, view.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name(), "view")

        if not self.config.view_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        view.columns = self.get_columns_for_table(conn, table_identifier)

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None
        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                table_identifier, self.platform
            )

        view_workunits = self.gen_dataset_workunits(
            view, project_id, dataset_name, lineage_info
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
        project_id: str,
        dataset_name: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]],
    ) -> Iterable[MetadataWorkUnit]:
        datahub_dataset_name = BigqueryTableIdentifier(
            project_id, dataset_name, table.name
        )
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            str(datahub_dataset_name),
            self.config.platform_instance,
            self.config.env,
        )
        if lineage_info is not None:
            upstream_lineage, upstream_column_props = lineage_info
        else:
            upstream_column_props = {}
            upstream_lineage = None

        status = Status(removed=False)
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "status", status)
        yield wu
        self.report.report_workunit(wu)

        schema_metadata = self.get_schema_metadata(table, str(datahub_dataset_name))
        wu = wrap_aspect_as_workunit(
            "dataset", dataset_urn, "schemaMetadata", schema_metadata
        )
        yield wu
        self.report.report_workunit(wu)

        dataset_properties = DatasetProperties(
            name=str(datahub_dataset_name),
            description=table.comment,
            qualifiedName=str(datahub_dataset_name),
            customProperties={**upstream_column_props},
        )
        wu = wrap_aspect_as_workunit(
            "dataset", dataset_urn, "datasetProperties", dataset_properties
        )
        yield wu
        self.report.report_workunit(wu)

        yield from self.add_table_to_dataset_container(
            dataset_urn,
            project_id,
            dataset_name,
        )
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            self.report.report_workunit(dpi_aspect)
            yield dpi_aspect

        subTypes = SubTypes(
            typeNames=["view"] if isinstance(table, BigqueryView) else ["table"]
        )
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "subTypes", subTypes)
        yield wu
        self.report.report_workunit(wu)

        yield from self._get_domain_wu(
            dataset_name=str(datahub_dataset_name),
            entity_urn=dataset_urn,
            entity_type="dataset",
        )

        if upstream_lineage is not None:
            # Emit the lineage work unit
            wu = wrap_aspect_as_workunit(
                "dataset", dataset_urn, "upstreamLineage", upstream_lineage
            )
            yield wu
            self.report.report_workunit(wu)

        if isinstance(table, BigqueryTable) and self.config.profiling.enabled:
            if self.config.profiling._allow_deny_patterns.allowed(
                datahub_dataset_name.raw_table_name()
            ):
                # Emit the profile work unit
                dataset_profile = DatasetProfile(
                    timestampMillis=round(datetime.now().timestamp() * 1000),
                    columnCount=len(table.columns),
                    rowCount=table.rows_count,
                )
                self.report.report_entity_profiled(str(datahub_dataset_name))
                wu = wrap_aspect_as_workunit(
                    "dataset",
                    dataset_urn,
                    "datasetProfile",
                    dataset_profile,
                )
                yield wu
                self.report.report_workunit(wu)

            else:
                self.report.report_dropped(f"Profile for {str(datahub_dataset_name)}")

        if isinstance(table, BigqueryView):
            view = cast(BigqueryView, table)
            view_definition_string = view.ddl
            view_properties_aspect = ViewProperties(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            wu = wrap_aspect_as_workunit(
                "dataset",
                dataset_urn,
                "viewProperties",
                view_properties_aspect,
            )
            yield wu
            self.report.report_workunit(wu)

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
                        self.BIGQUERY_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
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

    def get_report(self) -> BigQueryV2Report:
        return self.report

    def get_tables_for_dataset(
        self,
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
    ) -> List[BigqueryTable]:

        tables = self.db_tables.get(dataset_name)

        # In bigquery there is no way to query all tables in a Project id
        if not tables:
            return BigQueryDataDictionary.get_tables_for_dataset(
                conn, project_id, dataset_name
            )

        # Some schema may not have any table
        return tables.get(dataset_name, [])

    def get_views_for_dataset(
        self,
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
    ) -> List[BigqueryView]:

        views = self.db_views.get(project_id)

        # get all views for database failed,
        # falling back to get views for schema
        if not views:
            return BigQueryDataDictionary.get_views_for_dataset(
                conn, project_id, dataset_name
            )

        # Some schema may not have any table
        return views.get(dataset_name, [])

    def get_columns_for_table(
        self, conn: bigquery.Client, table_identifier: BigqueryTableIdentifier
    ) -> List[BigqueryColumn]:

        if (
            table_identifier.project_id,
            table_identifier.dataset,
        ) not in self.schema_columns.keys():
            columns = BigQueryDataDictionary.get_columns_for_dataset(
                conn,
                project_id=table_identifier.project_id,
                dataset_name=table_identifier.dataset,
            )
            self.schema_columns[
                (table_identifier.project_id, table_identifier.dataset)
            ] = columns
        else:
            columns = self.schema_columns[
                (table_identifier.project_id, table_identifier.dataset)
            ]

        # get all columns for schema failed,
        # falling back to get columns for table
        if not columns:
            return BigQueryDataDictionary.get_columns_for_table(conn, table_identifier)

        # Access to table but none of its columns - is this possible ?
        return columns.get(table_identifier.table, [])

    def add_config_to_report(self):
        self.report.include_table_lineage = self.config.include_table_lineage
        self.report.use_date_sharded_audit_log_tables = (
            self.config.use_date_sharded_audit_log_tables
        )
        self.report.log_page_size = self.config.log_page_size
        self.report.use_exported_bigquery_audit_metadata = (
            self.config.use_exported_bigquery_audit_metadata
        )

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def close(self) -> None:
        self.prepare_for_commit()

import atexit
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, cast

import pydantic
from google.cloud import bigquery
from google.cloud.bigquery.table import TableListItem

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
)
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
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.utilities.hive_schema_to_avro import (
    HiveColumnToAvroConverter,
    get_schema_fields_for_hive_column,
)
from datahub.utilities.mapping import Constants
from datahub.utilities.perf_timer import PerfTimer
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


@platform_name("BigQuery", doc_order=1)
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
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class BigqueryV2Source(StatefulIngestionSourceBase, TestableSource):
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    BIGQUERY_FIELD_TYPE_MAPPINGS: Dict[
        str,
        Type[
            Union[
                ArrayType,
                BytesType,
                BooleanType,
                NumberType,
                RecordType,
                StringType,
                TimeType,
                NullType,
            ]
        ],
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
        "ARRAY": ArrayType,
        "STRUCT": RecordType,
    }

    def __init__(self, ctx: PipelineContext, config: BigQueryV2Config):
        super(BigqueryV2Source, self).__init__(config, ctx)
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = BigQueryV2Report()
        self.platform: str = "bigquery"
        BigqueryTableIdentifier._BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX = (
            self.config.sharded_table_pattern
        )

        # For database, schema, tables, views, etc
        self.lineage_extractor = BigqueryLineageExtractor(config, self.report)
        self.usage_extractor = BigQueryUsageExtractor(config, self.report)
        self.profiler = BigqueryProfiler(config, self.report)

        # Currently caching using instance variables
        # TODO - rewrite cache for readability or use out of the box solution
        self.db_tables: Dict[str, Dict[str, List[BigqueryTable]]] = {}
        self.db_views: Dict[str, Dict[str, List[BigqueryView]]] = {}

        self.schema_columns: Dict[
            Tuple[str, str], Optional[Dict[str, List[BigqueryColumn]]]
        ] = {}

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=BaseSQLAlchemyCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

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
    def metada_read_capability_test(
        project_ids: List[str], profiling_enabled: bool
    ) -> CapabilityReport:
        for project_id in project_ids:
            try:
                logger.info((f"Metadata read capability test for project {project_id}"))
                client: bigquery.Client = bigquery.Client(project_id)
                assert client
                result = BigQueryDataDictionary.get_datasets_for_project_id(
                    client, project_id, 10
                )
                if len(result) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"Dataset query returned empty dataset. It is either empty or no dataset in project {project_id}",
                    )
                tables = BigQueryDataDictionary.get_tables_for_dataset(
                    conn=client,
                    project_id=project_id,
                    dataset_name=result[0].name,
                    tables={},
                    with_data_read_permission=profiling_enabled,
                )
                if len(tables) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"Tables query did not return any table. It is either empty or no tables in project {project_id}.{result[0].name}",
                    )

            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Dataset query failed with error: {e}",
                )

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
                failures_before_test = len(report.failures)
                usage_extractor.test_capability(project_id)
                if failures_before_test != len(report.failures):
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Usage capability test failed. Check the logs for further info",
                    )
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

            metada_read_capability = BigqueryV2Source.metada_read_capability_test(
                project_ids, connection_conf.profiling.enabled
            )
            if SourceCapability.SCHEMA_METADATA not in _report:
                _report[SourceCapability.SCHEMA_METADATA] = metada_read_capability

            if connection_conf.include_table_lineage:
                lineage_capability = BigqueryV2Source.lineage_capability_test(
                    connection_conf, project_ids, report
                )
                if SourceCapability.LINEAGE_COARSE not in _report:
                    _report[SourceCapability.LINEAGE_COARSE] = lineage_capability

            if connection_conf.include_usage_statistics:
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

        self.stale_entity_removal_handler.add_entity_to_state(
            type="container",
            urn=make_container_urn(
                guid=database_container_key.guid(),
            ),
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

        self.stale_entity_removal_handler.add_entity_to_state(
            type="container",
            urn=make_container_urn(
                guid=schema_container_key.guid(),
            ),
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
        logger.info("Getting projects")
        conn: bigquery.Client = self.get_bigquery_client()
        self.add_config_to_report()

        projects: List[BigqueryProject] = BigQueryDataDictionary.get_projects(conn)
        if len(projects) == 0:
            logger.warning(
                "Get projects didn't return any project. Maybe resourcemanager.projects.get permission is missing for the service account. You can assign predefined roles/bigquery.metadataViewer role to your service account."
            )
            return

        for project_id in projects:
            if not self.config.project_id_pattern.allowed(project_id.id):
                self.report.report_dropped(project_id.id)
                continue
            logger.info(f"Processing project: {project_id.id}")
            yield from self._process_project(conn, project_id)

        if self.config.profiling.enabled:
            logger.info("Starting profiling...")
            yield from self.profiler.get_workunits(self.db_tables)

        # Clean up stale entities if configured.
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def _process_project(
        self, conn: bigquery.Client, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        project_id = bigquery_project.id

        self.db_tables[project_id] = {}
        self.db_views[project_id] = {}

        database_workunits = self.gen_project_id_containers(project_id)

        for wu in database_workunits:
            self.report.report_workunit(wu)
            yield wu

        try:
            bigquery_project.datasets = (
                BigQueryDataDictionary.get_datasets_for_project_id(conn, project_id)
            )
        except Exception as e:
            logger.error(
                f"Unable to get datasets for project {project_id}, skipping. The error was: {e}"
            )
            return None

        if len(bigquery_project.datasets) == 0:
            logger.warning(
                f"No dataset found in {project_id}. Either there are no datasets in this project or missing bigquery.datasets.get permission. You can assign predefined roles/bigquery.metadataViewer role to your service account."
            )
            return

        self.report.num_project_datasets_to_scan[project_id] = len(
            bigquery_project.datasets
        )
        for bigquery_dataset in bigquery_project.datasets:

            if not self.config.dataset_pattern.allowed(bigquery_dataset.name):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                continue
            try:
                yield from self._process_schema(conn, project_id, bigquery_dataset)
            except Exception as e:
                logger.error(
                    f"Unable to get tables for dataset {bigquery_dataset.name} in project {project_id}, skipping. The error was: {e}"
                )
                continue

        if self.config.include_usage_statistics:
            logger.info(f"Generate usage for {project_id}")
            tables: Dict[str, List[str]] = {}

            for dataset in self.db_tables[project_id]:
                tables[dataset] = [
                    table.name for table in self.db_tables[project_id][dataset]
                ]

            for dataset in self.db_views[project_id]:
                if not tables[dataset]:
                    tables[dataset] = [
                        table.name for table in self.db_views[project_id][dataset]
                    ]
                else:
                    tables[dataset].extend(
                        [table.name for table in self.db_views[project_id][dataset]]
                    )

            yield from self.usage_extractor.generate_usage_for_project(
                project_id, tables
            )

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
            for table in bigquery_dataset.tables:
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

        table.columns = self.get_columns_for_table(
            conn, table_identifier, self.config.column_limit
        )
        if not table.columns:
            logger.warning(f"Unable to get columns for table: {table_identifier}")

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None

        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                project_id=project_id,
                dataset_name=schema_name,
                table=table,
                platform=self.platform,
            )

        table_workunits = self.gen_table_dataset_workunits(
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

        view.columns = self.get_columns_for_table(
            conn, table_identifier, column_limit=self.config.column_limit
        )

        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None
        if self.config.include_table_lineage:
            lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                project_id=project_id,
                dataset_name=dataset_name,
                table=view,
                platform=self.platform,
            )

        view_workunits = self.gen_view_dataset_workunits(
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

    def gen_table_dataset_workunits(
        self,
        table: BigqueryTable,
        project_id: str,
        dataset_name: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]],
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {}
        if table.expires:
            custom_properties["expiration_date"] = str(str(table.expires))

        if table.time_partitioning:
            custom_properties["time_partitioning"] = str(str(table.time_partitioning))

        if table.size_in_bytes:
            custom_properties["size_in_bytes"] = str(table.size_in_bytes)

        if table.active_billable_bytes:
            custom_properties["billable_bytes_active"] = str(
                table.active_billable_bytes
            )

        if table.long_term_billable_bytes:
            custom_properties["billable_bytes_long_term"] = str(
                table.long_term_billable_bytes
            )

        if table.max_partition_id:
            custom_properties["number_of_partitions"] = str(table.num_partitions)
            custom_properties["max_partition_id"] = str(table.max_partition_id)
            custom_properties["is_partitioned"] = str(True)

        if table.max_shard_id:
            custom_properties["max_shard_id"] = str(table.max_shard_id)
            custom_properties["is_sharded"] = str(True)

        tags_to_add = None
        if table.labels and self.config.capture_table_label_as_tag:
            tags_to_add = []
            tags_to_add.extend(
                [make_tag_urn(f"""{k}:{v}""") for k, v in table.labels.items()]
            )

        yield from self.gen_dataset_workunits(
            table=table,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_type="table",
            lineage_info=lineage_info,
            tags_to_add=tags_to_add,
            custom_properties=custom_properties,
        )

    def gen_view_dataset_workunits(
        self,
        table: BigqueryView,
        project_id: str,
        dataset_name: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]],
    ) -> Iterable[MetadataWorkUnit]:

        yield from self.gen_dataset_workunits(
            table=table,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_type="view",
            lineage_info=lineage_info,
        )

        view = cast(BigqueryView, table)
        view_definition_string = view.ddl
        view_properties_aspect = ViewProperties(
            materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
        )
        wu = wrap_aspect_as_workunit(
            "dataset",
            self.gen_dataset_urn(dataset_name, project_id, table.name),
            "viewProperties",
            view_properties_aspect,
        )
        yield wu
        self.report.report_workunit(wu)

    def gen_dataset_workunits(
        self,
        table: Union[BigqueryTable, BigqueryView],
        project_id: str,
        dataset_name: str,
        sub_type: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None,
        tags_to_add: Optional[List[str]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.gen_dataset_urn(dataset_name, project_id, table.name)

        status = Status(removed=False)
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "status", status)
        yield wu
        self.report.report_workunit(wu)

        datahub_dataset_name = BigqueryTableIdentifier(
            project_id, dataset_name, table.name
        )

        yield self.gen_schema_metadata(dataset_urn, table, str(datahub_dataset_name))

        if lineage_info is not None:
            upstream_lineage, upstream_column_props = lineage_info
        else:
            upstream_column_props = {}
            upstream_lineage = None

        if upstream_lineage is not None:
            # Emit the lineage work unit
            wu = wrap_aspect_as_workunit(
                "dataset", dataset_urn, "upstreamLineage", upstream_lineage
            )
            yield wu
            self.report.report_workunit(wu)

        dataset_properties = DatasetProperties(
            name=datahub_dataset_name.get_table_display_name(),
            description=table.comment,
            qualifiedName=str(datahub_dataset_name),
            customProperties={**upstream_column_props},
        )
        if custom_properties:
            dataset_properties.customProperties.update(custom_properties)

        wu = wrap_aspect_as_workunit(
            "dataset", dataset_urn, "datasetProperties", dataset_properties
        )
        yield wu
        self.report.report_workunit(wu)

        if tags_to_add:
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from self.add_table_to_dataset_container(
            dataset_urn,
            project_id,
            dataset_name,
        )
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            self.report.report_workunit(dpi_aspect)
            yield dpi_aspect

        subTypes = SubTypes(typeNames=[sub_type])
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "subTypes", subTypes)
        yield wu
        self.report.report_workunit(wu)

        yield from self._get_domain_wu(
            dataset_name=str(datahub_dataset_name),
            entity_urn=dataset_urn,
            entity_type="dataset",
        )

        self.stale_entity_removal_handler.add_entity_to_state(
            type=sub_type,
            urn=dataset_urn,
        )

    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "globalTags", tags)
        self.report.report_workunit(wu)
        return wu

    def gen_dataset_urn(self, dataset_name: str, project_id: str, table: str) -> str:
        datahub_dataset_name = BigqueryTableIdentifier(project_id, dataset_name, table)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            str(datahub_dataset_name),
            self.config.platform_instance,
            self.config.env,
        )
        return dataset_urn

    def gen_schema_fields(self, columns: List[BigqueryColumn]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []

        HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR = " "
        _COMPLEX_TYPE = re.compile("^(struct|array)")
        last_id = -1
        for col in columns:
            # if col.data_type is empty that means this column is part of a complex type
            if col.data_type is None or _COMPLEX_TYPE.match(col.data_type.lower()):
                # If the we have seen the ordinal position that most probably means we already processed this complex type
                if last_id != col.ordinal_position:
                    schema_fields.extend(
                        get_schema_fields_for_hive_column(
                            col.name, col.data_type.lower(), description=col.comment
                        )
                    )

                # We have to add complex type comments to the correct level
                if col.comment:
                    for idx, field in enumerate(schema_fields):
                        # Remove all the [version=2.0].[type=struct]. tags to get the field path
                        if (
                            re.sub(r"\[.*?\]\.", "", field.fieldPath, 0, re.MULTILINE)
                            == col.field_path
                        ):
                            field.description = col.comment
                            schema_fields[idx] = field
            else:
                field = SchemaField(
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
                schema_fields.append(field)
            last_id = col.ordinal_position
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_urn: str,
        table: Union[BigqueryTable, BigqueryView],
        dataset_name: str,
    ) -> MetadataWorkUnit:

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self.gen_schema_fields(table.columns),
        )
        wu = wrap_aspect_as_workunit(
            "dataset", dataset_urn, "schemaMetadata", schema_metadata
        )
        self.report.report_workunit(wu)
        return wu

    def get_report(self) -> BigQueryV2Report:
        return self.report

    def get_tables_for_dataset(
        self,
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
    ) -> List[BigqueryTable]:

        bigquery_tables: Optional[List[BigqueryTable]] = (
            self.db_tables[project_id].get(dataset_name)
            if project_id in self.db_tables
            else []
        )

        # In bigquery there is no way to query all tables in a Project id
        if not bigquery_tables:
            with PerfTimer() as timer:
                bigquery_tables = []
                table_count: int = 0
                table_items: Dict[str, TableListItem] = {}
                # Dict to store sharded table and the last seen max shard id
                sharded_tables: Dict[str, TableListItem] = defaultdict()
                # Partitions view throw exception if we try to query partition info for too many tables
                # so we have to limit the number of tables we query partition info.
                # The conn.list_tables returns table infos that information_schema doesn't contain and this
                # way we can merge that info with the queried one.
                # https://cloud.google.com/bigquery/docs/information-schema-partitions
                for table in conn.list_tables(f"{project_id}.{dataset_name}"):
                    table_identifier = BigqueryTableIdentifier(
                        project_id=project_id,
                        dataset=dataset_name,
                        table=table.table_id,
                    )

                    _, shard = BigqueryTableIdentifier.get_table_and_shard(
                        table_identifier.raw_table_name()
                    )
                    table_name = table_identifier.get_table_name().split(".")[-1]

                    # For sharded tables we only process the latest shard
                    # which has the highest date in the table name.
                    # Sharded tables look like: table_20220120
                    # We only has one special case where the table name is a date
                    # in this case we merge all these tables under dataset name as table name.
                    # For example some_dataset.20220110 will be turned to some_dataset.some_dataset
                    # It seems like there are some bigquery user who uses this way the tables.
                    if shard:
                        if not sharded_tables.get(table_identifier.get_table_name()):
                            # When table is only a shard we use dataset_name as table_name
                            sharded_tables[table_name] = table
                            continue
                        else:
                            stored_table_identifier = BigqueryTableIdentifier(
                                project_id=project_id,
                                dataset=dataset_name,
                                table=sharded_tables[table_name].table_id,
                            )
                            (
                                _,
                                stored_shard,
                            ) = BigqueryTableIdentifier.get_table_and_shard(
                                stored_table_identifier.raw_table_name()
                            )
                            # When table is none, we use dataset_name as table_name
                            table_name = table_identifier.get_table_name().split(".")[
                                -1
                            ]
                            assert stored_shard
                            if stored_shard < shard:
                                sharded_tables[table_name] = table
                            continue
                    else:
                        table_count = table_count + 1
                        table_items[table.table_id] = table

                    if str(table_identifier).startswith(
                        self.config.temp_table_dataset_prefix
                    ):
                        logger.debug(
                            f"Dropping temporary table {table_identifier.table}"
                        )
                        self.report.report_dropped(table_identifier.raw_table_name())
                        continue

                    if (
                        table_count % self.config.number_of_datasets_process_in_batch
                        == 0
                    ):
                        bigquery_tables.extend(
                            BigQueryDataDictionary.get_tables_for_dataset(
                                conn,
                                project_id,
                                dataset_name,
                                table_items,
                                with_data_read_permission=self.config.profiling.enabled,
                            )
                        )
                        table_items.clear()

                # Sharded tables don't have partition keys, so it is safe to add to the list as
                # it should not affect the number of tables will be touched in the partitions system view.
                # Because we have the batched query of get_tables_for_dataset to makes sure
                # we won't hit too many tables queried with partitions system view.
                # The key in the map is the actual underlying table name and not the friendly name and
                # that's why we need to get the actual table names and not the normalized ones.
                table_items.update(
                    {value.table_id: value for value in sharded_tables.values()}
                )

                if table_items:
                    bigquery_tables.extend(
                        BigQueryDataDictionary.get_tables_for_dataset(
                            conn,
                            project_id,
                            dataset_name,
                            table_items,
                            with_data_read_permission=self.config.profiling.enabled,
                        )
                    )

                self.db_tables[project_id][dataset_name] = bigquery_tables

                self.report.metadata_extraction_sec[
                    f"{project_id}.{dataset_name}"
                ] = round(timer.elapsed_seconds(), 2)

                return bigquery_tables

        # Some schema may not have any table
        return (
            self.db_tables[project_id].get(dataset_name, [])
            if project_id in self.db_tables
            else []
        )

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
                conn, project_id, dataset_name, self.config.profiling.enabled
            )

        # Some schema may not have any table
        return views.get(dataset_name, [])

    def get_columns_for_table(
        self,
        conn: bigquery.Client,
        table_identifier: BigqueryTableIdentifier,
        column_limit: Optional[int] = None,
    ) -> List[BigqueryColumn]:

        if (
            table_identifier.project_id,
            table_identifier.dataset,
        ) not in self.schema_columns.keys():
            columns = BigQueryDataDictionary.get_columns_for_dataset(
                conn,
                project_id=table_identifier.project_id,
                dataset_name=table_identifier.dataset,
                column_limit=column_limit,
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
            logger.warning(
                f"Couldn't get columns on the dataset level for {table_identifier}. Trying to get on table level..."
            )
            return BigQueryDataDictionary.get_columns_for_table(
                conn, table_identifier, self.config.column_limit
            )

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

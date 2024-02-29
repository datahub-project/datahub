import atexit
import logging
import os
import re
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Type, Union, cast

from google.cloud import bigquery
from google.cloud.bigquery.table import TableListItem

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import BigQueryDatasetKey, ContainerKey, ProjectIdKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_helper import (
    unquote_and_decode_unicode_escape_seq,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryTableSnapshot,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_EXTERNAL_DATASET_URL_TEMPLATE,
    BQ_EXTERNAL_TABLE_URL_TEMPLATE,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_schema_container,
    get_domain_wu,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import (
    METADATA_EXTRACTION,
    PROFILING,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
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
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.file_backed_collections import FileBackedDict
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
CLUSTERING_COLUMN_TAG = "CLUSTERING_COLUMN"


# We can't use close as it is not called if the ingestion is not successful
def cleanup(config: BigQueryV2Config) -> None:
    if config._credentials_path is not None:
        logger.debug(
            f"Deleting temporary credential file at {config._credentials_path}"
        )
        os.unlink(config._credentials_path)


@platform_name("BigQuery", doc_order=1)
@config_class(BigQueryV2Config)
@support_status(SupportStatus.CERTIFIED)
@capability(  # DataPlatformAspect is set to project id, but not added to urns as project id is in the container path
    SourceCapability.PLATFORM_INSTANCE,
    "Platform instance is pre-set to the BigQuery project id",
    supported=False,
)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration",
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_FINE, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, can be disabled via configuration `include_usage_statistics`",
)
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
        if self.config.enable_legacy_sharded_table_support:
            BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = ""

        self.bigquery_data_dictionary = BigQuerySchemaApi(
            self.report.schema_api_perf, self.config.get_bigquery_client()
        )
        self.sql_parser_schema_resolver = self._init_schema_resolver()

        redundant_lineage_run_skip_handler: Optional[
            RedundantLineageRunSkipHandler
        ] = None
        if self.config.enable_stateful_lineage_ingestion:
            redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        # For database, schema, tables, views, snapshots etc
        self.lineage_extractor = BigqueryLineageExtractor(
            config,
            self.report,
            dataset_urn_builder=self.gen_dataset_urn_from_ref,
            redundant_run_skip_handler=redundant_lineage_run_skip_handler,
        )

        redundant_usage_run_skip_handler: Optional[RedundantUsageRunSkipHandler] = None
        if self.config.enable_stateful_usage_ingestion:
            redundant_usage_run_skip_handler = RedundantUsageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        self.usage_extractor = BigQueryUsageExtractor(
            config,
            self.report,
            schema_resolver=self.sql_parser_schema_resolver,
            dataset_urn_builder=self.gen_dataset_urn_from_ref,
            redundant_run_skip_handler=redundant_usage_run_skip_handler,
        )

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        self.profiling_state_handler: Optional[ProfilingHandler] = None
        if self.config.enable_stateful_profiling:
            self.profiling_state_handler = ProfilingHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )
        self.profiler = BigqueryProfiler(
            config, self.report, self.profiling_state_handler
        )

        # Global store of table identifiers for lineage filtering
        self.table_refs: Set[str] = set()

        # Maps project -> view_ref, so we can find all views in a project
        self.view_refs_by_project: Dict[str, Set[str]] = defaultdict(set)
        # Maps project -> snapshot_ref, so we can find all snapshots in a project
        self.snapshot_refs_by_project: Dict[str, Set[str]] = defaultdict(set)
        # Maps view ref -> actual sql
        self.view_definitions: FileBackedDict[str] = FileBackedDict()
        # Maps snapshot ref -> Snapshot
        self.snapshots_by_ref: FileBackedDict[BigqueryTableSnapshot] = FileBackedDict()

        self.add_config_to_report()
        atexit.register(cleanup, config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigqueryV2Source":
        config = BigQueryV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def connectivity_test(client: bigquery.Client) -> CapabilityReport:
        ret = client.query("select 1")
        if ret.error_result:
            return CapabilityReport(
                capable=False, failure_reason=f"{ret.error_result['message']}"
            )
        else:
            return CapabilityReport(capable=True)

    @property
    def store_table_refs(self):
        return self.config.include_table_lineage or self.config.include_usage_statistics

    @staticmethod
    def metadata_read_capability_test(
        project_ids: List[str], config: BigQueryV2Config
    ) -> CapabilityReport:
        for project_id in project_ids:
            try:
                logger.info((f"Metadata read capability test for project {project_id}"))
                client: bigquery.Client = config.get_bigquery_client()
                assert client
                bigquery_data_dictionary = BigQuerySchemaApi(
                    BigQueryV2Report().schema_api_perf, client
                )
                result = bigquery_data_dictionary.get_datasets_for_project_id(
                    project_id, 10
                )
                if len(result) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"Dataset query returned empty dataset. It is either empty or no dataset in project {project_id}",
                    )
                tables = bigquery_data_dictionary.get_tables_for_dataset(
                    project_id=project_id,
                    dataset_name=result[0].name,
                    tables={},
                    with_data_read_permission=config.is_profiling_enabled(),
                )
                if len(list(tables)) == 0:
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
        lineage_extractor = BigqueryLineageExtractor(
            connection_conf, report, lambda ref: ""
        )
        for project_id in project_ids:
            try:
                logger.info(f"Lineage capability test for project {project_id}")
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
        usage_extractor = BigQueryUsageExtractor(
            connection_conf,
            report,
            schema_resolver=SchemaResolver(platform="bigquery"),
            dataset_urn_builder=lambda ref: "",
        )
        for project_id in project_ids:
            try:
                logger.info(f"Usage capability test for project {project_id}")
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
            connection_conf = BigQueryV2Config.parse_obj_allow_extras(config_dict)
            client: bigquery.Client = connection_conf.get_bigquery_client()
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

            metadata_read_capability = BigqueryV2Source.metadata_read_capability_test(
                project_ids, connection_conf
            )
            if SourceCapability.SCHEMA_METADATA not in _report:
                _report[SourceCapability.SCHEMA_METADATA] = metadata_read_capability

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

    def _init_schema_resolver(self) -> SchemaResolver:
        schema_resolution_required = (
            self.config.lineage_parse_view_ddl or self.config.lineage_use_sql_parser
        )
        schema_ingestion_enabled = (
            self.config.include_schema_metadata
            and self.config.include_tables
            and self.config.include_views
            and self.config.include_table_snapshots
        )

        if schema_resolution_required and not schema_ingestion_enabled:
            if self.ctx.graph:
                return self.ctx.graph.initialize_schema_resolver_from_datahub(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            else:
                logger.warning(
                    "Failed to load schema info from DataHub as DataHubGraph is missing. "
                    "Use `datahub-rest` sink OR provide `datahub-api` config in recipe. ",
                )
        return SchemaResolver(platform=self.platform, env=self.config.env)

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str, project_id: str
    ) -> MetadataWorkUnit:
        aspect = DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=(
                make_dataplatform_instance_urn(self.platform, project_id)
                if self.config.include_data_platform_instance
                else None
            ),
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=aspect
        ).as_workunit()

    def gen_dataset_key(self, db_name: str, schema: str) -> ContainerKey:
        return BigQueryDatasetKey(
            project_id=db_name,
            dataset_id=schema,
            platform=self.platform,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def gen_project_id_key(self, database: str) -> ContainerKey:
        return ProjectIdKey(
            project_id=database,
            platform=self.platform,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def gen_project_id_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_project_id_key(database)

        yield from gen_database_container(
            database=database,
            name=database,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_PROJECT],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            database_container_key=database_container_key,
        )

    def gen_dataset_containers(
        self, dataset: str, project_id: str, tags: Optional[Dict[str, str]] = None
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_dataset_key(project_id, dataset)

        tags_joined: Optional[List[str]] = None
        if tags and self.config.capture_dataset_label_as_tag:
            tags_joined = [f"{k}:{v}" for k, v in tags.items()]
        database_container_key = self.gen_project_id_key(database=project_id)

        yield from gen_schema_container(
            database=project_id,
            schema=dataset,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_DATASET],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
            external_url=(
                BQ_EXTERNAL_DATASET_URL_TEMPLATE.format(
                    project=project_id, dataset=dataset
                )
                if self.config.include_external_url
                else None
            ),
            tags=tags_joined,
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        projects = self._get_projects()
        if not projects:
            return

        if self.config.include_schema_metadata:
            for project_id in projects:
                self.report.set_ingestion_stage(project_id.id, METADATA_EXTRACTION)
                logger.info(f"Processing project: {project_id.id}")
                yield from self._process_project(project_id)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.get_usage_workunits(
                [p.id for p in projects], self.table_refs
            )

        if self.config.include_table_lineage:
            yield from self.lineage_extractor.get_lineage_workunits(
                [p.id for p in projects],
                self.sql_parser_schema_resolver,
                self.view_refs_by_project,
                self.view_definitions,
                self.snapshot_refs_by_project,
                self.snapshots_by_ref,
                self.table_refs,
            )

    def _get_projects(self) -> List[BigqueryProject]:
        logger.info("Getting projects")
        if self.config.project_ids or self.config.project_id:
            project_ids = self.config.project_ids or [self.config.project_id]  # type: ignore
            return [
                BigqueryProject(id=project_id, name=project_id)
                for project_id in project_ids
            ]
        else:
            return list(self._query_project_list())

    def _query_project_list(self) -> Iterable[BigqueryProject]:
        projects = self.bigquery_data_dictionary.get_projects()
        if not projects:  # Report failure on exception and if empty list is returned
            self.report.report_failure(
                "metadata-extraction",
                "Get projects didn't return any project. "
                "Maybe resourcemanager.projects.get permission is missing for the service account. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
            )
            return []

        for project in projects:
            if self.config.project_id_pattern.allowed(project.id):
                yield project
            else:
                self.report.report_dropped(project.id)

    def _process_project(
        self, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        db_tables: Dict[str, List[BigqueryTable]] = {}
        db_views: Dict[str, List[BigqueryView]] = {}
        db_snapshots: Dict[str, List[BigqueryTableSnapshot]] = {}

        project_id = bigquery_project.id
        try:
            bigquery_project.datasets = (
                self.bigquery_data_dictionary.get_datasets_for_project_id(project_id)
            )
        except Exception as e:
            error_message = f"Unable to get datasets for project {project_id}, skipping. The error was: {e}"
            if self.config.is_profiling_enabled():
                error_message = f"Unable to get datasets for project {project_id}, skipping. Does your service account has bigquery.datasets.get permission? The error was: {e}"
            logger.error(error_message)
            self.report.report_failure(
                "metadata-extraction",
                f"{project_id} - {error_message}",
            )
            return None

        if len(bigquery_project.datasets) == 0:
            more_info = (
                "Either there are no datasets in this project or missing bigquery.datasets.get permission. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account."
            )
            if self.config.exclude_empty_projects:
                self.report.report_dropped(project_id)
                warning_message = f"Excluded project '{project_id}' since no were datasets found. {more_info}"
            else:
                yield from self.gen_project_id_containers(project_id)
                warning_message = (
                    f"No datasets found in project '{project_id}'. {more_info}"
                )
            logger.warning(warning_message)
            return

        yield from self.gen_project_id_containers(project_id)

        self.report.num_project_datasets_to_scan[project_id] = len(
            bigquery_project.datasets
        )
        for bigquery_dataset in bigquery_project.datasets:
            if not is_schema_allowed(
                self.config.dataset_pattern,
                bigquery_dataset.name,
                project_id,
                self.config.match_fully_qualified_names,
            ):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                continue
            try:
                # db_tables, db_views, and db_snapshots are populated in the this method
                yield from self._process_schema(
                    project_id, bigquery_dataset, db_tables, db_views, db_snapshots
                )

            except Exception as e:
                error_message = f"Unable to get tables for dataset {bigquery_dataset.name} in project {project_id}, skipping. Does your service account has bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission? The error was: {e}"
                if self.config.is_profiling_enabled():
                    error_message = f"Unable to get tables for dataset {bigquery_dataset.name} in project {project_id}, skipping. Does your service account has bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission, bigquery.tables.getData permission? The error was: {e}"

                trace = traceback.format_exc()
                logger.error(trace)
                logger.error(error_message)
                self.report.report_failure(
                    "metadata-extraction",
                    f"{project_id}.{bigquery_dataset.name} - {error_message} - {trace}",
                )
                continue

        if self.config.is_profiling_enabled():
            logger.info(f"Starting profiling project {project_id}")
            self.report.set_ingestion_stage(project_id, PROFILING)
            yield from self.profiler.get_workunits(
                project_id=project_id,
                tables=db_tables,
            )

    def _process_schema(
        self,
        project_id: str,
        bigquery_dataset: BigqueryDataset,
        db_tables: Dict[str, List[BigqueryTable]],
        db_views: Dict[str, List[BigqueryView]],
        db_snapshots: Dict[str, List[BigqueryTableSnapshot]],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = bigquery_dataset.name

        yield from self.gen_dataset_containers(
            dataset_name, project_id, bigquery_dataset.labels
        )

        columns = None
        if (
            self.config.include_tables
            or self.config.include_views
            or self.config.include_table_snapshots
        ):
            columns = self.bigquery_data_dictionary.get_columns_for_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                column_limit=self.config.column_limit,
                run_optimized_column_query=self.config.run_optimized_column_query,
            )

        if self.config.include_tables:
            db_tables[dataset_name] = list(
                self.get_tables_for_dataset(project_id, dataset_name)
            )

            for table in db_tables[dataset_name]:
                table_columns = columns.get(table.name, []) if columns else []
                yield from self._process_table(
                    table=table,
                    columns=table_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )
        elif self.store_table_refs:
            # Need table_refs to calculate lineage and usage
            for table_item in self.bigquery_data_dictionary.list_tables(
                dataset_name, project_id
            ):
                identifier = BigqueryTableIdentifier(
                    project_id=project_id,
                    dataset=dataset_name,
                    table=table_item.table_id,
                )
                if not self.config.table_pattern.allowed(identifier.raw_table_name()):
                    self.report.report_dropped(identifier.raw_table_name())
                    continue
                try:
                    self.table_refs.add(
                        str(BigQueryTableRef(identifier).get_sanitized_table_ref())
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not create table ref for {table_item.path}: {e}"
                    )

        if self.config.include_views:
            db_views[dataset_name] = list(
                self.bigquery_data_dictionary.get_views_for_dataset(
                    project_id,
                    dataset_name,
                    self.config.is_profiling_enabled(),
                    self.report,
                )
            )

            for view in db_views[dataset_name]:
                view_columns = columns.get(view.name, []) if columns else []
                yield from self._process_view(
                    view=view,
                    columns=view_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )

        if self.config.include_table_snapshots:
            db_snapshots[dataset_name] = list(
                self.bigquery_data_dictionary.get_snapshots_for_dataset(
                    project_id,
                    dataset_name,
                    self.config.is_profiling_enabled(),
                    self.report,
                )
            )

            for snapshot in db_snapshots[dataset_name]:
                snapshot_columns = columns.get(snapshot.name, []) if columns else []
                yield from self._process_snapshot(
                    snapshot=snapshot,
                    columns=snapshot_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )

    # This method is used to generate the ignore list for datatypes the profiler doesn't support we have to do it here
    # because the profiler doesn't have access to columns
    def generate_profile_ignore_list(self, columns: List[BigqueryColumn]) -> List[str]:
        ignore_list: List[str] = []
        for column in columns:
            if not column.data_type or any(
                word in column.data_type.lower()
                for word in ["array", "struct", "geography", "json"]
            ):
                ignore_list.append(column.field_path)
        return ignore_list

    def _process_table(
        self,
        table: BigqueryTable,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, table.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name())

        if not self.config.table_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        if self.store_table_refs:
            self.table_refs.add(
                str(BigQueryTableRef(table_identifier).get_sanitized_table_ref())
            )
        table.column_count = len(columns)

        # We only collect profile ignore list if profiling is enabled and profile_table_level_only is false
        if (
            self.config.is_profiling_enabled()
            and not self.config.profiling.profile_table_level_only
        ):
            table.columns_ignore_from_profiling = self.generate_profile_ignore_list(
                columns
            )

        if not table.column_count:
            logger.warning(
                f"Table doesn't have any column or unable to get columns for table: {table_identifier}"
            )

        # If table has time partitioning, set the data type of the partitioning field
        if table.partition_info:
            table.partition_info.column = next(
                (
                    column
                    for column in columns
                    if column.name == table.partition_info.field
                ),
                None,
            )
        yield from self.gen_table_dataset_workunits(
            table, columns, project_id, dataset_name
        )

    def _process_view(
        self,
        view: BigqueryView,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, view.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name(), "view")

        if not self.config.view_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        if self.store_table_refs:
            table_ref = str(
                BigQueryTableRef(table_identifier).get_sanitized_table_ref()
            )
            self.table_refs.add(table_ref)
            if self.config.lineage_parse_view_ddl and view.view_definition:
                self.view_refs_by_project[project_id].add(table_ref)
                self.view_definitions[table_ref] = view.view_definition

        view.column_count = len(columns)
        if not view.column_count:
            logger.warning(
                f"View doesn't have any column or unable to get columns for table: {table_identifier}"
            )

        yield from self.gen_view_dataset_workunits(
            table=view,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
        )

    def _process_snapshot(
        self,
        snapshot: BigqueryTableSnapshot,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(
            project_id, dataset_name, snapshot.name
        )

        self.report.snapshots_scanned += 1

        if not self.config.table_snapshot_pattern.allowed(
            table_identifier.raw_table_name()
        ):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        snapshot.columns = columns
        snapshot.column_count = len(columns)
        if not snapshot.column_count:
            logger.warning(
                f"Snapshot doesn't have any column or unable to get columns for table: {table_identifier}"
            )

        if self.store_table_refs:
            table_ref = str(
                BigQueryTableRef(table_identifier).get_sanitized_table_ref()
            )
            self.table_refs.add(table_ref)
            if snapshot.base_table_identifier:
                self.snapshot_refs_by_project[project_id].add(table_ref)
                self.snapshots_by_ref[table_ref] = snapshot

        yield from self.gen_snapshot_dataset_workunits(
            table=snapshot,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
        )

    def gen_table_dataset_workunits(
        self,
        table: BigqueryTable,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {}
        if table.expires:
            custom_properties["expiration_date"] = str(table.expires)

        if table.partition_info:
            custom_properties["partition_info"] = str(table.partition_info)

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

        sub_types: List[str] = [DatasetSubTypes.TABLE]
        if table.max_shard_id:
            custom_properties["max_shard_id"] = str(table.max_shard_id)
            custom_properties["is_sharded"] = str(True)
            sub_types = ["sharded table"] + sub_types

        tags_to_add = None
        if table.labels and self.config.capture_table_label_as_tag:
            tags_to_add = []
            tags_to_add.extend(
                [make_tag_urn(f"""{k}:{v}""") for k, v in table.labels.items()]
            )

        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_types=sub_types,
            tags_to_add=tags_to_add,
            custom_properties=custom_properties,
        )

    def gen_view_dataset_workunits(
        self,
        table: BigqueryView,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_types=[DatasetSubTypes.VIEW],
        )

        view = cast(BigqueryView, table)
        view_definition_string = view.view_definition
        view_properties_aspect = ViewProperties(
            materialized=view.materialized,
            viewLanguage="SQL",
            viewLogic=view_definition_string or "",
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(
                project_id=project_id, dataset_name=dataset_name, table=table.name
            ),
            aspect=view_properties_aspect,
        ).as_workunit()

    def gen_snapshot_dataset_workunits(
        self,
        table: BigqueryTableSnapshot,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {}
        if table.ddl:
            custom_properties["snapshot_ddl"] = table.ddl
        if table.snapshot_time:
            custom_properties["snapshot_time"] = str(table.snapshot_time)
        if table.size_in_bytes:
            custom_properties["size_in_bytes"] = str(table.size_in_bytes)
        if table.rows_count:
            custom_properties["rows_count"] = str(table.rows_count)
        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_types=[DatasetSubTypes.BIGQUERY_TABLE_SNAPSHOT],
            custom_properties=custom_properties,
        )

    def gen_dataset_workunits(
        self,
        table: Union[BigqueryTable, BigqueryView, BigqueryTableSnapshot],
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
        sub_types: List[str],
        tags_to_add: Optional[List[str]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.gen_dataset_urn(
            project_id=project_id, dataset_name=dataset_name, table=table.name
        )

        status = Status(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=status
        ).as_workunit()

        datahub_dataset_name = BigqueryTableIdentifier(
            project_id, dataset_name, table.name
        )

        yield self.gen_schema_metadata(
            dataset_urn, table, columns, str(datahub_dataset_name)
        )

        dataset_properties = DatasetProperties(
            name=datahub_dataset_name.get_table_display_name(),
            description=unquote_and_decode_unicode_escape_seq(table.comment)
            if table.comment
            else "",
            qualifiedName=str(datahub_dataset_name),
            created=(
                TimeStamp(time=int(table.created.timestamp() * 1000))
                if table.created is not None
                else None
            ),
            lastModified=(
                TimeStamp(time=int(table.last_altered.timestamp() * 1000))
                if table.last_altered is not None
                else (
                    TimeStamp(time=int(table.created.timestamp() * 1000))
                    if table.created is not None
                    else None
                )
            ),
            externalUrl=(
                BQ_EXTERNAL_TABLE_URL_TEMPLATE.format(
                    project=project_id, dataset=dataset_name, table=table.name
                )
                if self.config.include_external_url
                else None
            ),
        )
        if custom_properties:
            dataset_properties.customProperties.update(custom_properties)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

        if tags_to_add:
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=self.gen_dataset_key(project_id, dataset_name),
        )
        yield self.get_dataplatform_instance_aspect(
            dataset_urn=dataset_urn, project_id=project_id
        )

        subTypes = SubTypes(typeNames=sub_types)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subTypes
        ).as_workunit()

        if self.domain_registry:
            yield from get_domain_wu(
                dataset_name=str(datahub_dataset_name),
                entity_urn=dataset_urn,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
            )

    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=tags
        ).as_workunit()

    def gen_dataset_urn(self, project_id: str, dataset_name: str, table: str) -> str:
        datahub_dataset_name = BigqueryTableIdentifier(project_id, dataset_name, table)
        return make_dataset_urn(
            self.platform,
            str(datahub_dataset_name),
            self.config.env,
        )

    def gen_dataset_urn_from_ref(self, ref: BigQueryTableRef) -> str:
        return self.gen_dataset_urn(
            ref.table_identifier.project_id,
            ref.table_identifier.dataset,
            ref.table_identifier.table,
        )

    def gen_schema_fields(self, columns: List[BigqueryColumn]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []

        # Below line affects HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR in global scope
        # TODO: Refractor this such that
        # converter = HiveColumnToAvroConverter(struct_type_separator=" ");
        # converter.get_schema_fields_for_hive_column(...)
        original_struct_type_separator = (
            HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR
        )
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
                            re.sub(
                                r"\[.*?\]\.",
                                "",
                                field.fieldPath.lower(),
                                0,
                                re.MULTILINE,
                            )
                            == col.field_path.lower()
                        ):
                            field.description = col.comment
                            schema_fields[idx] = field
                            break
            else:
                tags = []
                if col.is_partition_column:
                    tags.append(
                        TagAssociationClass(make_tag_urn(Constants.TAG_PARTITION_KEY))
                    )

                if col.cluster_column_position is not None:
                    tags.append(
                        TagAssociationClass(
                            make_tag_urn(
                                f"{CLUSTERING_COLUMN_TAG}_{col.cluster_column_position}"
                            )
                        )
                    )

                field = SchemaField(
                    fieldPath=col.name,
                    type=SchemaFieldDataType(
                        self.BIGQUERY_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
                    ),
                    # NOTE: nativeDataType will not be in sync with older connector
                    nativeDataType=col.data_type,
                    description=col.comment,
                    nullable=col.is_nullable,
                    globalTags=GlobalTagsClass(tags=tags),
                )
                schema_fields.append(field)
            last_id = col.ordinal_position
        HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR = (
            original_struct_type_separator
        )
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_urn: str,
        table: Union[BigqueryTable, BigqueryView, BigqueryTableSnapshot],
        columns: List[BigqueryColumn],
        dataset_name: str,
    ) -> MetadataWorkUnit:
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            # fields=[],
            fields=self.gen_schema_fields(columns),
        )

        if self.config.lineage_parse_view_ddl or self.config.lineage_use_sql_parser:
            self.sql_parser_schema_resolver.add_schema_metadata(
                dataset_urn, schema_metadata
            )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def get_report(self) -> BigQueryV2Report:
        return self.report

    def get_tables_for_dataset(
        self,
        project_id: str,
        dataset_name: str,
    ) -> Iterable[BigqueryTable]:
        # In bigquery there is no way to query all tables in a Project id
        with PerfTimer() as timer:
            # Partitions view throw exception if we try to query partition info for too many tables
            # so we have to limit the number of tables we query partition info.
            # The conn.list_tables returns table infos that information_schema doesn't contain and this
            # way we can merge that info with the queried one.
            # https://cloud.google.com/bigquery/docs/information-schema-partitions
            max_batch_size: int = (
                self.config.number_of_datasets_process_in_batch
                if not self.config.is_profiling_enabled()
                else self.config.number_of_datasets_process_in_batch_if_profiling_enabled
            )

            # We get the list of tables in the dataset to get core table properties and to be able to process the tables in batches
            # We collect only the latest shards from sharded tables (tables with _YYYYMMDD suffix) and ignore temporary tables
            table_items = self.get_core_table_details(
                dataset_name, project_id, self.config.temp_table_dataset_prefix
            )

            items_to_get: Dict[str, TableListItem] = {}
            for table_item in table_items.keys():
                items_to_get[table_item] = table_items[table_item]
                if len(items_to_get) % max_batch_size == 0:
                    yield from self.bigquery_data_dictionary.get_tables_for_dataset(
                        project_id,
                        dataset_name,
                        items_to_get,
                        with_data_read_permission=self.config.is_profiling_enabled(),
                    )
                    items_to_get.clear()

            if items_to_get:
                yield from self.bigquery_data_dictionary.get_tables_for_dataset(
                    project_id,
                    dataset_name,
                    items_to_get,
                    with_data_read_permission=self.config.is_profiling_enabled(),
                )

        self.report.metadata_extraction_sec[f"{project_id}.{dataset_name}"] = round(
            timer.elapsed_seconds(), 2
        )

    def get_core_table_details(
        self, dataset_name: str, project_id: str, temp_table_dataset_prefix: str
    ) -> Dict[str, TableListItem]:
        table_items: Dict[str, TableListItem] = {}
        # Dict to store sharded table and the last seen max shard id
        sharded_tables: Dict[str, TableListItem] = {}

        for table in self.bigquery_data_dictionary.list_tables(
            dataset_name, project_id
        ):
            table_identifier = BigqueryTableIdentifier(
                project_id=project_id,
                dataset=dataset_name,
                table=table.table_id,
            )

            _, shard = BigqueryTableIdentifier.get_table_and_shard(
                table_identifier.table
            )
            table_name = table_identifier.get_table_name().split(".")[-1]

            # Sharded tables look like: table_20220120
            # For sharded tables we only process the latest shard and ignore the rest
            # to find the latest shard we iterate over the list of tables and store the maximum shard id
            # We only have one special case where the table name is a date `20220110`
            # in this case we merge all these tables under dataset name as table name.
            # For example some_dataset.20220110 will be turned to some_dataset.some_dataset
            # It seems like there are some bigquery user who uses this non-standard way of sharding the tables.
            if shard:
                if table_name not in sharded_tables:
                    sharded_tables[table_name] = table
                    continue

                stored_table_identifier = BigqueryTableIdentifier(
                    project_id=project_id,
                    dataset=dataset_name,
                    table=sharded_tables[table_name].table_id,
                )
                _, stored_shard = BigqueryTableIdentifier.get_table_and_shard(
                    stored_table_identifier.table
                )
                # When table is none, we use dataset_name as table_name
                assert stored_shard
                if stored_shard < shard:
                    sharded_tables[table_name] = table
                continue
            elif str(table_identifier).startswith(temp_table_dataset_prefix):
                logger.debug(f"Dropping temporary table {table_identifier.table}")
                self.report.report_dropped(table_identifier.raw_table_name())
                continue

            table_items[table.table_id] = table
        # Adding maximum shards to the list of tables
        table_items.update({value.table_id: value for value in sharded_tables.values()})

        return table_items

    def add_config_to_report(self):
        self.report.include_table_lineage = self.config.include_table_lineage
        self.report.use_date_sharded_audit_log_tables = (
            self.config.use_date_sharded_audit_log_tables
        )
        self.report.log_page_size = self.config.log_page_size
        self.report.use_exported_bigquery_audit_metadata = (
            self.config.use_exported_bigquery_audit_metadata
        )
        self.report.stateful_lineage_ingestion_enabled = (
            self.config.enable_stateful_lineage_ingestion
        )
        self.report.stateful_usage_ingestion_enabled = (
            self.config.enable_stateful_usage_ingestion
        )
        self.report.window_start_time, self.report.window_end_time = (
            self.config.start_time,
            self.config.end_time,
        )

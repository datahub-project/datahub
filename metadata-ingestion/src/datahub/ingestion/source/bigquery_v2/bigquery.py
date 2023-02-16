import atexit
import logging
import os
import re
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, cast

from google.cloud import bigquery
from google.cloud.bigquery.table import TableListItem

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
    set_dataset_urn_to_lower,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import BigQueryDatasetKey, PlatformKey, ProjectIdKey
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
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_EXTERNAL_DATASET_URL_TEMPLATE,
    BQ_EXTERNAL_TABLE_URL_TEMPLATE,
    get_bigquery_client,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_schema_container,
    get_domain_wu,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantRunSkipHandler,
)
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
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
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.hive_schema_to_avro import (
    HiveColumnToAvroConverter,
    get_schema_fields_for_hive_column,
)
from datahub.utilities.mapping import Constants
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
    auto_workunit_reporter,
)
from datahub.utilities.time import datetime_to_ts_millis

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
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration",
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
        if self.config.enable_legacy_sharded_table_support:
            BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = ""

        set_dataset_urn_to_lower(self.config.convert_urns_to_lowercase)

        # For database, schema, tables, views, etc
        self.lineage_extractor = BigqueryLineageExtractor(config, self.report)
        self.usage_extractor = BigQueryUsageExtractor(config, self.report)

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=BaseSQLAlchemyCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        self.redundant_run_skip_handler = RedundantRunSkipHandler(
            source=self,
            config=self.config,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        self.profiling_state_handler: Optional[ProfilingHandler] = None
        if self.config.store_last_profiling_timestamps:
            self.profiling_state_handler = ProfilingHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )
        self.profiler = BigqueryProfiler(
            config, self.report, self.profiling_state_handler
        )

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

    @staticmethod
    def metada_read_capability_test(
        project_ids: List[str], config: BigQueryV2Config
    ) -> CapabilityReport:
        for project_id in project_ids:
            try:
                logger.info((f"Metadata read capability test for project {project_id}"))
                client: bigquery.Client = get_bigquery_client(config)
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
                    with_data_read_permission=config.profiling.enabled,
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
            connection_conf = BigQueryV2Config.parse_obj_allow_extras(config_dict)
            client: bigquery.Client = get_bigquery_client(connection_conf)
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
                project_ids, connection_conf
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
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=aspect
            ).as_workunit()
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
            instance=self.config.platform_instance,
            backcompat_instance_for_guid=self.config.env,
        )

    def gen_project_id_key(self, database: str) -> PlatformKey:
        return ProjectIdKey(
            project_id=database,
            platform=self.platform,
            instance=self.config.platform_instance,
            backcompat_instance_for_guid=self.config.env,
        )

    def gen_project_id_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_project_id_key(database)

        yield from gen_database_container(
            database=database,
            name=database,
            sub_types=["Project"],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            report=self.report,
            database_container_key=database_container_key,
        )

    def gen_dataset_containers(
        self, dataset: str, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_dataset_key(project_id, dataset)

        database_container_key = self.gen_project_id_key(database=project_id)

        yield from gen_schema_container(
            database=project_id,
            schema=dataset,
            sub_types=["Dataset"],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            report=self.report,
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
            external_url=BQ_EXTERNAL_DATASET_URL_TEMPLATE.format(
                project=project_id, dataset=dataset
            )
            if self.config.include_external_url
            else None,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Getting projects")
        conn: bigquery.Client = get_bigquery_client(self.config)
        self.add_config_to_report()

        projects: List[BigqueryProject]
        if self.config.project_id:
            project = BigqueryProject(
                id=self.config.project_id, name=self.config.project_id
            )
            projects = [project]
        else:
            try:
                projects = BigQueryDataDictionary.get_projects(conn)
                if len(projects) == 0:
                    logger.error(
                        "Get projects didn't return any project. Maybe resourcemanager.projects.get permission is missing for the service account. You can assign predefined roles/bigquery.metadataViewer role to your service account."
                    )
                    self.report.report_failure(
                        "metadata-extraction",
                        "Get projects didn't return any project. Maybe resourcemanager.projects.get permission is missing for the service account. You can assign predefined roles/bigquery.metadataViewer role to your service account.",
                    )
                    return
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(
                    f"Get projects didn't return any project. Maybe resourcemanager.projects.get permission is missing for the service account. You can assign predefined roles/bigquery.metadataViewer role to your service account. The error was: {e}"
                )
                logger.error(trace)
                self.report.report_failure(
                    "metadata-extraction",
                    f"Get projects didn't return any project. Maybe resourcemanager.projects.get permission is missing for the service account. You can assign predefined roles/bigquery.metadataViewer role to your service account. The error was: {e} Stacktrace: {trace}",
                )
                return None

        for project_id in projects:
            if not self.config.project_id_pattern.allowed(project_id.id):
                self.report.report_dropped(project_id.id)
                continue
            logger.info(f"Processing project: {project_id.id}")
            self.report.set_project_state(project_id.id, "Metadata Extraction")
            yield from self._process_project(conn, project_id)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_workunit_reporter(
                self.report,
                auto_status_aspect(self.get_workunits_internal()),
            ),
        )

    def _process_project(
        self, conn: bigquery.Client, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        db_tables: Dict[str, List[BigqueryTable]] = {}
        db_views: Dict[str, List[BigqueryView]] = {}

        project_id = bigquery_project.id

        yield from self.gen_project_id_containers(project_id)

        try:
            bigquery_project.datasets = (
                BigQueryDataDictionary.get_datasets_for_project_id(conn, project_id)
            )
        except Exception as e:
            error_message = f"Unable to get datasets for project {project_id}, skipping. The error was: {e}"
            if self.config.profiling.enabled:
                error_message = f"Unable to get datasets for project {project_id}, skipping. Does your service account has bigquery.datasets.get permission? The error was: {e}"
            logger.error(error_message)
            self.report.report_failure(
                "metadata-extraction",
                f"{project_id} - {error_message}",
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
            if not is_schema_allowed(
                self.config.dataset_pattern,
                bigquery_dataset.name,
                project_id,
                self.config.match_fully_qualified_names,
            ):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                continue
            try:
                # db_tables and db_views are populated in the this method
                yield from self._process_schema(
                    conn, project_id, bigquery_dataset, db_tables, db_views
                )

            except Exception as e:
                error_message = f"Unable to get tables for dataset {bigquery_dataset.name} in project {project_id}, skipping. Does your service account has bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission? The error was: {e}"
                if self.config.profiling.enabled:
                    error_message = f"Unable to get tables for dataset {bigquery_dataset.name} in project {project_id}, skipping. Does your service account has bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission, bigquery.tables.getData permission? The error was: {e}"

                trace = traceback.format_exc()
                logger.error(trace)
                logger.error(error_message)
                self.report.report_failure(
                    "metadata-extraction",
                    f"{project_id}.{bigquery_dataset.name} - {error_message} - {trace}",
                )
                continue

        if self.config.include_table_lineage:
            if (
                self.config.store_last_lineage_extraction_timestamp
                and self.redundant_run_skip_handler.should_skip_this_run(
                    cur_start_time_millis=datetime_to_ts_millis(self.config.start_time)
                )
            ):
                # Skip this run
                self.report.report_warning(
                    "lineage-extraction",
                    f"Skip this run as there was a run later than the current start time: {self.config.start_time}",
                )
                return

            if self.config.store_last_lineage_extraction_timestamp:
                # Update the checkpoint state for this run.
                self.redundant_run_skip_handler.update_state(
                    start_time_millis=datetime_to_ts_millis(self.config.start_time),
                    end_time_millis=datetime_to_ts_millis(self.config.end_time),
                )
            self.report.set_project_state(project_id, "Lineage Extraction")
            yield from self.generate_lineage(
                project_id, db_tables=db_tables, db_views=db_views
            )

        if self.config.include_usage_statistics:
            if (
                self.config.store_last_usage_extraction_timestamp
                and self.redundant_run_skip_handler.should_skip_this_run(
                    cur_start_time_millis=datetime_to_ts_millis(self.config.start_time)
                )
            ):
                self.report.report_warning(
                    "usage-extraction",
                    f"Skip this run as there was a run later than the current start time: {self.config.start_time}",
                )
                return

            if self.config.store_last_usage_extraction_timestamp:
                # Update the checkpoint state for this run.
                self.redundant_run_skip_handler.update_state(
                    start_time_millis=datetime_to_ts_millis(self.config.start_time),
                    end_time_millis=datetime_to_ts_millis(self.config.end_time),
                )

            self.report.set_project_state(project_id, "Usage Extraction")
            yield from self.generate_usage_statistics(
                project_id, db_tables=db_tables, db_views=db_views
            )

        if self.config.profiling.enabled:
            logger.info(f"Starting profiling project {project_id}")
            self.report.set_project_state(project_id, "Profiling")
            yield from self.profiler.get_workunits(
                project_id=project_id,
                tables=db_tables,
            )

    def generate_lineage(
        self,
        project_id: str,
        db_tables: Dict[str, List[BigqueryTable]],
        db_views: Dict[str, List[BigqueryView]],
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Generate lineage for {project_id}")
        lineage = self.lineage_extractor.calculate_lineage_for_project(project_id)

        for dataset in db_tables.keys():
            for table in db_tables[dataset]:
                dataset_urn = self.gen_dataset_urn(dataset, project_id, table.name)
                lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                    project_id=project_id,
                    dataset_name=dataset,
                    table=table,
                    platform=self.platform,
                    lineage_metadata=lineage,
                )
                if lineage_info:
                    yield from self.gen_lineage(dataset_urn, lineage_info)
        for dataset in db_views.keys():
            for view in db_views[dataset]:
                dataset_urn = self.gen_dataset_urn(dataset, project_id, view.name)
                lineage_info = self.lineage_extractor.get_upstream_lineage_info(
                    project_id=project_id,
                    dataset_name=dataset,
                    table=view,
                    platform=self.platform,
                    lineage_metadata=lineage,
                )
                if lineage_info:
                    yield from self.gen_lineage(dataset_urn, lineage_info)

    def generate_usage_statistics(
        self,
        project_id: str,
        db_tables: Dict[str, List[BigqueryTable]],
        db_views: Dict[str, List[BigqueryView]],
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Generate usage for {project_id}")
        tables: Dict[str, List[str]] = defaultdict()
        for dataset in db_tables.keys():
            tables[dataset] = [
                BigqueryTableIdentifier(
                    project_id, dataset, table.name
                ).get_table_name()
                for table in db_tables[dataset]
            ]
        for dataset in db_views.keys():
            tables[dataset].extend(
                [
                    BigqueryTableIdentifier(
                        project_id, dataset, view.name
                    ).get_table_name()
                    for view in db_views[dataset]
                ]
            )
        yield from self.usage_extractor.generate_usage_for_project(project_id, tables)

    def _process_schema(
        self,
        conn: bigquery.Client,
        project_id: str,
        bigquery_dataset: BigqueryDataset,
        db_tables: Dict[str, List[BigqueryTable]],
        db_views: Dict[str, List[BigqueryView]],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = bigquery_dataset.name

        yield from self.gen_dataset_containers(
            dataset_name,
            project_id,
        )

        columns = BigQueryDataDictionary.get_columns_for_dataset(
            conn,
            project_id=project_id,
            dataset_name=dataset_name,
            column_limit=self.config.column_limit,
        )

        if self.config.include_tables:
            db_tables[dataset_name] = self.get_tables_for_dataset(
                conn, project_id, dataset_name
            )

            for table in db_tables[dataset_name]:
                table_columns = columns.get(table.name, []) if columns else []

                yield from self._process_table(
                    conn, table, table_columns, project_id, dataset_name
                )

        if self.config.include_views:
            db_views[dataset_name] = self.get_views_for_dataset(
                conn, project_id, dataset_name
            )

            for view in db_views[dataset_name]:
                view_columns = columns.get(view.name, []) if columns else []
                yield from self._process_view(
                    view, view_columns, project_id, dataset_name
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
        conn: bigquery.Client,
        table: BigqueryTable,
        columns: List[BigqueryColumn],
        project_id: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, schema_name, table.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name())

        if not self.config.table_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        table.column_count = len(columns)

        # We only collect profile ignore list if profiling is enabled and profile_table_level_only is false
        if (
            self.config.profiling.enabled
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
            table, columns, project_id, schema_name
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

        sub_types: List[str] = ["table"]
        if table.max_shard_id:
            custom_properties["max_shard_id"] = str(table.max_shard_id)
            custom_properties["is_sharded"] = str(True)
            sub_types = ["sharded table", "table"]

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
            sub_types=["view"],
        )

        view = cast(BigqueryView, table)
        view_definition_string = view.view_definition
        view_properties_aspect = ViewProperties(
            materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(dataset_name, project_id, table.name),
            aspect=view_properties_aspect,
        ).as_workunit()

    def gen_dataset_workunits(
        self,
        table: Union[BigqueryTable, BigqueryView],
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
        sub_types: List[str],
        tags_to_add: Optional[List[str]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.gen_dataset_urn(dataset_name, project_id, table.name)

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
            description=table.comment,
            qualifiedName=str(datahub_dataset_name),
            created=TimeStamp(time=int(table.created.timestamp() * 1000))
            if table.created is not None
            else None,
            lastModified=TimeStamp(time=int(table.last_altered.timestamp() * 1000))
            if table.last_altered is not None
            else TimeStamp(time=int(table.created.timestamp() * 1000))
            if table.created is not None
            else None,
            externalUrl=BQ_EXTERNAL_TABLE_URL_TEMPLATE.format(
                project=project_id, dataset=dataset_name, table=table.name
            )
            if self.config.include_external_url
            else None,
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
            report=self.report,
            parent_container_key=self.gen_dataset_key(project_id, dataset_name),
        )
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

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
                report=self.report,
            )

    def gen_lineage(
        self,
        dataset_urn: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if lineage_info is None:
            return

        upstream_lineage, upstream_column_props = lineage_info
        if upstream_lineage is not None:
            if self.config.incremental_lineage:
                patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                    urn=dataset_urn
                )
                for upstream in upstream_lineage.upstreams:
                    patch_builder.add_upstream_lineage(upstream)

                yield from [
                    MetadataWorkUnit(
                        id=f"upstreamLineage-for-{dataset_urn}",
                        mcp_raw=mcp,
                    )
                    for mcp in patch_builder.build()
                ]
            else:
                yield from [
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=upstream_lineage
                    ).as_workunit()
                ]

    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=tags
        ).as_workunit()

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
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def get_report(self) -> BigQueryV2Report:
        return self.report

    def get_tables_for_dataset(
        self,
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
    ) -> List[BigqueryTable]:
        bigquery_tables: Optional[List[BigqueryTable]] = []

        # In bigquery there is no way to query all tables in a Project id
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

                # Sharded tables look like: table_20220120
                # For sharded tables we only process the latest shard and ignore the rest
                # to find the latest shard we iterate over the list of tables and store the maximum shard id
                # We only has one special case where the table name is a date `20220110`
                # in this case we merge all these tables under dataset name as table name.
                # For example some_dataset.20220110 will be turned to some_dataset.some_dataset
                # It seems like there are some bigquery user who uses this non-standard way of sharding the tables.
                if shard:
                    if not sharded_tables.get(table_name):
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
                    logger.debug(f"Dropping temporary table {table_identifier.table}")
                    self.report.report_dropped(table_identifier.raw_table_name())
                    continue

                if table_count % self.config.number_of_datasets_process_in_batch == 0:
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

        self.report.metadata_extraction_sec[f"{project_id}.{dataset_name}"] = round(
            timer.elapsed_seconds(), 2
        )

        return bigquery_tables

    def get_views_for_dataset(
        self,
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
    ) -> List[BigqueryView]:
        views: List[BigqueryView] = []

        views = BigQueryDataDictionary.get_views_for_dataset(
            conn, project_id, dataset_name, self.config.profiling.enabled
        )
        return views

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

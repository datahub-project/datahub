import atexit
import functools
import logging
import os
from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import (
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
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryProject,
    BigQuerySchemaApi,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.bigquery_v2.bigquery_test_connection import (
    BigQueryTestConnection,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
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
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)


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
    SourceCapability.CLASSIFICATION,
    "Optionally enabled via `classification.enabled`",
    supported=True,
)
class BigqueryV2Source(StatefulIngestionSourceBase, TestableSource):
    def __init__(self, ctx: PipelineContext, config: BigQueryV2Config):
        super().__init__(config, ctx)
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = BigQueryV2Report()
        self.platform: str = "bigquery"

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        BigqueryTableIdentifier._BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX = (
            self.config.sharded_table_pattern
        )
        if self.config.enable_legacy_sharded_table_support:
            BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = ""

        self.bigquery_data_dictionary = BigQuerySchemaApi(
            self.report.schema_api_perf,
            self.config.get_bigquery_client(),
        )
        if self.config.extract_policy_tags_from_catalog:
            self.bigquery_data_dictionary.datacatalog_client = (
                self.config.get_policy_tag_manager_client()
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
            dataset_urn_builder=self.gen_dataset_urn_from_raw_ref,
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
            dataset_urn_builder=self.gen_dataset_urn_from_raw_ref,
            redundant_run_skip_handler=redundant_usage_run_skip_handler,
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

        self.bq_schema_extractor = BigQuerySchemaGenerator(
            self.config,
            self.report,
            self.bigquery_data_dictionary,
            self.domain_registry,
            self.sql_parser_schema_resolver,
            self.profiler,
            self.gen_dataset_urn,
        )

        self.add_config_to_report()
        atexit.register(cleanup, config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigqueryV2Source":
        config = BigQueryV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return BigQueryTestConnection.test_connection(config_dict)

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
                    batch_size=self.config.schema_resolution_batch_size,
                )
            else:
                logger.warning(
                    "Failed to load schema info from DataHub as DataHubGraph is missing. "
                    "Use `datahub-rest` sink OR provide `datahub-api` config in recipe. ",
                )
        return SchemaResolver(platform=self.platform, env=self.config.env)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            functools.partial(
                auto_incremental_lineage, self.config.incremental_lineage
            ),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        projects = self._get_projects()
        if not projects:
            return

        if self.config.include_schema_metadata:
            for project in projects:
                yield from self.bq_schema_extractor.get_project_workunits(project)

        if self.config.include_usage_statistics:
            yield from self.usage_extractor.get_usage_workunits(
                [p.id for p in projects], self.bq_schema_extractor.table_refs
            )

        if self.config.include_table_lineage:
            yield from self.lineage_extractor.get_lineage_workunits(
                [p.id for p in projects],
                self.sql_parser_schema_resolver,
                self.bq_schema_extractor.view_refs_by_project,
                self.bq_schema_extractor.view_definitions,
                self.bq_schema_extractor.snapshot_refs_by_project,
                self.bq_schema_extractor.snapshots_by_ref,
                self.bq_schema_extractor.table_refs,
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
        try:
            projects = self.bigquery_data_dictionary.get_projects()

            if (
                not projects
            ):  # Report failure on exception and if empty list is returned
                self.report.failure(
                    title="Get projects didn't return any project. ",
                    message="Maybe resourcemanager.projects.get permission is missing for the service account. "
                    "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
                )
        except Exception as e:
            self.report.failure(
                title="Failed to get BigQuery Projects",
                message="Maybe resourcemanager.projects.get permission is missing for the service account. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account.",
                exc=e,
            )
            projects = []

        for project in projects:
            if self.config.project_id_pattern.allowed(project.id):
                yield project
            else:
                self.report.report_dropped(project.id)

    def gen_dataset_urn(
        self, project_id: str, dataset_name: str, table: str, use_raw_name: bool = False
    ) -> str:
        datahub_dataset_name = BigqueryTableIdentifier(project_id, dataset_name, table)
        return make_dataset_urn(
            self.platform,
            (
                str(datahub_dataset_name)
                if not use_raw_name
                else datahub_dataset_name.raw_table_name()
            ),
            self.config.env,
        )

    def gen_dataset_urn_from_raw_ref(self, ref: BigQueryTableRef) -> str:
        return self.gen_dataset_urn(
            ref.table_identifier.project_id,
            ref.table_identifier.dataset,
            ref.table_identifier.table,
            use_raw_name=True,
        )

    def get_report(self) -> BigQueryV2Report:
        return self.report

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

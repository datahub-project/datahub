import atexit
import functools
import logging
import os
from typing import Iterable, List, Optional

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
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigQuerySchemaApi,
    get_projects,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.bigquery_v2.bigquery_test_connection import (
    BigQueryTestConnection,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    BigQueryQueriesExtractor,
    BigQueryQueriesExtractorConfig,
)
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
from datahub.ingestion.source_report.ingestion_stage import QUERIES_EXTRACTION
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)


# We can't use close as it is not called if the ingestion is not successful
def cleanup(config: BigQueryV2Config) -> None:
    if config._credentials_path is not None:
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
@capability(
    SourceCapability.PARTITION_SUPPORT,
    "Enabled by default, partition keys and clustering keys are supported.",
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

        self.bigquery_data_dictionary = BigQuerySchemaApi(
            report=self.report.schema_api_perf,
            projects_client=config.get_projects_client(),
            client=config.get_bigquery_client(),
        )
        if self.config.extract_policy_tags_from_catalog:
            self.bigquery_data_dictionary.datacatalog_client = (
                self.config.get_policy_tag_manager_client()
            )

        with self.report.init_schema_resolver_timer:
            self.sql_parser_schema_resolver = self._init_schema_resolver()

        self.filters = BigQueryFilter(self.config, self.report)
        self.identifiers = BigQueryIdentifierBuilder(self.config, self.report)

        redundant_lineage_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = (
            None
        )
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
            schema_resolver=self.sql_parser_schema_resolver,
            identifiers=self.identifiers,
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
            identifiers=self.identifiers,
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
            self.identifiers,
            self.ctx.graph,
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
            self.config.use_queries_v2 or self.config.lineage_use_sql_parser
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
        projects = get_projects(
            self.bq_schema_extractor.schema_api,
            self.report,
            self.filters,
        )
        if not projects:
            return

        for project in projects:
            yield from self.bq_schema_extractor.get_project_workunits(project)

        with self.report.new_stage("*: View and Snapshot Lineage"):
            yield from self.lineage_extractor.get_lineage_workunits_for_views_and_snapshots(
                [p.id for p in projects],
                self.bq_schema_extractor.view_refs_by_project,
                self.bq_schema_extractor.view_definitions,
                self.bq_schema_extractor.snapshot_refs_by_project,
                self.bq_schema_extractor.snapshots_by_ref,
            )

        if self.config.use_queries_v2:
            # if both usage and lineage are disabled then skip queries extractor piece
            if (
                not self.config.include_usage_statistics
                and not self.config.include_table_lineage
            ):
                return

            with self.report.new_stage(
                f"*: {QUERIES_EXTRACTION}"
            ), BigQueryQueriesExtractor(
                connection=self.config.get_bigquery_client(),
                schema_api=self.bq_schema_extractor.schema_api,
                config=BigQueryQueriesExtractorConfig(
                    window=self.config,
                    user_email_pattern=self.config.usage.user_email_pattern,
                    include_lineage=self.config.include_table_lineage,
                    include_usage_statistics=self.config.include_usage_statistics,
                    include_operations=self.config.usage.include_operational_stats,
                    include_queries=self.config.include_queries,
                    include_query_usage_statistics=self.config.include_query_usage_statistics,
                    top_n_queries=self.config.usage.top_n_queries,
                    region_qualifiers=self.config.region_qualifiers,
                ),
                structured_report=self.report,
                filters=self.filters,
                identifiers=self.identifiers,
                schema_resolver=self.sql_parser_schema_resolver,
                discovered_tables=self.bq_schema_extractor.table_refs,
            ) as queries_extractor:
                self.report.queries_extractor = queries_extractor.report
                yield from queries_extractor.get_workunits_internal()
        else:
            if self.config.include_usage_statistics:
                yield from self.usage_extractor.get_usage_workunits(
                    [p.id for p in projects], self.bq_schema_extractor.table_refs
                )

            if self.config.include_table_lineage:
                yield from self.lineage_extractor.get_lineage_workunits(
                    [p.id for p in projects],
                    self.bq_schema_extractor.table_refs,
                )

        # Lineage BQ to GCS
        if (
            self.config.include_table_lineage
            and self.bq_schema_extractor.external_tables
        ):
            for dataset_urn, table in self.bq_schema_extractor.external_tables.items():
                yield from self.lineage_extractor.gen_lineage_workunits_for_external_table(
                    dataset_urn, table.ddl, graph=self.ctx.graph
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

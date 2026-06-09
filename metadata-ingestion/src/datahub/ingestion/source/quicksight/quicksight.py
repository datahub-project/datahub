import logging
from typing import Dict, Iterable, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.analyses import (
    AnalysesProcessor,
)
from datahub.ingestion.source.quicksight.processors.containers import (
    ContainersProcessor,
)
from datahub.ingestion.source.quicksight.processors.dashboards import (
    DashboardsProcessor,
)
from datahub.ingestion.source.quicksight.processors.data_sets import (
    DataSetsProcessor,
)
from datahub.ingestion.source.quicksight.processors.data_sources import (
    DataSourcesProcessor,
    ResolvedDataSource,
)
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.processors.users_groups import (
    UsersGroupsProcessor,
)
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@platform_name("QuickSight")
@config_class(QuickSightSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled via `extract_lineage`")
@capability(SourceCapability.LINEAGE_FINE, "Enabled via `extract_column_lineage`")
@capability(SourceCapability.OWNERSHIP, "Enabled via `extract_ownership`")
@capability(SourceCapability.TAGS, "Enabled via `extract_tags`")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class QuickSightSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts metadata from Amazon QuickSight, including dashboards,
    analyses, datasets, data sources, and the cross-platform lineage that
    connects QuickSight assets to their upstream warehouse/database tables.
    """

    config: QuickSightSourceConfig
    report: QuickSightSourceReport
    platform: str = "quicksight"

    def __init__(self, config: QuickSightSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = QuickSightSourceReport()
        self.api = QuickSightAPI(self.config, self.report)
        # ARN -> ResolvedDataSource, populated by DataSourcesProcessor and
        # consumed by the lineage extractor.
        self.data_source_map: Dict[str, ResolvedDataSource] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "QuickSightSource":
        config = QuickSightSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info(
            f"Starting QuickSight ingestion for account {self.api.aws_account_id} "
            f"in region {self.config.aws_region}"
        )

        # Shared across processors: resolves per-asset ownership (from
        # Describe*Permissions) and AWS resource tags (from ListTagsForResource)
        # so each entity carries those aspects in its single emission.
        enricher = AssetEnricher(self.config, self.report, self.api)

        # Containers first so the (optional) Namespace -> Folder hierarchy and
        # folder-membership map exist before assets reference their parent.
        containers_processor = ContainersProcessor(
            self.config, self.report, self.api, enricher
        )
        yield from containers_processor.get_workunits()
        # Resolves each asset's parent: its QuickSight folder when foldered, else
        # the namespace container (if enabled) or the platform root. Must run
        # after the containers processor above has built the folder map.
        parent_for = containers_processor.parent_for

        # Data sources double as both Dataset entities and the ARN -> platform
        # map consumed by the lineage extractor.
        data_sources_processor = DataSourcesProcessor(
            self.config,
            self.report,
            self.api,
            parent_resolver=parent_for,
            enricher=enricher,
        )
        yield from data_sources_processor.get_workunits()
        self.data_source_map = data_sources_processor.data_source_map

        # Datasets carry schema (from OutputColumns) and the upstream/column
        # lineage resolved against the data source map built above.
        data_sets_processor = DataSetsProcessor(
            self.config,
            self.report,
            self.api,
            self.ctx,
            parent_resolver=parent_for,
            data_source_map=self.data_source_map,
            enricher=enricher,
        )
        yield from data_sets_processor.get_workunits()

        # Analyses and dashboards both map to DataHub Dashboard entities; their
        # inputDatasets edges make the QuickSight Datasets show downstream lineage.
        analyses_processor = AnalysesProcessor(
            self.config,
            self.report,
            self.api,
            parent_resolver=parent_for,
            enricher=enricher,
        )
        yield from analyses_processor.get_workunits()

        dashboards_processor = DashboardsProcessor(
            self.config,
            self.report,
            self.api,
            parent_resolver=parent_for,
            enricher=enricher,
        )
        yield from dashboards_processor.get_workunits()

        # Identity last (opt-in). Users/groups are independent of the asset graph,
        # so ordering only affects when their workunits appear in the stream.
        users_groups_processor = UsersGroupsProcessor(
            self.config, self.report, self.api
        )
        yield from users_groups_processor.get_workunits()

    def get_report(self) -> QuickSightSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Three-step layered probe.

        QuickSight's ``AccessDeniedException`` messages always blame IAM even
        when the real cause is the QuickSight user role (must be ``AUTHOR`` or
        higher) or resource-level permissions. We therefore probe in order and
        surface a diagnostic that points at the most likely layer at fault:

        1. ``sts:GetCallerIdentity`` — confirms AWS credentials work.
        2. ``quicksight:ListNamespaces`` — denied to ``READER`` users, so a
           failure here (when step 1 succeeded) most likely means the QS role is
           ``READER`` rather than an IAM gap.
        3. ``quicksight:ListDashboards`` — broader QuickSight connectivity.
        """
        test_report = TestConnectionReport()
        try:
            config = QuickSightSourceConfig.model_validate(config_dict)
            report = QuickSightSourceReport()

            # Step 1: AWS credentials (STS).
            try:
                api = QuickSightAPI(config, report)
            except Exception as e:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason=(
                        "AWS credentials problem (sts:GetCallerIdentity failed). "
                        "Verify aws_profile / role assumption / instance role and "
                        f"that aws_region is correct. Underlying error: {e}"
                    ),
                )
                return test_report

            # Step 2: ListNamespaces (gated by both IAM and the QS user role).
            try:
                next(iter(api.list_namespaces()), None)
            except Exception as e:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason=(
                        "quicksight:ListNamespaces failed. The most likely cause is "
                        "a QuickSight user role of READER — it must be AUTHOR or "
                        "higher. If the role is already AUTHOR, verify the IAM policy "
                        "includes quicksight:ListNamespaces. (QuickSight error "
                        f"messages always blame IAM even when the role is at fault.) "
                        f"Underlying error: {e}"
                    ),
                )
                return test_report

            # Step 3: ListDashboards (broader connectivity).
            try:
                next(iter(api.list_dashboards()), None)
            except Exception as e:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason=(
                        "quicksight:ListDashboards failed. Check the IAM policy "
                        "includes quicksight:ListDashboards and that QuickSight "
                        f"connectivity is healthy. Underlying error: {e}"
                    ),
                )
                return test_report

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

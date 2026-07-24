import logging
from typing import Iterable

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
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.mapper import AasMapper
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.azure_analysis_services.xmla_client import (
    XmlaClient,
    XmlaClientError,
)
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)

_STAGE_DISCOVER_MODELS = "Discover tabular models"
_STAGE_EXTRACT_MODEL = "Extract tabular model"


@platform_name("Azure Analysis Services", id="azure-analysis-services")
@config_class(AzureAnalysisServicesConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.ANALYSIS_SERVICES_SERVER,
        SourceCapabilityModifier.SEMANTIC_MODEL,
    ],
)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, configure using `extract_lineage`",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configure using `extract_column_level_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class AzureAnalysisServicesSource(StatefulIngestionSourceBase, TestableSource):
    config: AzureAnalysisServicesConfig
    report: AzureAnalysisServicesReport

    def __init__(
        self, config: AzureAnalysisServicesConfig, ctx: PipelineContext
    ) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.ctx = ctx
        self.report = AzureAnalysisServicesReport()
        self.client = XmlaClient(self.config, self.report)
        self.lineage_extractor = AasLineageExtractor(self.config, self.report, ctx)
        self.mapper = AasMapper(
            config=self.config,
            report=self.report,
            ctx=ctx,
            server_name=self.client.server_display_name,
            lineage_extractor=self.lineage_extractor,
        )

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AzureAnalysisServicesSource":
        config = AzureAnalysisServicesConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = AzureAnalysisServicesConfig.model_validate(config_dict)
            client = XmlaClient(config, AzureAnalysisServicesReport())
            client.test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(_STAGE_DISCOVER_MODELS):
            try:
                catalogs = self.client.discover_databases()
            except XmlaClientError as e:
                self.report.failure(
                    title="Model discovery failed",
                    message="Could not list tabular models on the server.",
                    context=f"server={self.config.server}",
                    exc=e,
                )
                return

        for catalog in catalogs:
            if not self.config.database_pattern.allowed(catalog):
                self.report.report_database_filtered(catalog)
                continue
            with self.report.new_stage(f"{_STAGE_EXTRACT_MODEL}: {catalog}"):
                yield from self._process_catalog(catalog)

    def _process_catalog(self, catalog: str) -> Iterable[MetadataWorkUnit]:
        try:
            model = self.client.fetch_tabular_model(catalog)
        except XmlaClientError as e:
            self.report.warning(
                title="Model extraction failed",
                message="Skipped a tabular model that could not be read.",
                context=f"catalog={catalog}",
                exc=e,
            )
            return
        self.report.databases_scanned += 1
        self.report.relationships_scanned += len(model.relationships)
        self.report.roles_scanned += len(model.roles)
        yield from self.mapper.map_model(model)

    def get_report(self) -> AzureAnalysisServicesReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

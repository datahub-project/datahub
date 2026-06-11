from typing import Any, Dict, Iterable, List, Optional, Tuple

from botocore.exceptions import ClientError

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import ParentResolver
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.processors.visuals import VisualsExtractor
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import SUBTYPE_ANALYSIS
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import (
    PLATFORM,
    id_from_arn,
    make_dataset_urn,
)
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard


class AnalysesProcessor:
    """Emits each QuickSight Analysis as a DataHub Dashboard entity (``Analysis``
    subtype) with ``inputDatasets`` lineage to the QuickSight Datasets it reads.

    When ``extract_analysis_definitions`` is enabled, also emits a Chart entity
    per visual and links them to the Analysis via the ``charts`` edge.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
        parent_resolver: ParentResolver,
        enricher: AssetEnricher,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api
        self.parent_resolver = parent_resolver
        self.enricher = enricher
        self.visuals = VisualsExtractor(config, report, api)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for summary in self.api.list_analyses():
            analysis_id = summary.get("AnalysisId")
            name = summary.get("Name") or analysis_id or ""
            if not analysis_id:
                continue
            # list_analyses returns DELETED analyses for ~30 days; skip them.
            if summary.get("Status") == "DELETED":
                continue
            if not self.config.analysis_pattern.allowed(name):
                self.report.analyses.dropped(f"{analysis_id} ({name})")
                continue
            yield from self._emit_analysis(analysis_id, name, summary)

    def _emit_analysis(
        self, analysis_id: str, name: str, summary: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        analysis = self._describe(analysis_id)

        dataset_urns = [
            make_dataset_urn(
                self.api.aws_account_id,
                id_from_arn(arn),
                self.config.platform_instance,
                self.config.env,
            )
            for arn in (analysis.get("DataSetArns") or [])
        ]

        custom_properties = {"analysisId": analysis_id}
        if summary.get("Arn"):
            custom_properties["arn"] = summary["Arn"]
        status = (analysis or summary).get("Status")
        if status:
            custom_properties["status"] = str(status)

        parent = self.parent_resolver(analysis_id)

        chart_workunits: List[MetadataWorkUnit] = []
        chart_urns: List[str] = []
        if self.config.extract_analysis_definitions:
            chart_workunits, chart_urns = self._extract_charts(analysis_id, parent)

        dashboard = Dashboard(
            platform=PLATFORM,
            name=analysis_id,
            platform_instance=self.config.platform_instance,
            display_name=name,
            subtype=SUBTYPE_ANALYSIS,
            custom_properties=custom_properties,
            parent_container=parent if parent is not None else [],
            input_datasets=dataset_urns or None,
            charts=chart_urns or None,
            owners=self.enricher.owners("analysis", analysis_id),
            tags=self.enricher.tags(summary.get("Arn") or ""),
        )
        self.report.analyses.processed(f"{analysis_id} ({name})")
        yield from dashboard.as_workunits()
        yield from chart_workunits

    def _extract_charts(
        self, analysis_id: str, parent: Optional[Container]
    ) -> Tuple[List[MetadataWorkUnit], List[str]]:
        try:
            definition = self.api.describe_analysis_definition(analysis_id)
        except ClientError as e:
            self.report.warning(
                title="Could not describe analysis definition",
                message="Charts/visuals for this analysis will be omitted.",
                context=analysis_id,
                exc=e,
            )
            return [], []
        return self.visuals.extract(analysis_id, definition, parent)

    def _describe(self, analysis_id: str) -> Dict[str, Any]:
        try:
            return self.api.describe_analysis(analysis_id)
        except ClientError as e:
            self.report.warning(
                title="Could not describe analysis",
                message="Emitting the analysis without input-dataset lineage.",
                context=analysis_id,
                exc=e,
            )
            return {}

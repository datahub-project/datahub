import logging
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
from datahub.ingestion.source.quicksight.quicksight_constants import SUBTYPE_DASHBOARD
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import (
    PLATFORM,
    id_from_arn,
    make_dashboard_urn,
    make_dataset_urn,
)
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard

logger = logging.getLogger(__name__)


class DashboardsProcessor:
    """Emits each QuickSight Dashboard as a DataHub Dashboard entity
    (``Dashboard`` subtype) with ``inputDatasets`` lineage, and links it back to
    the source Analysis it was published from (via ``Version.SourceEntityArn``)
    as a related dashboard.

    When ``extract_dashboard_definitions`` is enabled, also emits a Chart entity
    per visual and links them to the Dashboard via the ``charts`` edge.
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
        for summary in self.api.list_dashboards():
            dashboard_id = summary.get("DashboardId")
            name = summary.get("Name") or dashboard_id or ""
            if not dashboard_id:
                continue
            if not self.config.dashboard_pattern.allowed(name):
                self.report.dashboards.dropped(f"{dashboard_id} ({name})")
                continue
            yield from self._emit_dashboard(dashboard_id, name, summary)

    def _emit_dashboard(
        self, dashboard_id: str, name: str, summary: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_obj = self._describe(dashboard_id)
        version = dashboard_obj.get("Version") or {}

        dataset_urns = [
            make_dataset_urn(
                self.api.aws_account_id,
                id_from_arn(arn),
                self.config.platform_instance,
                self.config.env,
            )
            for arn in (version.get("DataSetArns") or [])
        ]

        custom_properties = {"dashboardId": dashboard_id}
        if summary.get("Arn"):
            custom_properties["arn"] = summary["Arn"]

        parent = self.parent_resolver(dashboard_id)

        chart_workunits: List[MetadataWorkUnit] = []
        chart_urns: List[str] = []
        if self.config.extract_dashboard_definitions:
            chart_workunits, chart_urns = self._extract_charts(dashboard_id, parent)

        dashboard = Dashboard(
            platform=PLATFORM,
            name=dashboard_id,
            platform_instance=self.config.platform_instance,
            display_name=name,
            subtype=SUBTYPE_DASHBOARD,
            custom_properties=custom_properties,
            parent_container=parent if parent is not None else [],
            input_datasets=dataset_urns or None,
            charts=chart_urns or None,
            dashboards=self._source_analysis_dashboards(version) or None,
            owners=self.enricher.owners("dashboard", dashboard_id),
            tags=self.enricher.tags(summary.get("Arn") or ""),
        )
        self.report.dashboards.processed(f"{dashboard_id} ({name})")
        yield from dashboard.as_workunits()
        yield from chart_workunits

    def _extract_charts(
        self, dashboard_id: str, parent: Optional[Container]
    ) -> Tuple[List[MetadataWorkUnit], List[str]]:
        try:
            definition = self.api.describe_dashboard_definition(dashboard_id)
        except ClientError as e:
            logger.info(
                f"Could not describe QuickSight dashboard definition {dashboard_id}: {e}"
            )
            return [], []
        return self.visuals.extract(dashboard_id, definition, parent)

    def _source_analysis_dashboards(self, version: Dict[str, Any]) -> List[str]:
        """Link the published Dashboard back to the Analysis it was built from.

        Analyses are modeled as Dashboard entities, so we express the
        relationship as a dashboard-to-dashboard edge. ``SourceEntityArn`` may
        also point at a template or another dashboard; we only link analyses.
        """
        source_arn = version.get("SourceEntityArn") or ""
        if ":analysis/" not in source_arn:
            return []
        return [
            make_dashboard_urn(id_from_arn(source_arn), self.config.platform_instance)
        ]

    def _describe(self, dashboard_id: str) -> Dict[str, Any]:
        try:
            return self.api.describe_dashboard(dashboard_id)
        except ClientError as e:
            logger.info(f"Could not describe QuickSight dashboard {dashboard_id}: {e}")
            return {}

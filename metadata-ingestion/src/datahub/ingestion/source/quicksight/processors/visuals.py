import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import (
    DEFAULT_CHART_TYPE,
    SKIP_VISUAL_TYPES,
    VISUAL_TYPE_TO_CHART_TYPE,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import (
    PLATFORM,
    chart_id_from_visual,
    id_from_arn,
    make_dataset_urn,
)
from datahub.sdk.chart import Chart
from datahub.sdk.container import Container

logger = logging.getLogger(__name__)

# QuickSight visual titles use a lightweight markup wrapper, e.g.
# "<visual-title>\n  <b>Total Revenue</b>\n</visual-title>". Strip all tags to
# recover the human-readable label.
_TITLE_TAG_RE = re.compile(r"<[^>]+>")


class VisualsExtractor:
    """Builds DataHub Chart entities from a QuickSight analysis/dashboard
    ``Definition`` (``Sheets[].Visuals[]``).

    This is invoked by the analyses and dashboards processors (gated on the
    ``extract_*_definitions`` toggles) rather than run standalone, so the parent
    Dashboard entity can carry the resulting Chart edges in a single emission.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api

    def extract(
        self,
        parent_id: str,
        definition: Dict[str, Any],
        parent_container: Optional[Container],
    ) -> Tuple[List[MetadataWorkUnit], List[str]]:
        """Return ``(chart_workunits, chart_urns)`` for one definition.

        ``chart_urns`` is returned so the caller can attach the charts to the
        parent Dashboard's ``charts`` edge. ``parent_container`` is the parent
        Analysis/Dashboard's resolved container, so charts share its placement.
        """
        identifier_to_arn = {
            decl["Identifier"]: decl.get("DataSetArn", "")
            for decl in definition.get("DataSetIdentifierDeclarations") or []
            if decl.get("Identifier")
        }

        workunits: List[MetadataWorkUnit] = []
        chart_urns: List[str] = []

        for sheet in definition.get("Sheets") or []:
            for visual in sheet.get("Visuals") or []:
                if not visual:
                    continue
                result = self._build_chart(
                    parent_id, visual, identifier_to_arn, parent_container
                )
                if result is None:
                    continue
                chart, chart_urn = result
                workunits.extend(chart.as_workunits())
                chart_urns.append(chart_urn)

        return workunits, chart_urns

    def _build_chart(
        self,
        parent_id: str,
        visual: Dict[str, Any],
        identifier_to_arn: Dict[str, str],
        parent_container: Optional[Container],
    ) -> Optional[Tuple[Chart, str]]:
        # Each visual dict has exactly one key naming its type, e.g. "KPIVisual".
        visual_type = next(iter(visual.keys()), None)
        if not visual_type:
            return None
        inner = visual.get(visual_type) or {}
        visual_id = inner.get("VisualId")
        if not visual_id:
            return None

        if visual_type in SKIP_VISUAL_TYPES:
            self.report.charts.dropped(f"{parent_id}/{visual_id} ({visual_type})")
            return None

        chart_id = chart_id_from_visual(parent_id, visual_id)
        chart_type = VISUAL_TYPE_TO_CHART_TYPE.get(visual_type, DEFAULT_CHART_TYPE)
        title = self._extract_title(inner) or visual_id

        dataset_urns = self._input_datasets(inner, identifier_to_arn)

        custom_properties = {
            "visualId": visual_id,
            "quicksightVisualType": visual_type,
        }

        chart = Chart(
            platform=PLATFORM,
            name=chart_id,
            platform_instance=self.config.platform_instance,
            display_name=title,
            subtype=visual_type,
            chart_type=chart_type,
            custom_properties=custom_properties,
            parent_container=parent_container if parent_container is not None else [],
            input_datasets=dataset_urns or None,
        )
        self.report.charts.processed(f"{chart_id} ({visual_type})")
        return chart, str(chart.urn)

    def _input_datasets(
        self, inner: Dict[str, Any], identifier_to_arn: Dict[str, str]
    ) -> List[str]:
        """Resolve the QuickSight Datasets a visual reads.

        Visual configs reference datasets by ``DataSetIdentifier`` deep inside
        their field wells; the shape varies wildly across ~25 visual types, so we
        recursively collect every identifier rather than hard-coding per-type
        paths, then map identifiers to dataset URNs via the definition-level
        declarations.
        """
        identifiers = _collect_dataset_identifiers(inner)
        urns: List[str] = []
        for identifier in sorted(identifiers):
            arn = identifier_to_arn.get(identifier)
            if not arn:
                continue
            urns.append(
                make_dataset_urn(
                    self.api.aws_account_id,
                    id_from_arn(arn),
                    self.config.platform_instance,
                    self.config.env,
                )
            )
        return urns

    @staticmethod
    def _extract_title(inner: Dict[str, Any]) -> Optional[str]:
        format_text = (inner.get("Title") or {}).get("FormatText") or {}
        raw = format_text.get("RichText") or format_text.get("PlainText")
        if not raw:
            return None
        cleaned = _TITLE_TAG_RE.sub("", raw).strip()
        return cleaned or None


def _collect_dataset_identifiers(node: Any) -> Set[str]:
    """Recursively gather all ``DataSetIdentifier`` string values in a visual."""
    found: Set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "DataSetIdentifier" and isinstance(value, str):
                found.add(value)
            else:
                found |= _collect_dataset_identifiers(value)
    elif isinstance(node, list):
        for item in node:
            found |= _collect_dataset_identifiers(item)
    return found

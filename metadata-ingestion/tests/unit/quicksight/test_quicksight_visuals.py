from typing import Any, Dict, List, Optional
from unittest import mock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import (
    QuickSightNamespaceKey,
)
from datahub.ingestion.source.quicksight.processors.visuals import VisualsExtractor
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartTypeClass,
    SubTypesClass,
)

PARENT_ID = "dash-1"
DS_IDENTIFIER = "id-abc"
DS_ARN = "arn:aws:quicksight:us-east-1:064369473231:dataset/ds-1"
DS_URN = "urn:li:dataset:(urn:li:dataPlatform:quicksight,064369473231.ds-1,PROD)"


def _namespace_key() -> QuickSightNamespaceKey:
    return QuickSightNamespaceKey(
        platform="quicksight",
        instance=None,
        env="PROD",
        account_id="064369473231",
        namespace="default",
    )


def _extractor(config_dict=None) -> VisualsExtractor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    return VisualsExtractor(config, QuickSightSourceReport(), api)


def _visual(
    visual_type: str, visual_id: str, *, title: Optional[str] = None, with_ds=True
) -> Dict[str, Any]:
    inner: Dict[str, Any] = {"VisualId": visual_id}
    if title is not None:
        inner["Title"] = {
            "FormatText": {"RichText": f"<visual-title>{title}</visual-title>"}
        }
    if with_ds:
        inner["ChartConfiguration"] = {
            "FieldWells": {"Values": [{"Column": {"DataSetIdentifier": DS_IDENTIFIER}}]}
        }
    return {visual_type: inner}


def _definition(visuals: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "DataSetIdentifierDeclarations": [
            {"Identifier": DS_IDENTIFIER, "DataSetArn": DS_ARN}
        ],
        "Sheets": [{"SheetId": "sheet-1", "Visuals": visuals}],
    }


def _chart_info(
    workunits: List[MetadataWorkUnit], name: str
) -> Optional[ChartInfoClass]:
    for wu in workunits:
        if name in wu.metadata.entityUrn:  # type: ignore[union-attr]
            aspect = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect, ChartInfoClass):
                return aspect
    return None


def test_extract_emits_one_chart_per_visual_with_type_and_lineage():
    extractor = _extractor()
    definition = _definition(
        [
            _visual("KPIVisual", "v1", title="Total Revenue"),
            _visual("LineChartVisual", "v2", title="Trend"),
        ]
    )

    workunits, chart_urns = extractor.extract(PARENT_ID, definition, _namespace_key())

    assert chart_urns == [
        "urn:li:chart:(quicksight,dash-1_v1)",
        "urn:li:chart:(quicksight,dash-1_v2)",
    ]
    kpi = _chart_info(workunits, "dash-1_v1")
    assert kpi is not None
    assert kpi.type == ChartTypeClass.TABLE  # KPIVisual falls back to TABLE
    assert kpi.title == "Total Revenue"
    assert kpi.inputs == [DS_URN]
    assert kpi.customProperties["quicksightVisualType"] == "KPIVisual"

    line = _chart_info(workunits, "dash-1_v2")
    assert line is not None
    assert line.type == ChartTypeClass.LINE


def test_empty_visual_is_skipped():
    extractor = _extractor()
    definition = _definition(
        [
            _visual("KPIVisual", "v1", title="Keep"),
            {"EmptyVisual": {"VisualId": "v-empty"}},
        ]
    )

    workunits, chart_urns = extractor.extract(PARENT_ID, definition, _namespace_key())

    assert chart_urns == ["urn:li:chart:(quicksight,dash-1_v1)"]
    assert "dash-1/v-empty (EmptyVisual)" in extractor.report.charts.dropped_entities


def test_unknown_visual_type_falls_back_to_bar():
    extractor = _extractor()
    definition = _definition([_visual("FutureGizmoVisual", "v9", title="Mystery")])

    workunits, _ = extractor.extract(PARENT_ID, definition, _namespace_key())

    info = _chart_info(workunits, "dash-1_v9")
    assert info is not None
    assert info.type == ChartTypeClass.BAR
    assert info.customProperties["quicksightVisualType"] == "FutureGizmoVisual"


def test_visual_id_already_prefixed_is_not_doubled():
    extractor = _extractor()
    definition = _definition(
        [_visual("BarChartVisual", "dash-1_sheet-1_v1", title="T")]
    )

    _, chart_urns = extractor.extract(PARENT_ID, definition, _namespace_key())

    assert chart_urns == ["urn:li:chart:(quicksight,dash-1_sheet-1_v1)"]


def test_visual_without_dataset_identifier_has_no_inputs():
    extractor = _extractor()
    definition = _definition([_visual("TableVisual", "v1", title="T", with_ds=False)])

    workunits, _ = extractor.extract(PARENT_ID, definition, _namespace_key())

    info = _chart_info(workunits, "dash-1_v1")
    assert info is not None
    assert not info.inputs


def test_subtype_carries_raw_visual_type():
    extractor = _extractor()
    definition = _definition([_visual("PieChartVisual", "v1", title="Slices")])

    workunits, _ = extractor.extract(PARENT_ID, definition, _namespace_key())

    subtypes = [
        wu.metadata.aspect  # type: ignore[union-attr]
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), SubTypesClass)
    ]
    assert any("PieChartVisual" in st.typeNames for st in subtypes)

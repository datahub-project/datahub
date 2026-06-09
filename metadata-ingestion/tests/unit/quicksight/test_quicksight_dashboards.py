from typing import List, Optional
from unittest import mock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import (
    QuickSightNamespaceKey,
)
from datahub.ingestion.source.quicksight.processors.dashboards import (
    DashboardsProcessor,
)
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import DashboardInfoClass


def _namespace_key() -> QuickSightNamespaceKey:
    return QuickSightNamespaceKey(
        platform="quicksight",
        instance=None,
        env="PROD",
        account_id="064369473231",
        namespace="default",
    )


def _enricher(api: mock.MagicMock, report: QuickSightSourceReport) -> AssetEnricher:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", "extract_ownership": False, "extract_tags": False}
    )
    return AssetEnricher(config, report, api)


def _processor(api: mock.MagicMock, config_dict=None) -> DashboardsProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return DashboardsProcessor(
        config,
        report,
        api,
        parent_resolver=lambda _id: _namespace_key(),
        enricher=_enricher(api, report),
    )


def _mock_api(summaries) -> mock.MagicMock:
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    api.list_dashboards.return_value = summaries
    # Definition fetch is exercised separately; default to no visuals here.
    api.describe_dashboard_definition.return_value = {}
    return api


def _aspect(workunits: List[MetadataWorkUnit], cls):
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, cls):
            return aspect
    return None


def _source_dashboards(version: dict) -> Optional[List[str]]:
    processor = _processor(_mock_api([]))
    urns = processor._source_analysis_dashboards(version)
    return urns or None


def test_dashboard_emits_input_dataset_lineage():
    api = _mock_api([{"DashboardId": "dash-1", "Name": "Exec"}])
    api.describe_dashboard.return_value = {
        "Version": {
            "DataSetArns": [
                "arn:aws:quicksight:us-east-1:064369473231:dataset/ds-1",
            ]
        }
    }
    processor = _processor(api)

    workunits = list(processor.get_workunits())

    info = _aspect(workunits, DashboardInfoClass)
    assert info is not None
    assert [e.destinationUrn for e in (info.datasetEdges or [])] == [
        "urn:li:dataset:(urn:li:dataPlatform:quicksight,064369473231.ds-1,PROD)"
    ]
    assert "dash-1 (Exec)" in processor.report.dashboards.processed_entities


def test_dashboard_links_to_source_analysis():
    api = _mock_api([{"DashboardId": "dash-1", "Name": "Exec"}])
    api.describe_dashboard.return_value = {
        "Version": {
            "SourceEntityArn": "arn:aws:quicksight:us-east-1:064369473231:analysis/an-1",
        }
    }
    processor = _processor(api)

    workunits = list(processor.get_workunits())

    info = _aspect(workunits, DashboardInfoClass)
    assert info is not None
    assert [e.destinationUrn for e in (info.dashboards or [])] == [
        "urn:li:dashboard:(quicksight,an-1)"
    ]


def test_no_link_when_source_is_not_an_analysis():
    # SourceEntityArn pointing at a template must not create a dashboard link.
    assert (
        _source_dashboards(
            {
                "SourceEntityArn": "arn:aws:quicksight:us-east-1:064369473231:template/tmpl-1"
            }
        )
        is None
    )
    assert _source_dashboards({}) is None


def test_dashboard_pattern_filters_by_name():
    api = _mock_api(
        [
            {"DashboardId": "dash-1", "Name": "keep"},
            {"DashboardId": "dash-2", "Name": "drop"},
        ]
    )
    api.describe_dashboard.return_value = {"Version": {}}
    processor = _processor(api, {"dashboard_pattern": {"deny": ["drop"]}})

    list(processor.get_workunits())

    assert "dash-1 (keep)" in processor.report.dashboards.processed_entities
    assert "dash-2 (drop)" in processor.report.dashboards.dropped_entities

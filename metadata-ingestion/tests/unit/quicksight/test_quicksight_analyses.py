from typing import List
from unittest import mock

from botocore.exceptions import ClientError

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.analyses import AnalysesProcessor
from datahub.ingestion.source.quicksight.processors.containers import (
    QuickSightNamespaceKey,
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


def _processor(api: mock.MagicMock, config_dict=None) -> AnalysesProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return AnalysesProcessor(
        config,
        report,
        api,
        parent_resolver=lambda _id: _namespace_key(),
        enricher=_enricher(api, report),
    )


def _mock_api(summaries) -> mock.MagicMock:
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    api.list_analyses.return_value = summaries
    # Definition fetch is exercised separately; default to no visuals here.
    api.describe_analysis_definition.return_value = {}
    return api


def _dataset_edges(workunits: List[MetadataWorkUnit]) -> List[str]:
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, DashboardInfoClass):
            return [edge.destinationUrn for edge in (aspect.datasetEdges or [])]
    return []


def test_analysis_emits_input_dataset_lineage():
    api = _mock_api([{"AnalysisId": "an-1", "Name": "Sales"}])
    api.describe_analysis.return_value = {
        "DataSetArns": [
            "arn:aws:quicksight:us-east-1:064369473231:dataset/ds-1",
        ],
        "Status": "CREATION_SUCCESSFUL",
    }
    processor = _processor(api)

    workunits = list(processor.get_workunits())

    assert "an-1 (Sales)" in processor.report.analyses.processed_entities
    assert _dataset_edges(workunits) == [
        "urn:li:dataset:(urn:li:dataPlatform:quicksight,064369473231.ds-1,PROD)"
    ]


def test_deleted_analyses_are_skipped():
    api = _mock_api([{"AnalysisId": "an-dead", "Name": "Old", "Status": "DELETED"}])
    processor = _processor(api)

    assert list(processor.get_workunits()) == []
    api.describe_analysis.assert_not_called()


def test_analysis_pattern_filters_by_name():
    api = _mock_api(
        [
            {"AnalysisId": "an-1", "Name": "keep"},
            {"AnalysisId": "an-2", "Name": "drop"},
        ]
    )
    api.describe_analysis.return_value = {"DataSetArns": []}
    processor = _processor(api, {"analysis_pattern": {"deny": ["drop"]}})

    list(processor.get_workunits())

    assert "an-1 (keep)" in processor.report.analyses.processed_entities
    assert "an-2 (drop)" in processor.report.analyses.dropped_entities


def test_describe_failure_still_emits_summary_only_entity():
    api = _mock_api([{"AnalysisId": "an-1", "Name": "Sales"}])
    api.describe_analysis.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
        "DescribeAnalysis",
    )
    processor = _processor(api)

    workunits = list(processor.get_workunits())

    assert workunits  # entity still emitted
    assert _dataset_edges(workunits) == []  # no lineage without describe

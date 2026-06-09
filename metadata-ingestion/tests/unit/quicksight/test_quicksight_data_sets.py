from typing import Any, Dict, List, Optional
from unittest import mock

from botocore.exceptions import ClientError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.quicksight.processors.containers import (
    QuickSightNamespaceKey,
)
from datahub.ingestion.source.quicksight.processors.data_sets import DataSetsProcessor
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from datahub.sdk.container import Container


def _parent_container() -> Container:
    return Container(
        QuickSightNamespaceKey(
            platform="quicksight",
            instance=None,
            env="PROD",
            account_id="064369473231",
            namespace="default",
        ),
        display_name="default",
        subtype="Namespace",
    )


def _enricher(api: mock.MagicMock, report: QuickSightSourceReport) -> AssetEnricher:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", "extract_ownership": False, "extract_tags": False}
    )
    return AssetEnricher(config, report, api)


def _processor(
    api: mock.MagicMock, config_dict: Optional[Dict[str, Any]] = None
) -> DataSetsProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return DataSetsProcessor(
        config,
        report,
        api,
        PipelineContext(run_id="test"),
        parent_resolver=lambda _id: _parent_container(),
        data_source_map={},
        enricher=_enricher(api, report),
    )


def _mock_api(summaries: List[Dict[str, Any]]) -> mock.MagicMock:
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    api.list_data_sets.return_value = summaries
    return api


def test_build_schema_maps_column_types():
    fields = DataSetsProcessor._build_schema(
        [
            {"Name": "id", "Type": "INTEGER"},
            {"Name": "region", "Type": "STRING"},
            {"Name": "weird", "Type": "MYSTERY"},
        ]
    )

    by_path = {f.fieldPath: f for f in fields}
    assert isinstance(by_path["id"].type.type, NumberTypeClass)
    assert isinstance(by_path["region"].type.type, StringTypeClass)
    # Unknown QuickSight types fall back to NullType rather than failing.
    assert isinstance(by_path["weird"].type.type, NullTypeClass)


def test_file_type_dataset_degrades_to_summary_only():
    api = _mock_api([{"DataSetId": "ds-file", "Name": "CSV Upload"}])
    api.describe_data_set.side_effect = ClientError(
        {"Error": {"Code": "InvalidParameterValueException", "Message": "FILE"}},
        "DescribeDataSet",
    )
    processor = _processor(api)

    workunits = list(processor.get_workunits())

    assert workunits  # the entity is still emitted (summary-only)
    assert processor.report.num_file_datasets_summary_only == 1


def test_non_file_describe_error_propagates():
    api = _mock_api([{"DataSetId": "ds-1", "Name": "X"}])
    api.describe_data_set.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
        "DescribeDataSet",
    )
    processor = _processor(api)

    raised = False
    try:
        list(processor.get_workunits())
    except ClientError:
        raised = True
    assert raised


def test_dataset_pattern_filters_out_disallowed_datasets():
    api = _mock_api(
        [
            {"DataSetId": "ds-1", "Name": "keep"},
            {"DataSetId": "ds-2", "Name": "drop"},
        ]
    )
    api.describe_data_set.return_value = {"OutputColumns": [], "PhysicalTableMap": {}}
    processor = _processor(api, {"dataset_pattern": {"deny": ["drop"]}})

    list(processor.get_workunits())

    assert "ds-1 (keep)" in processor.report.datasets.processed_entities
    assert "ds-2 (drop)" in processor.report.datasets.dropped_entities


def test_dataset_invokes_lineage_extraction():
    api = _mock_api([{"DataSetId": "ds-1", "Name": "Orders"}])
    api.describe_data_set.return_value = {
        "OutputColumns": [{"Name": "order_id", "Type": "INTEGER"}],
        "PhysicalTableMap": {"t1": {"RelationalTable": {"DataSourceArn": "arn"}}},
    }
    processor = _processor(api)

    with mock.patch.object(
        processor.lineage_extractor, "get_upstream_lineage", return_value=None
    ) as mocked:
        list(processor.get_workunits())

    mocked.assert_called_once()

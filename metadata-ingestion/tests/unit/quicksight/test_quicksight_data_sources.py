from typing import Any, Dict, List, Optional
from unittest import mock

from datahub.ingestion.source.quicksight.processors.containers import (
    QuickSightNamespaceKey,
)
from datahub.ingestion.source.quicksight.processors.data_sources import (
    DataSourcesProcessor,
)
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.sdk.container import Container

_ARN_PREFIX = "arn:aws:quicksight:us-east-1:064369473231:datasource"


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
) -> DataSourcesProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return DataSourcesProcessor(
        config,
        report,
        api,
        parent_resolver=lambda _id: _parent_container(),
        enricher=_enricher(api, report),
    )


def _mock_api(summaries: List[Dict[str, Any]]) -> mock.MagicMock:
    api = mock.MagicMock()
    api.aws_account_id = "064369473231"
    api.list_data_sources.return_value = summaries
    api.describe_data_source.return_value = {}
    return api


def test_resolves_known_type_to_platform_and_dialect():
    api = _mock_api(
        [
            {
                "Arn": f"{_ARN_PREFIX}/ds-1",
                "DataSourceId": "ds-1",
                "Name": "Athena DS",
                "Type": "ATHENA",
            }
        ]
    )
    processor = _processor(api)
    list(processor.get_workunits())

    resolved = processor.data_source_map[f"{_ARN_PREFIX}/ds-1"]
    assert resolved.platform == "athena"
    assert resolved.dialect == "athena"


def test_unknown_type_resolves_to_no_platform_and_is_counted():
    api = _mock_api(
        [
            {
                "Arn": f"{_ARN_PREFIX}/ds-2",
                "DataSourceId": "ds-2",
                "Name": "Mystery",
                "Type": "MYSTERY",
            }
        ]
    )
    processor = _processor(api)
    list(processor.get_workunits())

    resolved = processor.data_source_map[f"{_ARN_PREFIX}/ds-2"]
    assert resolved.platform is None
    assert processor.report.num_unknown_data_source_types == 1


def test_data_source_pattern_filters_out_disallowed_sources():
    api = _mock_api(
        [
            {
                "Arn": f"{_ARN_PREFIX}/ds-1",
                "DataSourceId": "ds-1",
                "Name": "keep-me",
                "Type": "ATHENA",
            },
            {
                "Arn": f"{_ARN_PREFIX}/ds-2",
                "DataSourceId": "ds-2",
                "Name": "drop-me",
                "Type": "ATHENA",
            },
        ]
    )
    processor = _processor(api, {"data_source_pattern": {"deny": ["drop-me"]}})
    list(processor.get_workunits())

    assert f"{_ARN_PREFIX}/ds-1" in processor.data_source_map
    assert f"{_ARN_PREFIX}/ds-2" not in processor.data_source_map


def test_connection_parameters_flattened_into_properties():
    props = DataSourcesProcessor._connection_properties(
        {"DataSourceParameters": {"AthenaParameters": {"WorkGroup": "primary"}}}
    )
    assert props["AthenaParameters.WorkGroup"] == "primary"


def test_describe_failure_degrades_to_summary_only():
    api = _mock_api([])
    api.describe_data_source.side_effect = Exception("AccessDenied")
    processor = _processor(api)

    # A failed describe must not drop the entity — it degrades to empty details.
    assert processor._describe("ds-1") == {}


def test_s3_manifest_uri_extracted_for_s3_sources():
    api = _mock_api(
        [
            {
                "Arn": f"{_ARN_PREFIX}/ds-3",
                "DataSourceId": "ds-3",
                "Name": "S3 DS",
                "Type": "S3",
            }
        ]
    )
    api.describe_data_source.return_value = {
        "DataSourceParameters": {
            "S3Parameters": {
                "ManifestFileLocation": {
                    "Bucket": "my-bucket",
                    "Key": "data/manifest.json",
                }
            }
        }
    }
    processor = _processor(api)
    list(processor.get_workunits())

    resolved = processor.data_source_map[f"{_ARN_PREFIX}/ds-3"]
    assert resolved.s3_manifest_uri == "my-bucket/data/manifest.json"

from typing import Any, Dict, List, Optional
from unittest import mock

from datahub.ingestion.source.quicksight.extractors.data_sources import (
    DataSourcesExtractor,
)
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)

_ARN_PREFIX = "arn:aws:quicksight:us-east-1:064369473231:datasource"


def _processor(
    api: mock.MagicMock, config_dict: Optional[Dict[str, Any]] = None
) -> DataSourcesExtractor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    report = QuickSightSourceReport()
    return DataSourcesExtractor(config, report, api)


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
    processor.build_data_source_map()

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
    processor.build_data_source_map()

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
    processor.build_data_source_map()

    assert f"{_ARN_PREFIX}/ds-1" in processor.data_source_map
    assert f"{_ARN_PREFIX}/ds-2" not in processor.data_source_map


def test_describe_failure_degrades_gracefully():
    api = _mock_api([])
    api.describe_data_source.side_effect = Exception("AccessDenied")
    processor = _processor(api)

    # A failed describe must not raise — it degrades to empty connection details.
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
    processor.build_data_source_map()

    resolved = processor.data_source_map[f"{_ARN_PREFIX}/ds-3"]
    assert resolved.s3_manifest_uri == "my-bucket/data/manifest.json"

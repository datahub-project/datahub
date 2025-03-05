import datetime
import json
import os
import pathlib
import tempfile
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Union,
)
from unittest.mock import Mock, patch

import polars
import pyarrow as pa
import pytest
from freezegun import freeze_time
from polars import DataType
from polars.datatypes import DataTypeClass

from tests.utils import PytestConfig

from datahub.ingestion.api.common import PipelineContext

if TYPE_CHECKING:
    from _pytest.config.argparsing import Parser

from acryl_datahub_cloud.datahub_usage_reporting.usage_feature_reporter import (
    DataHubUsageFeatureReportingSource,
    DataHubUsageFeatureReportingSourceConfig,
    FreshnessFactor,
    RankingPolicy,
    UsagePercentileFactor,
)
from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing.compare_metadata_json import assert_metadata_files_equal

FROZEN_TIME = "2024-07-11 07:00:00"


def test_polars_to_arrow_schema() -> None:
    # Create a sample Polars schema
    polars_schema: Dict[str, Union[DataTypeClass, DataType]] = {
        "column1": polars.Int32(),
        "column2": polars.Float64(),
        "column3": polars.Utf8(),
        "column4": polars.String(),
        "column5": polars.Struct({"field1": polars.Int32(), "field2": polars.Utf8()}),
        "column6": polars.List(polars.Int32()),
    }

    # Expected Arrow schema
    expected_arrow_schema = pa.schema(
        [
            pa.field("column1", pa.int32()),
            pa.field("column2", pa.float64()),
            pa.field("column3", pa.string()),
            pa.field("column4", pa.string()),
            pa.field(
                "column5",
                pa.struct(
                    [pa.field("field1", pa.int32()), pa.field("field2", pa.string())]
                ),
            ),
            pa.field("column6", pa.list_(pa.int32())),
        ]
    )

    # Call the method
    arrow_schema = DataHubUsageFeatureReportingSource.polars_to_arrow_schema(
        polars_schema
    )

    # Assert the result
    assert arrow_schema == expected_arrow_schema


@freeze_time(FROZEN_TIME)
def test_search_search_score_with_zero_usage_percentile() -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=False,
        experimental_full_streaming=False,
        use_server_side_aggregation=True,
        generate_patch=False,
        disable_write_usage=False,
        set_upstream_table_max_modification_time_for_views=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )

    source = DataHubUsageFeatureReportingSource(
        ctx=PipelineContext(run_id="usage-source-test"), config=config
    )
    multipliers = source.search_score(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,mydb.my_schema_my_table_1,PROD)",
        1234,
        0,
    )
    assert multipliers.usageSearchScoreMultiplier == 0.5
    assert multipliers.usageFreshnessScoreMultiplier == 0.4
    assert multipliers.customDatahubScoreMultiplier == 1.0
    # The search Ranking multiplier's minimum value is 1. It can't be less than 1.
    assert multipliers.combinedSearchRankingMultiplier == 1


@freeze_time(FROZEN_TIME)
def test_search_search_score_with_zero_freshness() -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=False,
        experimental_full_streaming=False,
        use_server_side_aggregation=True,
        disable_write_usage=False,
        generate_patch=False,
        set_upstream_table_max_modification_time_for_views=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )

    source = DataHubUsageFeatureReportingSource(
        ctx=PipelineContext(run_id="usage-source-test"), config=config
    )
    multipliers = source.search_score(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,mydb.my_schema_my_table_1,PROD)",
        int(datetime.datetime.now().timestamp() * 1000),
        0,
    )
    assert multipliers.usageSearchScoreMultiplier == 0.5
    assert multipliers.usageFreshnessScoreMultiplier == 3.6
    assert multipliers.customDatahubScoreMultiplier == 1.0
    assert multipliers.combinedSearchRankingMultiplier == 1.8


def load_data_from_es_mock(
    test_file_prefix: str,
    index: str,
    query: Dict,
    process_function: Callable,
    aggregation_key: Optional[str] = None,
) -> Iterable[Dict]:
    batch_size = 5000
    file_map = {
        "datasetindex_v2": f"tests/test_data/test_{test_file_prefix}_datasets.jsonl",
        "dataset_datasetusagestatisticsaspect_v1": f"tests/test_data/test_{test_file_prefix}_datasetusages.jsonl",
        "graph_service_v1": f"tests/test_data/test_{test_file_prefix}_graph_service.jsonl",
        "chartindex_v2": f"tests/test_data/test_{test_file_prefix}_charts.jsonl",
        "dashboard_dashboardusagestatisticsaspect_v1": f"tests/test_data/test_{test_file_prefix}_dashboardusages.jsonl",
        "dashboardindex_v2": f"tests/test_data/test_{test_file_prefix}_dashboards.jsonl",
        "queryindex_v2": f"tests/test_data/test_{test_file_prefix}_queries.jsonl",
        "query_queryusagestatisticsaspect_v1": f"tests/test_data/test_{test_file_prefix}_queryusages.jsonl",
    }

    if index in file_map:
        if os.path.isfile(file_map[index]):
            with open(file_map[index]) as f:
                batch = []
                for line in f:
                    json_line = json.loads(line)
                    batch.append(json_line)
                    if len(batch) % batch_size == 0:
                        yield from process_function(batch)
                        batch = []
                if batch:
                    yield from process_function(batch)
    elif index == "dataset_operationaspect_v1":
        return
    elif index == "chart_chartusagestatisticsaspect_v1":
        return
    else:
        raise AssertionError(f"Unhandled index {index}")


def pytest_addoption(parser: "Parser") -> None:
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )
    parser.addoption("--copy-output-files", action="store_true", default=False)


def run_and_get_pipeline(pipeline_config_dict: Dict[str, Any]) -> Pipeline:
    pipeline = Pipeline.create(pipeline_config_dict)
    pipeline.run()
    pipeline.raise_from_status()
    return pipeline


@pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dataset_usage(
    load_data_from_es: Mock, pytestconfig: PytestConfig, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        streaming_mode=False,
        experimental_full_streaming=False,
        set_upstream_table_max_modification_time_for_views=True,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        use_server_side_aggregation=True,
        disable_write_usage=False,
        generate_patch=False,
    )
    tmp_path = pathlib.Path(tempfile.mkdtemp("usage_feature_reporter_test"))
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{tmp_path}/{test_name}_mcps.json",
            },
        },
    }
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)

    run_and_get_pipeline(pipeline_config_dict)

    check_golden_file(
        output_path=pathlib.Path(f"{tmp_path}/{test_name}_mcps.json"),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


@pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dataset_usage_with_ranking_factors(
    load_data_from_es: Mock, pytestconfig: PytestConfig, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=False,
        experimental_full_streaming=False,
        use_server_side_aggregation=True,
        disable_write_usage=False,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=False,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 0], value=0.0),
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(tempfile.mkdtemp("usage_feature_reporter_ranking_test"))
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    run_and_get_pipeline(pipeline_config_dict)

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


def check_golden_file(
    output_path: pathlib.Path,
    golden_path: pathlib.Path,
    ignore_paths: Sequence[str] = (),
) -> None:
    assert_metadata_files_equal(
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=ignore_paths,
        ignore_order=True,
    )


@pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
# @pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dataset_usage_with_ranking_factors_patch_enabled(
    load_data_from_es: Mock, pytestconfig: PytestConfig, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=False,
        experimental_full_streaming=False,
        use_server_side_aggregation=True,
        disable_write_usage=True,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(
        tempfile.mkdtemp("usage_feature_reporter_ranking_test_patch_enabled")
    )
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.raise_from_status()

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking_patch.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


@pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
# @pytest.mark.parametrize("test_name", ["dataset_usage", "dataset_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dataset_usage_with_ranking_factors_patch_enabled_in_streaming(
    load_data_from_es: Mock, pytestconfig: PytestConfig, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=True,
        experimental_full_streaming=True,
        use_server_side_aggregation=True,
        disable_write_usage=True,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=True,
        lookback_days=30,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(
        tempfile.mkdtemp("usage_feature_reporter_ranking_test_patch_enabled")
    )
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"

    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.raise_from_status()

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking_patch.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


@pytest.mark.parametrize("test_name", ["dashboard_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dashboard_usage_with_ranking_factors_patch_enabled(
    load_data_from_es: Mock, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=True,
        chart_usage_enabled=True,
        dataset_usage_enabled=False,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=True,
        experimental_full_streaming=True,
        use_server_side_aggregation=True,
        disable_write_usage=True,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 0], value=0.0),
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(
        tempfile.mkdtemp("usage_feature_reporter_ranking_test_patch_enabled")
    )
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.raise_from_status()

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking_patch.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


@pytest.mark.parametrize("test_name", ["dashboard_usage_incremental_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_dashboard_increremental_usage_with_ranking_factors_patch_enabled(
    load_data_from_es: Mock, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=True,
        chart_usage_enabled=True,
        dataset_usage_enabled=False,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=True,
        experimental_full_streaming=True,
        use_server_side_aggregation=True,
        disable_write_usage=True,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 0], value=0.0),
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(
        tempfile.mkdtemp("usage_feature_reporter_ranking_test_patch_enabled")
    )
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.raise_from_status()

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking_patch.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )


@pytest.mark.parametrize("test_name", ["query_usage_small"])
@patch.object(DataHubUsageFeatureReportingSource, "load_data_from_es")
@freeze_time(FROZEN_TIME)
def test_query_usage_with_ranking_factors_patch_enabled(
    load_data_from_es: Mock, test_name: str
) -> None:
    config = DataHubUsageFeatureReportingSourceConfig(
        query_usage_enabled=True,
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=False,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=True,
        sibling_usage_enabled=False,
        streaming_mode=True,
        experimental_full_streaming=True,
        use_server_side_aggregation=False,
        disable_write_usage=True,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=True,
        ranking_policy=RankingPolicy(
            freshness_factors=[
                FreshnessFactor(age_in_days=[0, 7], value=3.6),
                FreshnessFactor(age_in_days=[7, 30], value=1.3),
                FreshnessFactor(age_in_days=[30, 90], value=0.6),
                FreshnessFactor(age_in_days=[90], value=0.4),
            ],
            usage_percentile_factors=[
                UsagePercentileFactor(percentile=[0, 0], value=0.0),
                UsagePercentileFactor(percentile=[0, 10], value=0.5),
                UsagePercentileFactor(percentile=[10, 20], value=0.6),
                UsagePercentileFactor(percentile=[20, 30], value=0.7),
                UsagePercentileFactor(percentile=[30, 40], value=0.8),
                UsagePercentileFactor(percentile=[40, 45], value=0.91),
                UsagePercentileFactor(percentile=[45, 50], value=1.0),
                UsagePercentileFactor(percentile=[50, 55], value=1.25),
                UsagePercentileFactor(percentile=[55, 60], value=1.5),
                UsagePercentileFactor(percentile=[60, 65], value=1.75),
                UsagePercentileFactor(percentile=[70, 75], value=2.0),
                UsagePercentileFactor(percentile=[75, 80], value=2.5),
                UsagePercentileFactor(percentile=[80, 85], value=2.75),
                UsagePercentileFactor(percentile=[85, 90], value=3.0),
                UsagePercentileFactor(percentile=[90, 92], value=3.5),
                UsagePercentileFactor(percentile=[92, 95], value=4.0),
                UsagePercentileFactor(percentile=[95, 97], value=5.0),
                UsagePercentileFactor(percentile=[97, 100], value=6.0),
            ],
        ),
    )
    tmp_path = pathlib.Path(
        tempfile.mkdtemp("usage_feature_reporter_ranking_test_patch_enabled")
    )
    mcp_output_file = f"{tmp_path}/{test_name}_ranking_mcps.json"
    load_data_from_es.side_effect = partial(load_data_from_es_mock, test_name)
    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "datahub-usage-reporting",
            "config": dict(config),
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{mcp_output_file}",
            },
        },
    }

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.raise_from_status()

    check_golden_file(
        output_path=pathlib.Path(mcp_output_file),
        golden_path=pathlib.Path(f"tests/golden/golden_{test_name}_ranking_patch.json"),
        ignore_paths=["root[*]['systemMetadata']['created']"],
    )

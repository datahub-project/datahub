import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.mark.integration
def test_data_lake_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/data_lake/test_data"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "data-lake-test",
            "source": {
                "type": "data-lake",
                "config": {
                    "include_path": str(test_resources_dir),
                    "platform": "test",
                    "include_field_min_value": True,
                    "include_field_max_value": True,
                    "include_field_mean_value": True,
                    "include_field_median_value": True,
                    "include_field_stddev_value": True,
                    "include_field_quantiles": True,
                    "include_field_distinct_value_frequencies": True,
                    "include_field_histogram": True,
                    "include_field_sample_values": True,
                    # "column_allow_deny_patterns" : ["exclude"],
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/data_lake_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "data_lake_mces.json",
        golden_path=tmp_path / "data_lake_mces_golden.json",
    )

    # # Run the metadata ingestion pipeline.
    # runner = CliRunner()
    # with fs_helpers.isolated_filesystem(tmp_path):
    #     config_file = (test_resources_dir / "data_lake_to_file.yml").resolve()
    #     result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
    #     assert_result_ok(result)

    #     # Verify the output.
    #     mce_helpers.check_golden_file(
    #         pytestconfig,
    #         output_path="data_lake_mces.json",
    #         golden_path=test_resources_dir / "data_lake_mces_golden.json",
    #     )

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers


@pytest.mark.integration
def test_excel(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/excel"
    test_files = "tests/integration/excel/data/*.xlsx"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "excel-test",
            "source": {
                "type": "excel",
                "config": {
                    "path_list": [
                        str(test_files),
                    ],
                    "profiling": {
                        "enabled": True,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/excel_file_test.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "excel_file_test.json",
        golden_path=test_resources_dir / "excel_file_test_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
        ],
    )

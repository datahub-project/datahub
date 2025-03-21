import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


@pytest.mark.integration
def test_hdf5_compound_dataset(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hdf5"
    test_file = test_resources_dir / "data/twodim.h5"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "hdf5-test",
            "source": {
                "type": "hdf5",
                "config": {
                    "path_list": [
                        str(test_file),
                    ],
                    "profiling": {
                        "enabled": True,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/hdf5_twodim_col_test.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "hdf5_twodim_col_test.json",
        golden_path=test_resources_dir / "hdf5_twodim_col_test_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
        ],
    )

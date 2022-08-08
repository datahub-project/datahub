import json
import logging
import os

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"

SOURCE_FILES_PATH = "./tests/integration/delta_lake/sources/local"
source_files = os.listdir(SOURCE_FILES_PATH)


@pytest.mark.parametrize("source_file", source_files)
def test_delta_lake(pytestconfig, source_file, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/delta_lake"

    f = open(os.path.join(SOURCE_FILES_PATH, source_file))
    source = json.load(f)

    config_dict = {}
    config_dict["source"] = source
    config_dict["sink"] = {
        "type": "file",
        "config": {
            "filename": f"{tmp_path}/{source_file}",
        },
        # "type": "datahub-rest",
        # "config": {"server": "http://localhost:8080"}
    }

    config_dict["run_id"] = source_file

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    print(f"tmp pth: {tmp_path}")
    print(f"source file : {source_file}")
    print(f"testresource dir: {test_resources_dir}")
    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden_files/local/golden_mces_{source_file}",
    )


def test_data_lake_incorrect_config_raises_error(tmp_path, mock_time):
    config_dict = {}
    config_dict["sink"] = {
        "type": "file",
        "config": {
            "filename": f"{tmp_path}/mces.json",
        },
        # "type": "datahub-rest",
        # "config": {"server": "http://localhost:8080"}
    }

    # Case 1 : named variable in table name is not present in include
    source = {
        "type": "delta-lake",
        "config": {"base_path": "invalid/path"},
    }
    config_dict["source"] = source
    with pytest.raises(Exception) as e_info:
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

    logging.debug(e_info)

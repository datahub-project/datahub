import os
import pytest
import subprocess
import shutil

import mce_helpers


def test_serde_large(pytestconfig, tmp_path):
    config_filename = "file_to_file_large.yml"
    json_filename = "test_serde_large.json"
    output_filename = "output_large.json"

    test_resources_dir = pytestconfig.rootpath / "tests/unit/serde"

    config_file = test_resources_dir / config_filename
    shutil.copy(config_file, tmp_path)

    golden_file = test_resources_dir / json_filename
    shutil.copy(golden_file, tmp_path)

    ingest_command = f'cd {tmp_path} && gometa-ingest -c ./{config_filename}'
    ret = os.system(ingest_command)
    assert ret == 0

    output = mce_helpers.load_json_file(tmp_path / output_filename)
    golden = mce_helpers.load_json_file(golden_file)
    mce_helpers.assert_mces_equal(output, golden)

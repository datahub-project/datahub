import os
import pytest
import subprocess

import mce_helpers



def test_ingest(mysql, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mysql"

    config_file=(test_resources_dir / "mysql_to_file.yml").resolve()
    ingest_command=f'cd {tmp_path} && gometa-ingest -c {config_file}'
    ret = os.system(ingest_command)
    assert ret == 0

    output = mce_helpers.load_json_file(str(tmp_path / "mysql_mces.json"))
    golden = mce_helpers.load_json_file(str(test_resources_dir / "mysql_mce_golden.json"))
    mce_helpers.assert_mces_equal(output, golden)

import os
import pytest
import subprocess


def test_ingest(mysql, pytestconfig):
    config_file=os.path.join(str(pytestconfig.rootdir), "tests/integration/mysql", "mysql_to_file.yml")
    # delete the output directory. TODO: move to a better way to create an output test fixture
    os.system("rm -rf output")
    ingest_command=f'gometa-ingest -c {config_file}'
    ret = os.system(ingest_command)
    assert ret == 0


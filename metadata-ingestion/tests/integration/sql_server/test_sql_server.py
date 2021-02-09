import os
import pytest
import subprocess


import time
def test_ingest(sql_server, pytestconfig):
    docker="docker"
    command = f"{docker} exec testsqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'test!Password' -d master -i /setup/setup.sql"
    ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
    assert ret.returncode == 0
    config_file=os.path.join(str(pytestconfig.rootdir), "tests/integration/sql_server", "mssql_to_file.yml")
    # delete the output directory. TODO: move to a better way to create an output test fixture
    os.system("rm -rf output")
    ingest_command=f'gometa-ingest -c {config_file}'
    ret = os.system(ingest_command)
    assert ret == 0


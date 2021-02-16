import os
import subprocess


def test_ingest(sql_server, pytestconfig):
    docker = "docker"
    command = f"{docker} exec testsqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'test!Password' -d master -i /setup/setup.sql"
    ret = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert ret.returncode == 0
    config_file = os.path.join(
        str(pytestconfig.rootdir), "tests/integration/sql_server", "mssql_to_file.yml"
    )
    ingest_command = f'datahub ingest -c {config_file}'
    ret = os.system(ingest_command)
    assert ret == 0
    # TODO: move to a better way to create an output test fixture
    os.system("rm ./mssql_mces.json")

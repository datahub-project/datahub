import os
import subprocess

import mce_helpers


def test_mssql_ingest(sql_server, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_server"

    # Run the setup.sql file to populate the database.
    docker = "docker"
    command = f"{docker} exec testsqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'test!Password' -d master -i /setup/setup.sql"
    ret = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert ret.returncode == 0

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "mssql_to_file.yml").resolve()
    ingest_command = f'cd {tmp_path} && datahub ingest -c {config_file}'
    ret = os.system(ingest_command)
    assert ret == 0

    # Verify the output.
    output = mce_helpers.load_json_file(str(tmp_path / "mssql_mces.json"))
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "mssql_mce_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)

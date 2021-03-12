import subprocess

import fs_helpers
import mce_helpers
from click.testing import CliRunner

from datahub.entrypoints import datahub


def test_mssql_ingest(sql_server, pytestconfig, tmp_path, mock_time):
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
    runner = CliRunner()
    with fs_helpers.isolated_filesystem(tmp_path):
        result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
        assert result.exit_code == 0

        output = mce_helpers.load_json_file("mssql_mces.json")

    # Verify the output.
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "mssql_mce_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)

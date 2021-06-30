import subprocess

import pytest
from click.testing import CliRunner

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.slow
def test_hive_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hive"
    ) as docker_services:
        wait_for_port(docker_services, "testhiveserver2", 10000, timeout=120)

        # Set up the container.
        command = "docker exec testhiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
        subprocess.run(command, shell=True, check=True)

        # Run the metadata ingestion pipeline.
        runner = CliRunner()
        with fs_helpers.isolated_filesystem(tmp_path):
            config_file = (test_resources_dir / "hive_to_file.yml").resolve()
            result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
            assert result.exit_code == 0

            output = mce_helpers.load_json_file("hive_mces.json")

        # Verify the output.
        golden = mce_helpers.load_json_file(
            str(test_resources_dir / "hive_mces_golden.json")
        )
        mce_helpers.assert_mces_equal(output, golden)

import pytest
from click.testing import CliRunner

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.slow
def test_mysql_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mysql"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(docker_services, "testmysql", 3306)

        # Run the metadata ingestion pipeline.
        runner = CliRunner()
        with fs_helpers.isolated_filesystem(tmp_path):
            config_file = (test_resources_dir / "mysql_to_file.yml").resolve()
            result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
            assert result.exit_code == 0

            # Verify the output.
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path="mysql_mces.json",
                golden_path=test_resources_dir / "mysql_mces_golden.json",
            )

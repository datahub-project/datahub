import subprocess

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2022-01-29 13:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_cassandra_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/cassandra"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "cassandra"
    ) as docker_services:
        wait_for_port(docker_services, "cassandra_datahub_test", 9042, timeout=120)

        command = "docker exec cassandra_datahub_test cqlsh -f /cassandra_setup.cql"
        subprocess.run(command,
                       shell=True,
                       check=True)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "cassandra_to_file.yml").resolve()
        run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "cassandra_mces.json",
            golden_path=test_resources_dir / "cassandra_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS + [
                r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            ]
        )

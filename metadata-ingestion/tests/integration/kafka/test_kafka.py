import subprocess

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka"
    ) as docker_services:
        wait_for_port(docker_services, "test_broker", 59092, timeout=120)
        wait_for_port(docker_services, "test_schema_registry", 8081, timeout=120)

        # Set up topics and produce some data
        command = f"{test_resources_dir}/send_records.sh {test_resources_dir}"
        subprocess.run(command, shell=True, check=True)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "kafka_to_file.yml").resolve()
        run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "kafka_mces.json",
            golden_path=test_resources_dir / "kafka_mces_golden.json",
            ignore_paths=[],
        )

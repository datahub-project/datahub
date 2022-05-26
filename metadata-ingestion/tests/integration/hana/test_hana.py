import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_integration
def test_hana_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hana"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hana"
    ) as docker_services:
        # added longer timeout and pause due to slow start of hana
        wait_for_port(
            docker_services=docker_services,
            container_name="testhana",
            container_port=39041,
            hostname="localhost",
            timeout=700,
            pause=50,
        )

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "hana_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "hana_mces.json",
            golden_path=test_resources_dir / "hana_mces_golden.json",
        )

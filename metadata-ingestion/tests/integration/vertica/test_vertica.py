import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
@pytest.mark.skip("This does not work yet and needs to be fixed.")
def test_vertica_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/vertica"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "vertica"
    ) as docker_services:
        wait_for_port(docker_services, "vertica_ce", 5433, timeout=120)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "vertica_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "vertica.json",
            golden_path=test_resources_dir / "vertica_golden.json",
        )

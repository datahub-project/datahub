import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2025-02-24 09:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/druid"


@pytest.fixture(scope="module", autouse=True)
def druid_up(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker" / "docker-compose.yml", "druid"
    ) as docker_services:
        wait_for_port(docker_services, "coordinator", 8081, timeout=120)
        wait_for_port(docker_services, "broker", 8082, timeout=120)
        yield docker_compose_runner


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_druid_ingest(
    test_resources_dir,
    pytestconfig,
    tmp_path,
):
    config_file = (test_resources_dir / "recipes" / "druid_to_file.yml").resolve()
    output_path = tmp_path / "druid_mces.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "golden" / "druid_mces.json",
        ignore_paths=[],
    )

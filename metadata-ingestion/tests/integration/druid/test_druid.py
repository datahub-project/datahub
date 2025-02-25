import pathlib

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2025-02-24 09:00:00"
TESTS_DIR = pathlib.Path(__file__).parent
GOLDEN_FILES_DIR = TESTS_DIR / "golden"
DOCKER_DIR = TESTS_DIR / "docker"
RECIPES_DIR = TESTS_DIR / "recipes"


@pytest.fixture(scope="module")
def druid_up(docker_compose_runner):
    with docker_compose_runner(
        DOCKER_DIR / "docker-compose.yml", "druid"
    ) as docker_services:
        wait_for_port(docker_services, "coordinator", 8081, timeout=120)
        wait_for_port(docker_services, "broker", 8082, timeout=120)
        yield


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_druid_ingest(
    pytestconfig,
    druid_up,
    tmp_path,
):
    config_file = (RECIPES_DIR / "druid_to_file.yml").resolve()
    output_path = tmp_path / "druid_mces.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_FILES_DIR / "druid_mces.json",
        ignore_paths=[],
    )

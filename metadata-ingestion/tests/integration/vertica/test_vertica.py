import subprocess
from typing import List

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/vertica"


def is_vertica_responsive(container_name: str) -> bool:
    cmd = f"docker logs {container_name} 2>&1 | grep 'Vertica is now running' "
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def vertica_runner(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "vertica"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "vertica-ce",
            5433,
            timeout=120,
            checker=lambda: is_vertica_responsive("vertica-ce"),
        )

        commands = """
                    docker cp tests/integration/vertica/ddl.sql vertica-ce:/home/dbadmin/ &&
                    docker exec vertica-ce sh -c "/opt/vertica/bin/vsql -w abc123 -f /home/dbadmin/ddl.sql"
                """

        ret = subprocess.run(commands, shell=True, stdout=subprocess.DEVNULL)

        assert ret.returncode == 0

        yield docker_services

    # The image is pretty large, so we remove it after the test.
    cleanup_image("vertica/vertica-ce")


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_vertica_ingest_with_db(vertica_runner, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/vertica"

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "vertica_to_file.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
    )

    ignore_paths: List[str] = [
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['create_time'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['table_size'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['Projection_size'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['ROS_Count'\]",
        r"root\[\d+\]\['aspect'\].+\['customProperties'\]\['cluster_size'\]",
        r"root\[\d+\]\['aspect'\].+\['customProperties'\]\['udx_language'\]",
    ]
    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "vertica.json",
        golden_path=test_resources_dir / "vertica_mces_with_db_golden.json",
    )

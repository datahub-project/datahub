import subprocess
from typing import List

import pytest

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

STARROCKS_FE_PORT = 9030


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/starrocks"


def is_starrocks_up(container_name: str) -> bool:
    """Check if StarRocks is ready to accept connections and has BE available."""
    try:
        cmd = f"docker exec {container_name} mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1'"
        ret = subprocess.run(cmd, shell=True, capture_output=True)
        return ret.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def starrocks_runner(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "starrocks"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "teststarrocks",
            STARROCKS_FE_PORT,
            timeout=180,
            checker=lambda: is_starrocks_up("teststarrocks"),
        )
        # Run setup SQL after StarRocks is ready
        setup_sql = test_resources_dir / "setup" / "setup.sql"
        cmd = "docker exec -i teststarrocks mysql -h 127.0.0.1 -P 9030 -u root"
        with open(setup_sql) as f:
            subprocess.run(cmd, shell=True, stdin=f, check=True)
        yield docker_services


@pytest.mark.integration
def test_starrocks_ingest(
    starrocks_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
):
    # Run the metadata ingestion pipeline
    config_file = (test_resources_dir / "starrocks_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Paths that may vary between runs
    ignore_paths: List[str] = [
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['create_time'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]\['time'\]",
        r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
        r"root\[\d+\]\['systemMetadata'\]\['lastRunId'\]",
    ]

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "starrocks_mces.json",
        golden_path=test_resources_dir / "starrocks_mces_golden.json",
    )

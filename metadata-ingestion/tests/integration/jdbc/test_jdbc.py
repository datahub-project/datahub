import logging
import subprocess
import time
import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FROZEN_TIME = "2023-10-15 07:00:00"

# Port configurations
POSTGRES_PORT = 45432
MYSQL_PORT = 43306
MSSQL_PORT = 41433


def start_containers():
    """Start containers using docker-compose."""
    logger.info("Starting containers with docker-compose...")
    ret = subprocess.run(
        ["docker-compose", "-f", "tests/integration/jdbc/docker-compose.yml", "up", "-d"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if ret.returncode != 0:
        logger.error(f"Failed to start containers: {ret.stderr.decode()}")
        raise RuntimeError("Failed to start containers.")
    logger.info("Containers started successfully.")


def wait_for_container(container_name: str, checker, timeout=120):
    """Wait for a container to become ready."""
    logger.info(f"Waiting for container {container_name} to become ready...")
    for i in range(timeout):
        if checker(container_name):
            logger.info(f"Container {container_name} is ready.")
            return
        time.sleep(1)
        logger.debug(f"Attempt {i + 1}/{timeout}: Container {container_name} not ready yet.")
    raise TimeoutError(f"Container {container_name} is not ready after {timeout} seconds.")


def is_postgres_up(container_name: str) -> bool:
    """Check if postgres is up."""
    cmd = f"docker logs {container_name} 2>&1 | grep 'database system is ready to accept connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def is_mysql_up(container_name: str) -> bool:
    """Check if mysql is up."""
    cmd = f"docker logs {container_name} 2>&1 | grep 'ready for connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def is_mssql_up(container_name: str) -> bool:
    """Check if mssql is up."""
    cmd = f"docker logs {container_name} 2>&1 | grep 'SQL Server is now ready for client connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


@pytest.fixture(scope="session", autouse=True)
def setup_containers():
    """Start the required containers for integration tests."""
    start_containers()
    yield
    logger.info("Tearing down containers...")
    subprocess.run(
        ["docker-compose", "-f", "tests/integration/jdbc/docker-compose.yml", "down"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    logger.info("Containers stopped and cleaned up.")


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/jdbc"


@pytest.fixture(scope="module")
def postgres_runner():
    wait_for_container("testpostgres", is_postgres_up)
    yield


@pytest.fixture(scope="module")
def mysql_runner():
    wait_for_container("testmysql", is_mysql_up)
    yield


@pytest.fixture(scope="module")
def mssql_runner():
    wait_for_container("testmssql", is_mssql_up)
    yield


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_postgres_ingest(postgres_runner, pytestconfig, test_resources_dir, tmp_path):
    """Test PostgreSQL ingestion."""
    config_file = test_resources_dir / "postgres_to_file.yml"
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "postgres_mces.json",
        golden_path=test_resources_dir / "postgres_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest(mysql_runner, pytestconfig, test_resources_dir, tmp_path):
    """Test MySQL ingestion."""
    config_file = test_resources_dir / "mysql_to_file.yml"
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mysql_mces.json",
        golden_path=test_resources_dir / "mysql_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mssql_ingest(mssql_runner, pytestconfig, test_resources_dir, tmp_path):
    """Test MSSQL ingestion."""
    config_file = test_resources_dir / "mssql_to_file.yml"
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mssql_mces.json",
        golden_path=test_resources_dir / "mssql_mces_golden.json",
    )

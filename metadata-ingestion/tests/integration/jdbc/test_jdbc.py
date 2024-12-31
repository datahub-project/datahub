import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml
from freezegun import freeze_time

from tests.test_helpers import mce_helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FROZEN_TIME = "2023-10-15 07:00:00"

# Port configurations
POSTGRES_PORT = 45432
MYSQL_PORT = 43306
MSSQL_PORT = 41433


def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and optionally check for errors."""
    logger.debug(f"Running command: {cmd}")
    result = subprocess.run(
        args=cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if check and result.returncode != 0:
        logger.error(f"Command failed: {result.stderr}")
        raise RuntimeError(f"Command failed: {result.stderr}")
    return result


def prepare_config_file(source_config: Path, tmp_path: Path, database: str) -> Path:
    """Copy and modify config file to use temporary directory."""
    # Read the original config
    with open(source_config) as f:
        config = yaml.safe_load(f)

    # Update the sink config to use the temp directory
    if "sink" in config:
        config["sink"]["config"]["filename"] = str(tmp_path / f"{database}_mces.json")

    # Write the modified config to temp directory
    tmp_config = tmp_path / source_config.name
    with open(tmp_config, "w") as f:
        yaml.dump(config, f)

    return tmp_config


def run_datahub_ingest(config_path: str) -> None:
    """Run datahub ingest command in a new process."""
    cmd = [sys.executable, "-m", "datahub", "ingest", "-c", config_path]
    result = subprocess.run(
        args=cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**dict(os.environ), "_JAVA_OPTIONS": "-Xmx512m"},  # Ensure clean JVM state
    )
    if result.returncode != 0:
        logger.error(f"Ingest failed: stdout={result.stdout}, stderr={result.stderr}")
        raise RuntimeError(f"Ingest command failed: {result.stderr}")
    logger.info("Ingest completed successfully")


def start_containers():
    """Start containers using docker-compose."""
    logger.info("Starting containers with docker-compose...")
    try:
        run_command(
            "docker-compose -f tests/integration/jdbc/docker-compose.yml down -v"
        )
        run_command("docker-compose -f tests/integration/jdbc/docker-compose.yml up -d")
        logger.info("Containers started successfully.")
    except Exception as e:
        logger.error(f"Failed to start containers: {str(e)}")
        raise


def is_postgres_ready(container_name: str) -> bool:
    """Check if postgres is ready by testing connection."""
    cmd = f"docker exec {container_name} pg_isready -U postgres"
    result = run_command(cmd, check=False)
    return result.returncode == 0


def is_mysql_ready(container_name: str) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""
    cmd = f"docker logs {container_name} 2>&1 | grep '/usr/sbin/mysqld: ready for connections.' | grep 3306"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def is_mssql_ready(container_name: str) -> bool:
    """Check if mssql is ready by testing connection and database existence."""
    cmd = f"docker exec {container_name} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Password123!' -Q 'SELECT NAME FROM sys.databases WHERE NAME = \"test\"'"
    result = run_command(cmd, check=False)
    if result.returncode != 0:
        return False

    cmd = f"docker exec {container_name} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Password123!' -d test -Q 'SELECT COUNT(*) FROM sys.tables'"
    result = run_command(cmd, check=False)
    return result.returncode == 0


def wait_for_container(container_name: str, checker, timeout=120):
    """Wait for a container to become ready."""
    logger.info(f"Waiting for container {container_name} to become ready...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            if checker(container_name):
                logger.info(f"Container {container_name} is ready.")
                return
        except Exception as e:
            logger.debug(f"Check failed: {str(e)}")
        time.sleep(5)
        logger.debug(
            f"Container {container_name} not ready yet, elapsed time: {time.time() - start_time}s"
        )
    raise TimeoutError(
        f"Container {container_name} is not ready after {timeout} seconds."
    )


@pytest.fixture(scope="session", autouse=True)
def setup_containers():
    """Start the required containers for integration tests."""
    start_containers()
    yield
    logger.info("Tearing down containers...")
    run_command("docker-compose -f tests/integration/jdbc/docker-compose.yml down -v")
    logger.info("Containers stopped and cleaned up.")


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/jdbc"


@pytest.fixture(scope="function")
def postgres_runner():
    wait_for_container("testpostgres", is_postgres_ready)
    yield


@pytest.fixture(scope="function")
def mysql_runner():
    wait_for_container("testmysql", is_mysql_ready)
    yield


@pytest.fixture(scope="function")
def mssql_runner():
    wait_for_container("testmssql", is_mssql_ready)
    yield


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_postgres_ingest(postgres_runner, pytestconfig, test_resources_dir, tmp_path):
    """Test PostgreSQL ingestion."""
    config_file = test_resources_dir / "postgres_to_file.yml"
    tmp_config = prepare_config_file(config_file, tmp_path, "postgres")

    run_datahub_ingest(str(tmp_config))

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
    tmp_config = prepare_config_file(config_file, tmp_path, "mysql")

    run_datahub_ingest(str(tmp_config))

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
    tmp_config = prepare_config_file(config_file, tmp_path, "mssql")

    run_datahub_ingest(str(tmp_config))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mssql_mces.json",
        golden_path=test_resources_dir / "mssql_mces_golden.json",
    )

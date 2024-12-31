import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FROZEN_TIME = "2023-10-15 07:00:00"

# Port configurations
POSTGRES_PORT = 45432
MYSQL_PORT = 43306
MSSQL_PORT = 41433


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


def prepare_config_file(source_config: Path, tmp_path: Path, database: str) -> Path:
    """Copy and modify config file to use temporary directory."""
    with open(source_config) as f:
        config = yaml.safe_load(f)

    if "sink" in config:
        config["sink"]["config"]["filename"] = str(tmp_path / f"{database}_mces.json")

    tmp_config = tmp_path / source_config.name
    with open(tmp_config, "w") as f:
        yaml.dump(config, f)

    return tmp_config


def is_postgres_up(container_name: str) -> bool:
    """Check if postgres is up"""
    cmd = f"docker logs {container_name} 2>&1 | grep 'database system is ready to accept connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def is_mysql_up(container_name: str) -> bool:
    """Check if mysql is up"""
    cmd = f"docker logs {container_name} 2>&1 | grep 'ready for connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def is_mssql_up(container_name: str) -> bool:
    """Check if mssql is up"""
    cmd = f"docker logs {container_name} 2>&1 | grep 'SQL Server is now ready for client connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/jdbc"


def get_service_override_file(test_resources_dir: Path, service: str) -> Path:
    """Create a docker-compose override file for a single service."""
    with open(test_resources_dir / "docker-compose.yml") as f:
        config = yaml.safe_load(f)

    # Create a new compose file with just the specified service and its dependencies
    new_config = {
        "services": {service: config["services"][service]},
        "networks": config.get("networks", {}),
    }

    # Write temporary override file
    override_path = test_resources_dir / f"docker-compose.{service}.yml"
    with open(override_path, "w") as f:
        yaml.dump(new_config, f)

    return override_path


@pytest.fixture(scope="module")
def postgres_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    override_file = get_service_override_file(
        test_resources_dir=test_resources_dir, service="postgres"
    )
    with docker_compose_runner(override_file, "testpostgres") as docker_services:
        wait_for_port(
            docker_services,
            "testpostgres",
            POSTGRES_PORT,
            timeout=120,
            checker=lambda: is_postgres_up("testpostgres"),
        )
        yield docker_services


@pytest.fixture(scope="module")
def mysql_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    override_file = get_service_override_file(
        test_resources_dir=test_resources_dir, service="mysql"
    )
    with docker_compose_runner(override_file, "testmysql") as docker_services:
        wait_for_port(
            docker_services,
            "testmysql",
            MYSQL_PORT,
            timeout=120,
            checker=lambda: is_mysql_up("testmysql"),
        )
        yield docker_services


@pytest.fixture(scope="module")
def mssql_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    override_file = get_service_override_file(
        test_resources_dir=test_resources_dir, service="mssql"
    )
    with docker_compose_runner(override_file, "testmssql") as docker_services:
        wait_for_port(
            docker_services,
            "testmssql",
            MSSQL_PORT,
            timeout=120,
            checker=lambda: is_mssql_up("testmssql"),
        )
        yield docker_services


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

import subprocess
import time

import pytest
from freezegun import freeze_time

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2024-01-15 10:00:00"
TIMESCALEDB_PORT = 55432


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/timescaledb"


def is_timescaledb_up(container_name: str) -> bool:
    """Check if TimescaleDB is responsive on a container"""

    # Check if database is ready to accept connections
    cmd = f"docker logs {container_name} 2>&1 | grep 'database system is ready to accept connections'"
    ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0:
        print(f"Database not ready yet for {container_name}")
        return False

    # Check if initialization scripts have completed
    cmd = f"docker logs {container_name} 2>&1 | grep 'PostgreSQL init process complete'"
    ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0:
        # If init process complete message not found, check for TimescaleDB extension creation
        cmd = (
            f"docker logs {container_name} 2>&1 | grep 'CREATE EXTENSION.*timescaledb'"
        )
        ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if ret.returncode != 0:
            print(f"TimescaleDB extension not created yet for {container_name}")
            return False

    # Wait a bit for any remaining initialization
    time.sleep(2)

    # Use docker exec to test database connectivity
    cmd = f"docker exec {container_name} pg_isready -U tsdbuser -d tsdb -h localhost"
    ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0:
        print(f"pg_isready failed for {container_name}: {ret.stdout} {ret.stderr}")
        return False

    # Try a simple query to make sure database is fully operational
    cmd = f"docker exec {container_name} psql -U tsdbuser -d tsdb -c 'SELECT 1;'"
    ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0:
        print(f"Simple query failed for {container_name}: {ret.stdout} {ret.stderr}")
        return False

    # Check if TimescaleDB extension is actually loaded
    cmd = f"docker exec {container_name} psql -U tsdbuser -d tsdb -c \"SELECT extname FROM pg_extension WHERE extname = 'timescaledb';\""
    ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0 or "timescaledb" not in ret.stdout:
        print(f"TimescaleDB extension not loaded for {container_name}")
        return False

    print(f"TimescaleDB container {container_name} is fully ready")
    return True


@pytest.fixture(scope="module")
def timescaledb_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "timescaledb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "test-timescaledb",
            TIMESCALEDB_PORT,
            timeout=300,  # Increased timeout for CI environments
            checker=lambda: is_timescaledb_up("test-timescaledb"),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_timescaledb_ingest_with_db(
    timescaledb_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    """Test basic TimescaleDB ingestion with hypertables and continuous aggregates"""
    config_file = (test_resources_dir / "timescaledb_to_file.yml").resolve()

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "timescaledb_mces.json",
        golden_path=test_resources_dir / "timescaledb_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_timescaledb_ingest_with_jobs(
    timescaledb_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    """Test TimescaleDB ingestion with background jobs enabled"""
    config_file = (test_resources_dir / "timescaledb_with_jobs.yml").resolve()

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output includes job entities
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "timescaledb_jobs_mces.json",
        golden_path=test_resources_dir / "timescaledb_jobs_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_timescaledb_lineage(
    timescaledb_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    """Test column-level lineage for continuous aggregates"""
    config_file = (test_resources_dir / "timescaledb_lineage.yml").resolve()

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify lineage is correctly extracted
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "timescaledb_lineage_mces.json",
        golden_path=test_resources_dir / "timescaledb_lineage_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_timescaledb_all_databases(
    timescaledb_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    """Test TimescaleDB ingestion across multiple databases"""
    config_file = (test_resources_dir / "timescaledb_all_db.yml").resolve()

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify multi-database output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "timescaledb_all_db_mces.json",
        golden_path=test_resources_dir / "timescaledb_all_db_mces_golden.json",
    )

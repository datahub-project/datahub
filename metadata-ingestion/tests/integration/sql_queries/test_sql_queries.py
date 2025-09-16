import os
import pathlib
import subprocess
import time

import pytest
import requests
import yaml

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers import fs_helpers
from tests.test_helpers.docker_helpers import wait_for_port

# Ignore dynamic timestamp fields that change on every test run
IGNORE_PATHS = [
    # Ignore auditStamp timestamps in upstreamLineage aspects
    r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['auditStamp'\]\['time'\]",
    # Ignore auditStamp timestamps in patch lineage format
    r"root\[\d+\]\['aspect'\]\[\d+\]\['value'\]\['auditStamp'\]\['time'\]",
]


def check_mockserver_health():
    """Custom health check for MockServer using /health endpoint."""
    try:
        response = requests.get("http://localhost:8080/health", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


def cleanup_docker_containers():
    """Clean up any existing Docker containers and networks."""
    try:
        # Stop and remove any existing datahub-mock containers
        subprocess.run(
            ["docker", "stop", "datahub-mock"], capture_output=True, check=False
        )
        subprocess.run(
            ["docker", "rm", "datahub-mock"], capture_output=True, check=False
        )

        # Clean up any pytest Docker networks
        result = subprocess.run(
            [
                "docker",
                "network",
                "ls",
                "--filter",
                "name=pytest",
                "--format",
                "{{.Name}}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            for network in result.stdout.strip().split("\n"):
                if network:
                    subprocess.run(
                        ["docker", "network", "rm", network],
                        capture_output=True,
                        check=False,
                    )
    except Exception:
        # Ignore cleanup errors
        pass


def ensure_docker_running():
    """Ensure Docker daemon is running."""
    max_retries = 30
    for i in range(max_retries):
        try:
            subprocess.run(["docker", "ps"], capture_output=True, check=True)
            return True
        except subprocess.CalledProcessError:
            if i < max_retries - 1:
                time.sleep(2)
            else:
                raise RuntimeError(
                    "Docker daemon is not running. Please start Docker Desktop."
                ) from None
    return False


@pytest.fixture(scope="session", autouse=True)
def cleanup_docker_on_exit():
    """Ensure Docker cleanup happens at the end of the test session."""
    yield
    # This runs after all tests in the session complete
    cleanup_docker_containers()


@pytest.fixture(scope="module", autouse=True)
def docker_datahub_service(docker_compose_runner, pytestconfig):
    """Start Docker mock DataHub service for all tests."""
    # Ensure Docker is running
    ensure_docker_running()

    # Clean up any existing containers before starting
    cleanup_docker_containers()

    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_queries"

    try:
        with docker_compose_runner(
            test_resources_dir / "docker-compose.yml", "datahub-mock", cleanup=True
        ) as docker_services:
            wait_for_port(
                docker_services,
                container_name="datahub-mock",
                container_port=8080,
                timeout=60,
                checker=check_mockserver_health,
            )
            yield docker_services
    finally:
        # Ensure cleanup happens even if tests fail
        cleanup_docker_containers()


@pytest.mark.parametrize(
    "recipe_file,golden_file",
    [
        ("input/basic.yml", "golden/basic.json"),
        (
            "input/basic-with-schema-resolver.yml",
            "golden/basic-with-schema-resolver.json",
        ),
        (
            "input/session-temp-tables.yml",
            "golden/session-temp-tables.json",
        ),
        (
            "input/query-deduplication.yml",
            "golden/query-deduplication.json",
        ),
        (
            "input/explicit-lineage.yml",
            "golden/explicit-lineage.json",
        ),
        (
            "input/hex-origin.yml",
            "golden/hex-origin.json",
        ),
        (
            "input/patch-lineage.yml",
            "golden/patch-lineage.json",
        ),
        (
            "input/lazy-schema-loading.yml",
            "golden/lazy-schema-loading.json",
        ),
        (
            "input/streaming-processing.yml",
            "golden/streaming-processing.json",
        ),
        (
            "input/temp-table-patterns.yml",
            "golden/temp-table-patterns.json",
        ),
        (
            "input/combined-optimizations.yml",
            "golden/combined-optimizations.json",
        ),
        (
            "input/large-file-streaming.yml",
            "golden/large-file-streaming.json",
        ),
    ],
)
def test_sql_queries_ingestion(tmp_path, pytestconfig, recipe_file, golden_file):
    """Test SQL queries ingestion with different recipes and golden files."""
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/sql_queries"
    )

    # Load recipe
    with open(test_resources_dir / recipe_file) as f:
        recipe = yaml.safe_load(f)

    # Run with isolated filesystem so relative paths work
    with fs_helpers.isolated_filesystem(test_resources_dir):
        try:
            # Create and run pipeline with recipe as-is
            pipeline = Pipeline.create(recipe)
            pipeline.run()
            pipeline.raise_from_status()

            # Validate output against golden file (both files are now relative to test_resources_dir)
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path="./output.json",
                golden_path=golden_file,
                ignore_paths=IGNORE_PATHS,
            )
        finally:
            # Clean up output file if it exists
            if os.path.exists("./output.json"):
                os.remove("./output.json")

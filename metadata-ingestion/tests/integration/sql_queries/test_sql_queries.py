import os
import pathlib

import pytest
import requests
import yaml

from datahub.configuration.config_loader import EnvResolver
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


def check_mockserver_health(port: int) -> bool:
    """Custom health check for MockServer using /health endpoint."""
    try:
        response = requests.get(f"http://localhost:{port}/health", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def docker_datahub_service(docker_compose_runner, pytestconfig, request):
    """Start Docker mock DataHub service for all tests."""

    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_queries"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "datahub-mock", cleanup=True
    ) as docker_services:
        # The compose file exposes MockServer's port ephemerally, so a leaked
        # container from a prior run can never hold onto the port a fresh run
        # needs. Recipe ymls in this directory pick it up via ${SQL_QUERIES_MOCK_PORT}.
        mock_port = docker_services.port_for("datahub-mock", 8080)
        mp = pytest.MonkeyPatch()
        mp.setenv("SQL_QUERIES_MOCK_PORT", str(mock_port))
        request.addfinalizer(mp.undo)

        wait_for_port(
            docker_services,
            container_name="datahub-mock",
            container_port=8080,
            timeout=60,
            checker=lambda: check_mockserver_health(mock_port),
        )
        yield docker_services


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
            "input/temp-table-patterns.yml",
            "golden/temp-table-patterns.json",
        ),
    ],
)
def test_sql_queries_ingestion(tmp_path, pytestconfig, recipe_file, golden_file):
    """Test SQL queries ingestion with different recipes and golden files."""
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/sql_queries"
    )

    # Load recipe. Pipeline.create() (used below) doesn't expand ${VAR}
    # placeholders the way the CLI's load_config_file() does, so resolve
    # them here to pick up the ephemeral mock server port.
    with open(test_resources_dir / recipe_file) as f:
        recipe = yaml.safe_load(f)
    recipe = EnvResolver(environ=os.environ).resolve(recipe)

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

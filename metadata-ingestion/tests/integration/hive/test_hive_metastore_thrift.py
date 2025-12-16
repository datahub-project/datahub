"""
Integration tests for Hive Metastore with Thrift connection type.

Tests use `hive-metastore` connector with `connection_type: thrift`.

CI Test:
    - test_hive_thrift_ingest: Golden file validation (non-Kerberos)

Manual Kerberos Test:
    Kerberos tests must run FROM INSIDE the Docker container (Kerberos credentials
    are only available there). See kerberos/README.md for the full workflow:

    1. Build wheel: pip wheel . -w /tmp/datahub_wheels --no-deps
    2. Start Kerberized environment
    3. Copy wheel to container and install
    4. Run: docker exec kerberos-client datahub ingest -c /tmp/recipe.yaml
"""

import os
import subprocess

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"

# Set RUN_KERBEROS_TESTS=1 to run Kerberos tests manually
SKIP_KERBEROS = os.environ.get("RUN_KERBEROS_TESTS", "0") != "1"

# CI tests use integration_batch_1 (same as test_hive.py since they share docker-compose)
pytestmark = pytest.mark.integration_batch_1


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def hive_runner(docker_compose_runner, pytestconfig):
    """Start Hive environment using base docker-compose.yml."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hive", parallel=1
    ) as docker_services:
        wait_for_port(docker_services, "testhiveserver2", 10000, timeout=120)
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hive"


@pytest.fixture(scope="module")
def loaded_hive(hive_runner):
    """Set up Hive tables using HiveServer2."""
    command = "docker exec testhiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)
    yield


# =============================================================================
# CI Test (Non-Kerberos)
# =============================================================================


@freeze_time(FROZEN_TIME)
def test_hive_thrift_ingest(
    loaded_hive, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    """
    Test HMS Thrift ingestion without Kerberos.

    Validates:
    - Connection to HMS via Thrift API (port 9083)
    - Metadata extraction (databases, tables, views, schemas)
    - View lineage extraction
    - Golden file comparison
    """
    events_file = tmp_path / "test_hive_thrift_ingest.json"

    config = {
        "run_id": "hive-thrift-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9083",
                "use_kerberos": False,
                "database_pattern": {"allow": ["^db1$"]},
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(events_file)},
        },
    }

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # ignore_paths_v2 for MCP/PATCH format: external system timestamps that change each Docker run
    # These match the "path" field in PATCH operations like {"op": "add", "path": "/customProperties/create_date", "value": "..."}
    # Note: Internal timestamps (auditStamp, lastModified) are frozen via @freeze_time
    ignore_paths_v2 = [
        "/customProperties/create_date",  # Hive table creation date (external)
        "/customProperties/transient_lastDdlTime",  # Hive DDL timestamp (external)
    ]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=events_file,
        golden_path=test_resources_dir / "hive_thrift_mces_golden.json",
        ignore_paths_v2=ignore_paths_v2,
    )


# =============================================================================
# Kerberos Test (Manual Only)
#
# NOTE: This test is provided for reference but cannot run from the host machine.
# Kerberos credentials only exist inside the Docker container.
# See kerberos/README.md for the proper test workflow using docker exec.
# =============================================================================


@pytest.mark.skipif(
    SKIP_KERBEROS,
    reason="Kerberos requires Docker environment. See kerberos/README.md",
)
def test_hive_thrift_kerberized(tmp_path):
    """
    Test HMS Thrift ingestion with Kerberos authentication.

    NOTE: This test cannot run from host - Kerberos credentials are in Docker.
    Use the docker exec workflow in kerberos/README.md instead.

    This test is kept for documentation purposes to show the expected config.
    """
    events_file = tmp_path / "test_hive_thrift_kerberized.json"

    config = {
        "run_id": "hive-thrift-kerberized-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9083",
                "use_kerberos": True,
                "kerberos_service_name": "hive",
                "database_pattern": {"allow": ["^db1$"]},
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(events_file)},
        },
    }

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    assert events_file.exists()
    assert events_file.stat().st_size > 5000

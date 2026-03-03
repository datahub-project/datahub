"""
Integration tests for Hive Metastore with Thrift connection type.

Tests use `hive-metastore` connector with `connection_type: thrift`.
Uses the same docker-compose as SQL tests but connects via Thrift API (port 9083).

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
import re
import subprocess

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-09-23 12:00:00"  # Match SQL tests

# Set RUN_KERBEROS_TESTS=1 to run Kerberos tests manually
SKIP_KERBEROS = os.environ.get("RUN_KERBEROS_TESTS", "0") != "1"

# CI tests use integration_batch_1 (same as SQL tests since they share docker-compose)
pytestmark = pytest.mark.integration_batch_1


# =============================================================================
# Fixtures (shared with SQL tests via same docker-compose)
# =============================================================================


@pytest.fixture(scope="module")
def hive_metastore_runner(docker_compose_runner, pytestconfig):
    """Start Hive Metastore environment (shared with SQL tests)."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive-metastore"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hive-metastore"
    ) as docker_services:
        # Wait for HMS Thrift API
        wait_for_port(docker_services, "hive-metastore", 9083, timeout=120)
        # Wait for HiveServer2 (needed for setup)
        wait_for_port(docker_services, "hiveserver2", 10000, timeout=120)
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hive-metastore"


@pytest.fixture(scope="module")
def loaded_hive_metastore(hive_metastore_runner):
    """Set up Hive tables using HiveServer2."""
    command = "docker exec hiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)
    yield


# =============================================================================
# CI Tests (Non-Kerberos)
# =============================================================================


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "include_catalog_name_in_ids,simplify_nested_field_paths,use_dataset_pascalcase_subtype,test_suffix",
    [
        (False, False, False, "_thrift_1"),  # Basic
        (True, False, False, "_thrift_2"),  # With catalog IDs
        (False, True, False, "_thrift_3"),  # Simplified paths
        (False, False, True, "_thrift_4"),  # PascalCase subtypes
    ],
)
def test_hive_thrift_ingest(
    loaded_hive_metastore,
    test_resources_dir,
    pytestconfig,
    tmp_path,
    mock_time,
    include_catalog_name_in_ids,
    simplify_nested_field_paths,
    use_dataset_pascalcase_subtype,
    test_suffix,
):
    """
    Test HMS Thrift ingestion with various configurations.

    Validates:
    - Connection to HMS via Thrift API (port 9083)
    - Metadata extraction (databases, tables, views, schemas)
    - Configuration options (catalog IDs, simplified paths, PascalCase)
    - Golden file comparison
    """
    events_file = tmp_path / f"hive_thrift_mces{test_suffix}.json"

    config = {
        "run_id": "hive-thrift-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9083",
                "use_kerberos": False,
                "database_pattern": {"allow": ["^db1$"]},
                "include_catalog_name_in_ids": include_catalog_name_in_ids,
                "simplify_nested_field_paths": simplify_nested_field_paths,
                "use_dataset_pascalcase_subtype": use_dataset_pascalcase_subtype,
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
    ignore_paths_v2 = [
        "/customProperties/create_date",
        "/customProperties/transient_lastDdlTime",
    ]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=events_file,
        golden_path=test_resources_dir / f"hive_thrift_mces_golden{test_suffix}.json",
        ignore_paths_v2=ignore_paths_v2,
    )


# =============================================================================
# Platform Instance Test
# =============================================================================


@freeze_time(FROZEN_TIME)
def test_hive_thrift_instance_ingest(
    loaded_hive_metastore, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    """
    Test HMS Thrift ingestion with platform_instance.

    Validates:
    - All dataset URNs contain the platform instance
    - dataPlatformInstance aspects are emitted for all datasets
    """
    instance = "production_warehouse"
    platform = "hive"
    events_file = tmp_path / "hive_thrift_instance_mces.json"

    config = {
        "run_id": "hive-thrift-instance-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9083",
                "use_kerberos": False,
                "platform_instance": instance,
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

    # Assert that all events generated have instance-specific URNs
    urn_pattern = "^" + re.escape(
        f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}."
    )
    assert (
        mce_helpers.assert_mce_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one match"

    assert (
        mce_helpers.assert_mcp_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one MCP"

    # All dataset entities emitted must have a dataPlatformInstance aspect emitted
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="dataPlatformInstance",
            aspect_field_matcher={
                "instance": f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{platform},{instance})"
            },
            file=events_file,
        )
        >= 1
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

    Validated config matches kerberos/README.md Step 4:
    - host_port: hive-metastore:9083 (container network)
    - use_kerberos: true
    - kerberos_service_name: hive
    - database_pattern: db1 and db2

    Expected results: 9 tables, 62 events (see README.md)
    """
    events_file = tmp_path / "test_hive_thrift_kerberized.json"

    # Config validated in kerberos/README.md Step 4
    config = {
        "run_id": "hive-thrift-kerberized-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                # Use container hostname when running from inside Docker
                "host_port": "hive-metastore:9083",
                "use_kerberos": True,
                "kerberos_service_name": "hive",
                # Match kerberos/README.md recipe
                "database_pattern": {"allow": ["^db1$", "^db2$"]},
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
    # Expected: 9 tables, ~62 events (6-7 aspects per table + containers)
    assert events_file.stat().st_size > 5000

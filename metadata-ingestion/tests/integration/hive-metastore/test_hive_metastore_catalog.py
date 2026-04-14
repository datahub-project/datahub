"""
Integration tests for HMS 3.x multi-catalog support.

These tests verify the catalog functionality of the hive-metastore connector
with a real HMS 3.x instance supporting multiple catalogs.

Test Environment:
    - HMS 3.x with multi-catalog support (docker-compose.hms3.yml)
    - Catalogs: hive, spark_catalog, iceberg_catalog
    - Port 9084 (mapped from container's 9083)

Test Scenarios:
    1. Default catalog (no catalog_name specified) - uses 'hive' catalog
    2. Explicit catalog (catalog_name: spark_catalog) without catalog in URNs
    3. Explicit catalog with include_catalog_name_in_ids=True for URN generation

Usage:
    # Local mode (start HMS manually, then run tests)
    cd metadata-ingestion/tests/integration/hive-metastore
    docker compose -f docker-compose.hms3.yml up -d
    # Wait ~60-90s, then run setup
    python hms3/setup-catalogs.py
    # Run tests with HMS3_EXTERNAL to skip docker-compose
    HMS3_EXTERNAL=1 pytest tests/integration/hive-metastore/test_hive_metastore_catalog.py -v

Note: These tests are skipped by default in CI because the HMS 3.x Docker
environment requires specific platform compatibility. Set RUN_HMS3_TESTS=1
to force running these tests.
"""

import os
from typing import Dict, Sequence

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"

# Skip these tests by default in CI - HMS 3.x Docker setup has platform compatibility issues
# Run locally with: HMS3_EXTERNAL=1 pytest ... or force in CI with: RUN_HMS3_TESTS=1
pytestmark = [
    pytest.mark.integration_batch_1,
    pytest.mark.skipif(
        not os.environ.get("HMS3_EXTERNAL") and not os.environ.get("RUN_HMS3_TESTS"),
        reason="HMS 3.x tests skipped by default. Set HMS3_EXTERNAL=1 or RUN_HMS3_TESTS=1 to run.",
    ),
]


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def hms3_runner(docker_compose_runner, pytestconfig):
    """
    Start HMS 3.x environment using docker-compose.hms3.yml.

    Set HMS3_EXTERNAL=1 to skip Docker startup when HMS is already running
    (e.g., for local development/debugging).
    """
    if os.environ.get("HMS3_EXTERNAL"):
        # HMS is running externally (user started it manually)
        yield None
        return

    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive-metastore"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.hms3.yml", "hms3"
    ) as docker_services:
        # Wait for HMS to be ready (exposed on port 9084)
        wait_for_port(docker_services, "hive-metastore-hms3", 9083, timeout=180)
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    """Return the test resources directory."""
    return pytestconfig.rootpath / "tests/integration/hive-metastore"


@pytest.fixture(scope="module")
def loaded_hms3(hms3_runner, pytestconfig):
    """
    Set up test data in HMS 3.x by running the setup script from host.

    Creates:
    - Multiple catalogs (hive, spark_catalog, iceberg_catalog)
    - Databases in each catalog (test_db, analytics_db, delta_db)
    - Tables with various configurations and schemas

    Set HMS3_SKIP_SETUP=1 to skip running setup (if you already ran it manually).
    """
    if os.environ.get("HMS3_SKIP_SETUP"):
        # User already ran setup script manually
        yield
        return

    import importlib.util
    import sys

    setup_script = (
        pytestconfig.rootpath
        / "tests/integration/hive-metastore/hms3/setup-catalogs.py"
    )

    spec = importlib.util.spec_from_file_location("setup_catalogs", setup_script)
    if spec is None or spec.loader is None:
        pytest.fail(f"Could not load setup script: {setup_script}")

    setup_module = importlib.util.module_from_spec(spec)
    sys.modules["setup_catalogs"] = setup_module

    try:
        spec.loader.exec_module(setup_module)
        setup_module.run_setup(host="localhost", port=9084)
    except Exception as e:
        pytest.fail(f"Failed to set up HMS 3.x test data: {e}")

    yield


# =============================================================================
# Catalog API Tests
# =============================================================================


def test_catalog_api_list_catalogs(loaded_hms3):
    """Test that HMS 3.x returns multiple catalogs via Thrift API."""
    from pymetastore.hive_metastore import ThriftHiveMetastore
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket, TTransport

    socket = TSocket.TSocket("localhost", 9084)
    transport = TTransport.TBufferedTransport(socket)
    transport.open()

    try:
        client = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(transport))
        response = client.get_catalogs()
        catalogs = list(response.names) if response.names else []

        assert "hive" in catalogs, "Default 'hive' catalog should exist"
        assert "spark_catalog" in catalogs, "spark_catalog should exist"
        assert "iceberg_catalog" in catalogs, "iceberg_catalog should exist"
    finally:
        transport.close()


def test_catalog_api_namespace_isolation(loaded_hms3):
    """
    Test that same-named tables in different catalogs have different schemas.

    Both hive.test_db.users and spark_catalog.test_db.users exist but have
    different column definitions.
    """
    from pymetastore.hive_metastore import ThriftHiveMetastore
    from pymetastore.hive_metastore.ttypes import GetTableRequest
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket, TTransport

    socket = TSocket.TSocket("localhost", 9084)
    transport = TTransport.TBufferedTransport(socket)
    transport.open()

    try:
        client = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(transport))

        # Get hive.test_db.users
        hive_req = GetTableRequest(dbName="test_db", tblName="users", catName="hive")
        hive_table = client.get_table_req(hive_req).table
        hive_cols = {c.name for c in hive_table.sd.cols}

        # Get spark_catalog.test_db.users
        spark_req = GetTableRequest(
            dbName="test_db", tblName="users", catName="spark_catalog"
        )
        spark_table = client.get_table_req(spark_req).table
        spark_cols = {c.name for c in spark_table.sd.cols}

        # Verify they have different schemas
        assert hive_table.catName == "hive"
        assert spark_table.catName == "spark_catalog"

        # hive.test_db.users has: id, name, email, created_at
        assert "email" in hive_cols, "hive.test_db.users should have 'email' column"

        # spark_catalog.test_db.users has: user_id, username, active
        assert "active" in spark_cols, (
            "spark_catalog.test_db.users should have 'active' column"
        )

        # They should be different
        assert hive_cols != spark_cols, (
            "Tables in different catalogs should have different schemas"
        )
    finally:
        transport.close()


# =============================================================================
# Ingestion Tests with Golden Files
# =============================================================================

# Ignore paths for dynamic/timestamp-based fields
IGNORE_PATHS: Sequence[str] = [
    r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['transient_lastDdlTime'\]",
    r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['numfiles'\]",
    r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['totalsize'\]",
    r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['create_date'\]",
]

IGNORE_PATHS_V2: Sequence[str] = [
    "/customProperties/create_date",
    "/customProperties/transient_lastDdlTime",
    "/customProperties/numfiles",
    "/customProperties/totalsize",
    "/customProperties/COLUMN_STATS_ACCURATE",
    "/lastModified",
    "/created",
]


@freeze_time(FROZEN_TIME)
def test_ingest_default_catalog(
    loaded_hms3, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    """
    Test ingestion from default 'hive' catalog (no catalog_name specified).

    Expected:
    - Tables from hive.test_db should be ingested
    - URNs should NOT include catalog name (default behavior)
    - URN format: urn:li:dataset:(urn:li:dataPlatform:hive,test_db.users,PROD)
    """
    mce_out_file = "hive_metastore_hms3_default_catalog_mces.json"
    events_file = tmp_path / mce_out_file

    config: Dict = {
        "run_id": "hms3-default-catalog-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9084",
                "use_kerberos": False,
                "database_pattern": {"allow": ["^test_db$"]},
                # No catalog_name - uses default 'hive' catalog
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

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mce_out_file,
        golden_path=test_resources_dir
        / f"hms3/{mce_out_file.replace('.json', '_golden.json')}",
        ignore_paths=IGNORE_PATHS,
        ignore_paths_v2=IGNORE_PATHS_V2,
    )


@freeze_time(FROZEN_TIME)
def test_ingest_spark_catalog(
    loaded_hms3, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    """
    Test ingestion from spark_catalog with include_catalog_name_in_ids=False.

    Expected:
    - Tables from spark_catalog.test_db should be ingested
    - URNs should NOT include catalog name
    - URN format: urn:li:dataset:(urn:li:dataPlatform:hive,test_db.users,PROD)
    """
    mce_out_file = "hive_metastore_hms3_spark_catalog_mces.json"
    events_file = tmp_path / mce_out_file

    config: Dict = {
        "run_id": "hms3-spark-catalog-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9084",
                "use_kerberos": False,
                "catalog_name": "spark_catalog",
                "include_catalog_name_in_ids": False,
                "database_pattern": {"allow": ["^test_db$"]},
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

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mce_out_file,
        golden_path=test_resources_dir
        / f"hms3/{mce_out_file.replace('.json', '_golden.json')}",
        ignore_paths=IGNORE_PATHS,
        ignore_paths_v2=IGNORE_PATHS_V2,
    )


@freeze_time(FROZEN_TIME)
def test_ingest_spark_catalog_with_catalog_ids(
    loaded_hms3, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    """
    Test ingestion from spark_catalog with include_catalog_name_in_ids=True.

    Expected:
    - Tables from spark_catalog.test_db should be ingested
    - URNs SHOULD include catalog name
    - URN format: urn:li:dataset:(urn:li:dataPlatform:hive,spark_catalog.test_db.users,PROD)
    """
    mce_out_file = "hive_metastore_hms3_spark_catalog_with_ids_mces.json"
    events_file = tmp_path / mce_out_file

    config: Dict = {
        "run_id": "hms3-spark-catalog-ids-test",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9084",
                "use_kerberos": False,
                "catalog_name": "spark_catalog",
                "include_catalog_name_in_ids": True,
                "database_pattern": {"allow": ["^test_db$"]},
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

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mce_out_file,
        golden_path=test_resources_dir
        / f"hms3/{mce_out_file.replace('.json', '_golden.json')}",
        ignore_paths=IGNORE_PATHS,
        ignore_paths_v2=IGNORE_PATHS_V2,
    )


# =============================================================================
# URN Validation Tests
# =============================================================================


@freeze_time(FROZEN_TIME)
def test_urn_without_catalog_name(loaded_hms3, tmp_path, mock_time):
    """
    Test that URNs do NOT include catalog name when include_catalog_name_in_ids=False.
    """
    events_file = tmp_path / "urn_test_no_catalog.json"

    config: Dict = {
        "run_id": "hms3-urn-no-catalog",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9084",
                "use_kerberos": False,
                "catalog_name": "spark_catalog",
                "include_catalog_name_in_ids": False,
                "database_pattern": {"allow": ["^test_db$"]},
                "table_pattern": {"allow": ["^users$"]},
            },
        },
        "sink": {"type": "file", "config": {"filename": str(events_file)}},
    }

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    content = events_file.read_text()

    # URN should be test_db.users (no catalog prefix)
    assert "test_db.users" in content
    # Should NOT have spark_catalog prefix in URN
    assert "spark_catalog.test_db.users" not in content


@freeze_time(FROZEN_TIME)
def test_urn_with_catalog_name(loaded_hms3, tmp_path, mock_time):
    """
    Test that URNs DO include catalog name when include_catalog_name_in_ids=True.
    """
    events_file = tmp_path / "urn_test_with_catalog.json"

    config: Dict = {
        "run_id": "hms3-urn-with-catalog",
        "source": {
            "type": "hive-metastore",
            "config": {
                "connection_type": "thrift",
                "host_port": "localhost:9084",
                "use_kerberos": False,
                "catalog_name": "spark_catalog",
                "include_catalog_name_in_ids": True,
                "database_pattern": {"allow": ["^test_db$"]},
                "table_pattern": {"allow": ["^users$"]},
            },
        },
        "sink": {"type": "file", "config": {"filename": str(events_file)}},
    }

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    content = events_file.read_text()

    # URN should include catalog name
    assert "spark_catalog.test_db.users" in content

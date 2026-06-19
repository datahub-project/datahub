import json
import subprocess
from pathlib import Path
from typing import List

import pymysql
import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.mariadb import MariaDBSource
from datahub.testing import mce_helpers
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2024-04-14 07:00:00"
MARIADB_PORT = 3306
MARIADB_USAGE_PORT = 53301


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/mariadb"


def is_mariadb_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mariadb is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep 'port: {port}  mariadb.org binary distribution'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def mariadb_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mariadb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmariadb",
            MARIADB_PORT,
            timeout=120,
            checker=lambda: is_mariadb_up("testmariadb", MARIADB_PORT),
        )
        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("mariadb_to_file.yml", "mariadb_mces_golden.json"),
    ],
)
@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_mariadb_ingest_no_db(
    mariadb_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
    mock_time,
    config_file,
    golden_file,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to these.
    ignore_paths: List[str] = [
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['created'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_altered'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['distinctValueFrequencies'\]\[\d+\]\['value'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['max'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['min'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]\[\d+\]",
    ]

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "mariadb_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


# Read/transform workload run against the live server. It must run here (not in
# setup.sql) because MariaDB executes init scripts on a temporary server and then
# restarts, which wipes the in-memory performance_schema digests.
# The transform avoids variadic functions (e.g. SUBSTRING_INDEX): MariaDB's digest
# text truncates such argument lists to `...`, which the SQL parser cannot read, so
# performance_schema-mode lineage would be lost. general_log keeps literal SQL.
_USAGE_WORKLOAD = [
    "SELECT id, name FROM raw_customer_data WHERE id = 1",
    "SELECT id, name FROM raw_customer_data WHERE id = 2",
    "SELECT id, name FROM raw_customer_data WHERE id = 3",
    "SELECT COUNT(*) FROM raw_customer_data",
    "INSERT INTO processed_customers (customer_id, full_name) "
    "SELECT id, name FROM raw_customer_data",
]


def _execute_usage_workload() -> None:
    conn = pymysql.connect(
        host="localhost",
        port=MARIADB_USAGE_PORT,
        user="root",
        password="password",
        database="test_usage_db",
    )
    try:
        with conn.cursor() as cursor:
            for _ in range(3):
                for statement in _USAGE_WORKLOAD:
                    cursor.execute(statement)
                    cursor.fetchall()
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def mariadb_usage_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.usage.yml", "mariadb-usage"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmariadbusage",
            MARIADB_PORT,
            timeout=120,
            # Match the real server's ready line (with port), not the temporary
            # init server that logs the same banner without a port.
            checker=lambda: is_mariadb_up("testmariadbusage", MARIADB_PORT),
        )
        _execute_usage_workload()
        yield docker_services


def _run_usage_pipeline(usage_source: str, output_path: Path) -> list:
    pipeline = Pipeline.create(
        {
            "run_id": f"mariadb-usage-{usage_source}",
            "source": {
                "type": "mariadb",
                "config": {
                    "host_port": f"localhost:{MARIADB_USAGE_PORT}",
                    "database": "test_usage_db",
                    "username": "root",
                    "password": "password",
                    "include_usage_statistics": True,
                    "usage_source": usage_source,
                    "profiling": {"enabled": False},
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output_path.read_text())


def _aspects(mcps: list, aspect_name: str) -> list:
    return [m for m in mcps if m.get("aspectName") == aspect_name]


def _assert_query_lineage_present(mcps: list) -> None:
    lineages = _aspects(mcps, "upstreamLineage")
    found = any(
        "processed_customers" in m["entityUrn"]
        and any(
            "raw_customer_data" in upstream["dataset"]
            for upstream in m["aspect"]["json"]["upstreams"]
        )
        for m in lineages
    )
    assert found, "expected query lineage processed_customers <- raw_customer_data"


@pytest.mark.integration
def test_mariadb_usage_performance_schema(mariadb_usage_runner, tmp_path):
    mcps = _run_usage_pipeline("performance_schema", tmp_path / "perf.json")

    usage = _aspects(mcps, "datasetUsageStatistics")
    assert any("raw_customer_data" in m["entityUrn"] for m in usage), (
        "expected usage statistics for raw_customer_data"
    )
    # performance_schema digests are aggregated across users: no per-user breakdown.
    assert all(not m["aspect"]["json"].get("userCounts") for m in usage)

    _assert_query_lineage_present(mcps)


@pytest.mark.integration
def test_mariadb_usage_general_log(mariadb_usage_runner, tmp_path):
    mcps = _run_usage_pipeline("general_log", tmp_path / "glog.json")

    usage = _aspects(mcps, "datasetUsageStatistics")
    assert any("raw_customer_data" in m["entityUrn"] for m in usage), (
        "expected usage statistics for raw_customer_data"
    )
    # general_log carries the executing user, so usage is attributed per user.
    user_urns = {
        uc["user"]
        for m in usage
        for uc in (m["aspect"]["json"].get("userCounts") or [])
    }
    assert "urn:li:corpuser:root" in user_urns, (
        f"expected per-user attribution for root, got {user_urns}"
    )

    _assert_query_lineage_present(mcps)


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "host_port": "localhost:53300",
                "database": "test_db",
                "username": "root",
                "password": "password",
            },
            True,
        ),
        (
            {
                "host_port": "localhost:5330",
                "database": "wrong_db",
                "username": "wrong_user",
                "password": "wrong_pass",
            },
            False,
        ),
    ],
)
@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_mariadb_test_connection(mariadb_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(MariaDBSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )

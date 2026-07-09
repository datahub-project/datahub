import subprocess
from typing import List

import pytest
import time_machine

from datahub.ingestion.source.sql.mariadb import MariaDBSource
from datahub.testing import mce_helpers
from tests.test_helpers import mysql_usage_helpers, test_connection_helpers
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
    config_file,
    golden_file,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # These custom properties are set at ingest time and FROZEN_TIME does not apply to them.
    ignore_paths: List[str] = [
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['created'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_altered'\]",
    ]

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "mariadb_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


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
        mysql_usage_helpers.execute_usage_workload(
            port=MARIADB_USAGE_PORT, password="password"
        )
        yield docker_services


@pytest.mark.integration
def test_mariadb_usage_performance_schema(mariadb_usage_runner, tmp_path):
    mcps = mysql_usage_helpers.run_usage_pipeline(
        platform="mariadb",
        usage_source="performance_schema",
        port=MARIADB_USAGE_PORT,
        password="password",
        output_path=tmp_path / "perf.json",
    )

    usage = mysql_usage_helpers.aspects(mcps, "datasetUsageStatistics")
    assert any("raw_customer_data" in m["entityUrn"] for m in usage), (
        "expected usage statistics for raw_customer_data"
    )
    # performance_schema digests are aggregated across users: no per-user breakdown.
    assert all(not m["aspect"]["json"].get("userCounts") for m in usage)

    mysql_usage_helpers.assert_query_lineage_present(mcps)
    mysql_usage_helpers.assert_top_sql_queries(mcps, usage_source="performance_schema")


@pytest.mark.integration
def test_mariadb_usage_general_log(mariadb_usage_runner, tmp_path):
    mcps = mysql_usage_helpers.run_usage_pipeline(
        platform="mariadb",
        usage_source="general_log",
        port=MARIADB_USAGE_PORT,
        password="password",
        output_path=tmp_path / "glog.json",
    )

    usage = mysql_usage_helpers.aspects(mcps, "datasetUsageStatistics")
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

    mysql_usage_helpers.assert_query_lineage_present(mcps)
    mysql_usage_helpers.assert_top_sql_queries(mcps, usage_source="general_log")


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

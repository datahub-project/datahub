import subprocess
from datetime import datetime, timezone

import pytest
import time_machine

from datahub.ingestion.source.sql.mysql import MySQLSource
from datahub.testing import mce_helpers
from tests.test_helpers import mysql_usage_helpers, test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"
FROZEN_TIME_DT = datetime.fromisoformat(FROZEN_TIME).replace(tzinfo=timezone.utc)
MYSQL_PORT = 3306
MYSQL_USAGE_PORT = 53308


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/mysql"


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '/usr/sbin/mysqld: ready for connections.' | grep {port}"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def mysql_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmysql",
            MYSQL_PORT,
            timeout=120,
            checker=lambda: is_mysql_up("testmysql", MYSQL_PORT),
        )
        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("mysql_to_file_with_db.yml", "mysql_mces_with_db_golden.json"),
        ("mysql_to_file_no_db.yml", "mysql_mces_no_db_golden.json"),
        ("mysql_profile_table_level_only.yml", "mysql_table_level_only.json"),
        (
            "mysql_profile_table_row_count_estimate_only.yml",
            "mysql_table_row_count_estimate_only.json",
        ),
    ],
)
@time_machine.travel(FROZEN_TIME_DT, tick=False)
@pytest.mark.integration
def test_mysql_ingest_no_db(
    mysql_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
    config_file,
    golden_file,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mysql_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


@pytest.fixture(scope="module")
def mysql_usage_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.usage.yml", "mysql-usage"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmysqlusage",
            MYSQL_PORT,
            timeout=120,
            checker=lambda: is_mysql_up("testmysqlusage", MYSQL_PORT),
        )
        mysql_usage_helpers.execute_usage_workload(
            port=MYSQL_USAGE_PORT, password="example"
        )
        yield docker_services


@pytest.mark.integration
def test_mysql_usage_performance_schema(mysql_usage_runner, tmp_path):
    mcps = mysql_usage_helpers.run_usage_pipeline(
        platform="mysql",
        usage_source="performance_schema",
        port=MYSQL_USAGE_PORT,
        password="example",
        output_path=tmp_path / "perf.json",
    )

    usage = mysql_usage_helpers.aspects(mcps, "datasetUsageStatistics")
    assert any("raw_customer_data" in m["entityUrn"] for m in usage), (
        "expected usage statistics for raw_customer_data"
    )
    # performance_schema digests are aggregated across users: no per-user breakdown.
    assert all(not m["aspect"]["json"].get("userCounts") for m in usage)

    mysql_usage_helpers.assert_query_lineage_present(mcps)


@pytest.mark.integration
def test_mysql_usage_general_log(mysql_usage_runner, tmp_path):
    mcps = mysql_usage_helpers.run_usage_pipeline(
        platform="mysql",
        usage_source="general_log",
        port=MYSQL_USAGE_PORT,
        password="example",
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


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "host_port": "localhost:53307",
                "database": "northwind",
                "username": "root",
                "password": "example",
                "stateful_ingestion": {"enabled": "true"},
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
@time_machine.travel(FROZEN_TIME_DT, tick=False)
@pytest.mark.integration
def test_mysql_test_connection(mysql_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(MySQLSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )

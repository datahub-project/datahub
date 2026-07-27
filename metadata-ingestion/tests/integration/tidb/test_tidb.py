import subprocess
from datetime import datetime, timezone

import pymysql
import pytest
import time_machine

from datahub.ingestion.source.sql.tidb import TiDBSource
from datahub.testing import mce_helpers
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"
FROZEN_TIME_DT = datetime.fromisoformat(FROZEN_TIME).replace(tzinfo=timezone.utc)
TIDB_PORT = 4000
# Host port the container's 4000 is mapped to in docker-compose.yml.
TIDB_HOST_PORT = 54000


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/tidb"


def is_tidb_up(container_name: str) -> bool:
    """Check whether the TiDB server has finished starting up.

    TiDB logs this line once the MySQL protocol listener is accepting
    connections, unlike MySQL which logs "ready for connections".
    """
    cmd = f"docker logs {container_name} 2>&1 | grep 'server is running MySQL protocol'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def load_setup_sql(setup_sql_path: str) -> None:
    """Load the fixture schema into the running TiDB container.

    TiDB has no equivalent of MySQL's /docker-entrypoint-initdb.d hook, so the
    schema is applied from the test after the server is reachable.
    """
    with open(setup_sql_path) as f:
        script = f.read()

    statements = [s.strip() for s in script.split(";") if s.strip()]
    connection = pymysql.connect(
        host="localhost",
        port=TIDB_HOST_PORT,
        user="root",
        password="",
    )
    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
    finally:
        connection.close()


@pytest.fixture(scope="module")
def tidb_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "tidb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testtidb",
            TIDB_PORT,
            timeout=120,
            checker=lambda: is_tidb_up("testtidb"),
        )
        load_setup_sql(str(test_resources_dir / "setup" / "setup.sql"))
        yield docker_services


@time_machine.travel(FROZEN_TIME_DT, tick=False)
@pytest.mark.integration
def test_tidb_ingest(
    tidb_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
):
    config_file = (test_resources_dir / "tidb_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "tidb_mces.json",
        golden_path=test_resources_dir / "tidb_mces_golden.json",
    )


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "host_port": f"localhost:{TIDB_HOST_PORT}",
                "database": "datahub_test",
                "username": "root",
                "password": "",
            },
            True,
        ),
        (
            {
                "host_port": "localhost:5400",
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
def test_tidb_test_connection(tidb_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(TiDBSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )

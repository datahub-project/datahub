import time

import pytest

from tests.integration.mssql.common import CONTAINER, run_sqlcmd
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.fixture(scope="module")
def mssql_runner(docker_compose_runner, pytestconfig, request):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mssql"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "sql-server"
    ) as docker_services:
        # Ephemeral host port: a leaked container from a prior CI run can
        # never hold onto it. Recipe ymls in source_files/ pick it up via
        # ${MSSQL_PORT}.
        mssql_port = docker_services.port_for(CONTAINER, 1433)
        mp = pytest.MonkeyPatch()
        mp.setenv("MSSQL_PORT", str(mssql_port))
        request.addfinalizer(mp.undo)

        # Wait for SQL Server to be ready. We wait an extra couple seconds, as the port being available
        # does not mean the server is accepting connections.
        # TODO: find a better way to check for liveness.
        wait_for_port(docker_services, CONTAINER, 1433)
        time.sleep(5)

        # Run the setup.sql file to populate the database; -b and -V 1, to fail on error
        ret = run_sqlcmd("-d", "master", "-i", "/setup/setup.sql", "-V", "1")
        if ret.returncode != 0:
            print(f"sqlcmd return code: {ret.returncode}")
            print(f"sqlcmd stdout:\n{ret.stdout}")
            print(f"sqlcmd stderr:\n{ret.stderr}")
            raise Exception(
                "SQL Server setup failed. Check the output above for details."
            )

        yield docker_services

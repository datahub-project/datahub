"""MSSQL JDBC integration tests."""
import logging
from pathlib import Path
from typing import Any

import pytest
from freezegun import freeze_time

from datahub.testing.docker_utils import wait_for_port
from tests.integration.jdbc.test_jdbc_common import (
    get_db_container_checker,
    prepare_config_file,
    run_datahub_ingest,
)
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2023-10-15 07:00:00"
MSSQL_PORT = 41433
MSSQL_READY_MSG = "SQL Server is now ready for client connections"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    return pytestconfig.rootpath / "tests/integration/jdbc"


@pytest.fixture(scope="module")
def mssql_runner(
    docker_compose_runner: Any, pytestconfig: Any, test_resources_dir: Path
) -> Any:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.mssql.yml", "testmssql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmssql",
            MSSQL_PORT,
            timeout=120,
            checker=get_db_container_checker("testmssql", MSSQL_READY_MSG),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mssql_ingest(
    mssql_runner: Any, pytestconfig: Any, test_resources_dir: Path, tmp_path: Path
) -> None:
    """Test MSSQL ingestion."""
    config_file = test_resources_dir / "mssql_to_file.yml"
    tmp_config = prepare_config_file(config_file, tmp_path, "mssql")
    run_datahub_ingest(str(tmp_config))
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mssql_mces.json",
        golden_path=test_resources_dir / "mssql_mces_golden.json",
    )

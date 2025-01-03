"""MySQL JDBC integration tests."""
import logging
from pathlib import Path
from typing import Any

import pytest
from freezegun import freeze_time

from datahub.testing.docker_utils import wait_for_port
from tests.integration.jdbc.test_jdbc_common import (
    get_db_container_checker,
    prepare_config_file,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

FROZEN_TIME = "2025-01-01 07:00:00"
MYSQL_PORT = 43306
MYSQL_READY_MSG = "ready for connections"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    return pytestconfig.rootpath / "tests/integration/jdbc"


@pytest.fixture(scope="module")
def mysql_runner(
    docker_compose_runner: Any, pytestconfig: Any, test_resources_dir: Path
) -> Any:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.mysql.yml", "testmysql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmysql",
            MYSQL_PORT,
            timeout=120,
            checker=get_db_container_checker("testmysql", MYSQL_READY_MSG),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest(
    mysql_runner: Any, pytestconfig: Any, test_resources_dir: Path, tmp_path: Path
) -> None:
    """Test MySQL ingestion."""
    config_file = test_resources_dir / "mysql_to_file.yml"
    tmp_config = prepare_config_file(config_file, tmp_path, "mysql")

    run_datahub_cmd(["ingest", "-c", f"{tmp_config}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mysql_mces.json",
        golden_path=test_resources_dir / "mysql_mces_golden.json",
        ignore_paths=[],
    )

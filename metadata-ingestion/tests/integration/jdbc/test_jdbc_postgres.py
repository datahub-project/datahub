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
POSTGRES_PORT = 45432
POSTGRES_READY_MSG = "database system is ready to accept connections"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    return pytestconfig.rootpath / "tests/integration/jdbc"


@pytest.fixture(scope="module")
def postgres_runner(
    docker_compose_runner: Any, pytestconfig: Any, test_resources_dir: Path
) -> Any:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.postgres.yml", "testpostgres"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testpostgres",
            POSTGRES_PORT,
            timeout=120,
            checker=get_db_container_checker("testpostgres", POSTGRES_READY_MSG),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_postgres_ingest(
    postgres_runner: Any, pytestconfig: Any, test_resources_dir: Path, tmp_path: Path
) -> None:
    """Test Postgres ingestion."""
    config_file = test_resources_dir / "postgres_to_file.yml"
    tmp_config = prepare_config_file(config_file, tmp_path, "postgres")

    run_datahub_cmd(["ingest", "-c", f"{tmp_config}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "postgres_mces.json",
        golden_path=test_resources_dir / "postgres_mces_golden.json",
        ignore_paths=[],
    )

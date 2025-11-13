import logging

import pytest
import requests
from freezegun import freeze_time

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration_batch_1
FROZEN_TIME = "2021-10-25 13:00:00"
CONFLUENT_CLOUD_MOCK_SERVER = "http://localhost:8888"


def check_mock_api_ready(server_url: str = CONFLUENT_CLOUD_MOCK_SERVER) -> bool:
    """
    Check if the Confluent Cloud mock API is ready.

    Args:
        server_url: The base URL of the mock API

    Returns:
        bool: True if the API is ready, False otherwise
    """
    try:
        response = requests.get(f"{server_url}/health")
        logger.debug(
            f"check-mock-api-ready: health check: {response.status_code} {response.json()}"
        )
        response.raise_for_status()
        return response.json().get("status") == "healthy"
    except Exception as e:
        logger.debug(f"check-mock-api-ready: exception: {e}")
        return False


@pytest.fixture(scope="module")
def confluent_cloud_mock_runner(
    docker_compose_runner, pytestconfig, test_resources_dir
):
    docker_compose_file = [str(test_resources_dir / "docker-compose.override.yml")]

    with docker_compose_runner(
        docker_compose_file, "kafka-connect-confluent-cloud"
    ) as docker_services:
        docker_services.wait_until_responsive(
            timeout=60,
            pause=5,
            check=lambda: check_mock_api_ready(CONFLUENT_CLOUD_MOCK_SERVER),
        )

        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka-connect-confluent-cloud"


@freeze_time(FROZEN_TIME)
def test_kafka_connect_confluent_cloud_ingest(
    confluent_cloud_mock_runner, pytestconfig, tmp_path, test_resources_dir
):
    """Test ingestion from Confluent Cloud mock API"""
    config_file = (
        test_resources_dir / "kafka_connect_confluent_cloud_to_file.yml"
    ).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_confluent_cloud_mces.json",
        golden_path=test_resources_dir
        / "kafka_connect_confluent_cloud_mces_golden.json",
        ignore_paths=[],
    )

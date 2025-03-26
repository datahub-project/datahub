import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

# Test resources and constants
FROZEN_TIME = "2025-03-25 12:00:00"

pytestmark = pytest.mark.integration_batch_2


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hex"


def is_hex_mock_api_up(container_name: str) -> bool:
    """Check if the mock API server is up and running"""
    try:
        response = requests.get("http://localhost:8000/health")
        response.raise_for_status()
        return True
    except (requests.RequestException, ConnectionError):
        return False


@pytest.fixture(scope="module")
def hex_mock_api_runner(docker_compose_runner, test_resources_dir):
    docker_dir = test_resources_dir / "docker"

    # Start Docker Compose
    with docker_compose_runner(
        docker_dir / "docker-compose.yml", "hex-mock"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "hex-mock-api",
            8000,
            timeout=30,
            checker=lambda: is_hex_mock_api_up("hex-mock-api"),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_hex_ingestion(pytestconfig, hex_mock_api_runner, test_resources_dir, tmp_path):
    """Test Hex metadata ingestion using a mock API server."""
    # Path for the golden file
    golden_dir = test_resources_dir / "golden"
    golden_path = golden_dir / "hex_mce_golden.json"

    # Create the pipeline
    pipeline = Pipeline.create(
        {
            "run_id": "hex-test",
            "source": {
                "type": "hex",
                "config": {
                    "workspace_name": "test-workspace",
                    "token": "test-token",
                    "base_url": "http://localhost:8000/api/v1",  # Mock API URL
                    "platform_instance": "hex_test",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/hex_mces.json",
                },
            },
        }
    )

    # Run the pipeline
    pipeline.run()
    pipeline.raise_from_status()

    # Check against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/hex_mces.json",
        golden_path=golden_path,
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )

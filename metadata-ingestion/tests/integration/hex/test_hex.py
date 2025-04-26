import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2025-03-25 12:00:00"

pytestmark = pytest.mark.integration_batch_2


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hex"


def is_mock_api_up(port: int) -> bool:
    """Check if the mock API server is up and running"""
    try:
        response = requests.get(f"http://localhost:{port}/health")
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
            checker=lambda: is_mock_api_up(8000),
        )
        wait_for_port(
            docker_services,
            "datahub-mock-api",
            8010,
            timeout=30,
            checker=lambda: is_mock_api_up(8010),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_hex_ingestion(pytestconfig, hex_mock_api_runner, test_resources_dir, tmp_path):
    # Path for the golden file
    golden_dir = test_resources_dir / "golden"
    golden_path = golden_dir / "hex_mce_golden.json"

    # Create the pipeline
    pipeline = Pipeline.create(
        {
            "pipeline_name": "test-hex",
            "source": {
                "type": "hex",
                "config": {
                    "workspace_name": "test-workspace",
                    "token": "test-token",
                    "base_url": "http://localhost:8000/api/v1",  # Mock Hex API URL
                    "platform_instance": "hex_test",
                    "include_lineage": False,
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


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_hex_ingestion_with_lineage(
    pytestconfig, hex_mock_api_runner, test_resources_dir, tmp_path
):
    # Path for the golden file
    golden_dir = test_resources_dir / "golden"
    golden_path = golden_dir / "hex_mce_golden_with_lineage.json"

    # Create the pipeline
    pipeline = Pipeline.create(
        {
            "pipeline_name": "test-hex-with-lineage",
            "datahub_api": {
                "server": "http://localhost:8010",  # Mock DataHub API URL
            },
            "source": {
                "type": "hex",
                "config": {
                    "workspace_name": "some-hex-workspace",
                    "token": "test-token",
                    "base_url": "http://localhost:8000/api/v1",  # Mock Hex API URL
                    "platform_instance": "hex_test",
                    "include_lineage": True,
                    "datahub_page_size": 1,  # Force pagination
                    "stateful_ingestion": {
                        "enabled": False,
                    },
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

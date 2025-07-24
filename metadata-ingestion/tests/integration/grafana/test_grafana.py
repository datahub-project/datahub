import logging
import time
from base64 import b64encode

import pytest
import pytest_docker.plugin
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers import fs_helpers
from tests.test_helpers.docker_helpers import cleanup_image

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2024-07-12 12:00:00"

logger = logging.getLogger(__name__)


class GrafanaClient:
    def __init__(self, url, admin_user, admin_password):
        self.url = url
        self.auth = (admin_user, admin_password)
        self.headers = {
            "Authorization": f"Basic {b64encode(f'{admin_user}:{admin_password}'.encode()).decode()}",
            "Content-Type": "application/json",
        }

    def create_service_account(self, name, role):
        service_account_payload = {"name": name, "role": role, "isDisabled": False}
        try:
            response = requests.post(
                f"{self.url}/api/serviceaccounts",
                headers=self.headers,
                json=service_account_payload,
            )
            response.raise_for_status()
            service_account = response.json()
            return service_account
        except requests.exceptions.RequestException as e:
            logging.error(f"Error creating service account: {e}")
            return None

    def create_api_key(self, service_account_id, key_name, role):
        api_key_payload = {"name": key_name, "role": role}
        try:
            response = requests.post(
                f"{self.url}/api/serviceaccounts/{service_account_id}/tokens",
                headers=self.headers,
                json=api_key_payload,
            )
            response.raise_for_status()
            api_key = response.json()
            return api_key["key"]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error creating API key: {e}")
            return None


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/grafana"


@pytest.fixture(scope="module")
def test_api_key(loaded_grafana):
    # Get the actual mapped port from Docker services
    grafana_port = loaded_grafana.port_for("grafana", 3000)
    url = f"http://localhost:{grafana_port}"
    admin_user = "admin"
    admin_password = "admin"

    grafana_client = GrafanaClient(url, admin_user, admin_password)

    service_account = grafana_client.create_service_account(
        name="example-service-account", role="Admin"
    )
    if service_account:
        api_key = grafana_client.create_api_key(
            service_account_id=service_account["id"],
            key_name="example-api-key",
            role="Admin",
        )
        if api_key:
            return api_key
        else:
            pytest.fail("Failed to create API key for the service account")
    else:
        pytest.fail("Failed to create service account")


@pytest.fixture(scope="module")
def loaded_grafana(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "grafana"
    ) as docker_services:
        # Docker Compose now waits for health check to pass before considering service ready
        # Verify we can access the API endpoints as an additional safety check
        verify_grafana_api_ready(docker_services)
        yield docker_services

    cleanup_image("grafana/grafana")


def verify_grafana_api_ready(docker_services: pytest_docker.plugin.Services) -> None:
    """Robust verification that Grafana API is fully accessible after health check passes"""
    import requests

    grafana_port = docker_services.port_for("grafana", 3000)
    base_url = f"http://localhost:{grafana_port}"

    # Wait for API endpoints to be fully ready (health check might pass but API still initializing)
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Test both basic API access and service account creation capability
            api_url = f"{base_url}/api/search"
            resp = requests.get(api_url, auth=("admin", "admin"), timeout=10)

            if resp.status_code == 200:
                # Also verify service account API is ready (needed for test_api_key fixture)
                # Service accounts might not be available in all Grafana versions
                sa_url = f"{base_url}/api/serviceaccounts"
                sa_resp = requests.get(sa_url, auth=("admin", "admin"), timeout=10)

                if sa_resp.status_code == 200:
                    logging.info(
                        f"Grafana API endpoints fully ready with service accounts (attempt {attempt + 1})"
                    )
                    return
                elif sa_resp.status_code == 404:
                    # Service accounts API not available - this is okay for older Grafana versions
                    logging.info(
                        f"Grafana API ready, service accounts not available (attempt {attempt + 1})"
                    )
                    return
                else:
                    logging.debug(
                        f"Service account API not ready yet: {sa_resp.status_code}"
                    )
            else:
                logging.debug(f"Basic API not ready yet: {resp.status_code}")

        except Exception as e:
            logging.debug(f"API readiness check failed (attempt {attempt + 1}): {e}")

        if attempt < max_attempts - 1:
            time.sleep(2)

    logging.warning(f"Grafana API may not be fully ready after {max_attempts} attempts")
    # Don't fail here - let the test proceed and provide better error info if needed


@freeze_time(FROZEN_TIME)
def test_grafana_basic_ingest(
    loaded_grafana, pytestconfig, tmp_path, test_resources_dir, test_api_key
):
    """Test ingestion with lineage enabled"""

    with fs_helpers.isolated_filesystem(tmp_path):
        pipeline = Pipeline.create(
            {
                "run_id": "grafana-test",
                "source": {
                    "type": "grafana",
                    "config": {
                        "url": "http://localhost:3000",
                        "service_account_token": test_api_key,
                        "ingest_tags": False,
                        "ingest_owners": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": "./grafana_basic_mcps.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="grafana_basic_mcps.json",
            golden_path=test_resources_dir / "grafana_basic_mcps_golden.json",
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            ],
        )


@freeze_time(FROZEN_TIME)
def test_grafana_ingest(
    loaded_grafana, pytestconfig, tmp_path, test_resources_dir, test_api_key
):
    """Test ingestion with lineage enabled"""

    with fs_helpers.isolated_filesystem(tmp_path):
        pipeline = Pipeline.create(
            {
                "run_id": "grafana-test",
                "source": {
                    "type": "grafana",
                    "config": {
                        "url": "http://localhost:3000",
                        "service_account_token": test_api_key,
                        "ingest_tags": True,
                        "ingest_owners": True,
                        "connection_to_platform_map": {
                            "test-postgres": {
                                "platform": "postgres",
                                "database": "grafana",
                                "platform_instance": "local",
                                "env": "PROD",
                            },
                            "test-prometheus": {
                                "platform": "prometheus",
                                "platform_instance": "local",
                                "env": "PROD",
                            },
                        },
                        "platform_instance": "local-grafana",
                        "env": "PROD",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": "./grafana_mcps.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="grafana_mcps.json",
            golden_path=test_resources_dir / "grafana_mcps_golden.json",
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            ],
        )

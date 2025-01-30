import logging
import time
from base64 import b64encode

import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port

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
def test_api_key():
    # Example usage:
    url = "http://localhost:3000"
    admin_user = "admin"
    admin_password = "admin"

    grafana_client = GrafanaClient(url, admin_user, admin_password)

    # Step 1: Create the service account
    service_account = grafana_client.create_service_account(
        name="example-service-account", role="Viewer"
    )
    if service_account:
        print(f"Service Account Created: {service_account}")

        # Step 2: Create the API key for the service account
        api_key = grafana_client.create_api_key(
            service_account_id=service_account["id"],
            key_name="example-api-key",
            role="Admin",
        )
        if api_key:
            print("Service Account API Key:", api_key)
            return api_key
        else:
            print("Failed to create API key for the service account")
    else:
        print("Failed to create service account")


@pytest.fixture(scope="module")
def loaded_grafana(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "grafana"
    ) as docker_services:
        wait_for_port(
            docker_services,
            container_name="grafana",
            container_port=3000,
            timeout=300,
        )
        yield docker_services

    # The Grafana image can be large, so we remove it after the test.
    cleanup_image("grafana/grafana")


@freeze_time(FROZEN_TIME)
def test_grafana_dashboard(loaded_grafana, pytestconfig, tmp_path, test_resources_dir):
    # Wait for Grafana to be up and running
    url = "http://localhost:3000/api/health"
    for i in range(30):
        logging.info("waiting for Grafana to start...")
        time.sleep(5)
        resp = requests.get(url)
        if resp.status_code == 200:
            logging.info(f"Grafana started after waiting {i * 5} seconds")
            break
    else:
        pytest.fail("Grafana did not start in time")

    # Check if the default dashboard is loaded
    dashboard_url = "http://localhost:3000/api/dashboards/uid/default"
    resp = requests.get(dashboard_url, auth=("admin", "admin"))
    assert resp.status_code == 200, "Failed to load default dashboard"
    dashboard = resp.json()

    assert dashboard["dashboard"]["title"] == "Default Dashboard", (
        "Default dashboard title mismatch"
    )
    assert any(panel["type"] == "text" for panel in dashboard["dashboard"]["panels"]), (
        "Default dashboard missing text panel"
    )

    # Verify the output. (You can add further checks here if needed)
    logging.info("Default dashboard verified successfully")


@freeze_time(FROZEN_TIME)
def test_grafana_ingest(
    loaded_grafana, pytestconfig, tmp_path, test_resources_dir, test_api_key
):
    # Wait for Grafana to be up and running
    url = "http://localhost:3000/api/health"
    for i in range(30):
        logging.info("waiting for Grafana to start...")
        time.sleep(5)
        resp = requests.get(url)
        if resp.status_code == 200:
            logging.info(f"Grafana started after waiting {i * 5} seconds")
            break
    else:
        pytest.fail("Grafana did not start in time")

    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):
        # Run grafana ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "grafana-test-simple",
                "source": {
                    "type": "grafana",
                    "config": {
                        "url": "http://localhost:3000",
                        "service_account_token": test_api_key,
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

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="grafana_mcps.json",
            golden_path=test_resources_dir / "grafana_mcps_golden.json",
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_event_time'\]",
            ],
        )

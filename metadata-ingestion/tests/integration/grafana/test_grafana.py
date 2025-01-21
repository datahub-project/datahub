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
    url = "http://localhost:3000"
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
        wait_for_port(
            docker_services,
            container_name="grafana",
            container_port=3000,
            timeout=300,
        )
        yield docker_services

    cleanup_image("grafana/grafana")


def wait_for_grafana(url: str, max_attempts: int = 30, sleep_time: int = 5) -> bool:
    """Helper function to wait for Grafana to start"""
    for i in range(max_attempts):
        logging.info("waiting for Grafana to start...")
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                logging.info(f"Grafana started after waiting {i * sleep_time} seconds")
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(sleep_time)

    pytest.fail("Grafana did not start in time")


@freeze_time(FROZEN_TIME)
def test_grafana_basic_ingest(
    loaded_grafana, pytestconfig, tmp_path, test_resources_dir, test_api_key
):
    """Test ingestion with lineage enabled"""
    wait_for_grafana("http://localhost:3000/api/health")

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
    wait_for_grafana("http://localhost:3000/api/health")

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

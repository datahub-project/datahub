import logging
import time
from base64 import b64encode

import pytest
import pytest_docker.plugin
import requests
from freezegun import freeze_time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers import fs_helpers
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2024-07-12 12:00:00"

# Expected dashboards that should be provisioned during the test setup
# If new dashboards are added to the test setup, they should be listed here
# to ensure provisioning is complete before running ingestion tests
EXPECTED_DASHBOARDS = {"Test Integration Dashboard"}

logger = logging.getLogger(__name__)


class GrafanaClient:
    def __init__(self, url, admin_user, admin_password):
        self.url = url
        self.auth = (admin_user, admin_password)
        self.headers = {
            "Authorization": f"Basic {b64encode(f'{admin_user}:{admin_password}'.encode()).decode()}",
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504, 429],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def create_service_account(self, name, role, max_retries=5):
        service_account_payload = {"name": name, "role": role, "isDisabled": False}

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    f"{self.url}/api/serviceaccounts",
                    headers=self.headers,
                    json=service_account_payload,
                    timeout=15,
                )
                response.raise_for_status()
                service_account = response.json()
                logging.info(
                    f"Successfully created service account '{name}' on attempt {attempt + 1}"
                )
                return service_account
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logging.warning(
                        f"Attempt {attempt + 1} failed to create service account: {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logging.error(
                        f"Failed to create service account after {max_retries} attempts: {e}"
                    )
                    return None
        return None

    def create_api_key(self, service_account_id, key_name, role, max_retries=5):
        api_key_payload = {"name": key_name, "role": role}

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    f"{self.url}/api/serviceaccounts/{service_account_id}/tokens",
                    headers=self.headers,
                    json=api_key_payload,
                    timeout=15,
                )
                response.raise_for_status()
                api_key = response.json()
                logging.info(
                    f"Successfully created API key '{key_name}' on attempt {attempt + 1}"
                )
                return api_key["key"]
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logging.warning(
                        f"Attempt {attempt + 1} failed to create API key: {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logging.error(
                        f"Failed to create API key after {max_retries} attempts: {e}"
                    )
                    return None
        return None


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/grafana"


@pytest.fixture(scope="module")
def grafana_provisioning_complete(loaded_grafana):
    """Ensure all Grafana entities are provisioned before running tests"""
    verify_grafana_entities_provisioned(timeout=180)
    return True


@pytest.fixture(scope="module")
def test_api_key(loaded_grafana):
    # Get the actual mapped port from Docker services

    url = "http://localhost:3000"
    admin_user = "admin"
    admin_password = "admin"

    # Wait for Grafana to be fully ready before creating service account
    verify_grafana_fully_ready(loaded_grafana, timeout=180)

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
        # Wait for all services to be ready
        wait_for_port(docker_services, "postgres", 5432, timeout=90)

        # Prometheus container doesn't have bash, so use a simple HTTP check
        def check_prometheus_ready():
            try:
                prometheus_port = docker_services.port_for("prometheus", 9090)
                response = requests.get(
                    f"http://localhost:{prometheus_port}/-/ready", timeout=5
                )
                return response.status_code == 200
            except (requests.exceptions.RequestException, Exception):
                return False

        wait_for_port(
            docker_services,
            "prometheus",
            9090,
            timeout=90,
            checker=check_prometheus_ready,
        )
        wait_for_port(docker_services, "grafana", 3000, timeout=180)

        # Additional verification that Grafana API is fully accessible
        verify_grafana_api_ready(docker_services)
        yield docker_services

    cleanup_image("grafana/grafana")


def verify_grafana_api_ready(docker_services: pytest_docker.plugin.Services) -> None:
    """Robust verification that Grafana API is fully accessible after health check passes"""

    base_url = "http://localhost:3000"

    # Configure requests session with retries
    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    # Wait for API endpoints to be fully ready (health check might pass but API still initializing)
    max_attempts = 60
    for attempt in range(max_attempts):
        try:
            # Test both basic API access and service account creation capability
            api_url = f"{base_url}/api/search"
            resp = session.get(api_url, auth=("admin", "admin"), timeout=15)

            if resp.status_code == 200:
                # Also verify service account API is ready (needed for test_api_key fixture)
                sa_url = f"{base_url}/api/serviceaccounts"
                sa_resp = session.get(sa_url, auth=("admin", "admin"), timeout=15)

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
            time.sleep(3)

    logging.warning(f"Grafana API may not be fully ready after {max_attempts} attempts")
    # Don't fail here - let the test proceed and provide better error info if needed


def verify_grafana_fully_ready(
    docker_services: pytest_docker.plugin.Services, timeout: int = 120
) -> None:
    """Extended verification that Grafana is fully ready for service account operations"""
    base_url = "http://localhost:3000"

    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    end_time = time.time() + timeout

    while time.time() < end_time:
        try:
            # Test multiple endpoints to ensure full readiness
            endpoints_to_check = [
                f"{base_url}/api/health",
                f"{base_url}/api/org",
                f"{base_url}/api/serviceaccounts",
            ]

            all_ready = True
            for endpoint in endpoints_to_check:
                resp = session.get(endpoint, auth=("admin", "admin"), timeout=10)
                if resp.status_code not in [
                    200,
                    404,
                ]:  # 404 is OK for service accounts in older versions
                    all_ready = False
                    break

            if all_ready:
                logging.info("Grafana is fully ready for operations")
                return

        except Exception as e:
            logging.debug(f"Grafana readiness check failed: {e}")

        time.sleep(2)

    logging.warning(f"Grafana may not be fully ready after {timeout}s timeout")


def verify_grafana_entities_provisioned(timeout: int = 180) -> None:
    """Wait for Grafana entities to be provisioned before running ingestion tests

    This function can be extended in the future to validate other entity types
    like data sources, folders, etc.
    """
    base_url = "http://localhost:3000"

    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    end_time = time.time() + timeout

    while time.time() < end_time:
        try:
            # Check if the expected dashboards exist
            dashboards_url = f"{base_url}/api/search"
            resp = session.get(dashboards_url, auth=("admin", "admin"), timeout=15)

            if resp.status_code == 200:
                dashboards = resp.json()
                found_dashboards = {dashboard.get("title") for dashboard in dashboards}

                if found_dashboards == EXPECTED_DASHBOARDS:
                    logging.info(
                        f"All expected dashboards provisioned: {EXPECTED_DASHBOARDS}"
                    )
                    return

                logging.debug(
                    f"Expected dashboards: {EXPECTED_DASHBOARDS}, Found: {found_dashboards}"
                )

        except Exception as e:
            logging.debug(f"Entity provisioning check failed: {e}")

        time.sleep(3)

    logging.warning(f"Entity provisioning may not be complete after {timeout}s timeout")


@freeze_time(FROZEN_TIME)
def test_grafana_basic_ingest(
    loaded_grafana,
    pytestconfig,
    tmp_path,
    test_resources_dir,
    test_api_key,
    grafana_provisioning_complete,
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
    loaded_grafana,
    pytestconfig,
    tmp_path,
    test_resources_dir,
    test_api_key,
    grafana_provisioning_complete,
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

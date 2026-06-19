import base64
import hashlib
import hmac
import json
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
import requests
import tenacity

from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.cube.constants import (
    API_ENDPOINT_DATA_SOURCES,
    API_ENDPOINT_ENTITIES,
    API_ENDPOINT_ENTITIES_ALL,
    API_ENDPOINT_META,
)
from datahub.ingestion.source.cube.cube_api import CubeAPIClient
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port

pytestmark = pytest.mark.integration_batch_4

# Must match CUBEJS_API_SECRET in docker/docker-compose.yml.
CUBE_API_SECRET = "datahub-test-secret-0123456789abcdef"


def _load(test_resources_dir: Path, name: str) -> Dict[str, Any]:
    with open(test_resources_dir / "setup" / name) as f:
        return json.load(f)


def _run_pipeline(recipe: Path, output_path: Path) -> None:
    config = load_config_file(recipe)
    pipeline = Pipeline.create(
        {
            "run_id": "cube-test",
            **config,
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/integration/cube"


def _make_jwt(secret: str) -> str:
    # Cube Core requires an HS256 JWT signed with CUBEJS_API_SECRET; the claims
    # are unused. Built with stdlib to avoid a hard dependency on PyJWT.
    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header = b64(b'{"alg":"HS256","typ":"JWT"}')
    payload = b64(b"{}")
    signing_input = f"{header}.{payload}".encode()
    signature = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    return f"{header}.{payload}.{b64(signature)}"


@pytest.fixture(scope="module")
def cube_token() -> str:
    return _make_jwt(CUBE_API_SECRET)


@tenacity.retry(
    stop=tenacity.stop_after_delay(180),
    wait=tenacity.wait_fixed(3),
    reraise=True,
)
def _wait_for_cube_model(api_url: str, token: str) -> None:
    # Cube returns 5xx until it has compiled the model and connected to the
    # warehouse, so wait until /v1/meta serves the compiled cubes.
    response = requests.get(
        f"{api_url}/{API_ENDPOINT_META}",
        headers={"Authorization": token},
        timeout=10,
    )
    response.raise_for_status()
    cubes = response.json().get("cubes", [])
    if not cubes:
        raise AssertionError("Cube has not compiled the data model yet")


@pytest.fixture(scope="module")
def loaded_cube(docker_compose_runner, test_resources_dir: Path, cube_token: str):  # type: ignore[no-untyped-def]
    with docker_compose_runner(
        test_resources_dir / "docker" / "docker-compose.yml", "cube"
    ) as docker_services:
        wait_for_port(docker_services, "test-postgres", 5432, timeout=120)
        # Cube's image has no bash, so probe its HTTP API directly rather than
        # via the bash-based wait_for_port helper.
        cube_port = docker_services.port_for("test-cube", 4000)
        _wait_for_cube_model(f"http://localhost:{cube_port}/cubejs-api", cube_token)
        yield cube_port
    cleanup_image("cubejs/cube")


def test_cube_core_ingest(
    loaded_cube: int,
    pytestconfig: pytest.Config,
    tmp_path: Any,
    test_resources_dir: Path,
    cube_token: str,
) -> None:
    output_path = tmp_path / "cube_core_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "cube-core-test",
            "source": {
                "type": "cube",
                "config": {
                    "api_url": f"http://localhost:{loaded_cube}/cubejs-api",
                    "api_token": cube_token,
                    "deployment_type": "CORE",
                    # Pinned so the container externalUrl is stable regardless of
                    # the ephemeral Docker host port.
                    "deployment_url": "http://localhost:4000",
                    "platform_instance": "local_cube",
                    "warehouse_platform": "postgres",
                    "warehouse_database": "cube_demo",
                    "parse_sql_for_lineage": True,
                    "include_column_lineage": True,
                    "meta_mapping": {
                        "owner": {
                            "match": ".*",
                            "operation": "add_owner",
                            "config": {"owner_type": "user"},
                        },
                        "certified": {
                            "match": True,
                            "operation": "add_tag",
                            "config": {"tag": "certified"},
                        },
                    },
                    "column_meta_mapping": {
                        "classification": {
                            "match": ".*",
                            "operation": "add_tag",
                            "config": {"tag": "Sensitive"},
                        },
                    },
                    "domain": {"urn:li:domain:cube_demo": {"allow": ["orders.*"]}},
                    "stateful_ingestion": {"enabled": False},
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "cube_core_mces_golden.json",
    )


def test_cube_cloud_metadata_api(pytestconfig: pytest.Config, tmp_path: Any) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/cube"
    output_path = tmp_path / "cube_cloud_mces.json"
    meta_response = _load(test_resources_dir, "cloud_meta.json")
    entities_response = _load(test_resources_dir, "cloud_entities.json")
    data_sources_response = _load(test_resources_dir, "cloud_data_sources.json")

    def mock_request(
        self: Any,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        bearer: bool = False,
    ) -> Dict[str, Any]:
        # Cloud ingestion merges /v1/meta (structural) with the Metadata API.
        if endpoint == API_ENDPOINT_META:
            return meta_response
        if endpoint == API_ENDPOINT_ENTITIES_ALL:
            return entities_response
        if endpoint == API_ENDPOINT_DATA_SOURCES:
            return data_sources_response
        if endpoint == API_ENDPOINT_ENTITIES:
            return {"data": {"entities": []}}
        return {}

    with patch.object(CubeAPIClient, "_request", mock_request):
        _run_pipeline(test_resources_dir / "cube_cloud_to_file.yml", output_path)

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "cube_cloud_mces_golden.json",
    )

"""Static checks on the debug Java services in docker-compose.gms.yml.

GMS, system-update, the MAE consumer and the MCE consumer all component-scan
`EmbeddingProviderFactory`, whose bean resolves Google Application Default Credentials while the
context starts when the provider is `vertex_ai`. A debug service that lacks the GCP session fails
that startup, and system-update failing stops GMS from starting at all.
"""

from pathlib import Path
from typing import Dict, List

import pytest

yaml = pytest.importorskip("yaml")

GMS_COMPOSE = Path(__file__).resolve().parents[3] / "docker/profiles/docker-compose.gms.yml"

# Service-name prefixes whose Spring context builds the embedding provider.
EMBEDDING_SCANNING_PREFIXES = (
    "datahub-gms-debug",
    "system-update-debug",
    "datahub-mae-consumer-debug",
    "datahub-mce-consumer-debug",
)

REQUIRED_BEDROCK_REGION_ENV = (
    "AWS_REGION",
    "BEDROCK_EMBEDDING_AWS_REGION",
    "BEDROCK_AWS_REGION",
)

REQUIRED_GCP_ENV = (
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "VERTEX_AI_PROJECT_ID",
    "VERTEX_AI_LOCATION",
)


def _debug_embedding_services() -> Dict[str, dict]:
    services = yaml.safe_load(GMS_COMPOSE.read_text())["services"]
    return {
        name: service
        for name, service in services.items()
        if name.startswith(EMBEDDING_SCANNING_PREFIXES)
    }


def _volume_sources(service: dict) -> List[str]:
    return [str(volume) for volume in service.get("volumes", [])]


def test_every_embedding_scanning_debug_service_is_covered() -> None:
    names = _debug_embedding_services()

    for prefix in EMBEDDING_SCANNING_PREFIXES:
        assert any(name.startswith(prefix) for name in names), prefix


@pytest.mark.parametrize("name", sorted(_debug_embedding_services()))
def test_debug_service_gets_the_same_bedrock_region(name: str) -> None:
    environment = _debug_embedding_services()[name]["environment"]

    missing = [key for key in REQUIRED_BEDROCK_REGION_ENV if key not in environment]
    assert not missing, f"{name} does not pass through {missing}"


@pytest.mark.parametrize("name", sorted(_debug_embedding_services()))
def test_debug_service_gets_gcp_session(name: str) -> None:
    service = _debug_embedding_services()[name]
    environment = service["environment"]
    volumes = _volume_sources(service)

    missing_env = [key for key in REQUIRED_GCP_ENV if key not in environment]
    assert not missing_env, f"{name} does not pass through {missing_env}"
    assert any("DATAHUB_DEV_GCLOUD_DIR" in volume for volume in volumes), name
    assert any("GOOGLE_APPLICATION_CREDENTIALS" in volume for volume in volumes), name


@pytest.mark.parametrize("name", sorted(_debug_embedding_services()))
def test_debug_service_keeps_aws_sdk_config_loading_when_it_has_aws_mounts(
    name: str,
) -> None:
    service = _debug_embedding_services()[name]
    has_aws_mount = any("DATAHUB_DEV_AWS_DIR" in v for v in _volume_sources(service))

    # AWS_SDK_LOAD_CONFIG defaults to 1 only through the dev AWS env, which a service that replaces
    # its environment: block would lose, leaving an SSO profile in ~/.aws/config unread.
    if has_aws_mount:
        assert "AWS_SDK_LOAD_CONFIG" in service["environment"], name

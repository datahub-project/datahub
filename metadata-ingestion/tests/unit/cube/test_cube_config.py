from typing import Dict

import pytest

from datahub.ingestion.source.cube.config import CubeDeploymentType, CubeSourceConfig


def _config(**overrides: object) -> CubeSourceConfig:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "t",
    }
    base.update(overrides)
    return CubeSourceConfig.model_validate(base)


def test_api_url_trailing_slash_is_stripped() -> None:
    cfg = _config(api_url="https://demo.cubecloud.dev/cubejs-api/")
    assert cfg.api_url == "https://demo.cubecloud.dev/cubejs-api"


def test_api_url_requires_scheme() -> None:
    with pytest.raises(ValueError):
        _config(api_url="demo.cubecloud.dev/cubejs-api")


def test_api_url_rejects_empty() -> None:
    with pytest.raises(ValueError):
        _config(api_url="   ")


def test_request_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError):
        _config(request_timeout_sec=0)


def test_defaults() -> None:
    cfg = _config()
    assert cfg.deployment_type == CubeDeploymentType.CORE
    assert cfg.include_cubes is True
    assert cfg.include_views is True
    assert cfg.ingest_lineage is True
    assert cfg.convert_lineage_urns_to_lowercase is True
    assert cfg.enable_meta_mapping is True


def test_meta_mapping_requires_operation_and_match() -> None:
    with pytest.raises(ValueError):
        _config(meta_mapping={"owner": {"match": ".*"}})
    with pytest.raises(ValueError):
        _config(meta_mapping={"owner": {"operation": "add_tag"}})


def test_control_plane_credentials_required_together() -> None:
    # A partial Control Plane configuration is rejected.
    with pytest.raises(ValueError):
        _config(cloud_api_key="k", deployment_id="1")
    # The full trio is accepted.
    cfg = _config(cloud_api_key="k", deployment_id="1", environment_id="2")
    assert cfg.deployment_id == "1"
    assert cfg.environment_id == "2"

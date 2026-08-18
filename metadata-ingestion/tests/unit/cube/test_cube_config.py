from typing import Dict

import pytest

from datahub.ingestion.source.cube.config import CubeSourceConfig


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


def test_meta_mapping_requires_operation_and_match() -> None:
    with pytest.raises(ValueError):
        _config(meta_mapping={"owner": {"match": ".*"}})
    with pytest.raises(ValueError):
        _config(meta_mapping={"owner": {"operation": "add_tag"}})


def test_cloud_api_key_and_deployment_id_required_together() -> None:
    with pytest.raises(ValueError):
        _config(cloud_api_key="k")
    with pytest.raises(ValueError):
        _config(deployment_id="1")


def test_cloud_api_key_with_deployment_id_is_valid_for_platform_api() -> None:
    # The Platform API (reports/workbooks) needs only the key + deployment id;
    # environment_id is not required.
    cfg = _config(deployment_type="CLOUD", cloud_api_key="k", deployment_id="1")
    assert cfg.deployment_id == "1"
    assert cfg.environment_id is None


def test_environment_id_requires_key_and_deployment_id() -> None:
    with pytest.raises(ValueError):
        _config(deployment_type="CLOUD", environment_id="2")
    cfg = _config(
        deployment_type="CLOUD",
        cloud_api_key="k",
        deployment_id="1",
        environment_id="2",
    )
    assert cfg.environment_id == "2"


def test_cloud_credentials_rejected_for_core_deployment() -> None:
    # The Cloud-only credentials are meaningless against a Core deployment and
    # must be rejected rather than silently ignored.
    with pytest.raises(ValueError):
        _config(deployment_type="CORE", cloud_api_key="k", deployment_id="1")

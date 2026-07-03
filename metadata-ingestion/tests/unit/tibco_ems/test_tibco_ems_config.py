import pytest

from datahub.ingestion.source.tibco_ems.config import TibcoEmsSourceConfig

_BASE_URL = "https://ems.example.com:8080"


def test_basic_auth_accepted() -> None:
    config = TibcoEmsSourceConfig.model_validate(
        {"base_url": _BASE_URL, "username": "u", "password": "p"}
    )
    assert config.base_url == _BASE_URL


def test_token_auth_accepted() -> None:
    config = TibcoEmsSourceConfig.model_validate(
        {"base_url": _BASE_URL, "token": "abc"}
    )
    assert config.token is not None


def test_missing_credentials_rejected() -> None:
    with pytest.raises(ValueError):
        TibcoEmsSourceConfig.model_validate({"base_url": _BASE_URL})


def test_base_url_trailing_slash_stripped() -> None:
    config = TibcoEmsSourceConfig.model_validate(
        {"base_url": f"{_BASE_URL}/", "token": "abc"}
    )
    assert config.base_url == _BASE_URL

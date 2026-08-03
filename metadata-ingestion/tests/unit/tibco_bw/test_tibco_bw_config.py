import pytest
from pydantic import ValidationError

from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.constants import DEFAULT_CLOUD_BASE_URL


def test_on_prem_requires_base_url_and_credentials() -> None:
    with pytest.raises(ValidationError):
        TibcoBwSourceConfig.model_validate({"deployment": "on_prem"})

    with pytest.raises(ValidationError):
        TibcoBwSourceConfig.model_validate(
            {"deployment": "on_prem", "base_url": "http://bw:8079"}
        )


def test_on_prem_accepts_basic_auth() -> None:
    config = TibcoBwSourceConfig.model_validate(
        {
            "deployment": "on_prem",
            "base_url": "http://bw:8079/",
            "username": "user",
            "password": "secret",
        }
    )
    # Trailing slash is stripped so URL joins are predictable.
    assert config.base_url == "http://bw:8079"


def test_cloud_requires_token() -> None:
    with pytest.raises(ValidationError):
        TibcoBwSourceConfig.model_validate({"deployment": "cloud"})


def test_cloud_defaults_base_url() -> None:
    config = TibcoBwSourceConfig.model_validate({"deployment": "cloud", "token": "abc"})
    assert config.base_url == DEFAULT_CLOUD_BASE_URL

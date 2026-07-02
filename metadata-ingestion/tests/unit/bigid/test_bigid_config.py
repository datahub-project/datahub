import pytest
from pydantic import ValidationError

from datahub.ingestion.source.bigid.config import BigIDSourceConfig


def test_config_requires_token():
    with pytest.raises(ValidationError, match="user_token or access_token"):
        BigIDSourceConfig.model_validate({"bigid_url": "https://bigid.example.com"})


def test_config_accepts_single_token():
    cfg = BigIDSourceConfig.model_validate(
        {"bigid_url": "https://bigid.example.com", "access_token": "tok"}
    )
    assert cfg.access_token.get_secret_value() == "tok"


def test_config_rejects_non_http_url():
    with pytest.raises(ValidationError, match="http or https"):
        BigIDSourceConfig.model_validate(
            {"bigid_url": "ftp://bigid.example.com", "access_token": "tok"}
        )

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig


def test_api_key_rejects_blank_and_whitespace():
    for bad in ("", "   "):
        with pytest.raises(ValidationError):
            HightouchAPIConfig(api_key=bad)


def test_base_url_accepts_https_and_strips_trailing_slash():
    cfg = HightouchAPIConfig(api_key="k", base_url="https://api.hightouch.com/api/v1/")
    assert cfg.base_url == "https://api.hightouch.com/api/v1"


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://api.hightouch.com/api/v1",
        # Loopback HTTP is rejected too: the Bearer token would still go in the clear.
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
)
def test_base_url_rejects_plaintext_http(bad_url):
    with pytest.raises(ValidationError):
        HightouchAPIConfig(api_key="k", base_url=bad_url)

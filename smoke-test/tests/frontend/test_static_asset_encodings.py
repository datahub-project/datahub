"""Live-stack check that Play serves Vite .br/.gz sidecars without double-gzip."""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin

import pytest
import requests

from tests.utilities.domains import Domain
from tests.utils import get_frontend_url

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.domain(Domain.PLATFORM),
    pytest.mark.p0,
]

_GZIP_MAGIC = b"\x1f\x8b"
_JS_SCRIPT_SRC = re.compile(
    r'<script[^>]+src=["\']([^"\']*assets/index-[^"\']+\.js)["\']',
    re.IGNORECASE,
)


def _frontend_base() -> str:
    return get_frontend_url().rstrip("/") + "/"


def _hashed_js_url() -> str:
    index_url = _frontend_base()
    response = requests.get(
        index_url,
        headers={"Accept-Encoding": "identity"},
        timeout=30,
    )
    assert response.status_code == 200, f"index.html failed: {response.status_code}"
    match = _JS_SCRIPT_SRC.search(response.text)
    assert match is not None, "Could not find hashed assets/index-*.js in index.html"
    return urljoin(index_url, match.group(1))


def _get_asset(url: str, accept_encoding: str) -> requests.Response:
    response = requests.get(
        url,
        headers={"Accept-Encoding": accept_encoding},
        timeout=60,
        stream=True,
    )
    response.raw.decode_content = False
    return response


def _content_encoding(response: requests.Response) -> Optional[str]:
    value = response.headers.get("Content-Encoding")
    if value is None:
        return None
    stripped = value.strip().lower()
    return stripped or None


def _content_length(response: requests.Response) -> int:
    raw = response.headers.get("Content-Length")
    assert raw is not None, f"missing Content-Length: {dict(response.headers)}"
    return int(raw)


def test_hashed_js_asset_encodings_prefer_brotli_then_gzip() -> None:
    """br clients get brotli sidecars; gzip-only still gzip; identity is uncompressed."""
    url = _hashed_js_url()
    logger.info("Checking encodings for %s", url)

    br_response = _get_asset(url, "br,gzip")
    gzip_response = _get_asset(url, "gzip")
    identity_response = _get_asset(url, "identity")

    try:
        assert br_response.status_code == 200
        assert gzip_response.status_code == 200
        assert identity_response.status_code == 200

        assert _content_encoding(br_response) == "br"
        assert _content_encoding(gzip_response) == "gzip"
        assert _content_encoding(identity_response) is None

        for response in (br_response, gzip_response, identity_response):
            vary = response.headers.get("Vary", "")
            assert "accept-encoding" in vary.lower(), f"Vary missing Accept-Encoding: {vary!r}"

        br_len = _content_length(br_response)
        gzip_len = _content_length(gzip_response)
        identity_len = _content_length(identity_response)
        assert br_len < gzip_len < identity_len, (
            f"expected br < gzip < identity lengths, got {br_len}, {gzip_len}, {identity_len}"
        )

        br_magic = br_response.raw.read(4)
        gzip_magic = gzip_response.raw.read(4)
        identity_magic = identity_response.raw.read(4)
        assert gzip_magic.startswith(_GZIP_MAGIC), f"gzip body missing gzip magic: {gzip_magic!r}"
        assert not br_magic.startswith(_GZIP_MAGIC), (
            f"br response body looks like gzip (header stripped?): {br_magic!r}"
        )
        assert not identity_magic.startswith(_GZIP_MAGIC), (
            f"identity body looks gzip-compressed: {identity_magic!r}"
        )
        logger.info(
            "encodings ok br=%s gzip=%s identity=%s",
            br_len,
            gzip_len,
            identity_len,
        )
    finally:
        br_response.close()
        gzip_response.close()
        identity_response.close()

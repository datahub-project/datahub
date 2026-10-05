#!/usr/bin/env python3
"""Unit tests for check_product_update_cta (run by test-github-scripts.yml)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import check_product_update_cta as cta  # noqa: E402


def _write_json(root: Path, relpath: str, payload: dict) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_effective_cta_prefers_primary_when_both_present() -> None:
    payload = {
        "primaryCtaText": "Read",
        "primaryCtaLink": "https://example.com/primary",
        "ctaLink": "https://example.com/legacy",
    }
    assert cta.effective_cta_link(payload) == "https://example.com/primary"


def test_effective_cta_falls_back_to_legacy() -> None:
    payload = {"ctaLink": "https://example.com/legacy"}
    assert cta.effective_cta_link(payload) == "https://example.com/legacy"


def test_cta_links_include_secondary() -> None:
    payload = {
        "ctaLink": "https://example.com/primary",
        "secondaryCtaLink": "https://example.com/secondary",
    }
    assert cta.cta_links(payload) == [
        "https://example.com/primary",
        "https://example.com/secondary",
    ]


def test_cta_links_skip_null_secondary() -> None:
    payload = {"ctaLink": "https://example.com/primary", "secondaryCtaLink": "null"}
    assert cta.cta_links(payload) == ["https://example.com/primary"]


def test_probe_url_rejects_non_http() -> None:
    status, error = cta.probe_url("javascript:alert(1)", attempts=1)
    assert status is None
    assert error is not None
    assert "http(s)" in error


def test_probe_url_treats_404_as_failure() -> None:
    error_response = HTTPError(
        "https://example.com/missing", 404, "Not Found", hdrs=None, fp=None
    )
    with patch("urllib.request.urlopen", side_effect=error_response):
        status, error = cta.probe_url("https://example.com/missing", attempts=1)
    assert status == 404
    assert error == "HTTP 404"


def test_probe_url_accepts_200() -> None:
    response = MagicMock()
    response.getcode.return_value = 200
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    with patch("urllib.request.urlopen", return_value=response):
        status, error = cta.probe_url("https://example.com/ok", attempts=1)
    assert status == 200
    assert error is None


def test_check_repo_skips_disabled_flavor(tmp_path: Path) -> None:
    _write_json(
        tmp_path,
        "metadata-service/configuration/src/main/resources/product-update.json",
        {"enabled": False, "ctaLink": "https://example.com/core"},
    )
    _write_json(
        tmp_path,
        "metadata-service/configuration/src/main/resources/product-update-saas.json",
        {"enabled": True, "ctaLink": "https://example.com/cloud"},
    )
    response = MagicMock()
    response.getcode.return_value = 200
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    with patch("urllib.request.urlopen", return_value=response) as mocked:
        failures = cta.check_repo(tmp_path)
    assert failures == []
    assert mocked.call_count == 1


def test_check_repo_reports_404(tmp_path: Path) -> None:
    _write_json(
        tmp_path,
        "metadata-service/configuration/src/main/resources/product-update.json",
        {"enabled": True, "ctaLink": "https://example.com/core"},
    )
    _write_json(
        tmp_path,
        "metadata-service/configuration/src/main/resources/product-update-saas.json",
        {"enabled": True, "ctaLink": "https://example.com/cloud"},
    )
    error_response = HTTPError(
        "https://example.com/cloud", 404, "Not Found", hdrs=None, fp=None
    )

    def _open(request, timeout=None):  # noqa: ANN001
        url = request.full_url if hasattr(request, "full_url") else request
        if "cloud" in str(url):
            raise error_response
        response = MagicMock()
        response.getcode.return_value = 200
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        return response

    with patch("urllib.request.urlopen", side_effect=_open):
        failures = cta.check_repo(tmp_path)
    assert len(failures) == 1
    assert "product-update-saas.json" in failures[0]
    assert "HTTP 404" in failures[0]


def test_probe_url_retries_url_error() -> None:
    response = MagicMock()
    response.getcode.return_value = 200
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    with (
        patch(
            "urllib.request.urlopen",
            side_effect=[URLError("temporary"), response],
        ),
        patch("check_product_update_cta.time.sleep"),
    ):
        status, error = cta.probe_url("https://example.com/ok", attempts=2)
    assert status == 200
    assert error is None

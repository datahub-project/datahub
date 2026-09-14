#!/usr/bin/env python3
"""Probe product-update toast CTA URLs so unpublished blog posts cannot merge.

Live HTTP checks belong here (GitHub Actions job product_update_cta_live), not in
Gradle, so PRs stay creatable and the rest of CI can go green while a CTA 404s.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

JSON_PATHS = (
    "metadata-service/configuration/src/main/resources/product-update.json",
    "metadata-service/configuration/src/main/resources/product-update-saas.json",
)

USER_AGENT = "DataHub-product-update-cta-ci/1.0"
HTTP_TIMEOUT_SECONDS = 10
HTTP_ATTEMPTS = 3


def _is_present_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != "" and value != "null"


def effective_cta_link(payload: dict[str, Any]) -> str:
    if _is_present_text(payload.get("primaryCtaText")) and _is_present_text(
        payload.get("primaryCtaLink")
    ):
        return str(payload["primaryCtaLink"]).strip()
    cta_link = payload.get("ctaLink")
    return str(cta_link).strip() if _is_present_text(cta_link) else ""


def cta_links(payload: dict[str, Any]) -> list[str]:
    links: list[str] = []
    primary = effective_cta_link(payload)
    if primary:
        links.append(primary)
    secondary = payload.get("secondaryCtaLink")
    if _is_present_text(secondary):
        links.append(str(secondary).strip())
    return links


def probe_url(url: str, attempts: int = HTTP_ATTEMPTS) -> tuple[int | None, str | None]:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None, f"CTA must be an http(s) URL: {url}"

    last_error: str | None = None
    last_status: int | None = None
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": USER_AGENT},
    )
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                last_status = int(response.getcode())
                if 200 <= last_status < 400:
                    return last_status, None
        except urllib.error.HTTPError as exc:
            last_status = int(exc.code)
            last_error = f"HTTP {last_status}"
            if last_status in (404, 410):
                break
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < attempts:
            time.sleep(0.25 * attempt)

    if last_status is not None:
        return last_status, f"HTTP {last_status}"
    return None, last_error or "request failed"


def collect_flavor_links(repo_root: Path) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for relpath in JSON_PATHS:
        path = repo_root / relpath
        if not path.is_file():
            raise FileNotFoundError(f"Missing product update JSON: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"{relpath} is not a JSON object")
        if payload.get("enabled") is False:
            continue
        for link in cta_links(payload):
            found.append((relpath, link))
    return found


def check_repo(repo_root: Path) -> list[str]:
    failures: list[str] = []
    for relpath, url in collect_flavor_links(repo_root):
        _status, error = probe_url(url)
        if error is not None:
            failures.append(
                f"{relpath} CTA is not reachable: {url} ({error}). "
                "Point the toast at a published page; cloud-router will republish this JSON. "
                "Re-run product_update_cta_live after the URL returns 2xx — no full CI rerun needed."
            )
    return failures


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="DataHub repo root (defaults to two parents above this script)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = (
        args.repo_root.resolve()
        if args.repo_root is not None
        else Path(__file__).resolve().parents[2]
    )
    try:
        failures = check_repo(repo_root)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if failures:
        for line in failures:
            print(f"ERROR: {line}", file=sys.stderr)
        return 1
    print("OK: product-update CTA URLs are reachable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

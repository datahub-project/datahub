from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import requests
from datahub.utilities.server_config_util import RestServiceConfig

from lib.graphql import log


@dataclass(frozen=True)
class GmsConfigSnapshot:
    gms_host: str
    gms_version: Optional[str]
    gms_commit: Optional[str]
    server_type: Optional[str]
    error: Optional[str] = None


def gms_host_from_url(gms_url: str) -> str:
    parsed = urlparse(gms_url)
    return parsed.netloc or gms_url


def fetch_gms_config(
    gms_url: str,
    token: Optional[str] = None,
    *,
    timeout_sec: float = 15.0,
) -> GmsConfigSnapshot:
    host = gms_host_from_url(gms_url)
    url = f"{gms_url.rstrip('/')}/config"
    headers: dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        resp.raise_for_status()
        config = RestServiceConfig(raw_config=resp.json())
        return GmsConfigSnapshot(
            gms_host=host,
            gms_version=config.service_version,
            gms_commit=config.commit_hash,
            server_type=config.server_type,
        )
    except (requests.RequestException, OSError) as exc:
        log(f"WARN: failed to fetch GMS config from {url}: {exc}")
        return GmsConfigSnapshot(
            gms_host=host,
            gms_version="unknown",
            gms_commit=None,
            server_type=None,
            error=str(exc),
        )

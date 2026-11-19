"""What a GMS is: its version and deployment, read from its ``/config`` endpoint.

Tool version gating and the GraphQL compatibility compiler key off this, so it
is read the same way everywhere: one cached answer per GMS URL, refreshed every
60 seconds. ``/config`` needs no credential, so the lookup never borrows a
caller's token.
"""

import dataclasses
import threading
import time
from typing import Any, Callable, Mapping, Optional

import requests

from .version_requirements import parse_version

SERVER_INFO_CACHE_TTL_SECONDS = 60.0
CONFIG_REQUEST_TIMEOUT_SECONDS = 10.0

# GMS reports this when its build has no release tag (GitVersion.java's fallback
# for local and unreleased builds). No release ships it, so it means "unknown".
_UNKNOWN_VERSION = (0, 0, 0, 0)


@dataclasses.dataclass(frozen=True)
class GmsServerInfo:
    version: Optional[str]
    """``versions["acryldata/datahub"].version``, e.g. "v1.7.0" or "v0.3.16.1"."""

    server_env: Optional[str]
    """``datahub.serverEnv``: "core" for DataHub Core, anything else for Cloud."""

    auth_enabled: Optional[bool] = None
    """``authentication.enabled``; None on a GMS that doesn't report it."""

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "GmsServerInfo":
        versions = config.get("versions") or {}
        datahub_version = versions.get("acryldata/datahub") or {}
        datahub = config.get("datahub") or {}
        authentication = config.get("authentication") or {}
        enabled = authentication.get("enabled")
        return cls(
            version=datahub_version.get("version") or None,
            server_env=datahub.get("serverEnv") or None,
            auth_enabled=enabled if isinstance(enabled, bool) else None,
        )

    @property
    def is_cloud(self) -> bool:
        return self.server_env is not None and self.server_env != "core"

    @property
    def parsed_version(self) -> Optional[tuple[int, int, int, int]]:
        """The release as a 4-tuple, or None when GMS doesn't know its own version."""
        if not self.version:
            return None
        try:
            parsed = parse_version(self.version)
        except ValueError:
            return None
        return None if parsed == _UNKNOWN_VERSION else parsed


def fetch_gms_config(gms_url: str) -> Mapping[str, Any]:
    response = requests.get(
        f"{gms_url.rstrip('/')}/config", timeout=CONFIG_REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    config = response.json()
    if not isinstance(config, dict):
        raise ValueError(f"GMS /config returned {type(config).__name__}, not an object")
    return config


class GmsServerInfoCache:
    """Per-GMS-URL ``GmsServerInfo``, fetched at most once per TTL.

    Owned by a host, not module-global. Failures are not cached and propagate,
    so a caller can fail closed while GMS is unreachable.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = SERVER_INFO_CACHE_TTL_SECONDS,
        fetch_config: Callable[[str], Mapping[str, Any]] = fetch_gms_config,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._fetch_config = fetch_config
        self._clock = clock
        self._entries: dict[str, tuple[float, GmsServerInfo]] = {}
        self._lock = threading.Lock()

    def get(self, gms_url: str) -> GmsServerInfo:
        key = gms_url.rstrip("/")
        # One fetch at a time: a burst of requests on an expired entry shares one
        # call to GMS instead of each making its own.
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and self._clock() < entry[0]:
                return entry[1]
            info = GmsServerInfo.from_config(self._fetch_config(key))
            self._entries[key] = (self._clock() + self._ttl_seconds, info)
            return info

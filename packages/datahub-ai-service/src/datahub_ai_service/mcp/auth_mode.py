"""Whether GMS requires authentication, so /mcp can follow it.

GMS reports it in /config (``authentication.enabled``). Older GMS versions don't,
so for them datahub-ai-service asks GraphQL ``me`` with no credential: a 401 or 403 means
auth is on, and a well-formed answer naming a user means it is off. Anything
else (an error, an odd body, no answer) is unknown, and unknown fails closed.
"""

import enum
import threading
import time
from typing import Any, Callable, Optional

import requests
from loguru import logger

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo

PROBE_TTL_SECONDS = 60.0
# An unclear answer is kept only this long, so it's asked again soon, while
# requests queued behind a probe that timed out don't each run their own.
UNCLEAR_PROBE_TTL_SECONDS = 5.0
_ME_PROBE = {"query": "query authProbe { me { corpUser { urn } } }"}


class GmsAuth(enum.Enum):
    ON = "on"
    OFF = "off"
    UNKNOWN = "unknown"


def probe_gms_auth(gms_url: str, timeout_seconds: float) -> GmsAuth:
    """Ask GMS who an unauthenticated caller is."""
    try:
        response = requests.post(
            f"{gms_url.rstrip('/')}/api/graphql",
            json=_ME_PROBE,
            timeout=timeout_seconds,
        )
    except requests.RequestException as e:
        logger.warning(f"GMS auth probe failed: {e}")
        return GmsAuth.UNKNOWN
    if response.status_code in (401, 403):
        return GmsAuth.ON
    if response.status_code != 200:
        return GmsAuth.UNKNOWN
    try:
        body: Any = response.json()
    except ValueError:
        return GmsAuth.UNKNOWN
    if not isinstance(body, dict) or body.get("errors"):
        return GmsAuth.UNKNOWN
    urn = (((body.get("data") or {}).get("me") or {}).get("corpUser") or {}).get("urn")
    return GmsAuth.OFF if isinstance(urn, str) and urn else GmsAuth.UNKNOWN


class GmsAuthDetector:
    """The current GMS auth mode, refreshed at most once a minute.

    ``detect`` may call GMS and blocks; ``last`` only reports the latest answer.
    """

    def __init__(
        self,
        gms_url: str,
        server_info: Callable[[], GmsServerInfo],
        *,
        timeout_seconds: float,
        probe: Callable[[str, float], GmsAuth] = probe_gms_auth,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._gms_url = gms_url
        self._server_info = server_info
        self._timeout_seconds = timeout_seconds
        self._probe = probe
        self._clock = clock
        self._lock = threading.Lock()
        self._probed: Optional[tuple[float, GmsAuth]] = None
        self._last = GmsAuth.UNKNOWN

    @property
    def last(self) -> GmsAuth:
        return self._last

    def detect(self) -> GmsAuth:
        try:
            enabled = self._server_info().auth_enabled
        except Exception as e:
            logger.warning(f"Could not read GMS /config: {e}")
            return self._record(GmsAuth.UNKNOWN)
        if enabled is not None:
            return self._record(GmsAuth.ON if enabled else GmsAuth.OFF)
        return self._record(self._probe_cached())

    def _probe_cached(self) -> GmsAuth:
        with self._lock:
            if self._probed is not None and self._clock() < self._probed[0]:
                return self._probed[1]
            result = self._probe(self._gms_url, self._timeout_seconds)
            ttl = (
                UNCLEAR_PROBE_TTL_SECONDS
                if result is GmsAuth.UNKNOWN
                else PROBE_TTL_SECONDS
            )
            self._probed = (self._clock() + ttl, result)
            return result

    def _record(self, result: GmsAuth) -> GmsAuth:
        if result is not self._last:
            if result is GmsAuth.OFF:
                logger.info("GMS auth is off; MCP requests run as the system user")
            else:
                logger.info(f"GMS auth is {result.value}")
        self._last = result
        return result

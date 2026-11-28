import sys
import time
from typing import Optional

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS,
    MSTR_ENDPOINT_DOSSIER_KEY_SEGMENT_RE,
    MSTR_ENDPOINT_ID_PLACEHOLDER,
    MSTR_ENDPOINT_KEY_REPLACEMENT,
    MSTR_ENDPOINT_OBJECT_ID_SEGMENT_RE,
    MSTR_ENDPOINT_UUID_SEGMENT_RE,
)

try:
    import resource
except ImportError:  # pragma: no cover - not available on Windows
    resource = None  # type: ignore[assignment]

_BYTES_PER_MB = 1024 * 1024
_KILOBYTES_PER_MB = 1024


def endpoint_template(method: str, path: str) -> str:
    path = path.split("?", 1)[0]
    path = MSTR_ENDPOINT_OBJECT_ID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = MSTR_ENDPOINT_UUID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = MSTR_ENDPOINT_DOSSIER_KEY_SEGMENT_RE.sub(MSTR_ENDPOINT_KEY_REPLACEMENT, path)
    return f"{method.upper()} {path}"


def peak_rss_mb() -> Optional[float]:
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and kilobytes on Linux.
    divisor = _BYTES_PER_MB if sys.platform == "darwin" else _KILOBYTES_PER_MB
    return round(peak / divisor, 1)


def dashboard_seconds_bucket(seconds: float) -> str:
    lower = 0
    for upper in MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS:
        if seconds < upper:
            return f"{lower}-{upper}s"
        lower = upper
    return f">={lower}s"


class IntervalGate:
    def __init__(self, every_items: int, every_seconds: float) -> None:
        self._every_items = every_items
        self._every_seconds = every_seconds
        self._last_count = 0
        self._last_time = time.perf_counter()

    def due(self, count: int) -> bool:
        return (
            count - self._last_count >= self._every_items
            or time.perf_counter() - self._last_time >= self._every_seconds
        )

    def mark(self, count: int) -> None:
        self._last_count = count
        self._last_time = time.perf_counter()

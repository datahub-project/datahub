import re
import sys
from typing import Optional

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS,
    MSTR_ENDPOINT_ID_PLACEHOLDER,
    MSTR_ENDPOINT_KEY_REPLACEMENT,
)

try:
    import resource
except ImportError:  # pragma: no cover - not available on Windows
    resource = None  # type: ignore[assignment]

# 32-hex object ids and UUIDs, so one endpoint is one row.
_OBJECT_ID_SEGMENT_RE = re.compile(r"/[0-9A-Fa-f]{32}(?=/|$)")
_UUID_SEGMENT_RE = re.compile(
    r"/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}(?=/|$)"
)
# Not 32-hex object ids, so a literal match would make one row per visualization.
_DOSSIER_KEY_SEGMENT_RE = re.compile(r"/(chapters|pages|visualizations)/[^/]+")


def endpoint_template(method: str, path: str) -> str:
    path = path.split("?", 1)[0]
    path = _OBJECT_ID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = _UUID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = _DOSSIER_KEY_SEGMENT_RE.sub(MSTR_ENDPOINT_KEY_REPLACEMENT, path)
    return f"{method.upper()} {path}"


def peak_rss_mb() -> Optional[float]:
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and kilobytes on Linux.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(peak / divisor, 1)


def dashboard_seconds_bucket(seconds: float) -> str:
    lower = 0
    for upper in MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS:
        if seconds < upper:
            return f"{lower}-{upper}s"
        lower = upper
    return f">={lower}s"

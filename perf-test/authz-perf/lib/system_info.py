from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import requests

from lib.graphql import log

_VIEW_AUTHORIZATION_DOT_KEY = "authorization.view.enabled"
_VIEW_AUTHORIZATION_ENV_KEY = "VIEW_AUTHORIZATION_ENABLED"

_AUTH_PROPERTY_KEYS = (
    _VIEW_AUTHORIZATION_DOT_KEY,
    _VIEW_AUTHORIZATION_ENV_KEY,
    "authorization.defaultAuthorizer.enabled",
    "authorization.restApiAuthorization",
)


@dataclass(frozen=True)
class SystemInfoSnapshot:
    view_authorization_enabled: Optional[bool]
    properties: dict[str, str]
    error: Optional[str] = None


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in ("true", "1", "yes"):
        return True
    if normalized in ("false", "0", "no"):
        return False
    return None


def _view_authorization_from_properties(properties: dict[str, Any]) -> Optional[bool]:
    for key in (_VIEW_AUTHORIZATION_DOT_KEY, _VIEW_AUTHORIZATION_ENV_KEY):
        parsed = _parse_bool(properties.get(key))
        if parsed is not None:
            return parsed
    return None


def fetch_system_info(
    gms_url: str,
    token: Optional[str] = None,
    *,
    timeout_sec: float = 15.0,
) -> SystemInfoSnapshot:
    url = f"{gms_url.rstrip('/')}/openapi/v1/system-info/properties/simple"
    headers: dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        resp.raise_for_status()
        raw = resp.json()
    except (requests.RequestException, OSError, ValueError) as exc:
        log(f"WARN: failed to fetch system-info from {url}: {exc}")
        return SystemInfoSnapshot(
            view_authorization_enabled=None,
            properties={},
            error=str(exc),
        )

    properties = {
        key: str(raw[key])
        for key in _AUTH_PROPERTY_KEYS
        if key in raw and raw[key] is not None
    }
    view_enabled = _view_authorization_from_properties(raw)
    if view_enabled is None:
        log(
            "WARN: system-info missing view authorization flag "
            f"({ _VIEW_AUTHORIZATION_DOT_KEY!r}); using fixture expectations as-is"
        )
    else:
        log(
            f"system-info: authorization.view.enabled={view_enabled} "
            f"(host={gms_url})"
        )
    return SystemInfoSnapshot(
        view_authorization_enabled=view_enabled,
        properties=properties,
    )


def warn_authorization_settings_drift(
    snapshots: dict[str, SystemInfoSnapshot],
) -> None:
    if len(snapshots) < 2:
        return

    by_target = {
        name: snap.view_authorization_enabled for name, snap in snapshots.items()
    }
    known = {value for value in by_target.values() if value is not None}
    if len(known) <= 1:
        return

    details = ", ".join(
        f"{name}={value!r}" for name, value in sorted(by_target.items())
    )
    log(
        "WARN: authorization.view.enabled differs across targets: "
        f"{details}. Deny scenarios (fixture 403) expect HTTP 200 where view "
        "authorization is disabled; results remain comparable using per-target "
        "effective expectations recorded in JSONL metadata."
    )

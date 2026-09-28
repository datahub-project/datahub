"""Shared helpers for authorization smoke tests."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_VIEW_AUTHORIZATION_PROPERTY = "authorization.view.enabled"


def is_view_authorization_enabled(auth_session: Any) -> bool:
    """Return True when GMS has subject-derived view authorization enabled."""
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v1/system-info/properties/simple"
    )
    response.raise_for_status()
    value = response.json().get(_VIEW_AUTHORIZATION_PROPERTY)
    enabled = str(value).lower() in ("true", "1")
    logger.info(
        "View authorization enabled=%s (%s=%r)",
        enabled,
        _VIEW_AUTHORIZATION_PROPERTY,
        value,
    )
    return enabled

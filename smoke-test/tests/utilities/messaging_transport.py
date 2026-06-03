"""Detect active GMS metadata messaging transport for smoke tests."""

from __future__ import annotations

import logging
from typing import Any, Dict, Literal

logger = logging.getLogger(__name__)

MessagingTransport = Literal["kafka", "pgqueue"]


def fetch_gms_config(auth_session) -> Dict[str, Any]:
    """Load the public GMS ``/config`` JSON (same source as the Python CLI)."""
    response = auth_session.get(f"{auth_session.gms_url()}/config")
    response.raise_for_status()
    return response.json()


def pgqueue_configured(gms_config: Dict[str, Any]) -> bool:
    """True when GMS exposes a ``pgQueue`` section (``postgres.pgQueue.enabled``)."""
    return "pgQueue" in gms_config


def get_active_messaging_transport(auth_session) -> MessagingTransport:
    """Return ``kafka`` or ``pgqueue`` for the running GMS instance.

    When ``/config`` includes ``pgQueue``, the metadata bus uses pgQueue unless the
    operations API reports otherwise (e.g. postgres-cdc profiles still on Kafka).
    """
    gms_config = fetch_gms_config(auth_session)
    if not pgqueue_configured(gms_config):
        return "kafka"

    try:
        response = auth_session.get(
            f"{auth_session.gms_url()}/openapi/operations/messaging/transport"
        )
        if response.status_code == 200:
            transport = response.json().get("transport")
            if isinstance(transport, str):
                normalized = transport.lower()
                if normalized in ("kafka", "pgqueue"):
                    return normalized  # type: ignore[return-value]
    except Exception as exc:
        logger.debug("Could not read messaging transport from GMS: %s", exc)

    return "pgqueue"


def is_pgqueue_transport(auth_session) -> bool:
    return get_active_messaging_transport(auth_session) == "pgqueue"


def build_pgqueue_sink_config(
    *,
    schema_registry_url: str,
    host_port: str,
    database: str,
    username: str,
    password: str,
) -> Dict[str, Any]:
    """Recipe fragment for the ``datahub-pg-queue`` ingestion sink."""
    return {
        "type": "datahub-pg-queue",
        "config": {
            "queue": {
                "host_port": host_port,
                "database": database,
                "username": username,
                "password": password,
            },
            "schema_registry_url": schema_registry_url,
        },
    }

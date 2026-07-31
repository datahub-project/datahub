from __future__ import annotations

import json
import logging
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests

log = logging.getLogger(__name__)


@dataclass
class AspectResponse:
    data: dict[str, Any]
    system_metadata: dict[str, Any]

    @property
    def schema_version(self) -> int:
        return int(self.system_metadata.get("schemaVersion", 1))


class DataHubClient:
    def __init__(self, gms_url: str, token: str | None = None) -> None:
        self._base = gms_url.rstrip("/")
        self._session = requests.Session()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.headers["Content-Type"] = "application/json"

    def set_token(self, token: str | None) -> None:
        """Update the bearer token on the live session.

        Used by SetupOldStackPhase after `datahub init` rotates the token,
        so downstream phases that already captured a reference to this client
        pick up the fresh credential without needing to be reconstructed.
        """
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        else:
            self._session.headers.pop("Authorization", None)

    def wait_healthy(self, timeout_s: int = 60) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                resp = self._session.get(f"{self._base}/health", timeout=5)
                if resp.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        raise TimeoutError(f"GMS at {self._base} not healthy after {timeout_s}s")

    def ingest_mcp(
        self,
        urn: str,
        aspect_name: str,
        data: dict[str, Any],
        system_metadata: dict[str, Any] | None = None,
    ) -> None:
        proposal: dict[str, Any] = {
            "entityType": self._entity_type_from_urn(urn),
            "entityUrn": urn,
            "aspectName": aspect_name,
            "changeType": "UPSERT",
            "aspect": {
                "contentType": "application/json",
                "value": json.dumps(data),
            },
        }
        if system_metadata is not None:
            proposal["systemMetadata"] = system_metadata
        resp = self._session.post(
            f"{self._base}/aspects?action=ingestProposal",
            json={"proposal": proposal},
            timeout=30,
        )
        resp.raise_for_status()

    def get_aspect(self, urn: str, aspect_name: str) -> AspectResponse:
        # OpenAPI v3 with systemMetadata=true returns schemaVersion from the
        # mutation-hook-processed read path; the legacy RestLI /aspects endpoint
        # omits systemMetadata entirely.
        entity_type = self._entity_type_from_urn(urn)
        encoded = urllib.parse.quote(urn, safe="")
        resp = self._session.get(
            f"{self._base}/openapi/v3/entity/{entity_type}/{encoded}/{aspect_name}",
            params={"systemMetadata": "true"},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        data: dict[str, Any] = body.get("value") or {}
        sys_meta: dict[str, Any] = body.get("systemMetadata") or {}
        return AspectResponse(data=data, system_metadata=sys_meta)

    def delete_entity(self, urn: str) -> None:
        encoded = urllib.parse.quote(urn, safe="")
        resp = self._session.delete(
            f"{self._base}/entities/v2/{encoded}",
            timeout=30,
        )
        if resp.status_code not in (200, 204, 404):
            resp.raise_for_status()

    def get_config(self) -> dict[str, Any]:
        resp = self._session.get(f"{self._base}/config", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _entity_type_from_urn(urn: str) -> str:
        if urn.startswith("urn:li:dataset:"):
            return "dataset"
        if urn.startswith("urn:li:dashboard:"):
            return "dashboard"
        if urn.startswith("urn:li:dataHubUpgrade:"):
            return "dataHubUpgrade"
        if urn.startswith("urn:li:dataPlatform:"):
            return "dataPlatform"
        raise ValueError(f"Cannot derive entity type from URN: {urn!r}")

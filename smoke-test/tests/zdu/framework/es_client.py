"""Two-headed Elasticsearch client.

- The ``gms_*`` methods speak to DataHub GMS (used by Suite N validation).
- The ``list_indices`` / ``get_alias_targets`` / ``get_doc_count`` /
  ``get_doc`` (by urn field) / ``get_mappings`` / ``list_tasks`` methods speak directly
  to Elasticsearch — required for ZDU assertions that must bypass the
  DataHub abstraction layer (alias swap, doc parity, reindex tasks).
"""

from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

_HTTP_NOT_FOUND = 404


class ElasticsearchClient:
    def __init__(
        self,
        gms_url: str,
        es_url: str = "http://localhost:9200",
        token: str | None = None,
    ) -> None:
        self._gms_url = gms_url.rstrip("/")
        self._es_url = es_url.rstrip("/")
        self._gms_session = requests.Session()
        if token:
            self._gms_session.headers["Authorization"] = f"Bearer {token}"
        # ES has no auth in the dev compose; keep sessions separate so the
        # bearer token never leaks into direct-ES calls.
        self._es_session = requests.Session()

    # ── GMS-mediated ────────────────────────────────────────────────────────

    def search_for_entity(self, urn: str, entity_type: str = "DASHBOARD") -> bool:
        resp = self._gms_session.get(
            f"{self._gms_url}/entities/search",
            params={"q": "*", "type": entity_type.upper(), "count": "500"},
            timeout=30,
        )
        resp.raise_for_status()
        entities = resp.json().get("value", {}).get("entities", [])
        return any(e.get("entity", {}).get("urn") == urn for e in entities)

    def get_doc_count_for_entity_type(self, entity_type: str) -> int:
        resp = self._gms_session.get(
            f"{self._gms_url}/entities/search",
            params={"q": "*", "type": entity_type.upper(), "count": "0"},
            timeout=30,
        )
        resp.raise_for_status()
        return int(resp.json().get("value", {}).get("numEntities", 0))

    # ── Direct ES ───────────────────────────────────────────────────────────

    def list_indices(self, prefix: str) -> list[str]:
        resp = self._es_session.get(
            f"{self._es_url}/_cat/indices",
            params={"format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        return [r["index"] for r in resp.json() if r["index"].startswith(prefix)]

    def get_alias_targets(self, alias: str) -> list[str]:
        resp = self._es_session.get(f"{self._es_url}/_alias/{alias}", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return []
        resp.raise_for_status()
        return list(resp.json().keys())

    def get_alias_map(self) -> dict[str, list[str]]:
        """Return physical index name -> alias names, for every index."""
        resp = self._es_session.get(f"{self._es_url}/_alias", timeout=30)
        resp.raise_for_status()
        return {
            index: list((body or {}).get("aliases", {}).keys())
            for index, body in resp.json().items()
        }

    def get_refresh_interval(self, index_or_alias: str) -> str | None:
        """Return ``index.refresh_interval`` of the index an alias resolves to."""
        resp = self._es_session.get(
            f"{self._es_url}/{index_or_alias}/_settings/index.refresh_interval",
            timeout=30,
        )
        if resp.status_code == _HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        for body in resp.json().values():
            value = (
                (body or {})
                .get("settings", {})
                .get("index", {})
                .get("refresh_interval")
            )
            if value is not None:
                return str(value)
        return None

    def get_doc_count(self, index_name: str) -> int:
        resp = self._es_session.get(f"{self._es_url}/{index_name}/_count", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return 0
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    def get_doc(self, index_name: str, urn: str) -> dict[str, Any] | None:
        # Look up by the ``urn`` field, not ``_id``: Search V3 entity indices
        # use a hashed ``_id``, while V2 used the URL-encoded URN. A term
        # query on ``urn`` works for both.
        resp = self._es_session.post(
            f"{self._es_url}/{index_name}/_search",
            json={"size": 1, "query": {"term": {"urn": urn}}},
            timeout=30,
        )
        if resp.status_code == _HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        if not hits:
            return None
        source = hits[0].get("_source")
        return source if isinstance(source, dict) else None

    def get_mappings(self, index_name: str) -> dict[str, Any]:
        resp = self._es_session.get(f"{self._es_url}/{index_name}/_mapping", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return {}
        resp.raise_for_status()
        body: dict[str, Any] = resp.json()
        first: dict[str, Any] = next(iter(body.values()), {})
        mappings = first.get("mappings", {})
        return mappings if isinstance(mappings, dict) else {}

    def cat_indices(self) -> str:
        """Return ``GET /_cat/indices?v`` body. Empty string on error."""
        try:
            resp = self._es_session.get(f"{self._es_url}/_cat/indices?v", timeout=30)
            resp.raise_for_status()
            return resp.text or ""
        except Exception as exc:
            log.debug("cat_indices failed: %s", exc)
            return ""

    def cat_aliases(self) -> str:
        """Return ``GET /_cat/aliases?v`` body. Empty string on error."""
        try:
            resp = self._es_session.get(f"{self._es_url}/_cat/aliases?v", timeout=30)
            resp.raise_for_status()
            return resp.text or ""
        except Exception as exc:
            log.debug("cat_aliases failed: %s", exc)
            return ""

    def count_matching_urn(self, index_or_alias: str, urn: str) -> int:
        """Count docs in ``index_or_alias`` whose ``urn`` field equals ``urn``.

        Used by the data-integrity snapshot (Suite D) to verify that gap/dual
        URNs landed in the entity + systemMetadata indices post-upgrade. Uses
        the ``q="urn:\"<urn>\""`` lucene query syntax — note the inner quotes
        keep the colon-separated URN tokens together rather than letting
        lucene split on ``:``.
        """
        resp = self._es_session.get(
            f"{self._es_url}/{index_or_alias}/_count",
            params={"q": f'urn:"{urn}"'},
            timeout=30,
        )
        if resp.status_code == _HTTP_NOT_FOUND:
            return 0
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    def list_tasks(
        self, action_pattern: str = "indices:data/write/reindex"
    ) -> list[dict[str, Any]]:
        resp = self._es_session.get(
            f"{self._es_url}/_tasks",
            params={"actions": action_pattern, "detailed": "true"},
            timeout=30,
        )
        resp.raise_for_status()
        out: list[dict[str, Any]] = []
        for node in resp.json().get("nodes", {}).values():
            for task_id, task in node.get("tasks", {}).items():
                out.append({**task, "task_id": task_id})
        return out

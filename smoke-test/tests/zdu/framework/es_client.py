"""Two-headed Elasticsearch client.

- The ``gms_*`` methods speak to DataHub GMS (used by Suite A validation).
- The ``list_indices`` / ``get_alias_targets`` / ``get_doc_count`` /
  ``get_doc`` / ``get_mappings`` / ``list_tasks`` methods speak directly
  to Elasticsearch — required for ZDU assertions that must bypass the
  DataHub abstraction layer (alias swap, doc parity, reindex tasks).
"""

from __future__ import annotations

import logging
import urllib.parse
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

    def get_doc_count(self, index_name: str) -> int:
        resp = self._es_session.get(f"{self._es_url}/{index_name}/_count", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return 0
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    def get_doc(self, index_name: str, doc_id: str) -> dict[str, Any] | None:
        # DataHub stores the URL-encoded URN literally as the ES ``_id`` —
        # ``urn:li:dashboard:(test,zdu-tc-15)`` is stored under
        # ``_id = "urn%3Ali%3Adashboard%3A%28test%2Czdu-tc-15%29"``.
        # ES path-decodes URL-encoded path components once during routing,
        # so to look up the literal URL-encoded ``_id`` we must encode the
        # caller's URN twice: once to produce the stored ``_id``, and once
        # more so ES's path-decoding leaves us with the encoded ``_id``.
        once = urllib.parse.quote(doc_id, safe="")
        twice = urllib.parse.quote(once, safe="")
        resp = self._es_session.get(
            f"{self._es_url}/{index_name}/_doc/{twice}", timeout=30
        )
        if resp.status_code == _HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        source = resp.json().get("_source")
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

    def clone_index_with_partial_docs(
        self, source_alias: str, target_name: str, doc_count: int
    ) -> int:
        """Create ``target_name`` from ``source_alias``'s mapping and copy up
        to ``doc_count`` docs from the source. Returns the actual count
        copied.

        Used by the TC-112 fault-injection phase to produce an intentional
        doc-count mismatch between an alias and a candidate "next index".
        Idempotent: deletes target first if it already exists.
        """
        # 1. Drop the target if it lingers from a prior run.
        self._es_session.delete(f"{self._es_url}/{target_name}", timeout=30)

        # 2. Read source mapping AND settings via dedicated endpoints.
        mappings_resp = self._es_session.get(
            f"{self._es_url}/{source_alias}/_mapping", timeout=30
        )
        mappings_resp.raise_for_status()
        mappings_body: dict[str, Any] = mappings_resp.json()
        first_mappings: dict[str, Any] = next(iter(mappings_body.values()), {})
        mappings = first_mappings.get("mappings", {})

        settings_resp = self._es_session.get(
            f"{self._es_url}/{source_alias}/_settings", timeout=30
        )
        settings_resp.raise_for_status()
        settings_body: dict[str, Any] = settings_resp.json()
        first_settings: dict[str, Any] = next(iter(settings_body.values()), {})
        full_index_settings = first_settings.get("settings", {}).get("index", {})
        # Strip ES-managed settings that can't be set on create.
        immutable_keys = {
            "uuid",
            "provided_name",
            "creation_date",
            "version",
            "routing",
            "history",
            "store",
            "transform",
            "lifecycle",
        }
        index_settings = {
            k: v for k, v in full_index_settings.items() if k not in immutable_keys
        }

        # 3. Create the target with matching mapping + minimal settings.
        create_body: dict[str, Any] = {"mappings": mappings}
        if index_settings:
            create_body["settings"] = {"index": index_settings}
        create_resp = self._es_session.put(
            f"{self._es_url}/{target_name}",
            json=create_body,
            timeout=60,
        )
        create_resp.raise_for_status()

        # 4. Copy up to ``doc_count`` docs via _reindex with max_docs.
        reindex_resp = self._es_session.post(
            f"{self._es_url}/_reindex",
            json={
                "max_docs": max(0, doc_count),
                "source": {"index": source_alias, "size": min(doc_count, 1000)},
                "dest": {"index": target_name},
                "conflicts": "proceed",
            },
            params={"refresh": "true", "wait_for_completion": "true"},
            timeout=120,
        )
        reindex_resp.raise_for_status()
        created = int(reindex_resp.json().get("created", 0))
        log.info(
            "[es] cloned %s -> %s with %d docs (requested %d)",
            source_alias,
            target_name,
            created,
            doc_count,
        )
        return created

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

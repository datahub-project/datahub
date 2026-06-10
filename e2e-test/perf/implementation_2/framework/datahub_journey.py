"""
DataHubJourney — base journey for all DataHub Grasshopper load tests.

Extends BaseJourney (locust-grasshopper) with:
  - Auth header injection: DATAHUB_GMS_TOKEN → Bearer, or actor-spoof fallback
  - graphql() helper with HTTP + GQL error detection and Grasshopper check recording
  - Shared manifest-backed URN/tag/platform pools (populated from seed-output/manifest.json)
"""

import json
import os
from pathlib import Path
from typing import Any

from grasshopper.lib.journeys.base_journey import BaseJourney
from grasshopper.lib.util.utils import check
from locust import between

# Seed manifest written by datapacks/seed_search.py — shared by both implementations
_PERF_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST_PATH = _PERF_ROOT / "seed-output" / "manifest.json"


def _load_manifest() -> dict:
    if _MANIFEST_PATH.exists():
        try:
            return json.loads(_MANIFEST_PATH.read_text())
        except Exception as exc:
            print(f"[manifest] WARNING: could not load {_MANIFEST_PATH}: {exc}")
    else:
        print(
            f"[manifest] {_MANIFEST_PATH} not found — "
            "using random URNs (run datapacks/seed_search.py first for realistic results)"
        )
    return {}


_MANIFEST: dict = _load_manifest()


class DataHubJourney(BaseJourney):
    """
    Base Grasshopper journey for DataHub GMS load tests.
    All domain journeys subclass this; they inherit auth injection and GraphQL helpers.
    """

    abstract = True
    wait_time = between(1, 3)

    # Manifest data — read-only singletons, safe to share across all user instances
    _manifest_urns: list[str] = _MANIFEST.get("sample_urns", {}).get("dataset", [])
    _manifest_tags: list[str] = _MANIFEST.get("vocabulary", {}).get("tags", [])
    _manifest_platforms: list[str] = _MANIFEST.get("vocabulary", {}).get("platforms", [])

    def on_start(self) -> None:
        # Must call super first: BaseJourney.on_start() merges scenario params, sets
        # thresholds, registers VUs, and normalizes self.environment.host from target_url.
        super().on_start()
        # scenario_args is populated after super().on_start() via _merge_incoming_defaults_and_params
        token = os.environ.get("DATAHUB_GMS_TOKEN", "") or self.scenario_args.get(
            "gms_token", ""
        )
        self._headers: dict[str, str] = {"Content-Type": "application/json"}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        else:
            # Local dev: actor-spoof header (no auth middleware)
            self._headers["X-DataHub-Actor"] = "urn:li:corpuser:datahub"

    def graphql(
        self,
        query: str,
        variables: dict[str, Any],
        name: str,
        record_check: bool = True,
    ) -> dict[str, Any] | None:
        """
        POST a GraphQL operation to /api/graphql.

        Marks the Locust request as failed on HTTP errors or a non-null GraphQL errors array.
        When record_check=True also records a Grasshopper check() for the result so that
        correctness failures are visible as named metrics in the console and InfluxDB.
        Returns the parsed response body dict, or None on any failure.
        """
        payload = {"query": query, "variables": variables}
        with self.client.post(
            "/api/graphql",
            json=payload,
            headers=self._headers,
            catch_response=True,
            name=name,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")
                if record_check:
                    check(
                        f"{name}: HTTP success",
                        False,
                        env=self.environment,
                        tags={"endpoint": name, "check_type": "http_status"},
                    )
                return None

            body: dict[str, Any] = resp.json()
            errors = body.get("errors")
            if errors:
                msg = errors[0].get("message", str(errors[0]))
                resp.failure(f"GraphQL error: {msg}")
                if record_check:
                    check(
                        f"{name}: no GraphQL errors",
                        False,
                        env=self.environment,
                        tags={"endpoint": name, "check_type": "graphql_errors"},
                    )
                return None

            resp.success()
            if record_check:
                check(
                    f"{name}: success",
                    True,
                    env=self.environment,
                    tags={"endpoint": name, "check_type": "success"},
                )
            return body

"""Thin HTTP client for the Lightdash REST API.

Encapsulates auth, retries, timeouts, JSON unwrapping (``{"status":"ok","results":...}``)
and translation to Pydantic models — the source class never touches ``requests``
directly.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.lightdash.models import (
    LightdashChart,
    LightdashChartSummary,
    LightdashDashboard,
    LightdashDashboardSummary,
    LightdashExplore,
    LightdashOrganization,
    LightdashProject,
    LightdashProjectSummary,
    LightdashSpace,
)

logger = logging.getLogger(__name__)


class LightdashAPIError(RuntimeError):
    """Raised for non-recoverable Lightdash API errors (auth, 4xx, malformed response)."""


class LightdashClient:
    """Stateless wrapper around the Lightdash REST API."""

    def __init__(
        self,
        base_url: str,
        personal_access_token: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl

        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)

        self._session = requests.Session()
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        # Lightdash uses "ApiKey <pat>", NOT "Bearer <pat>" — verified against the live API.
        self._session.headers.update(
            {
                "Authorization": f"ApiKey {personal_access_token}",
                "Accept": "application/json",
                "User-Agent": "datahub-lightdash-source/0.1.0",
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> LightdashClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = self._session.get(
                url, params=params, timeout=self.timeout_seconds, verify=self.verify_ssl
            )
        except requests.RequestException as e:
            raise LightdashAPIError(f"Request to {url} failed: {e}") from e

        if resp.status_code == 401:
            raise LightdashAPIError(
                f"Unauthorized for {url} — check the personal_access_token "
                "(Lightdash expects 'Authorization: ApiKey <pat>')."
            )
        if not resp.ok:
            raise LightdashAPIError(
                f"GET {url} returned HTTP {resp.status_code}: {resp.text[:300]}"
            )

        try:
            payload = resp.json()
        except ValueError as e:
            raise LightdashAPIError(f"GET {url} returned non-JSON body: {e}") from e

        if not isinstance(payload, dict) or payload.get("status") != "ok":
            raise LightdashAPIError(
                f"GET {url} returned a non-ok envelope: {str(payload)[:300]}"
            )
        return payload.get("results")

    # ---- health / identity -------------------------------------------------

    def health(self) -> dict[str, Any]:
        """``GET /api/v1/health`` — does NOT require auth, useful for connection testing."""
        url = f"{self.base_url}/api/v1/health"
        resp = self._session.get(
            url, timeout=self.timeout_seconds, verify=self.verify_ssl
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("results", payload)

    def get_organization(self) -> LightdashOrganization:
        return LightdashOrganization.model_validate(self._get("/api/v1/org"))

    # ---- projects ----------------------------------------------------------

    def list_projects(self) -> list[LightdashProjectSummary]:
        results = self._get("/api/v1/org/projects") or []
        return [LightdashProjectSummary.model_validate(r) for r in results]

    def get_project(self, project_uuid: str) -> LightdashProject:
        return LightdashProject.model_validate(
            self._get(f"/api/v1/projects/{project_uuid}")
        )

    # ---- spaces ------------------------------------------------------------

    def list_spaces(self, project_uuid: str) -> list[LightdashSpace]:
        results = self._get(f"/api/v1/projects/{project_uuid}/spaces") or []
        return [LightdashSpace.model_validate(r) for r in results]

    # ---- charts ------------------------------------------------------------

    def list_charts(self, project_uuid: str) -> list[LightdashChartSummary]:
        results = self._get(f"/api/v1/projects/{project_uuid}/charts") or []
        return [LightdashChartSummary.model_validate(r) for r in results]

    def get_chart(self, chart_uuid: str) -> LightdashChart:
        return LightdashChart.model_validate(self._get(f"/api/v1/saved/{chart_uuid}"))

    # ---- dashboards --------------------------------------------------------

    def list_dashboards(self, project_uuid: str) -> list[LightdashDashboardSummary]:
        results = self._get(f"/api/v1/projects/{project_uuid}/dashboards") or []
        return [LightdashDashboardSummary.model_validate(r) for r in results]

    def get_dashboard(self, dashboard_uuid: str) -> LightdashDashboard:
        return LightdashDashboard.model_validate(
            self._get(f"/api/v1/dashboards/{dashboard_uuid}")
        )

    # ---- explores ----------------------------------------------------------

    def get_explore(self, project_uuid: str, explore_name: str) -> LightdashExplore:
        return LightdashExplore.model_validate(
            self._get(f"/api/v1/projects/{project_uuid}/explores/{explore_name}")
        )
